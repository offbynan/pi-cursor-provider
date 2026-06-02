# Fix: at most one live bridge per session

## Background

Each Pi session uses an h2-bridge child process to proxy requests to Cursor's
gRPC API. The bridge should be spawned once per LLM request and torn down when
the response is complete (or the session is cancelled). In practice, the proxy
can end up with multiple live bridge processes per session:

- `cleanupBridge` sends a `cancelAction` and closes the bridge's stdin, but
  does not force-kill the process. The bridge stays alive until Cursor closes
  the HTTP/2 stream, which may never happen if Cursor is stuck.
- The inline cleanup in `handleChatCompletion` (new user message path) calls
  `bridge.end()` directly, **skipping `sendCancelAction`**. Cursor never
  receives the cancel, so the TCP connection stays open and the bridge process
  lingers indefinitely.
- Nothing tracks live bridges outside of `activeBridges`, which only covers
  bridges paused waiting for tool results — not bridges that are actively
  streaming.

The root cause is Pi sending a new request before the previous bridge has fully
closed. Fixing that requires a change in Pi core. The changes below add a
defensive layer in the provider that catches and corrects the situation at spawn
time.

---

## Goal

At most one live bridge process per session key at any time. If a stale bridge
is detected when spawning a new one, kill it immediately and surface a warning
to the user.

---

## Changes (all in `proxy.ts`)

### 1. Add `sessionBridges` map

```typescript
const sessionBridges = new Map<string, BridgeHandle>();
```

Add next to `activeBridges`. Tracks **all** live bridges for a session key,
from spawn to close. `activeBridges` only covers paused bridges (waiting for
tool results); `sessionBridges` covers the full lifetime.

Add to `__testInternals` and `cleanupAllSessionState`.

---

### 2. Register/deregister in `startBridge`

Add `bridgeKey: string` parameter.

Before spawning:
- Check `sessionBridges.get(bridgeKey)`
- If alive: `console.error`, set `staleBridgeKilled = true`, call `proc.kill()`
- Delete from `sessionBridges`

After spawning: `sessionBridges.set(bridgeKey, bridge)`.

Return `staleBridgeKilled: boolean` alongside existing return values.

---

### 3. Deregister on close

In both `bridge.onClose` handlers (`writeSSEStream` and
`handleNonStreamingResponse`), add:

```typescript
if (sessionBridges.get(bridgeKey) === bridge) sessionBridges.delete(bridgeKey);
```

Identity check (`=== bridge`) prevents accidentally removing a newer bridge for
the same key.

---

### 4. Fix `cleanupBridge`: deregister + force-kill timeout

1. Add the same identity-guarded `sessionBridges.delete`.

2. After `bridge.end()`, schedule a force-kill so zombie processes don't linger
   if Cursor never sends `END_STREAM`:

```typescript
setTimeout(() => { try { bridge.proc.kill(); } catch {} }, 10_000);
```

10 seconds gives Cursor time to acknowledge the `cancelAction` and close
cleanly.

---

### 5. Fix `handleChatCompletion`: use `cleanupBridge`

The inline new-user-message path skips `sendCancelAction`. Replace:

```typescript
// before
clearInterval(activeBridge.heartbeatTimer);
activeBridge.bridge.end();
activeBridges.delete(bridgeKey);

// after
cleanupBridge(activeBridge.bridge, activeBridge.heartbeatTimer, bridgeKey);
```

This ensures Cursor always receives the cancel signal, which closes the TCP
connection cleanly and is the primary fix for zombie processes.

---

### 6. Surface stale-bridge detection to the user

Add optional `staleBridgeKilled = false` parameter to `writeSSEStream`. If
true, emit a warning SSE chunk immediately after `res.writeHead`:

```
⚠ A previous request for this session was still running and has been cancelled.
```

`handleToolResultResume` and `writeSSEStreamForTests` call `writeSSEStream`
without this parameter — no changes needed there.

Pass `staleBridgeKilled` through:
`handleStreamingResponse` → `startBridge` → `writeSSEStream`.

Add `bridgeKey: string` to `handleNonStreamingResponse` so it can call
`startBridge` with it. The call site in `handleChatCompletion` already has
`bridgeKey` in scope.

---

## Scope

~60 lines changed/added across ~10 locations in `proxy.ts` only. No changes to
`h2-bridge.mjs` or `index.ts`. Test files may need minor updates for the new
`startBridge` signature.

---

## What this does and doesn't fix

**Fixes:**
- Zombie bridge processes linger at most 10 seconds instead of indefinitely
- Cursor always receives a cancel signal when a bridge is abandoned
- If a second bridge would be spawned despite all the above, it is caught at
  spawn time and the old one is force-killed

**Does not fix:**
- The root cause: Pi sends a new request before the previous bridge has fully
  closed. Fixing this requires Pi core to sequence requests so a new provider
  request is not issued until the previous bridge's `onClose` has fired.
