# pi-cursor-provider

[Pi](https://github.com/badlogic/pi-mono) extension that provides access to [Cursor](https://cursor.com) models (Claude, GPT, Gemini, Grok, Kimi, Composer) via OAuth and a local OpenAI-compatible proxy.

[![npm version](https://img.shields.io/npm/v/@offbynan/pi-cursor-provider.svg)](https://www.npmjs.com/package/@offbynan/pi-cursor-provider)

Forked from [ndraiman/pi-cursor-provider](https://github.com/ndraiman/pi-cursor-provider).

## Install

```bash
# Via pi
pi install npm:@offbynan/pi-cursor-provider

# Or manually
git clone https://github.com/offbynan/pi-cursor-provider ~/.pi/agent/extensions/cursor-provider
cd ~/.pi/agent/extensions/cursor-provider
npm install
```

## Usage

```
/login cursor     # authenticate via browser
/model            # select a Cursor model
```

## How it works

```
pi  →  openai-completions  →  localhost:PORT/v1/chat/completions
                                      ↓
                              proxy.ts (HTTP server)
                                      ↓
                              h2-bridge.mjs (Node HTTP/2)
                                      ↓
                              api2.cursor.sh gRPC
```

1. **PKCE OAuth** — browser-based login to Cursor, no client secret needed
2. **Model discovery** — queries Cursor's `GetUsableModels` gRPC endpoint
3. **Local proxy** — translates OpenAI `/v1/chat/completions` to Cursor's protobuf/HTTP2 Connect protocol
4. **Tool routing** — rejects Cursor's native tools, exposes pi's tools via MCP

## Configuration

| Env var | Default | Description |
| ------- | ------- | ----------- |
| `PI_CURSOR_PROVIDER_DEBUG` | off | Set to any truthy value to enable JSONL debug logging |
| `PI_CURSOR_PROVIDER_DEBUG_FILE` | auto in tmpdir | Override the debug log file path |
| `PI_CURSOR_BRIDGE_INITIAL_TIMEOUT_MS` | `120000` | Kill bridge if no HTTP/2 activity within this many ms of spawn |
| `PI_CURSOR_BRIDGE_ACTIVITY_TIMEOUT_MS` | `300000` | Kill bridge if no HTTP/2 activity for this many ms after the first frame |
| `PI_CURSOR_TURN_ARCHIVE_THRESHOLD` | `20` | Keep this many recent turns as raw blobs; older turns are archived as inline text |
| `PI_CURSOR_RAW_MODELS` | off | Set to disable model deduplication and see all raw Cursor model IDs |

## Changes vs upstream

**This fork improves on the upstream in sixteen areas:**

- **Image support** — base64 `image_url` content parts are forwarded to Cursor end-to-end; the upstream silently drops them
- **`pi -p` exit fix** — non-interactive mode no longer hangs after printing a response
- **Dead eviction code removed** — unreachable 30-minute TTL eviction logic is gone
- **Accurate context windows** — per-model inference instead of a hardcoded 200 k for every model
- **Post-compaction sync** — cached checkpoint is cleared on `session_compact` so both sides stay in sync
- **Context window scaling** — token counts are scaled when Cursor enforces a tighter runtime cap than the model implies
- **Per-model cost estimation** — detailed price table (input / output / cache) covering all current model families
- **Model deduplication** — effort-suffix variants (`-low`, `-medium`, `-high`, …) are collapsed into one entry; pi's reasoning-level setting drives the suffix automatically
- **Thinking-tag filtering** — inline `<think>` / `<reasoning>` tags are stripped from the response and routed to `reasoning_content`
- **Structured debug logging** — opt-in JSONL event log (`PI_CURSOR_PROVIDER_DEBUG=1`) with a bundled timeline viewer
- **Bridge timeout hardening** — initial and activity timeouts raised and made configurable so large checkpoints don't cause premature bridge termination during compaction
- **Bridge termination error propagation** — a bridge crash now surfaces as a real error to pi instead of returning a silent empty success, preventing compaction failures from appearing as blank responses
- **Conversation history archiving** — turns beyond a configurable tail are folded into a `ConversationSummaryArchive` blob with inline text, capping `getBlobArgs` round-trips at O(tail) instead of O(history length) and dramatically speeding up compaction on long sessions
- **SSE keepalive during blob-fetching** — periodic `: ping` comments keep the SSE connection alive while Cursor is fetching blobs, preventing pi's request timeout from firing before the first token arrives
- **Conversation state preserved on transient errors** — a bridge timeout or Connect error no longer wipes the conversation state; the last good checkpoint survives so the next request resumes in-place instead of rebuilding from scratch
- **Checkpoint saved on client disconnect** — if pi closes the connection (e.g. request timeout) after Cursor already sent a checkpoint, that checkpoint is preserved for the retry

### Image support

This fork extends the proxy to handle images in OpenAI-style `image_url` content parts:

- **Base64 images** — `data:image/png;base64,...` payloads are extracted from the request, stored as blobs in Cursor's protobuf format, and forwarded to the upstream API.
- **Multi-turn state** — images are tracked per conversation turn and threaded correctly through session checkpoints, forks, and resumes.
- **Transparent to callers** — no API changes; just include standard `image_url` content parts in your messages as you would with any OpenAI-compatible client.

The upstream repo does not support images at all — they are silently ignored or cause request failures. This fork handles them properly end-to-end.

### `pi -p` exit fix

The upstream repo causes `pi -p` (non-interactive mode) to hang indefinitely after printing a response. Two bugs were responsible:

1. **Empty end-stream body misclassified as error.** Cursor's Connect end-stream frame often has a 0-byte body. `JSON.parse("")` throws, so the proxy took the error path even on clean completions.
2. **Bridge never unref'd on error path.** `bridge.end()` and `bridge.unref()` were only called in the success branch. On the error path the h2-bridge child process stayed ref'd, blocking process exit.

This fork fixes both: empty and non-JSON end-stream bodies are treated as success, and the bridge is always unref'd regardless of the outcome.

### Removed dead eviction code

The upstream proxy included a 30-minute TTL eviction mechanism (`evictStaleConversations`, `CONVERSATION_TTL_MS`, `sessionScoped`, `lastAccessMs`). All conversations created by pi include a session ID, permanently exempting them from TTL eviction, so this code was never reachable. This fork removes it.

### Accurate per-model context window inference

Cursor's `GetUsableModels` RPC does not return context window sizes, so the upstream proxy hardcodes 200 k for every model. This fork exports an `inferContextWindow(id)` function that derives the correct window from known model families:

| Family | Window |
| ------ | ------ |
| Claude 4.6 Sonnet / Opus | 1 M |
| All other Claude | 200 k |
| Gemini 2.5 / 3.x | 1 M |
| GPT nano / mini variants | 128 k |
| GPT-5.5+ | 1 M |
| GPT-5.x (other) | 400 k |
| Grok 4 | 256 k |
| Kimi K2.x | 262 k |
| Anything with `-1m` suffix | 1 M |
| Unknown / Composer | 200 k |

This ensures pi uses the right compaction thresholds and token budget for each model.

### Post-compaction session sync

When pi compacts its message list (the `session_compact` lifecycle event), the proxy's cached conversation checkpoint still reflects the full pre-compaction conversation. Continuing without clearing that cache would cause a history mismatch, forcing an expensive full reconstruction on the next request.

This fork listens for `session_compact` and eagerly clears the stored checkpoint for the affected session, so both sides stay in sync at zero extra cost.

### Context window scaling when Cursor enforces a tighter cap

Cursor sometimes enforces a tighter context window at runtime than what the model ID implies (for example, capping Gemini at 200 k even though we registered 1 M). In that case the raw `usedTokens` from Cursor's `ConversationTokenDetails` would appear far below pi's compaction threshold, so pi would never compact — then Cursor would eventually error with a context-overflow.

This fork reads `maxTokens` from `ConversationTokenDetails` and, when Cursor's cap is tighter than the inferred window, scales `total_tokens` proportionally:

```
total_tokens = round(usedTokens × piWindow / cursorWindow)
```

That makes pi's compaction threshold fire at the right time relative to the window Cursor is actually enforcing.

### Per-model cost estimation

The upstream repo provides no cost data, so pi cannot show per-turn cost estimates for Cursor models.

This fork ships a detailed cost table (input / output / cache-read / cache-write prices in $/M tokens) covering every current model family — Claude 4.x, GPT-5.x, Gemini 2.5/3.x, Grok 4, Kimi K2, and Composer — plus a pattern-based fallback for variants not yet in the table. Pi uses this data to display cost estimates after each turn.

### Model deduplication with reasoning-effort mapping

Cursor's `GetUsableModels` RPC can return dozens of near-duplicate IDs that differ only by effort suffix (e.g. `gpt-5.4-low`, `gpt-5.4-medium`, `gpt-5.4-high`, `gpt-5.4-xhigh`). The upstream passes all of these through verbatim, producing a cluttered model list where the user must manually pick the right suffix and pi's reasoning-effort setting is ignored.

This fork deduplicates them: model variants that share the same base ID and differ only by effort suffix are collapsed into a single entry with `supportsReasoningEffort: true` and an effort map keyed by pi's reasoning levels (`minimal` / `low` / `medium` / `high` / `xhigh`). Pi's thinking-level setting then drives the effort suffix automatically, and the model list stays manageable. See the [Model Mapping](#model-mapping) section for the full deduplication rules.

### Thinking-tag filtering

Some models (notably certain Gemini variants) emit reasoning content inline with the response, wrapped in tags like `<think>`, `<thinking>`, `<reasoning>`, or `<thought>`. The upstream passes this through as raw text, polluting the main response with unrendered XML tags.

This fork detects and strips these tags in the proxy's stream processor, routing the extracted content to the `reasoning_content` SSE field so pi renders it as structured reasoning rather than as part of the assistant's reply.

### Structured debug logging

The upstream has no observability. This fork adds opt-in JSONL event logging (set `PI_CURSOR_PROVIDER_DEBUG=1`) covering every stage of a request: HTTP ingress, message parsing, checkpoint reads/writes, bridge lifecycle, tool call pauses, tool result resumes, and stream completion. A bundled `debug:timeline` script converts a raw log file into a compact human-readable timeline for diagnosing proxy behaviour.

```bash
npm run debug:timeline -- --latest
```

### Bridge timeout hardening

The upstream `h2-bridge.mjs` used a 30-second initial connection timeout and a 120-second activity timeout. Large conversations require Cursor to deserialise a big checkpoint and complete many `getBlobArgs` round-trips before it starts streaming tokens, which regularly exceeded these limits and caused compaction to fail with a `terminated` error.

This fork raises the defaults (120 s initial, 300 s activity) and makes them configurable via `PI_CURSOR_BRIDGE_INITIAL_TIMEOUT_MS` and `PI_CURSOR_BRIDGE_ACTIVITY_TIMEOUT_MS` (see [Configuration](#configuration)).

### Bridge termination error propagation

In the upstream, if the `h2-bridge` child process exits before producing any response (e.g. due to a timeout), the proxy sends a `finish_reason: "stop"` with empty content on the streaming path, and a silent 200 OK on the non-streaming path. Pi receives what looks like a successful but empty response, then fails compaction with an opaque `terminated` error.

This fork checks the bridge exit code in both paths:
- **Streaming path** — if the bridge exits with code ≠ 0 before any response, an SSE error chunk is sent so pi surfaces a real failure.
- **Non-streaming path** — same condition returns a 502 JSON error.
- **Both paths** — the conversation state is preserved so the next retry can resume from the last good checkpoint rather than rebuilding from scratch.

### Conversation history archiving

Cursor's `AgentService/Run` RPC is stateless per request: each turn sends the full conversation state as a checkpoint blob, and the server fetches individual turn blobs via `getBlobArgs` as needed. For a long conversation every request incurs O(history) round-trips; the compaction turn is the worst case because Cursor must read the entire history to generate a summary.

This fork folds turns older than a configurable tail into a single `ConversationSummaryArchive` protobuf blob that stores the transcript as **inline text**. The server reads one blob instead of hundreds, cutting round-trips from O(N) to O(tail):

| Scenario | `getBlobArgs` before | `getBlobArgs` after |
| ---------------------- | --------------------- | ------------------- |
| 100-turn compaction | ~300 | ~61 |
| 20-turn normal turn | ~60 | ~60 (unchanged) |

The tail size is configurable via `PI_CURSOR_TURN_ARCHIVE_THRESHOLD` (default 20, see [Configuration](#configuration)).

Archiving is conservative: old turns are only replaced if every required blob is already in the local store. If any blob is missing the turns are left as-is, so no context is silently dropped.

### SSE keepalive during blob-fetching

Before the first token arrives, the proxy is silent: it sends HTTP 200 headers immediately but emits no SSE events while Cursor fetches conversation blobs. If pi's HTTP client has a request timeout (or a "time since last data" idle timeout), it fires during this window and the request is aborted with `Error: Request timed out.`

This fork starts a 15-second keepalive timer alongside the SSE stream. While the response is open and no data has been sent yet, the timer periodically writes an SSE comment (`: ping`) which is invisible to pi's message parser but resets any inactivity timer in the HTTP layer.

### Conversation state preserved on transient errors

Previously, a bridge timeout (`exit code ≠ 0`) or a Connect-level error from Cursor caused the proxy to call `conversationStates.delete(convKey)`, wiping the stored checkpoint. On the next request pi would rebuild the Cursor conversation from scratch — losing any context accumulated since the last compaction.

Neither failure mode actually invalidates the checkpoint. A bridge timeout means Cursor stopped responding to the current request, not that its conversation state is corrupt. A Connect error (e.g. rate limit, transient upstream failure) also leaves the prior checkpoint intact.

This fork removes both deletes. The last good checkpoint survives errors, so the next request resumes from where the conversation was rather than starting over.

### Checkpoint saved on client disconnect

When pi closes the SSE connection (e.g. its own request timeout fires), the proxy previously guarded checkpoint persistence behind `if (!cancelled)`, discarding any checkpoint that Cursor had already sent for that turn. On the next request the proxy used a stale checkpoint, losing the partial turn's context.

This fork removes the `!cancelled` guard. If Cursor sent a checkpoint before the disconnect, it is saved and the retry picks it up.

## Model Mapping

Cursor exposes many model variants that encode **effort level** (`low`, `medium`, `high`, `xhigh`, `max`, `none`) and **speed** (`-fast`) or **thinking** (`-thinking`) in the model ID. This extension deduplicates them so pi's reasoning effort setting controls the effort level.

### How it works

Each raw Cursor model ID is parsed into components:

```
{base}-{effort}[-fast|-thinking]
```

Examples:

| Raw Cursor ID                  | Base                | Effort   | Variant     |
| ------------------------------ | ------------------- | -------- | ----------- |
| `gpt-5.4-medium`               | `gpt-5.4`           | `medium` | —           |
| `gpt-5.4-high-fast`            | `gpt-5.4`           | `high`   | `-fast`     |
| `claude-4.6-opus-max-thinking` | `claude-4.6-opus`   | `max`    | `-thinking` |
| `gpt-5.1-codex-max-high`       | `gpt-5.1-codex-max` | `high`   | —           |
| `composer-2`                   | `composer-2`        | —        | —           |

Models sharing the same `(base, variant)` with **≥2 effort levels** and a sensible default (`medium` or no-suffix) are collapsed into a single entry with `supportsReasoningEffort: true`. Pi's thinking level maps to the effort suffix:

| Pi Level  | Cursor Suffix                   |
| --------- | ------------------------------- |
| `minimal` | `none` (if available) or `low`  |
| `low`     | `low`                           |
| `medium`  | `medium` or no suffix (default) |
| `high`    | `high`                          |
| `xhigh`   | `max` (Claude) or `xhigh` (GPT) |

The proxy inserts the effort before `-fast`/`-thinking`:

```
pi selects: gpt-5.4-fast  +  effort: high    →  Cursor receives: gpt-5.4-high-fast
pi selects: gpt-5.4       +  effort: medium  →  Cursor receives: gpt-5.4-medium
pi selects: composer-2    +  (no effort)     →  Cursor receives: composer-2
```

**Collapsed** when Cursor returns either:

- **Multiple** effort suffixes for the same `(base, -fast, -thinking)` group, or
- **A single** variant whose parsed effort suffix is **non-empty** (for example only `claude-4.5-opus-high` is listed). The suffix is removed from the displayed ID so pi's reasoning-effort setting supplies it.

**Left as-is** when the group has **one** variant and the parsed effort suffix is **empty** — typically IDs with no effort segment, such as `composer-2`, `gemini-3.1-pro`, or `kimi-k2.5`.

### Disabling the mapping

To see all raw Cursor model variants without dedup:

```bash
PI_CURSOR_RAW_MODELS=1 pi
```

## Session Management

The proxy maintains per-session conversation state to enable multi-turn conversations with tool call continuations and clean lifecycle handling.

### State storage

- **Keyed by session ID** — pi injects its session ID into every request via a `before_provider_request` hook; the proxy uses it to key both bridge state and the stored conversation checkpoint.
- **Checkpoint** — Cursor sends a `conversationCheckpointUpdate` message after each completed turn. The proxy stores the latest checkpoint and reuses it on the next request, so Cursor picks up exactly where it left off without rebuilding the full conversation from scratch.
- **Blob store** — protobuf blobs referenced by the checkpoint are cached locally and served back to Cursor on demand via `getBlobArgs` / `setBlobArgs`.
- **In-memory only** — all state lives in process memory. A proxy restart loses checkpoints; the next request rebuilds from pi's message history.

### Tool continuations

When Cursor requests a tool call, the proxy pauses the SSE stream, stores the live bridge in memory, and returns the tool call to pi. When pi sends the result on the next request, the proxy forwards it into the same in-flight Cursor run so the continuation stays part of the original turn.

### Lifecycle cleanup

Session state is cleared on pi lifecycle events — session switch, fork, `/tree`, shutdown, and post-compaction — so stale checkpoints never carry over into a new context.

### Error resilience

A bridge timeout or Connect-level error from Cursor does not wipe the stored checkpoint. The last good checkpoint survives transient failures and is used on the next retry. If Cursor sends a checkpoint before a client disconnect, that checkpoint is also preserved.

## Requirements

- [Pi](https://github.com/badlogic/pi-mono)
- [Node.js](https://nodejs.org) >= 18
- Active [Cursor](https://cursor.com) subscription

## Development

```bash
npm install
npm test
```

## Debug log timeline

When `PI_CURSOR_PROVIDER_DEBUG=1` is enabled, the proxy writes timestamped JSONL logs to `os.tmpdir()` by default. You can turn a log into a compact human-readable timeline with:

```bash
npm run debug:timeline -- --latest
npm run debug:timeline -- /path/to/pi-cursor-provider-debug-2026-04-08T14-06-07-565Z-41184.log
```

Add `--json` if you want the parsed summary as JSON instead of formatted text.

## Credits

OAuth flow and gRPC proxy adapted from [opencode-cursor](https://github.com/ephraimduncan/opencode-cursor) by Ephraim Duncan.
