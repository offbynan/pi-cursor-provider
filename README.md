# pi-cursor-provider

> **This fork improves on the upstream in three areas:** image support, correct `pi -p` exit behaviour, and removal of dead eviction code. See the sections below for details.

[![npm version](https://img.shields.io/npm/v/pi-cursor-provider.svg)](https://www.npmjs.com/package/pi-cursor-provider)

[Pi](https://github.com/badlogic/pi-mono) extension that provides access to [Cursor](https://cursor.com) models via OAuth authentication and a local OpenAI-compatible proxy.

Forked from [ndraiman/pi-cursor-provider](https://github.com/ndraiman/pi-cursor-provider).

## Changes vs upstream

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

## Install

```bash
# Via pi install
pi install npm:pi-cursor-provider

# Or manually
git clone https://github.com/ndraiman/pi-cursor-provider ~/.pi/agent/extensions/cursor-provider
cd ~/.pi/agent/extensions/cursor-provider
npm install
```

## Usage

```
/login cursor     # authenticate via browser
/model            # select a Cursor model
```

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
pi selects: gpt-5.4-fast  +  effort: high  →  Cursor receives: gpt-5.4-high-fast
pi selects: gpt-5.4       +  effort: medium  →  Cursor receives: gpt-5.4-medium
pi selects: composer-2     +  (no effort)     →  Cursor receives: composer-2
```

When a group is **collapsed**, the proxy registers one model with `supportsReasoningEffort: true` and an internal effort map (see table above).

**Collapsed** when Cursor returns either:

- **Multiple** effort suffixes for the same `(base, -fast, -thinking)` group, or
- **A single** variant whose parsed effort suffix is **non-empty** (for example only `claude-4.5-opus-high` is listed). The suffix is removed from the displayed ID so Pi's reasoning-effort setting supplies it.

**Left as-is** (raw Cursor ID on that row, `supportsReasoningEffort: false`) when the group has **one** variant and the parsed effort suffix is **empty**—typically IDs with no effort segment, such as `composer-2`, `gemini-3.1-pro`, or `kimi-k2.5`.

### Disabling the mapping

To see all raw Cursor model variants without dedup:

```bash
PI_CURSOR_RAW_MODELS=1 pi
```

## Session Management

The proxy maintains conversation state per pi session, enabling multi-turn conversations with Cursor models while preserving forks, tool continuations, and interruptions correctly.

### How it works

- **Session tracking** — pi's session ID is injected into requests via a `before_provider_request` hook. The proxy keys bridge state and stored conversation state from that real session ID.
- **Checkpoints** — Cursor returns a conversation checkpoint after completed turns. The proxy stores that checkpoint, plus the completed-turn count and a fingerprint of the completed structured history, and reuses it only when the incoming history still matches.
- **Session-scoped state** — real pi session state is kept in memory until explicit cleanup or process restart. Anonymous fallback state can still be TTL-evicted.
- **Lifecycle cleanup** — session state is cleaned up on pi lifecycle events such as session switch, fork, `/tree`, and shutdown.

### Tool continuations

When Cursor pauses for a tool call, the proxy keeps the live upstream bridge open and waits for pi to send the tool result on the next request. That tool result is sent back into the same in-flight Cursor run, so the tool continuation stays part of the original user turn instead of inflating completed history.

### Interruptions

If the client disconnects or interrupts a turn mid-stream, the proxy cancels the upstream Cursor run and does **not** commit the pending checkpoint. Checkpoints are only committed after a turn finishes successfully.

### Session fork

When you navigate back in pi's session tree and branch from an earlier point, the proxy discards the stored checkpoint whenever the completed history no longer matches the stored checkpoint metadata. That includes both:

- completed turn count mismatches, and
- same-depth branch changes detected via completed-history fingerprint mismatch.

After discarding a stale checkpoint, the proxy reconstructs proper protobuf conversation turns from the message history pi sends, so Cursor sees the actual conversation structure at the fork point.

### Session resume

Conversation state is stored in memory. If the proxy restarts, checkpoints are lost. On the next request, pi sends the full conversation history, and the proxy reconstructs structured protobuf turns from that history instead of relying on an inline plaintext fallback.

That reconstruction preserves:

- assistant messages
- tool calls
- tool results
- final assistant text after tool results

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
