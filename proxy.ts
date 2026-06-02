/**
 * Local OpenAI-compatible proxy: translates /v1/chat/completions to Cursor's gRPC protocol.
 *
 * Based on https://github.com/ephraimduncan/opencode-cursor by Ephraim Duncan.
 * Uses Node's http2 via a child process bridge (h2-bridge.mjs).
 */
import {
  create,
  fromBinary,
  fromJson,
  type JsonValue,
  toBinary,
  toJson,
} from "@bufbuild/protobuf";
import { ValueSchema } from "@bufbuild/protobuf/wkt";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve as pathResolve, dirname, join as pathJoin } from "node:path";
import { fileURLToPath } from "node:url";
import {
  AgentClientMessageSchema,
  AgentRunRequestSchema,
  AgentServerMessageSchema,
  CancelActionSchema,
  ClientHeartbeatSchema,
  ConversationActionSchema,
  ConversationStateStructureSchema,
  ConversationStepSchema,
  AgentConversationTurnStructureSchema,
  ConversationTurnStructureSchema,
  AssistantMessageSchema,
  BackgroundShellSpawnResultSchema,
  DeleteResultSchema,
  DeleteRejectedSchema,
  DiagnosticsResultSchema,
  ExecClientMessageSchema,
  FetchErrorSchema,
  FetchResultSchema,
  GetBlobResultSchema,
  GrepErrorSchema,
  GrepResultSchema,
  KvClientMessageSchema,
  LsRejectedSchema,
  LsResultSchema,
  McpArgsSchema,
  McpErrorSchema,
  McpResultSchema,
  McpSuccessSchema,
  McpTextContentSchema,
  McpToolCallSchema,
  McpToolDefinitionSchema,
  McpToolErrorSchema,
  McpToolResultSchema,
  McpToolResultContentItemSchema,
  ModelDetailsSchema,
  ReadRejectedSchema,
  ReadResultSchema,
  RequestContextResultSchema,
  RequestContextSchema,
  RequestContextSuccessSchema,
  SelectedContextSchema,
  SelectedImageSchema,
  SetBlobResultSchema,
  ShellRejectedSchema,
  ShellResultSchema,
  ShellStreamSchema,
  ToolCallSchema,
  UserMessageActionSchema,
  UserMessageSchema,
  WriteRejectedSchema,
  WriteResultSchema,
  WriteShellStdinErrorSchema,
  WriteShellStdinResultSchema,
  ConversationSummaryArchiveSchema,
  GetUsableModelsRequestSchema,
  GetUsableModelsResponseSchema,
  type AgentServerMessage,
  type ConversationStateStructure,
  type ExecServerMessage,
  type KvServerMessage,
  type McpToolDefinition,
  type UserMessage,
} from "./proto/agent_pb.js";

const CURSOR_API_URL = "https://api2.cursor.sh";
const CONNECT_END_STREAM_FLAG = 0b00000010;
// Use import.meta.url for bridge path resolution (jiti supports this)
const BRIDGE_PATH = pathResolve(
  dirname(fileURLToPath(import.meta.url)),
  "h2-bridge.mjs",
);

// ── Types ──

interface OpenAIToolCall {
  id: string;
  type: "function";
  function: { name: string; arguments: string };
}

interface ContentPart {
  type: string;
  text?: string;
  image_url?: { url: string };
}

interface OpenAIMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string | null | ContentPart[];
  tool_call_id?: string;
  tool_calls?: OpenAIToolCall[];
}

interface OpenAIToolDef {
  type: "function";
  function: {
    name: string;
    description?: string;
    parameters?: Record<string, unknown>;
  };
}

interface ChatCompletionRequest {
  model: string;
  messages: OpenAIMessage[];
  stream?: boolean;
  temperature?: number;
  max_tokens?: number;
  tools?: OpenAIToolDef[];
  tool_choice?: unknown;
  reasoning_effort?: string;
  user?: string;
  pi_session_id?: string;
}

interface CursorRequestPayload {
  requestBytes: Uint8Array;
  blobStore: Map<string, Uint8Array>;
  mcpTools: McpToolDefinition[];
}

interface PendingExec {
  execId: string;
  execMsgId: number;
  toolCallId: string;
  toolName: string;
  decodedArgs: string;
}

interface StderrData {
  responseHeaders?: { status: number; grpcStatus: number | null };
  exitReason?: "timeout" | "connection_error" | "stream_error";
}

interface BridgeHandle {
  proc: Pick<ChildProcess, "kill">;
  readonly alive: boolean;
  write(data: Uint8Array): void;
  end(): void;
  unref(): void;
  onData(cb: (chunk: Buffer) => void): void;
  onClose(cb: (code: number) => void): void;
  getStderr(): StderrData;
}

export type BridgeFactory = (options: SpawnBridgeOptions) => BridgeHandle;

interface ActiveBridge {
  bridge: BridgeHandle;
  heartbeatTimer: ReturnType<typeof setInterval>;
  blobStore: Map<string, Uint8Array>;
  mcpTools: McpToolDefinition[];
  pendingExecs: PendingExec[];
  currentTurn: ParsedTurn;
}

export interface StoredConversation {
  conversationId: string;
  checkpoint: Uint8Array | null;
  blobStore: Map<string, Uint8Array>;
  /**
   * Cursor's actual context window for this conversation, populated from
   * ConversationTokenDetails.maxTokens in checkpoint updates.  Used to correct
   * our static inferContextWindow() estimate when Cursor enforces a tighter cap.
   */
  effectiveContextWindow?: number;
}

interface StreamState {
  toolCallIndex: number;
  pendingExecs: PendingExec[];
  outputTokens: number;
  /** usedTokens from Cursor's ConversationTokenDetails. */
  totalTokens: number;
  /** maxTokens from Cursor's ConversationTokenDetails; 0 = not yet received. */
  cursorContextWindow: number;
  /** inferContextWindow(modelId) — our static estimate for this model. */
  inferredContextWindow: number;
}

interface ToolResultInfo {
  toolCallId: string;
  content: string;
}

export interface ParsedToolResult {
  content: string;
  isError: boolean;
}

export interface ParsedAssistantTextStep {
  kind: "assistantText";
  text: string;
}

export interface ParsedToolCallStep {
  kind: "toolCall";
  toolCallId: string;
  toolName: string;
  arguments: Record<string, unknown>;
  result?: ParsedToolResult;
}

export type ParsedTurnStep = ParsedAssistantTextStep | ParsedToolCallStep;

export interface ParsedImage {
  mimeType: string;
  data: Uint8Array;
}

function extractImagesFromContent(
  content: OpenAIMessage["content"],
): ParsedImage[] {
  if (!Array.isArray(content)) return [];
  const images: ParsedImage[] = [];
  for (const part of content) {
    if (part.type !== "image_url" || !part.image_url?.url) continue;
    const match = part.image_url.url.match(/^data:([^;]+);base64,(.+)$/);
    if (!match) continue;
    images.push({
      mimeType: match[1]!,
      data: Buffer.from(match[2]!, "base64"),
    });
  }
  return images;
}

export interface ParsedTurn {
  userText: string;
  images: ParsedImage[];
  steps: ParsedTurnStep[];
}

interface ParsedMessages {
  systemPrompt: string;
  userText: string;
  userImages: ParsedImage[];
  turns: ParsedTurn[];
  toolResults: ToolResultInfo[];
}

// ── State ──

const activeBridges = new Map<string, ActiveBridge>();
const conversationStates = new Map<string, StoredConversation>();
let bridgeFactory: BridgeFactory = spawnBridge;
let debugRequestCounter = 0;
let debugLogFilePath: string | undefined;

function isProxyDebugEnabled(): boolean {
  const raw = process.env.PI_CURSOR_PROVIDER_DEBUG?.trim().toLowerCase();
  return !!raw && raw !== "0" && raw !== "false" && raw !== "off";
}

function truncateDebugString(value: string, max = 4000): string {
  return value.length > max
    ? `${value.slice(0, max)}…<truncated ${value.length - max} chars>`
    : value;
}

function sanitizeForDebug(value: Record<string, unknown>): Record<string, unknown>;
function sanitizeForDebug(value: unknown): unknown;
function sanitizeForDebug(value: unknown): unknown {
  if (value == null) return value;
  if (typeof value === "string") return truncateDebugString(value);
  if (typeof value === "number" || typeof value === "boolean") return value;
  if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
    const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
    return {
      __type: value instanceof Uint8Array ? "Uint8Array" : "Buffer",
      byteLength: bytes.length,
      sha256: createHash("sha256").update(bytes).digest("hex").slice(0, 16),
    };
  }
  if (Array.isArray(value)) return value.map((item) => sanitizeForDebug(item));
  if (value instanceof Map) {
    return {
      __type: "Map",
      size: value.size,
      entries: Array.from(value.entries())
        .slice(0, 20)
        .map(([k, v]) => [sanitizeForDebug(k), sanitizeForDebug(v)]),
    };
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).map(
      ([key, inner]) => {
        if (key === "accessToken") return [key, "<redacted>"] as const;
        if (key === "data" && typeof inner === "string")
          return [key, `<redacted base64 ${inner.length} chars>`] as const;
        return [key, sanitizeForDebug(inner)] as const;
      },
    );
    return Object.fromEntries(entries);
  }
  return String(value);
}

function getDebugLogFilePath(): string {
  const configured = process.env.PI_CURSOR_PROVIDER_DEBUG_FILE?.trim();
  if (configured) return configured;
  if (debugLogFilePath) return debugLogFilePath;
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  debugLogFilePath = pathJoin(
    tmpdir(),
    `pi-cursor-provider-debug-${stamp}-${process.pid}.log`,
  );
  return debugLogFilePath;
}

function debugLog(event: string, data?: Record<string, unknown>): void {
  if (!isProxyDebugEnabled()) return;
  const line = JSON.stringify({
    ts: new Date().toISOString(),
    pid: process.pid,
    event,
    ...(data ? sanitizeForDebug(data) : {}),
  });
  const file = getDebugLogFilePath();
  try {
    appendFileSync(file, `${line}\n`, "utf8");
  } catch (error) {
    console.error("[pi-cursor-provider] failed to write debug log", error);
    console.error(`[pi-cursor-provider] ${line}`);
  }
}

function nextDebugRequestId(): string {
  debugRequestCounter += 1;
  return `req-${debugRequestCounter}`;
}

export const __testInternals = {
  activeBridges,
  conversationStates,
};

export function setBridgeFactoryForTests(factory?: BridgeFactory): void {
  bridgeFactory = factory ?? spawnBridge;
}

let proxyServer: ReturnType<typeof createServer> | undefined;
let proxyPort: number | undefined;
let proxyAccessTokenProvider: (() => Promise<string>) | undefined;

// ── Bridge spawn ──

function lpEncode(data: Uint8Array): Buffer {
  const buf = Buffer.alloc(4 + data.length);
  buf.writeUInt32BE(data.length, 0);
  buf.set(data, 4);
  return buf;
}

function frameConnectMessage(data: Uint8Array, flags = 0): Buffer {
  const frame = Buffer.alloc(5 + data.length);
  frame[0] = flags;
  frame.writeUInt32BE(data.length, 1);
  frame.set(data, 5);
  return frame;
}

interface SpawnBridgeOptions {
  accessToken: string;
  rpcPath: string;
  url?: string;
  unary?: boolean;
}

function spawnBridge(options: SpawnBridgeOptions): BridgeHandle {
  debugLog("bridge.spawn", {
    rpcPath: options.rpcPath,
    url: options.url ?? CURSOR_API_URL,
    unary: options.unary ?? false,
  });
  const proc = spawn("node", [BRIDGE_PATH], {
    stdio: ["pipe", "pipe", "pipe"],
  });

  const stderrData: StderrData = {};
  let stderrBuf = "";
  proc.stderr!.on("data", (chunk: Buffer) => {
    stderrBuf += chunk.toString("utf8");
    let nl: number;
    while ((nl = stderrBuf.indexOf("\n")) !== -1) {
      const line = stderrBuf.slice(0, nl).trim();
      stderrBuf = stderrBuf.slice(nl + 1);
      if (!line) continue;
      debugLog("bridge.stderr", { rpcPath: options.rpcPath, line });
      try {
        const parsed = JSON.parse(line) as Record<string, unknown>;
        if (parsed["type"] === "response_headers") {
          stderrData.responseHeaders = {
            status: parsed["status"] as number,
            grpcStatus: parsed["grpcStatus"] as number | null,
          };
        } else if (parsed["type"] === "exit_reason") {
          stderrData.exitReason = parsed["reason"] as StderrData["exitReason"];
        }
      } catch {
        // not structured JSON — ignore
      }
    }
  });

  const config = JSON.stringify({
    accessToken: options.accessToken,
    url: options.url ?? CURSOR_API_URL,
    path: options.rpcPath,
    unary: options.unary ?? false,
  });
  proc.stdin!.write(lpEncode(new TextEncoder().encode(config)));

  const cbs = {
    data: null as ((chunk: Buffer) => void) | null,
    close: null as ((code: number) => void) | null,
  };

  let exited = false;
  let exitCode = 1;

  let pending = Buffer.alloc(0);
  proc.stdout!.on("data", (chunk: Buffer) => {
    pending = Buffer.concat([pending, chunk]);
    while (pending.length >= 4) {
      const len = pending.readUInt32BE(0);
      if (pending.length < 4 + len) break;
      const payload = pending.subarray(4, 4 + len);
      pending = pending.subarray(4 + len);
      cbs.data?.(Buffer.from(payload));
    }
  });

  proc.on("exit", (code) => {
    exited = true;
    exitCode = code ?? 1;
    // Destroy stdio pipes immediately so their handles don't keep the event
    // loop alive after the bridge exits (critical for `pi -p` to exit cleanly).
    try { proc.stdout!.destroy(); } catch {}
    try { proc.stdin!.destroy(); } catch {}
    try { proc.stderr!.destroy(); } catch {}
    debugLog("bridge.exit", { rpcPath: options.rpcPath, exitCode });
    cbs.close?.(exitCode);
  });

  return {
    proc,
    get alive() {
      return !exited;
    },
    write(data: Uint8Array) {
      try {
        proc.stdin!.write(lpEncode(data));
      } catch {}
    },
    end() {
      try {
        proc.stdin!.write(lpEncode(new Uint8Array(0)));
        proc.stdin!.end();
      } catch {}
    },
    onData(cb: (chunk: Buffer) => void) {
      cbs.data = cb;
    },
    unref() {
      try {
        proc.unref();
        (proc.stdout as { unref?: () => void } | null)?.unref?.();
      } catch {}
    },
    onClose(cb: (code: number) => void) {
      if (exited) {
        queueMicrotask(() => cb(exitCode));
      } else {
        cbs.close = cb;
      }
    },
    getStderr() {
      return stderrData;
    },
  };
}

// ── Bridge failure classification ──

function classifyBridgeFailure(code: number, stderr: StderrData): string {
  const hadHeaders = !!stderr.responseHeaders;
  const status = stderr.responseHeaders?.status;
  const reason = stderr.exitReason;

  // Timeout (exit 2 or explicit reason)
  if (code === 2 || reason === "timeout") {
    return hadHeaders
      ? "Cursor did not respond within 5 minutes \u2014 try again"
      : "Could not reach Cursor's API within 2 minutes \u2014 check your network";
  }

  // Connection or stream error (exit 1)
  if (status === 401 || status === 403) {
    return "Cursor authentication expired \u2014 run /login cursor to re-authenticate";
  }
  if (status === 429) {
    return "Cursor rate limited \u2014 try again shortly";
  }
  if (status !== undefined && status >= 500 && status < 600) {
    return `Cursor server error (${status}) \u2014 try again`;
  }
  if (!hadHeaders) {
    return "Could not connect to Cursor's API \u2014 check your network";
  }

  return `Cursor bridge terminated (exit ${code}) before response \u2014 try again or shorten the conversation`;
}

// ── Unary RPC (for model discovery) ──

export async function callCursorUnaryRpc(options: {
  accessToken: string;
  rpcPath: string;
  requestBody: Uint8Array;
  url?: string;
  timeoutMs?: number;
}): Promise<{ body: Uint8Array; exitCode: number; timedOut: boolean }> {
  const bridge = bridgeFactory({
    accessToken: options.accessToken,
    rpcPath: options.rpcPath,
    url: options.url,
    unary: true,
  });
  const chunks: Buffer[] = [];
  return new Promise((resolve) => {
    let timedOut = false;
    const timeoutMs = options.timeoutMs ?? 5_000;
    const timeout =
      timeoutMs > 0
        ? setTimeout(() => {
            timedOut = true;
            try {
              bridge.proc.kill();
            } catch {}
          }, timeoutMs)
        : undefined;

    bridge.onData((chunk) => {
      chunks.push(Buffer.from(chunk));
    });
    bridge.onClose((exitCode) => {
      if (timeout) clearTimeout(timeout);
      resolve({ body: Buffer.concat(chunks), exitCode, timedOut });
    });

    bridge.write(options.requestBody);
    bridge.end();
  });
}

// ── Model discovery ──

export interface CursorModel {
  id: string;
  name: string;
  reasoning: boolean;
  contextWindow: number;
  maxTokens: number;
}

let cachedModels: CursorModel[] | null = null;

export async function getCursorModels(apiKey: string): Promise<CursorModel[]> {
  if (cachedModels) return cachedModels;
  try {
    const requestPayload = create(GetUsableModelsRequestSchema, {});
    const requestBody = toBinary(GetUsableModelsRequestSchema, requestPayload);
    const response = await callCursorUnaryRpc({
      accessToken: apiKey,
      rpcPath: "/agent.v1.AgentService/GetUsableModels",
      requestBody,
    });
    if (
      !response.timedOut &&
      response.exitCode === 0 &&
      response.body.length > 0
    ) {
      let decoded: ReturnType<typeof fromBinary<typeof GetUsableModelsResponseSchema>> | null = null;
      try {
        decoded = fromBinary(GetUsableModelsResponseSchema, response.body);
      } catch {
        // Try Connect framing
        const body = decodeConnectUnaryBody(response.body);
        if (body) {
          try {
            decoded = fromBinary(GetUsableModelsResponseSchema, body);
          } catch {}
        }
      }
      if (decoded?.models?.length) {
        const models = normalizeCursorModels(decoded.models);
        if (models.length > 0) {
          cachedModels = models;
          return models;
        }
      }
    }
  } catch (err) {
    console.error(
      "[cursor-provider] Model discovery failed:",
      err instanceof Error ? err.message : err,
    );
  }
  console.warn("[cursor-provider] Model discovery returned no models");
  return [];
}

function decodeConnectUnaryBody(payload: Uint8Array): Uint8Array | null {
  if (payload.length < 5) return null;
  let offset = 0;
  while (offset + 5 <= payload.length) {
    const flags = payload[offset]!;
    const view = new DataView(
      payload.buffer,
      payload.byteOffset + offset,
      payload.byteLength - offset,
    );
    const messageLength = view.getUint32(1, false);
    const frameEnd = offset + 5 + messageLength;
    if (frameEnd > payload.length) return null;
    if ((flags & 0b0000_0001) !== 0) return null;
    if ((flags & 0b0000_0010) === 0)
      return payload.subarray(offset + 5, frameEnd);
    offset = frameEnd;
  }
  return null;
}

/**
 * Infer context window size from the model ID.
 *
 * Cursor's GetUsableModels RPC does not expose context window sizes, so we
 * derive them from known model families.  Update when new major versions ship.
 *
 * Sources:
 *  - Claude: platform.claude.ai/docs — claude-4.6-sonnet / claude-4.6-opus: native 1M context
 *    (GA Mar 2026), but Cursor enforces a 200k cap via ConversationTokenDetails.maxTokens.
 *    Registered at 200k to match Cursor's actual limit; all other Claude incl. 4.5, 4, Haiku: 200k.
 *  - Gemini: ai.google.dev/gemini-api/docs — all 2.5 / 3.x models: 1M.
 *  - GPT: chatai.guide — GPT-5.x: 400k; GPT-5.5+: 1M; nano/mini variants: 128k.
 *  - Grok 4: docs.x.ai — 256k.
 *  - Kimi K2.x: platform.kimi.ai — 262,144 tokens (256k).
 */
export function inferContextWindow(id: string): number {
  const lower = id.toLowerCase();

  // Any model with an explicit -1m suffix (e.g. claude-4-sonnet-1m)
  if (lower.includes("-1m")) return 1_048_576;

  // ── Claude ────────────────────────────────────────────────────────────────
  // Sonnet 4.6 / Opus 4.6 natively support 1M but Cursor enforces 200k server-side.
  // Registering at 200k avoids spurious 5× scaling in computeUsage.
  // All other Claude (4.5, 4, Haiku, …) are also 200k.
  if (lower.startsWith("claude-")) return 200_000;

  // ── Gemini ────────────────────────────────────────────────────────────────
  // Gemini 2.5 / 3.x family: 1M context.
  if (lower.startsWith("gemini-")) return 1_048_576;

  // ── GPT ───────────────────────────────────────────────────────────────────
  // nano / mini variants: 128k.  GPT-5.5+: 1M.  Everything else (5.x): 400k.
  if (/^gpt-[0-9.]*-(nano|mini)/.test(lower)) return 128_000;
  if (lower.startsWith("gpt-5.5")) return 1_048_576;
  if (lower.startsWith("gpt-")) return 400_000;

  // ── Grok ──────────────────────────────────────────────────────────────────
  // Grok 4 series: 256k.
  if (lower.startsWith("grok-")) return 256_000;

  // ── Kimi ──────────────────────────────────────────────────────────────────
  // Kimi K2.x: 262,144 tokens (256k).
  if (lower.startsWith("kimi-")) return 262_144;

  // Composer, default, unknown: 200k.
  return 200_000;
}

function normalizeCursorModels(models: readonly unknown[]): CursorModel[] {
  const byId = new Map<string, CursorModel>();
  for (const model of models) {
    if (!model || typeof model !== "object") continue;
    const m = model as Record<string, unknown>;
    const rawId = m["modelId"];
    const id = typeof rawId === "string" ? rawId.trim() : "";
    if (!id) continue;
    const name = String(m["displayName"] || m["displayNameShort"] || m["displayModelId"] || id);
    byId.set(id, {
      id,
      name,
      reasoning: Boolean(m["thinkingDetails"]),
      contextWindow: inferContextWindow(id),
      maxTokens: 64_000,
    });
  }
  return [...byId.values()].sort((a, b) => a.id.localeCompare(b.id));
}

// ── Proxy server ──

export function getProxyPort(): number | undefined {
  return proxyPort;
}

export async function startProxy(
  getAccessToken: () => Promise<string>,
): Promise<number> {
  proxyAccessTokenProvider = getAccessToken;
  if (proxyServer && proxyPort) return proxyPort;

  return new Promise((resolve, reject) => {
    const server = createServer(async (req, res) => {
      const url = new URL(req.url ?? "/", `http://localhost`);
      const requestId = nextDebugRequestId();
      debugLog("http.request", {
        requestId,
        method: req.method,
        pathname: url.pathname,
        headers: req.headers,
      });

      // Prevent HTTP keep-alive from holding the event loop open after a
      // response completes (critical for `pi -p` to exit cleanly).
      res.setHeader("Connection", "close");

      if (req.method === "GET" && url.pathname === "/v1/models") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ object: "list", data: [] }));
        return;
      }

      if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
        try {
          const body = await readBody(req);
          const parsed = JSON.parse(body) as ChatCompletionRequest;
          debugLog("http.chat.body", { requestId, body: parsed });
          if (!proxyAccessTokenProvider)
            throw new Error("No access token provider");
          const accessToken = await proxyAccessTokenProvider();
          await handleChatCompletion(parsed, accessToken, req, res, requestId);
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          debugLog("http.chat.error", {
            requestId,
            message,
            stack: err instanceof Error ? err.stack : undefined,
          });
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({
              error: { message, type: "server_error", code: "internal_error" },
            }),
          );
        }
        return;
      }

      res.writeHead(404);
      res.end("Not Found");
    });

    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (typeof addr === "object" && addr) {
        proxyPort = addr.port;
        proxyServer = server;
        // Don't hold the event loop open — pi -p must be able to exit cleanly
        // after a response without an explicit shutdown signal.
        server.unref();
        // unref() only covers the listening socket; accepted connection sockets
        // are ref'd by default. Unref each accepted socket so keep-alive HTTP
        // connections don't prevent process exit either.
        server.on("connection", (socket) => socket.unref());
        debugLog("proxy.start", {
          port: proxyPort,
          debugLogFile: isProxyDebugEnabled()
            ? getDebugLogFilePath()
            : undefined,
        });
        resolve(proxyPort);
      } else {
        reject(new Error("Failed to bind proxy"));
      }
    });
  });
}

export function cleanupAllSessionState(): void {
  debugLog("session.cleanup_all", {
    activeBridgeCount: activeBridges.size,
    conversationCount: conversationStates.size,
  });
  for (const [bridgeKey, active] of activeBridges) {
    cleanupBridge(active.bridge, active.heartbeatTimer, bridgeKey);
  }
  conversationStates.clear();
}

export function stopProxy(): void {
  debugLog("proxy.stop", { port: proxyPort });
  if (proxyServer) {
    proxyServer.close();
    proxyServer = undefined;
    proxyPort = undefined;
    proxyAccessTokenProvider = undefined;
  }
  cleanupAllSessionState();
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

// ── Request handling ──

/**
 * Insert reasoning effort into model ID, before -fast/-thinking suffix.
 * e.g. model="gpt-5.4" + effort="medium" → "gpt-5.4-medium"
 *      model="gpt-5.4-fast" + effort="high" → "gpt-5.4-high-fast"
 * If no effort provided, returns model as-is.
 */
export function resolveModelId(
  model: string,
  reasoningEffort?: string,
): string {
  if (!reasoningEffort) return model;

  let suffix = "";
  let base = model;
  if (base.endsWith("-fast")) {
    suffix = "-fast";
    base = base.slice(0, -5);
  } else if (base.endsWith("-thinking")) {
    suffix = "-thinking";
    base = base.slice(0, -9);
  }

  return `${base}-${reasoningEffort}${suffix}`;
}

async function handleChatCompletion(
  body: ChatCompletionRequest,
  accessToken: string,
  req: IncomingMessage,
  res: ServerResponse,
  requestId: string,
): Promise<void> {
  const { systemPrompt, userText, userImages, turns, toolResults } =
    parseMessages(body.messages);
  const modelId = resolveModelId(body.model, body.reasoning_effort);
  const tools = body.tools ?? [];

  debugLog("chat.parsed_messages", {
    requestId,
    systemPrompt,
    userText,
    turns,
    toolResults,
    messageCount: body.messages.length,
    model: body.model,
    resolvedModelId: modelId,
    stream: body.stream !== false,
  });

  if (!userText && toolResults.length === 0) {
    debugLog("chat.no_user_message", { requestId, messages: body.messages });
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        error: {
          message: "No user message found",
          type: "invalid_request_error",
        },
      }),
    );
    return;
  }

  const sessionId = derivePiSessionId(body);
  const bridgeKey = deriveBridgeKey(body.messages, sessionId);
  const convKey = deriveConversationKey(body.messages, sessionId);
  const activeBridge = activeBridges.get(bridgeKey);
  debugLog("chat.session_keys", {
    requestId,
    sessionId,
    bridgeKey,
    convKey,
    hasActiveBridge: !!activeBridge,
  });

  if (activeBridge && toolResults.length > 0) {
    debugLog("chat.resume_tool_results", {
      requestId,
      bridgeKey,
      toolResults,
      pendingExecs: activeBridge.pendingExecs,
    });
    activeBridges.delete(bridgeKey);
    if (activeBridge.bridge.alive) {
      handleToolResultResume(
        activeBridge,
        toolResults,
        modelId,
        bridgeKey,
        convKey,
        turns,
        req,
        res,
        body.stream !== false,
        requestId,
      );
      return;
    }
    clearInterval(activeBridge.heartbeatTimer);
    activeBridge.bridge.end();
  }

  if (activeBridge && activeBridges.has(bridgeKey)) {
    clearInterval(activeBridge.heartbeatTimer);
    activeBridge.bridge.end();
    activeBridges.delete(bridgeKey);
  }

  let stored = conversationStates.get(convKey);
  debugLog("chat.stored_state.before", { requestId, convKey, stored });
  if (!stored) {
    stored = {
      conversationId: deterministicConversationId(convKey),
      checkpoint: null,
      blobStore: new Map(),
    };
    conversationStates.set(convKey, stored);
  }

  const mcpTools = buildMcpToolDefinitions(tools);
  const effectiveUserText =
    userText ||
    (toolResults.length > 0
      ? toolResults.map((r) => r.content).join("\n")
      : "");
  if (!stored.checkpoint) {
    debugLog("chat.no_checkpoint", {
      requestId,
      convKey,
      conversationId: stored.conversationId,
    });
  }
  const payload = buildCursorRequest(
    modelId,
    systemPrompt,
    effectiveUserText,
    turns,
    stored.conversationId,
    stored.checkpoint,
    stored.blobStore,
    userImages,
  );
  debugLog("chat.cursor_request", {
    requestId,
    conversationId: stored.conversationId,
    effectiveUserText,
    turnCount: turns.length,
    hasCheckpoint: !!stored.checkpoint,
    payload,
  });
  payload.mcpTools = mcpTools;

  const currentTurn: ParsedTurn = {
    userText: effectiveUserText,
    images: userImages,
    steps: [],
  };

  if (body.stream === false) {
    debugLog("chat.dispatch_nonstream", { requestId, convKey });
    await handleNonStreamingResponse(
      payload,
      accessToken,
      modelId,
      convKey,
      turns,
      currentTurn,
      req,
      res,
      requestId,
    );
  } else {
    debugLog("chat.dispatch_stream", { requestId, bridgeKey, convKey });
    handleStreamingResponse(
      payload,
      accessToken,
      modelId,
      bridgeKey,
      convKey,
      turns,
      currentTurn,
      req,
      res,
      requestId,
    );
  }
}

// ── Message parsing ──

function textContent(content: OpenAIMessage["content"]): string {
  if (content == null) return "";
  if (typeof content === "string") return content;
  return content
    .filter((p) => p.type === "text" && p.text)
    .map((p) => p.text!)
    .join("\n");
}

function parseToolCallArguments(raw: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
    return { value: parsed };
  } catch {
    return raw ? { __raw: raw } : {};
  }
}

function isToolCallStep(step: ParsedTurnStep): step is ParsedToolCallStep {
  return step.kind === "toolCall";
}

function getTurnToolCallResults(
  turn: ParsedTurn,
): Map<string, ParsedToolResult> {
  const results = new Map<string, ParsedToolResult>();
  for (const step of turn.steps) {
    if (step.kind === "toolCall" && step.result)
      results.set(step.toolCallId, step.result);
  }
  return results;
}

function appendAssistantTextToTurn(turn: ParsedTurn, text: string): void {
  if (!text) return;
  const last = turn.steps.at(-1);
  if (last?.kind === "assistantText") {
    last.text += text;
  } else {
    turn.steps.push({ kind: "assistantText", text });
  }
}

function stripTurnRuntimeState(
  turn: ParsedTurn & {
    toolCallById?: Map<string, ParsedToolCallStep>;
    sawToolResult?: boolean;
    sawAssistantAfterToolResult?: boolean;
  },
): ParsedTurn {
  return { userText: turn.userText, images: turn.images, steps: turn.steps };
}

export function parseMessages(messages: OpenAIMessage[]): ParsedMessages {
  let systemPrompt = "You are a helpful assistant.";
  const turns: ParsedTurn[] = [];

  debugLog("parse_messages.start", { messages });

  const systemParts = messages
    .filter((m) => m.role === "system")
    .map((m) => textContent(m.content));
  if (systemParts.length > 0) systemPrompt = systemParts.join("\n");

  const nonSystem = messages.filter((m) => m.role !== "system");
  let currentTurn:
    | (ParsedTurn & {
        toolCallById: Map<string, ParsedToolCallStep>;
        sawToolResult: boolean;
        sawAssistantAfterToolResult: boolean;
      })
    | null = null;

  const finalizeCurrentTurn = () => {
    if (!currentTurn) return;
    turns.push(stripTurnRuntimeState(currentTurn));
    currentTurn = null;
  };

  for (const msg of nonSystem) {
    if (msg.role === "user") {
      finalizeCurrentTurn();
      currentTurn = {
        userText: textContent(msg.content),
        images: extractImagesFromContent(msg.content),
        steps: [],
        toolCallById: new Map(),
        sawToolResult: false,
        sawAssistantAfterToolResult: false,
      };
      continue;
    }

    if (!currentTurn) continue;

    if (msg.role === "assistant") {
      const text = textContent(msg.content);
      if (text) {
        if (currentTurn.sawToolResult)
          currentTurn.sawAssistantAfterToolResult = true;
        currentTurn.steps.push({ kind: "assistantText", text });
      }

      for (const toolCall of msg.tool_calls ?? []) {
        const step: ParsedToolCallStep = {
          kind: "toolCall",
          toolCallId: toolCall.id,
          toolName: toolCall.function.name,
          arguments: parseToolCallArguments(toolCall.function.arguments),
        };
        currentTurn.steps.push(step);
        currentTurn.toolCallById.set(step.toolCallId, step);
      }
      continue;
    }

    if (msg.role === "tool") {
      const toolCallId = msg.tool_call_id ?? "";
      const content = textContent(msg.content);
      const existing = toolCallId
        ? currentTurn.toolCallById.get(toolCallId)
        : undefined;
      if (existing) {
        existing.result = { content, isError: false };
      } else {
        const step: ParsedToolCallStep = {
          kind: "toolCall",
          toolCallId,
          toolName: "",
          arguments: {},
          result: { content, isError: false },
        };
        currentTurn.steps.push(step);
        if (toolCallId) currentTurn.toolCallById.set(toolCallId, step);
      }
      currentTurn.sawToolResult = true;
    }
  }

  let userText = "";
  let userImages: ParsedImage[] = [];
  let toolResults: ToolResultInfo[] = [];

  if (currentTurn) {
    const toolCallSteps = currentTurn.steps.filter(isToolCallStep);
    const hasAnyToolResults = toolCallSteps.some((step) => step.result);
    const lastStep = currentTurn.steps.at(-1);
    const isToolContinuation = lastStep?.kind === "toolCall";

    if (currentTurn.steps.length === 0 || isToolContinuation) {
      userText = currentTurn.userText;
      userImages = currentTurn.images;
      if (hasAnyToolResults) {
        toolResults = toolCallSteps
          .filter((step) => step.result)
          .map((step) => ({
            toolCallId: step.toolCallId,
            content: step.result!.content,
          }));
      }
    } else {
      turns.push(stripTurnRuntimeState(currentTurn));
    }
  }

  const parsed = { systemPrompt, userText, userImages, turns, toolResults };
  debugLog("parse_messages.end", parsed);
  return parsed;
}

// ── Tool definitions ──

function buildMcpToolDefinitions(tools: OpenAIToolDef[]): McpToolDefinition[] {
  return tools.map((t) => {
    const fn = t.function;
    const jsonSchema: JsonValue =
      fn.parameters && typeof fn.parameters === "object"
        ? (fn.parameters as JsonValue)
        : { type: "object", properties: {}, required: [] };
    const inputSchema = toBinary(
      ValueSchema,
      fromJson(ValueSchema, jsonSchema),
    );
    return create(McpToolDefinitionSchema, {
      name: fn.name,
      description: fn.description || "",
      providerIdentifier: "pi",
      toolName: fn.name,
      inputSchema,
    });
  });
}

function decodeMcpArgValue(value: Uint8Array): unknown {
  try {
    const parsed = fromBinary(ValueSchema, value);
    return toJson(ValueSchema, parsed);
  } catch {}
  return new TextDecoder().decode(value);
}

function decodeMcpArgsMap(
  args: Record<string, Uint8Array>,
): Record<string, unknown> {
  const decoded: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(args))
    decoded[key] = decodeMcpArgValue(value);
  return decoded;
}

// ── Build Cursor protobuf request ──

function encodeMcpArgValue(value: unknown): Uint8Array {
  try {
    return toBinary(ValueSchema, fromJson(ValueSchema, value as JsonValue));
  } catch {
    return new TextEncoder().encode(String(value));
  }
}

function encodeMcpArgsMap(
  args: Record<string, unknown>,
): Record<string, Uint8Array> {
  const encoded: Record<string, Uint8Array> = {};
  for (const [key, value] of Object.entries(args))
    encoded[key] = encodeMcpArgValue(value);
  return encoded;
}

// No generated schema for selectedContextBlob; emit raw wire format for the two
// fields Cursor actually reads: field 1 (repeated bytes) rootPromptMessagesJson
// refs, field 22 (string) clientName. blobId.length < 128 (SHA256 = 32 bytes).
function buildSelectedContextBlob(
  rootPromptBlobIds: Uint8Array[],
  clientName: string,
): Uint8Array {
  const parts: Uint8Array[] = [];
  for (const blobId of rootPromptBlobIds) {
    parts.push(new Uint8Array([0x0a, blobId.length, ...blobId]));
  }
  const clientBytes = new TextEncoder().encode(clientName);
  parts.push(new Uint8Array([0xb2, 0x01, clientBytes.length, ...clientBytes]));
  const total = parts.reduce((n, p) => n + p.length, 0);
  const result = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    result.set(p, offset);
    offset += p.length;
  }
  return result;
}

function storeAsBlob(
  data: Uint8Array,
  blobStore: Map<string, Uint8Array>,
): Uint8Array {
  const id = new Uint8Array(createHash("sha256").update(data).digest());
  blobStore.set(Buffer.from(id).toString("hex"), data);
  return id;
}

function buildSelectedImages(
  images: ParsedImage[],
): ReturnType<typeof create<typeof SelectedImageSchema>>[] {
  return images.map((img) =>
    create(SelectedImageSchema, {
      uuid: crypto.randomUUID(),
      path: "",
      mimeType: img.mimeType,
      dataOrBlobId: { case: "data", value: img.data },
    }),
  );
}

function createUserMessage(
  text: string,
  selectedContextBlob: Uint8Array,
  images?: ParsedImage[],
): UserMessage {
  const messageId = crypto.randomUUID();
  return create(UserMessageSchema, {
    text,
    messageId,
    selectedContext: create(SelectedContextSchema, {
      selectedImages: images?.length ? buildSelectedImages(images) : [],
    }),
    mode: 1,
    selectedContextBlob,
    correlationId: messageId,
  });
}

function buildTurnStepBytes(step: ParsedTurnStep): Uint8Array {
  if (step.kind === "assistantText") {
    return toBinary(
      ConversationStepSchema,
      create(ConversationStepSchema, {
        message: {
          case: "assistantMessage",
          value: create(AssistantMessageSchema, { text: step.text }),
        },
      }),
    );
  }

  const toolName = step.toolName || "tool";
  const mcpToolCall = create(McpToolCallSchema, {
    args: create(McpArgsSchema, {
      name: toolName,
      args: encodeMcpArgsMap(step.arguments),
      toolCallId: step.toolCallId,
      providerIdentifier: "pi",
      toolName,
    }),
    ...(step.result && {
      result: create(McpToolResultSchema, {
        result: step.result.isError
          ? {
              case: "error",
              value: create(McpToolErrorSchema, { error: step.result.content }),
            }
          : {
              case: "success",
              value: create(McpSuccessSchema, {
                content: [
                  create(McpToolResultContentItemSchema, {
                    content: {
                      case: "text",
                      value: create(McpTextContentSchema, {
                        text: step.result.content,
                      }),
                    },
                  }),
                ],
                isError: false,
              }),
            },
      }),
    }),
  });

  return toBinary(
    ConversationStepSchema,
    create(ConversationStepSchema, {
      message: {
        case: "toolCall",
        value: create(ToolCallSchema, {
          tool: {
            case: "mcpToolCall",
            value: mcpToolCall,
          },
        }),
      },
    }),
  );
}

// Number of most-recent turns to keep as raw blobs; older turns are folded
// into a ConversationSummaryArchive blob with inline text.  Keeping only the
// tail as raw blobs caps the number of blob fetches the server needs per
// request to O(THRESHOLD) instead of O(conversation_length), which is the
// primary driver of compaction slowness for long sessions.
const TURN_ARCHIVE_THRESHOLD =
  parseInt(process.env.PI_CURSOR_TURN_ARCHIVE_THRESHOLD ?? "") || 20;

/**
 * Renders parsed turns (already-decoded OpenAI messages) as plain text for
 * use as the `summary` field of a ConversationSummaryArchive.  Tool results
 * are truncated so the archive blob stays small.
 */
function buildTurnsTranscript(turns: ParsedTurn[]): string {
  const parts: string[] = [
    `[Earlier conversation — ${turns.length} turn(s)]\n`,
  ];
  for (const [i, turn] of turns.entries()) {
    parts.push(`Turn ${i + 1}:`);
    if (turn.userText) parts.push(`User: ${turn.userText.slice(0, 1000)}`);
    for (const step of turn.steps) {
      if (step.kind === "assistantText") {
        if (step.text) parts.push(`Assistant: ${step.text.slice(0, 800)}`);
      } else if (step.kind === "toolCall") {
        const argsStr = JSON.stringify(step.arguments).slice(0, 300);
        parts.push(`Tool: ${step.toolName}(${argsStr})`);
        if (step.result?.content) {
          parts.push(`Result: ${step.result.content.slice(0, 400)}`);
        }
      }
    }
    parts.push("");
  }
  return parts.join("\n");
}

/**
 * Extracts a human-readable transcript of a single turn from the blob store.
 * Returns null if required blobs are missing (turn is left as a raw blob).
 */
function extractTextFromTurnBlob(
  turnBlobId: Uint8Array,
  blobStore: Map<string, Uint8Array>,
): string | null {
  try {
    const turnData = blobStore.get(Buffer.from(turnBlobId).toString("hex"));
    if (!turnData) return null;

    const turnStructure = fromBinary(ConversationTurnStructureSchema, turnData);
    if (turnStructure.turn.case !== "agentConversationTurn") return null;

    const agentTurn = turnStructure.turn.value;
    const lines: string[] = [];

    const userMsgData = blobStore.get(
      Buffer.from(agentTurn.userMessage).toString("hex"),
    );
    if (userMsgData) {
      const userMsg = fromBinary(UserMessageSchema, userMsgData);
      if (userMsg.text) lines.push(`User: ${userMsg.text.slice(0, 1000)}`);
    } else {
      return null; // can't represent this turn without its user message
    }

    for (const stepBlobId of agentTurn.steps) {
      const stepData = blobStore.get(
        Buffer.from(stepBlobId).toString("hex"),
      );
      if (!stepData) continue;
      const step = fromBinary(ConversationStepSchema, stepData);
      if (step.message.case === "assistantMessage") {
        const text = step.message.value.text;
        if (text) lines.push(`Assistant: ${text.slice(0, 800)}`);
      } else if (step.message.case === "toolCall") {
        const tc = step.message.value;
        if (tc.tool.case === "mcpToolCall") {
          const mcp = tc.tool.value;
          const name = mcp.args?.name ?? "tool";
          lines.push(`Tool: ${name}`);
          if (mcp.result?.result.case === "success") {
            const content = mcp.result.result.value.content
              .map((c) =>
                c.content.case === "text" ? c.content.value.text : "",
              )
              .join("")
              .slice(0, 400);
            if (content) lines.push(`Result: ${content}`);
          }
        }
      }
    }
    return lines.join("\n") || null;
  } catch {
    return null;
  }
}

export function buildCursorRequest(
  modelId: string,
  systemPrompt: string,
  userText: string,
  turns: ParsedTurn[],
  conversationId: string,
  checkpoint: Uint8Array | null,
  existingBlobStore?: Map<string, Uint8Array>,
  userImages?: ParsedImage[],
): CursorRequestPayload {
  debugLog("cursor_request.build.start", {
    modelId,
    systemPrompt,
    userText,
    turns,
    conversationId,
    checkpoint,
    existingBlobStore,
  });
  const blobStore = new Map<string, Uint8Array>(existingBlobStore ?? []);

  const systemBytes = new TextEncoder().encode(
    JSON.stringify({ role: "system", content: systemPrompt }),
  );
  const systemBlobId = storeAsBlob(systemBytes, blobStore);
  const selectedCtxBlob = storeAsBlob(
    buildSelectedContextBlob([systemBlobId], "pi"),
    blobStore,
  );

  let conversationState;
  if (checkpoint) {
    conversationState = fromBinary(
      ConversationStateStructureSchema,
      checkpoint,
    );
    // Archive old turns from the checkpoint when the tail is too long.
    // Each raw turn blob requires ~3 getBlobArgs round-trips from the server;
    // replacing old turns with a single ConversationSummaryArchive blob (inline
    // text) cuts that to 1 fetch for all archived history.
    if (conversationState.turns.length > TURN_ARCHIVE_THRESHOLD) {
      const oldTurnIds = conversationState.turns.slice(
        0,
        conversationState.turns.length - TURN_ARCHIVE_THRESHOLD,
      );
      const recentTurnIds = conversationState.turns.slice(
        -TURN_ARCHIVE_THRESHOLD,
      );

      const archiveLines: string[] = [
        `[Earlier conversation \u2014 ${oldTurnIds.length} turn(s)]\n`,
      ];
      let archivedCount = 0;
      for (const [i, oldTurnId] of oldTurnIds.entries()) {
        const text = extractTextFromTurnBlob(oldTurnId, blobStore);
        if (text === null) continue; // blob missing — leave turn as-is
        archiveLines.push(`Turn ${i + 1}:\n${text}`);
        archiveLines.push("");
        archivedCount++;
      }

      // Only replace turns with archive if we could represent all of them;
      // a partial archive would silently drop context the server needs.
      if (archivedCount === oldTurnIds.length) {
        const archive = create(ConversationSummaryArchiveSchema, {
          summarizedMessages: oldTurnIds,
          summary: archiveLines.join("\n"),
          windowTail: oldTurnIds.length,
          summaryMessage: new Uint8Array(0),
        });
        const archiveBlobId = storeAsBlob(
          toBinary(ConversationSummaryArchiveSchema, archive),
          blobStore,
        );
        conversationState.turns = recentTurnIds;
        conversationState.summaryArchives = [
          ...conversationState.summaryArchives,
          archiveBlobId,
        ];
        debugLog("cursor_request.turns_archived", {
          archivedCount,
          remaining: recentTurnIds.length,
          totalArchives: conversationState.summaryArchives.length,
        });
      }
    }
  } else {
    // When rebuilding from scratch (no checkpoint), archive old parsed turns
    // directly — we have their text, no blob parsing needed.
    const olderTurns =
      turns.length > TURN_ARCHIVE_THRESHOLD
        ? turns.slice(0, turns.length - TURN_ARCHIVE_THRESHOLD)
        : [];
    const recentTurns =
      turns.length > TURN_ARCHIVE_THRESHOLD
        ? turns.slice(-TURN_ARCHIVE_THRESHOLD)
        : turns;

    const summaryArchives: Uint8Array[] = [];
    if (olderTurns.length > 0) {
      const archive = create(ConversationSummaryArchiveSchema, {
        summarizedMessages: [], // no blob IDs yet — turns haven't been stored
        summary: buildTurnsTranscript(olderTurns),
        windowTail: olderTurns.length,
        summaryMessage: new Uint8Array(0),
      });
      summaryArchives.push(
        storeAsBlob(
          toBinary(ConversationSummaryArchiveSchema, archive),
          blobStore,
        ),
      );
      debugLog("cursor_request.turns_archived_from_scratch", {
        archivedCount: olderTurns.length,
        remaining: recentTurns.length,
      });
    }

    const turnBlobIds: Uint8Array[] = [];
    for (const turn of recentTurns) {
      const userMsg = createUserMessage(
        turn.userText,
        selectedCtxBlob,
        turn.images,
      );
      const userMsgBlobId = storeAsBlob(
        toBinary(UserMessageSchema, userMsg),
        blobStore,
      );
      const stepBlobIds = turn.steps.map((s) =>
        storeAsBlob(buildTurnStepBytes(s), blobStore),
      );

      const agentTurn = create(AgentConversationTurnStructureSchema, {
        userMessage: userMsgBlobId,
        steps: stepBlobIds,
        requestId: crypto.randomUUID(),
      });
      const turnStructure = create(ConversationTurnStructureSchema, {
        turn: { case: "agentConversationTurn", value: agentTurn },
      });
      turnBlobIds.push(
        storeAsBlob(
          toBinary(ConversationTurnStructureSchema, turnStructure),
          blobStore,
        ),
      );
    }

    conversationState = create(ConversationStateStructureSchema, {
      rootPromptMessagesJson: [systemBlobId],
      turns: turnBlobIds,
      todos: [],
      pendingToolCalls: [],
      previousWorkspaceUris: [],
      mode: 1,
      fileStates: {},
      fileStatesV2: {},
      summaryArchives,
      turnTimings: [],
      subagentStates: {},
      selfSummaryCount: 0,
      readPaths: [],
      clientName: "pi",
    });
  }

  const userMessage = createUserMessage(userText, selectedCtxBlob, userImages);
  const action = create(ConversationActionSchema, {
    action: {
      case: "userMessageAction",
      value: create(UserMessageActionSchema, { userMessage }),
    },
  });
  const modelDetails = create(ModelDetailsSchema, {
    modelId,
    displayModelId: modelId,
    displayName: modelId,
  });
  const runRequest = create(AgentRunRequestSchema, {
    conversationState,
    action,
    modelDetails,
    conversationId,
  });
  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "runRequest", value: runRequest },
  });

  const payload = {
    requestBytes: toBinary(AgentClientMessageSchema, clientMessage),
    blobStore,
    mcpTools: [],
  };
  debugLog("cursor_request.build.end", payload);
  return payload;
}

// ── Server message processing ──

function processServerMessage(
  msg: AgentServerMessage,
  blobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
  sendFrame: (data: Uint8Array) => void,
  state: StreamState,
  onText: (text: string, isThinking?: boolean) => void,
  onMcpExec: (exec: PendingExec) => void,
  onCheckpoint?: (checkpointBytes: Uint8Array) => void,
): void {
  const msgCase = msg.message.case;
  debugLog("server_message", { msgCase, msg });

  if (msgCase === "interactionUpdate") {
    const update = msg.message.value as any;
    const updateCase = update.message?.case;
    if (updateCase === "textDelta") {
      const delta = update.message.value.text || "";
      if (delta) onText(delta, false);
    } else if (updateCase === "thinkingDelta") {
      const delta = update.message.value.text || "";
      if (delta) onText(delta, true);
    } else if (updateCase === "tokenDelta") {
      state.outputTokens += update.message.value.tokens ?? 0;
    }
  } else if (msgCase === "kvServerMessage") {
    handleKvMessage(msg.message.value as KvServerMessage, blobStore, sendFrame);
  } else if (msgCase === "execServerMessage") {
    handleExecMessage(
      msg.message.value as ExecServerMessage,
      mcpTools,
      sendFrame,
      onMcpExec,
    );
  } else if (msgCase === "conversationCheckpointUpdate") {
    const stateStructure = msg.message.value as ConversationStateStructure;
    if ((stateStructure as any).tokenDetails) {
      const td = (stateStructure as any).tokenDetails as { usedTokens?: number; maxTokens?: number };
      if (td.usedTokens) state.totalTokens = td.usedTokens;
      if (td.maxTokens) state.cursorContextWindow = td.maxTokens;
    }
    if (onCheckpoint) {
      onCheckpoint(toBinary(ConversationStateStructureSchema, stateStructure));
    }
  }
}

function sendKvResponse(
  kvMsg: KvServerMessage,
  messageCase: string,
  value: unknown,
  sendFrame: (data: Uint8Array) => void,
): void {
  const response = create(KvClientMessageSchema, {
    id: (kvMsg as any).id,
    message: { case: messageCase as any, value: value as any },
  });
  const clientMsg = create(AgentClientMessageSchema, {
    message: { case: "kvClientMessage", value: response },
  });
  sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMsg)));
}

function handleKvMessage(
  kvMsg: KvServerMessage,
  blobStore: Map<string, Uint8Array>,
  sendFrame: (data: Uint8Array) => void,
): void {
  const kvCase = (kvMsg as any).message.case;
  if (kvCase === "getBlobArgs") {
    const blobId = (kvMsg as any).message.value.blobId;
    const blobIdKey = Buffer.from(blobId).toString("hex");
    const blobData = blobStore.get(blobIdKey);
    sendKvResponse(
      kvMsg,
      "getBlobResult",
      create(GetBlobResultSchema, blobData ? { blobData } : {}),
      sendFrame,
    );
  } else if (kvCase === "setBlobArgs") {
    const { blobId, blobData } = (kvMsg as any).message.value;
    blobStore.set(Buffer.from(blobId).toString("hex"), blobData);
    sendKvResponse(
      kvMsg,
      "setBlobResult",
      create(SetBlobResultSchema, {}),
      sendFrame,
    );
  }
}

function handleExecMessage(
  execMsg: ExecServerMessage,
  mcpTools: McpToolDefinition[],
  sendFrame: (data: Uint8Array) => void,
  onMcpExec: (exec: PendingExec) => void,
): void {
  const execCase = (execMsg as any).message.case;
  const REJECT_REASON =
    "Tool not available in this environment. Use the MCP tools provided instead.";

  if (execCase === "requestContextArgs") {
    const requestContext = create(RequestContextSchema, {
      rules: [],
      repositoryInfo: [],
      tools: mcpTools,
      gitRepos: [],
      projectLayouts: [],
      mcpInstructions: [],
      fileContents: {},
      customSubagents: [],
    });
    const result = create(RequestContextResultSchema, {
      result: {
        case: "success",
        value: create(RequestContextSuccessSchema, { requestContext }),
      },
    });
    sendExecResult(execMsg, "requestContextResult", result, sendFrame);
    return;
  }

  if (execCase === "mcpArgs") {
    const mcpArgs = (execMsg as any).message.value;
    const decoded = decodeMcpArgsMap(mcpArgs.args ?? {});
    onMcpExec({
      execId: (execMsg as any).execId,
      execMsgId: (execMsg as any).id,
      toolCallId: mcpArgs.toolCallId || crypto.randomUUID(),
      toolName: mcpArgs.toolName || mcpArgs.name,
      decodedArgs: JSON.stringify(decoded),
    });
    return;
  }

  // Reject native Cursor tools so model falls back to MCP tools
  if (execCase === "readArgs") {
    const args = (execMsg as any).message.value;
    sendExecResult(
      execMsg,
      "readResult",
      create(ReadResultSchema, {
        result: {
          case: "rejected",
          value: create(ReadRejectedSchema, {
            path: args.path,
            reason: REJECT_REASON,
          }),
        },
      }),
      sendFrame,
    );
    return;
  }
  if (execCase === "lsArgs") {
    const args = (execMsg as any).message.value;
    sendExecResult(
      execMsg,
      "lsResult",
      create(LsResultSchema, {
        result: {
          case: "rejected",
          value: create(LsRejectedSchema, {
            path: args.path,
            reason: REJECT_REASON,
          }),
        },
      }),
      sendFrame,
    );
    return;
  }
  if (execCase === "grepArgs") {
    sendExecResult(
      execMsg,
      "grepResult",
      create(GrepResultSchema, {
        result: {
          case: "error",
          value: create(GrepErrorSchema, { error: REJECT_REASON }),
        },
      }),
      sendFrame,
    );
    return;
  }
  if (execCase === "writeArgs") {
    const args = (execMsg as any).message.value;
    sendExecResult(
      execMsg,
      "writeResult",
      create(WriteResultSchema, {
        result: {
          case: "rejected",
          value: create(WriteRejectedSchema, {
            path: args.path,
            reason: REJECT_REASON,
          }),
        },
      }),
      sendFrame,
    );
    return;
  }
  if (execCase === "deleteArgs") {
    const args = (execMsg as any).message.value;
    sendExecResult(
      execMsg,
      "deleteResult",
      create(DeleteResultSchema, {
        result: {
          case: "rejected",
          value: create(DeleteRejectedSchema, {
            path: args.path,
            reason: REJECT_REASON,
          }),
        },
      }),
      sendFrame,
    );
    return;
  }
  if (execCase === "shellArgs") {
    const args = (execMsg as any).message.value;
    sendExecResult(
      execMsg,
      "shellResult",
      create(ShellResultSchema, {
        result: {
          case: "rejected",
          value: create(ShellRejectedSchema, {
            command: args.command ?? "",
            workingDirectory: args.workingDirectory ?? "",
            reason: REJECT_REASON,
            isReadonly: false,
          }),
        },
      }),
      sendFrame,
    );
    return;
  }
  if (execCase === "shellStreamArgs") {
    const args = (execMsg as any).message.value;
    sendExecResult(
      execMsg,
      "shellStream",
      create(ShellStreamSchema, {
        event: {
          case: "rejected",
          value: create(ShellRejectedSchema, {
            command: args.command ?? "",
            workingDirectory: args.workingDirectory ?? "",
            reason: REJECT_REASON,
            isReadonly: false,
          }),
        },
      }),
      sendFrame,
    );
    return;
  }
  if (execCase === "backgroundShellSpawnArgs") {
    const args = (execMsg as any).message.value;
    sendExecResult(
      execMsg,
      "backgroundShellSpawnResult",
      create(BackgroundShellSpawnResultSchema, {
        result: {
          case: "rejected",
          value: create(ShellRejectedSchema, {
            command: args.command ?? "",
            workingDirectory: args.workingDirectory ?? "",
            reason: REJECT_REASON,
            isReadonly: false,
          }),
        },
      }),
      sendFrame,
    );
    return;
  }
  if (execCase === "writeShellStdinArgs") {
    sendExecResult(
      execMsg,
      "writeShellStdinResult",
      create(WriteShellStdinResultSchema, {
        result: {
          case: "error",
          value: create(WriteShellStdinErrorSchema, { error: REJECT_REASON }),
        },
      }),
      sendFrame,
    );
    return;
  }
  if (execCase === "fetchArgs") {
    const args = (execMsg as any).message.value;
    sendExecResult(
      execMsg,
      "fetchResult",
      create(FetchResultSchema, {
        result: {
          case: "error",
          value: create(FetchErrorSchema, {
            url: args.url ?? "",
            error: REJECT_REASON,
          }),
        },
      }),
      sendFrame,
    );
    return;
  }
  if (execCase === "diagnosticsArgs") {
    sendExecResult(
      execMsg,
      "diagnosticsResult",
      create(DiagnosticsResultSchema, {}),
      sendFrame,
    );
    return;
  }

  // Unknown exec types
  const miscCaseMap: Record<string, string> = {
    listMcpResourcesExecArgs: "listMcpResourcesExecResult",
    readMcpResourceExecArgs: "readMcpResourceExecResult",
    recordScreenArgs: "recordScreenResult",
    computerUseArgs: "computerUseResult",
  };
  const resultCase = miscCaseMap[execCase as string];
  if (resultCase) {
    sendExecResult(execMsg, resultCase, create(McpResultSchema, {}), sendFrame);
    return;
  }

  // Catch-all: log and attempt a generic rejection so the bridge doesn't hang
  console.error(
    `[cursor-provider] UNHANDLED exec case: "${execCase}". Bridge may stall.`,
  );
  // Try to derive the result case name from the args case name
  const guessedResult = (execCase as string)?.replace(/Args$/, "Result");
  if (guessedResult && guessedResult !== execCase) {
    sendExecResult(
      execMsg,
      guessedResult,
      create(McpResultSchema, {}),
      sendFrame,
    );
  }
}

function sendExecResult(
  execMsg: ExecServerMessage,
  messageCase: string,
  value: unknown,
  sendFrame: (data: Uint8Array) => void,
): void {
  const execClientMessage = create(ExecClientMessageSchema, {
    id: (execMsg as any).id,
    execId: (execMsg as any).execId,
    message: { case: messageCase as any, value: value as any },
  });
  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "execClientMessage", value: execClientMessage },
  });
  sendFrame(
    frameConnectMessage(toBinary(AgentClientMessageSchema, clientMessage)),
  );
}

// ── Key derivation ──

export function derivePiSessionId(
  body: Pick<ChatCompletionRequest, "pi_session_id" | "user">,
): string | undefined {
  const raw = body.pi_session_id ?? body.user;
  if (typeof raw !== "string") return undefined;
  const trimmed = raw.trim();
  return trimmed ? trimmed : undefined;
}

export function deriveBridgeKeyFromSessionId(sessionId: string): string {
  return createHash("sha256")
    .update(`bridge:${sessionId}`)
    .digest("hex")
    .slice(0, 16);
}

export function deriveConversationKeyFromSessionId(sessionId: string): string {
  return createHash("sha256")
    .update(`conv:${sessionId}`)
    .digest("hex")
    .slice(0, 16);
}

export function deriveBridgeKey(
  messages: OpenAIMessage[],
  sessionId?: string,
): string {
  if (sessionId) return deriveBridgeKeyFromSessionId(sessionId);
  const firstUserMsg = messages.find((m) => m.role === "user");
  const firstUserText = firstUserMsg ? textContent(firstUserMsg.content) : "";
  return createHash("sha256")
    .update(`bridge:${firstUserText.slice(0, 200)}`)
    .digest("hex")
    .slice(0, 16);
}

export function deriveConversationKey(
  messages: OpenAIMessage[],
  sessionId?: string,
): string {
  if (sessionId) return deriveConversationKeyFromSessionId(sessionId);
  const firstUserMsg = messages.find((m) => m.role === "user");
  const firstUserText = firstUserMsg ? textContent(firstUserMsg.content) : "";
  return createHash("sha256")
    .update(`conv:${firstUserText.slice(0, 200)}`)
    .digest("hex")
    .slice(0, 16);
}

export function cleanupSessionState(sessionId?: string): void {
  if (!sessionId) return;
  const bridgeKey = deriveBridgeKeyFromSessionId(sessionId);
  const convKey = deriveConversationKeyFromSessionId(sessionId);
  const active = activeBridges.get(bridgeKey);
  debugLog("session.cleanup", {
    sessionId,
    bridgeKey,
    convKey,
    hasActiveBridge: !!active,
    hadConversation: conversationStates.has(convKey),
  });
  if (active) cleanupBridge(active.bridge, active.heartbeatTimer, bridgeKey);
  conversationStates.delete(convKey);
}

export function deterministicConversationId(convKey: string): string {
  const hex = createHash("sha256")
    .update(`cursor-conv-id:${convKey}`)
    .digest("hex")
    .slice(0, 32);
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    `4${hex.slice(13, 16)}`,
    `${(0x8 | (parseInt(hex[16]!, 16) & 0x3)).toString(16)}${hex.slice(17, 20)}`,
    hex.slice(20, 32),
  ].join("-");
}

// ── Thinking tag filter ──

const THINKING_TAG_NAMES = [
  "think",
  "thinking",
  "reasoning",
  "thought",
  "think_intent",
];
const MAX_THINKING_TAG_LEN = 16;

function createThinkingTagFilter() {
  let buffer = "";
  let inThinking = false;
  return {
    process(text: string) {
      const input = buffer + text;
      buffer = "";
      let content = "";
      let reasoning = "";
      let lastIdx = 0;
      const re = new RegExp(
        `<(/?)(?:${THINKING_TAG_NAMES.join("|")})\\s*>`,
        "gi",
      );
      let match: RegExpExecArray | null;
      while ((match = re.exec(input)) !== null) {
        const before = input.slice(lastIdx, match.index);
        if (inThinking) reasoning += before;
        else content += before;
        inThinking = match[1] !== "/";
        lastIdx = re.lastIndex;
      }
      const rest = input.slice(lastIdx);
      const ltPos = rest.lastIndexOf("<");
      if (
        ltPos >= 0 &&
        rest.length - ltPos < MAX_THINKING_TAG_LEN &&
        /^<\/?[a-z_]*$/i.test(rest.slice(ltPos))
      ) {
        buffer = rest.slice(ltPos);
        const before = rest.slice(0, ltPos);
        if (inThinking) reasoning += before;
        else content += before;
      } else {
        if (inThinking) reasoning += rest;
        else content += rest;
      }
      return { content, reasoning };
    },
    flush() {
      const b = buffer;
      buffer = "";
      if (!b) return { content: "", reasoning: "" };
      return inThinking
        ? { content: "", reasoning: b }
        : { content: b, reasoning: "" };
    },
  };
}

// ── Connect frame parser ──

function createConnectFrameParser(
  onMessage: (bytes: Uint8Array) => void,
  onEndStream: (bytes: Uint8Array) => void,
): (incoming: Buffer) => void {
  let pending = Buffer.alloc(0);
  return (incoming: Buffer) => {
    pending = Buffer.concat([pending, incoming]);
    while (pending.length >= 5) {
      const flags = pending[0]!;
      const msgLen = pending.readUInt32BE(1);
      if (pending.length < 5 + msgLen) break;
      const messageBytes = pending.subarray(5, 5 + msgLen);
      pending = pending.subarray(5 + msgLen);
      if (flags & CONNECT_END_STREAM_FLAG) onEndStream(messageBytes);
      else onMessage(messageBytes);
    }
  };
}

const CONTEXT_OVERFLOW_MSG =
  "context length exceeded \u2014 Cursor rejected the request as too large";

function isContextOverflowMessage(msg: string): boolean {
  return /context|token|length|overflow|too.?long|too.?large/i.test(msg);
}

function mapConnectErrorCode(code: string, message: string): string {
  switch (code) {
    case "unauthenticated":
      return "Cursor authentication expired \u2014 run /login cursor";
    case "resource_exhausted":
      return CONTEXT_OVERFLOW_MSG;
    case "deadline_exceeded":
      return "Cursor request timed out server-side \u2014 try again";
    case "unavailable":
      return "Cursor service unavailable \u2014 try again";
    case "internal":
      return "Cursor internal error \u2014 try again";
    case "invalid_argument":
      return isContextOverflowMessage(message) ? CONTEXT_OVERFLOW_MSG : message;
    default:
      return message;
  }
}

function parseConnectEndStream(data: Uint8Array): Error | null {
  if (data.length === 0) return null;
  try {
    const payload = JSON.parse(new TextDecoder().decode(data));
    const error = payload?.error;
    if (error) {
      const code = String(error.code ?? "unknown");
      const rawMessage = String(error.message ?? "Unknown error");
      return new Error(mapConnectErrorCode(code, rawMessage));
    }
    return null;
  } catch {
    return null;
  }
}

function makeHeartbeatBytes(): Uint8Array {
  const heartbeat = create(AgentClientMessageSchema, {
    message: {
      case: "clientHeartbeat",
      value: create(ClientHeartbeatSchema, {}),
    },
  });
  return frameConnectMessage(toBinary(AgentClientMessageSchema, heartbeat));
}

function computeUsage(state: StreamState) {
  const completion_tokens = state.outputTokens;
  const usedTokens = state.totalTokens || completion_tokens;

  // If Cursor enforces a tighter context window than we inferred, scale
  // total_tokens proportionally so pi's compaction threshold fires before
  // Cursor errors — rather than after.
  //
  // Example: Cursor caps Gemini at 200 k but we registered 1 M.
  //   usedTokens=197k → total_tokens = round(197k × 1M/200k) = 985k
  //   985k > 1M − 16k (reserveTokens) → pi triggers compaction ✓
  let total_tokens = usedTokens;
  const cursorWindow = state.cursorContextWindow;
  const piWindow = state.inferredContextWindow;
  if (cursorWindow > 0 && piWindow > cursorWindow) {
    total_tokens = Math.round(usedTokens * piWindow / cursorWindow);
  }

  const prompt_tokens = Math.max(0, total_tokens - completion_tokens);
  return { prompt_tokens, completion_tokens, total_tokens };
}

function respondWithPendingToolCalls(
  modelId: string,
  pendingExecs: PendingExec[],
  stream: boolean,
  res: ServerResponse,
): void {
  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);
  const toolCalls = pendingExecs.map((exec, index) => ({
    index,
    id: exec.toolCallId,
    type: "function" as const,
    function: { name: exec.toolName, arguments: exec.decodedArgs },
  }));

  if (stream) {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "close",
    });
    for (const toolCall of toolCalls) {
      res.write(
        `data: ${JSON.stringify({
          id: completionId,
          object: "chat.completion.chunk",
          created,
          model: modelId,
          choices: [
            {
              index: 0,
              delta: { tool_calls: [toolCall] },
              finish_reason: null,
            },
          ],
        })}\n\n`,
      );
    }
    res.write(
      `data: ${JSON.stringify({
        id: completionId,
        object: "chat.completion.chunk",
        created,
        model: modelId,
        choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }],
      })}\n\n`,
    );
    res.write("data: [DONE]\n\n");
    res.end();
    return;
  }

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(
    JSON.stringify({
      id: completionId,
      object: "chat.completion",
      created,
      model: modelId,
      choices: [
        {
          index: 0,
          message: { role: "assistant", content: null, tool_calls: toolCalls },
          finish_reason: "tool_calls",
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    }),
  );
}

// ── Streaming response ──

function startBridge(accessToken: string, requestBytes: Uint8Array) {
  const bridge = bridgeFactory({
    accessToken,
    rpcPath: "/agent.v1.AgentService/Run",
  });
  debugLog("bridge.start_run", { requestBytes });
  bridge.write(frameConnectMessage(requestBytes));
  const heartbeatTimer = setInterval(
    () => bridge.write(makeHeartbeatBytes()),
    5_000,
  );
  // Don't hold the event loop open between heartbeats.
  heartbeatTimer.unref();
  return { bridge, heartbeatTimer };
}

function handleStreamingResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  modelId: string,
  bridgeKey: string,
  convKey: string,
  completedTurns: ParsedTurn[],
  currentTurn: ParsedTurn,
  req: IncomingMessage,
  res: ServerResponse,
  requestId: string,
): void {
  debugLog("stream.start", { requestId, bridgeKey, convKey, modelId });
  const { bridge, heartbeatTimer } = startBridge(
    accessToken,
    payload.requestBytes,
  );
  writeSSEStream(
    bridge,
    heartbeatTimer,
    payload.blobStore,
    payload.mcpTools,
    modelId,
    bridgeKey,
    convKey,
    completedTurns,
    currentTurn,
    req,
    res,
    requestId,
  );
}

function sendCancelAction(bridge: BridgeHandle): void {
  debugLog("bridge.cancel_action", {});
  const action = create(ConversationActionSchema, {
    action: { case: "cancelAction", value: create(CancelActionSchema, {}) },
  });
  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "conversationAction", value: action },
  });
  bridge.write(
    frameConnectMessage(toBinary(AgentClientMessageSchema, clientMessage)),
  );
}

function cleanupBridge(
  bridge: BridgeHandle,
  heartbeatTimer: ReturnType<typeof setInterval>,
  bridgeKey: string,
): void {
  debugLog("bridge.cleanup", { bridgeKey, alive: bridge.alive });
  clearInterval(heartbeatTimer);
  if (bridge.alive) {
    sendCancelAction(bridge);
    bridge.end();
  }
  activeBridges.delete(bridgeKey);
}

function writeSSEStream(
  bridge: BridgeHandle,
  heartbeatTimer: ReturnType<typeof setInterval>,
  blobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
  modelId: string,
  bridgeKey: string,
  convKey: string,
  completedTurns: ParsedTurn[],
  currentTurn: ParsedTurn,
  req: IncomingMessage,
  res: ServerResponse,
  requestId?: string,
): void {
  debugLog("stream.writer_start", {
    requestId,
    bridgeKey,
    convKey,
    modelId,
    completedTurnCount: completedTurns.length,
    currentTurn,
  });
  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "close",
  });

  let closed = false;
  let keepAliveTimer: ReturnType<typeof setInterval> | undefined;
  const sendSSE = (data: object) => {
    if (closed) return;
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };
  const sendDone = () => {
    if (closed) return;
    res.write("data: [DONE]\n\n");
  };
  const closeResponse = () => {
    if (closed) return;
    closed = true;
    clearInterval(keepAliveTimer);
    res.end();
  };

  const makeChunk = (
    delta: Record<string, unknown>,
    finishReason: string | null = null,
  ) => ({
    id: completionId,
    object: "chat.completion.chunk",
    created,
    model: modelId,
    choices: [{ index: 0, delta, finish_reason: finishReason }],
  });

  const makeUsageChunk = () => {
    const { prompt_tokens, completion_tokens, total_tokens } =
      computeUsage(state);
    return {
      id: completionId,
      object: "chat.completion.chunk",
      created,
      model: modelId,
      choices: [],
      usage: { prompt_tokens, completion_tokens, total_tokens },
    };
  };

  const storedForState = conversationStates.get(convKey);
  const state: StreamState = {
    toolCallIndex: 0,
    pendingExecs: [],
    outputTokens: 0,
    totalTokens: 0,
    cursorContextWindow: storedForState?.effectiveContextWindow ?? 0,
    inferredContextWindow: inferContextWindow(modelId),
  };
  const tagFilter = createThinkingTagFilter();
  let mcpExecReceived = false;
  let cancelled = false;
  let latestCheckpoint: Uint8Array | null = null;

  // Keep the SSE connection alive during the silent blob-fetching phase so
  // pi's request timeout does not fire before the first token arrives.
  keepAliveTimer = setInterval(() => {
    if (!closed) res.write(": ping\n\n");
  }, 15_000);
  keepAliveTimer.unref();

  // Detect client disconnect (e.g. user pressed Escape in pi)
  const onClientClose = () => {
    if (cancelled || closed) return;
    debugLog("stream.client_close", { requestId, bridgeKey, convKey });
    cancelled = true;
    cleanupBridge(bridge, heartbeatTimer, bridgeKey);
    closeResponse();
  };
  req.on("close", onClientClose);
  res.on("close", onClientClose);

  const processChunk = createConnectFrameParser(
    (messageBytes) => {
      try {
        const serverMessage = fromBinary(
          AgentServerMessageSchema,
          messageBytes,
        );
        processServerMessage(
          serverMessage,
          blobStore,
          mcpTools,
          (data) => bridge.write(data),
          state,
          (text, isThinking) => {
            if (isThinking) {
              sendSSE(makeChunk({ reasoning_content: text }));
            } else {
              const { content, reasoning } = tagFilter.process(text);
              if (reasoning)
                sendSSE(makeChunk({ reasoning_content: reasoning }));
              if (content) {
                appendAssistantTextToTurn(currentTurn, content);
                sendSSE(makeChunk({ content }));
              }
            }
          },
          (exec) => {
            state.pendingExecs.push(exec);
            mcpExecReceived = true;

            const flushed = tagFilter.flush();
            if (flushed.reasoning)
              sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
            if (flushed.content) {
              appendAssistantTextToTurn(currentTurn, flushed.content);
              sendSSE(makeChunk({ content: flushed.content }));
            }

            currentTurn.steps.push({
              kind: "toolCall",
              toolCallId: exec.toolCallId,
              toolName: exec.toolName,
              arguments: parseToolCallArguments(exec.decodedArgs),
            });

            const toolCallIndex = state.toolCallIndex++;
            sendSSE(
              makeChunk({
                tool_calls: [
                  {
                    index: toolCallIndex,
                    id: exec.toolCallId,
                    type: "function",
                    function: {
                      name: exec.toolName,
                      arguments: exec.decodedArgs,
                    },
                  },
                ],
              }),
            );

            activeBridges.set(bridgeKey, {
              bridge,
              heartbeatTimer,
              blobStore,
              mcpTools,
              pendingExecs: state.pendingExecs,
              currentTurn,
            });
            debugLog("stream.tool_call_pause", {
              requestId,
              bridgeKey,
              exec,
              pendingExecs: state.pendingExecs,
              currentTurn,
            });

            sendSSE(makeChunk({}, "tool_calls"));
            sendDone();
            closeResponse();
          },
          (checkpointBytes) => {
            latestCheckpoint = checkpointBytes;
            const stored = conversationStates.get(convKey);
            if (stored) {
              stored.checkpoint = checkpointBytes;
              for (const [k, v] of blobStore) stored.blobStore.set(k, v);
              if (state.cursorContextWindow > 0) {
                stored.effectiveContextWindow = state.cursorContextWindow;
              }
            }
            debugLog("stream.checkpoint_buffered", {
              requestId,
              convKey,
              checkpointBytes,
            });
          },
        );
      } catch (err) {
        console.error(
          "[cursor-provider] Stream message processing error:",
          err instanceof Error ? err.message : err,
        );
      }
    },
    (endStreamBytes) => {
      const endError = parseConnectEndStream(endStreamBytes);
      // Always stop heartbeats and unref the bridge regardless of error/success
      // so the parent process is not kept alive waiting for HTTP/2 END_STREAM.
      clearInterval(heartbeatTimer);
      bridge.end();
      bridge.unref();
      if (endError) {
        console.error(
          `[cursor-provider] Cursor stream error (${modelId}):`,
          endError.message,
        );
        sendSSE(makeChunk({ content: endError.message }, "error"));
        sendSSE(makeUsageChunk());
        sendDone();
        closeResponse();
      } else {
        // Cursor's Connect-level response is complete. Send the SSE response
        // immediately without waiting for HTTP/2 END_STREAM, which Cursor can
        // delay by several seconds after the Connect end-stream frame.
        const flushed = tagFilter.flush();
        if (flushed.reasoning)
          sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
        if (flushed.content) {
          appendAssistantTextToTurn(currentTurn, flushed.content);
          sendSSE(makeChunk({ content: flushed.content }));
        }
        sendSSE(makeChunk({}, "stop"));
        sendSSE(makeUsageChunk());
        sendDone();
        closeResponse();
      }
    },
  );

  bridge.onData(processChunk);

  bridge.onClose((code) => {
    debugLog("stream.bridge_close", {
      requestId,
      bridgeKey,
      convKey,
      code,
      cancelled,
      mcpExecReceived,
      currentTurn,
      latestCheckpoint,
    });
    clearInterval(heartbeatTimer);
    req.removeListener("close", onClientClose);
    res.removeListener("close", onClientClose);
    const stored = conversationStates.get(convKey);
    if (stored) {
      for (const [k, v] of blobStore) stored.blobStore.set(k, v);
      if (latestCheckpoint) {
        stored.checkpoint = latestCheckpoint;
        debugLog("stream.checkpoint_committed", { requestId, convKey, stored });
      }
      if (state.cursorContextWindow > 0) {
        stored.effectiveContextWindow = state.cursorContextWindow;
      }
    }
    if (cancelled) return;
    if (!mcpExecReceived) {
      if (code !== 0) {
        // Bridge was killed before receiving any response (e.g. timeout waiting
        // for Cursor to process a large checkpoint during compaction). Treat as
        // an error so callers (like pi compaction) see a real failure instead of
        // an empty successful-looking response.
        console.error(
          `[cursor-provider] Bridge exited (code ${code}) before receiving response (${modelId})`,
        );
        const failureMsg = classifyBridgeFailure(code, bridge.getStderr());
        sendSSE(makeChunk({ content: failureMsg }, "error"));
        sendSSE(makeUsageChunk());
        sendDone();
        closeResponse();
      } else {
        const flushed = tagFilter.flush();
        if (flushed.reasoning)
          sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
        if (flushed.content) {
          appendAssistantTextToTurn(currentTurn, flushed.content);
          sendSSE(makeChunk({ content: flushed.content }));
        }
        sendSSE(makeChunk({}, "stop"));
        sendSSE(makeUsageChunk());
        sendDone();
        closeResponse();
      }
    } else if (code !== 0) {
      sendSSE(makeChunk({ content: "Bridge connection lost" }, "error"));
      sendSSE(makeUsageChunk());
      sendDone();
      closeResponse();
      activeBridges.delete(bridgeKey);
    } else {
      // Bridge closed cleanly after a tool call pause. The HTTP response was
      // already ended by the MCP exec handler; just ensure cleanup.
      activeBridges.delete(bridgeKey);
      closeResponse();
    }
  });
}

export function writeSSEStreamForTests(args: {
  bridge: BridgeHandle;
  heartbeatTimer: ReturnType<typeof setInterval>;
  blobStore?: Map<string, Uint8Array>;
  mcpTools?: McpToolDefinition[];
  modelId: string;
  bridgeKey: string;
  convKey: string;
  completedTurns: ParsedTurn[];
  currentTurn: ParsedTurn;
  req: IncomingMessage;
  res: ServerResponse;
  requestId?: string;
}): void {
  writeSSEStream(
    args.bridge,
    args.heartbeatTimer,
    args.blobStore ?? new Map(),
    args.mcpTools ?? [],
    args.modelId,
    args.bridgeKey,
    args.convKey,
    args.completedTurns,
    args.currentTurn,
    args.req,
    args.res,
    args.requestId,
  );
}

// ── Tool result resume ──

function handleToolResultResume(
  active: ActiveBridge,
  toolResults: ToolResultInfo[],
  modelId: string,
  bridgeKey: string,
  convKey: string,
  completedTurns: ParsedTurn[],
  req: IncomingMessage,
  res: ServerResponse,
  stream: boolean,
  requestId?: string,
): void {
  const {
    bridge,
    heartbeatTimer,
    blobStore,
    mcpTools,
    pendingExecs,
    currentTurn,
  } = active;
  debugLog("tool_resume.start", {
    requestId,
    bridgeKey,
    convKey,
    toolResults,
    pendingExecs,
    currentTurn,
  });

  for (const result of toolResults) {
    const turnToolStep = currentTurn.steps.find(
      (step): step is ParsedToolCallStep =>
        step.kind === "toolCall" && step.toolCallId === result.toolCallId,
    );
    if (turnToolStep) {
      turnToolStep.result = { content: result.content, isError: false };
    }
  }

  const turnResults = getTurnToolCallResults(currentTurn);
  const unresolvedExecs = pendingExecs.filter(
    (exec) => !turnResults.has(exec.toolCallId),
  );
  if (unresolvedExecs.length > 0) {
    activeBridges.set(bridgeKey, {
      bridge,
      heartbeatTimer,
      blobStore,
      mcpTools,
      pendingExecs,
      currentTurn,
    });
    debugLog("tool_resume.partial_wait", {
      requestId,
      bridgeKey,
      unresolvedExecs,
      currentTurn,
    });
    respondWithPendingToolCalls(modelId, unresolvedExecs, stream, res);
    return;
  }

  for (const exec of pendingExecs) {
    const result = turnResults.get(exec.toolCallId);
    if (!result) continue;
    const mcpResult = create(McpResultSchema, {
      result: {
        case: "success",
        value: create(McpSuccessSchema, {
          content: [
            create(McpToolResultContentItemSchema, {
              content: {
                case: "text",
                value: create(McpTextContentSchema, { text: result.content }),
              },
            }),
          ],
          isError: false,
        }),
      },
    });

    const execClientMessage = create(ExecClientMessageSchema, {
      id: exec.execMsgId,
      execId: exec.execId,
      message: { case: "mcpResult" as any, value: mcpResult as any },
    });
    const clientMessage = create(AgentClientMessageSchema, {
      message: { case: "execClientMessage", value: execClientMessage },
    });
    bridge.write(
      frameConnectMessage(toBinary(AgentClientMessageSchema, clientMessage)),
    );
    debugLog("tool_resume.sent_result", { requestId, exec, result });
  }

  // Tool results belong to the same user turn that initiated the tool calls.
  // parseMessages keeps tool continuations out of completed history, so completedTurns
  // already reflects the correct history covered before this in-flight turn.
  writeSSEStream(
    bridge,
    heartbeatTimer,
    blobStore,
    mcpTools,
    modelId,
    bridgeKey,
    convKey,
    completedTurns,
    currentTurn,
    req,
    res,
    requestId,
  );
}

// ── Non-streaming response ──

async function handleNonStreamingResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  modelId: string,
  convKey: string,
  completedTurns: ParsedTurn[],
  currentTurn: ParsedTurn,
  req: IncomingMessage,
  res: ServerResponse,
  requestId?: string,
): Promise<void> {
  debugLog("nonstream.start", {
    requestId,
    convKey,
    modelId,
    currentTurn,
    completedTurnCount: completedTurns.length,
  });
  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);

  const { bridge, heartbeatTimer } = startBridge(
    accessToken,
    payload.requestBytes,
  );
  let cancelled = false;

  const onClientClose = () => {
    if (cancelled) return;
    debugLog("nonstream.client_close", { requestId, convKey });
    cancelled = true;
    clearInterval(heartbeatTimer);
    if (bridge.alive) {
      sendCancelAction(bridge);
      bridge.end();
    }
  };
  req.on("close", onClientClose);
  res.on("close", onClientClose);
  const storedForNonStream = conversationStates.get(convKey);
  const state: StreamState = {
    toolCallIndex: 0,
    pendingExecs: [],
    outputTokens: 0,
    totalTokens: 0,
    cursorContextWindow: storedForNonStream?.effectiveContextWindow ?? 0,
    inferredContextWindow: inferContextWindow(modelId),
  };
  const tagFilter = createThinkingTagFilter();
  let fullText = "";
  let nonStreamError: Error | null = null;
  let latestCheckpoint: Uint8Array | null = null;

  return new Promise((resolve) => {
    bridge.onData(
      createConnectFrameParser(
        (messageBytes) => {
          try {
            const serverMessage = fromBinary(
              AgentServerMessageSchema,
              messageBytes,
            );
            processServerMessage(
              serverMessage,
              payload.blobStore,
              payload.mcpTools,
              (data) => bridge.write(data),
              state,
              (text, isThinking) => {
                if (isThinking) return;
                const { content } = tagFilter.process(text);
                fullText += content;
                appendAssistantTextToTurn(currentTurn, content);
              },
              (exec) => {
                // Non-streaming mode cannot pause for tool calls. Reject each
                // exec request immediately so Cursor can complete the turn.
                const execClientMessage = create(ExecClientMessageSchema, {
                  id: exec.execMsgId,
                  execId: exec.execId,
                  message: {
                    case: "mcpResult" as any,
                    value: create(McpResultSchema, {
                      result: {
                        case: "error",
                        value: create(McpErrorSchema, {
                          error:
                            "Tool calls not supported in non-streaming mode",
                        }),
                      },
                    }) as any,
                  },
                });
                const clientMessage = create(AgentClientMessageSchema, {
                  message: {
                    case: "execClientMessage",
                    value: execClientMessage,
                  },
                });
                bridge.write(
                  frameConnectMessage(
                    toBinary(AgentClientMessageSchema, clientMessage),
                  ),
                );
                debugLog("nonstream.exec_rejected", { requestId, exec });
              },
              (checkpointBytes) => {
                latestCheckpoint = checkpointBytes;
                const stored = conversationStates.get(convKey);
                if (stored) {
                  stored.checkpoint = checkpointBytes;
                  for (const [k, v] of payload.blobStore)
                    stored.blobStore.set(k, v);

                }
                debugLog("nonstream.checkpoint_buffered", {
                  requestId,
                  convKey,
                  checkpointBytes,
                });
              },
            );
          } catch (err) {
            console.error(
              "[cursor-provider] Non-stream message processing error:",
              err instanceof Error ? err.message : err,
            );
          }
        },
        (endStreamBytes) => {
          const endError = parseConnectEndStream(endStreamBytes);
          // Always unref regardless of error/success.
          clearInterval(heartbeatTimer);
          bridge.end();
          bridge.unref();
          if (endError) {
            console.error(
              `[cursor-provider] Cursor non-stream error (${modelId}):`,
              endError.message,
            );
            nonStreamError = endError;
          }
        },
      ),
    );

    bridge.onClose((code) => {
      debugLog("nonstream.bridge_close", {
        requestId,
        convKey,
        code,
        cancelled,
        nonStreamError: nonStreamError?.message,
        currentTurn,
        latestCheckpoint,
      });
      clearInterval(heartbeatTimer);
      req.removeListener("close", onClientClose);
      res.removeListener("close", onClientClose);
      const stored = conversationStates.get(convKey);
      if (stored) {
        for (const [k, v] of payload.blobStore) stored.blobStore.set(k, v);
        if (latestCheckpoint) {
          stored.checkpoint = latestCheckpoint;
          debugLog("nonstream.checkpoint_committed", {
            requestId,
            convKey,
            stored,
          });
        }
        if (state.cursorContextWindow > 0) {
          stored.effectiveContextWindow = state.cursorContextWindow;
        }
      }

      if (cancelled) {
        if (!res.headersSent) {
          res.writeHead(499, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({
              error: {
                message: "Client closed request",
                type: "aborted",
                code: "client_closed",
              },
            }),
          );
        }
        resolve();
        return;
      }

      if (nonStreamError) {
        res.writeHead(502, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            error: {
              message: nonStreamError.message,
              type: "upstream_error",
              code: "cursor_error",
            },
          }),
        );
        resolve();
        return;
      }

      if (code !== 0) {
        console.error(
          `[cursor-provider] Bridge exited (code ${code}) before non-stream response (${modelId})`,
        );
        res.writeHead(502, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            error: {
              message: classifyBridgeFailure(code, bridge.getStderr()),
              type: "upstream_error",
              code: "bridge_terminated",
            },
          }),
        );
        resolve();
        return;
      }

      const flushed = tagFilter.flush();
      fullText += flushed.content;
      appendAssistantTextToTurn(currentTurn, flushed.content);
      const usage = computeUsage(state);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          id: completionId,
          object: "chat.completion",
          created,
          model: modelId,
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: fullText },
              finish_reason: "stop",
            },
          ],
          usage,
        }),
      );
      resolve();
    });
  });
}
