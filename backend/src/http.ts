import type { Env } from "./env.js";
import { errorMessage, logWarn } from "./logging.js";

export type FailureDetails = {
  operation: string;
  chain?: "celo" | "base";
  error?: unknown;
  recovered?: boolean;
  entryId?: string;
  buyer?: string;
  txHash?: string;
  request?: { method: string; path: string; body: string | undefined };
};

export type FailureReporter = (details: FailureDetails) => Promise<void>;

const reportedErrors = new WeakSet<object>();

/** Reports caught failures once per error object, including request context when available. */
export async function notifySlackOnFailure(env: Pick<Env, "SLACK_WEBHOOK_URL">, details: FailureDetails): Promise<void> {
  if (!env.SLACK_WEBHOOK_URL) return;
  if (typeof details.error === "object" && details.error !== null) {
    if (reportedErrors.has(details.error)) return;
    reportedErrors.add(details.error);
  }
  const error = details.error as { code?: unknown; name?: unknown; receipt?: { hash?: unknown; status?: unknown } } | undefined;
  const allowedCodes = new Set(["CALL_EXCEPTION", "NETWORK_ERROR", "SERVER_ERROR", "TIMEOUT", "BAD_DATA", "INSUFFICIENT_FUNDS", "NONCE_EXPIRED", "TRANSACTION_REPLACED"]);
  const category = error?.name === "BuyerDepositBelowMinimum" ? "BUYER_DEPOSIT_BELOW_MINIMUM"
    : error?.name === "CeloTransactionReverted" ? "TRANSACTION_REVERTED"
    : error?.name === "CeloReceiptPending" ? "RECEIPT_PENDING"
    : typeof error?.code === "string" && allowedCodes.has(error.code) ? error.code : "OPERATION_FAILED";
  const txHash = details.txHash ?? error?.receipt?.hash;
  const text = [
    "Worker operation failure",
    `operation: ${details.operation.replace(/[^a-zA-Z0-9_./:-]/g, "_").slice(0, 160)}`,
    `chain: ${details.chain ?? "unspecified"}`,
    `outcome: ${details.recovered ? "recovered_with_fallback" : "failed_or_unconfirmed"}`,
    `category: ${category}`,
    ...(details.request ? [
      `method: ${details.request.method}`,
      `path: ${details.request.path}`,
      `body: ${details.request.body === undefined ? "[empty]" : truncate(details.request.body, 3000)}`
    ] : []),
    ...(details.entryId && /^(?:0x[a-fA-F0-9]{64}:\d+|stream:0x[a-fA-F0-9]{40}:\d+)$/.test(details.entryId) ? [`entryId: ${details.entryId}`] : []),
    ...(details.buyer && /^0x[a-fA-F0-9]{40}$/.test(details.buyer) ? [`buyer: ${details.buyer}`] : []),
    ...(typeof txHash === "string" && /^0x[a-fA-F0-9]{64}$/.test(txHash) ? [`txHash: ${txHash}`] : [])
  ].join("\n");
  try {
    const response = await fetch(env.SLACK_WEBHOOK_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text }),
      signal: AbortSignal.timeout(5000)
    });
    if (!response.ok) logWarn("slack.notify.failed", { status: response.status });
  } catch {
    logWarn("slack.notify.error", { operation: "webhook_delivery" });
  }
}

export async function parseJson(request: Request): Promise<unknown> {
  const contentType = request.headers.get("content-type") ?? "";
  if (!contentType.includes("application/json")) throw new Error("content-type must be application/json");
  return request.json();
}

export async function notifySlackOnRequestError(env: Env, details: { method: string; path: string; body: string | undefined; message: string; error?: unknown }): Promise<void> {
  await notifySlackOnFailure(env, { operation: `${details.method}:${details.path.split("?")[0]}`, request: details, error: details.error });
}

export async function readRequestBody(request: Pick<Request, "text">): Promise<string | undefined> {
  try {
    const body = await request.text();
    if (!body) return undefined;
    return truncate(body, 3000);
  } catch (error) {
    return `[unavailable: ${errorMessage(error)}]`;
  }
}

function truncate(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength)}... [truncated]`;
}

export function json(body: unknown, status = 200): Response {
  return cors(Response.json(body, { status }));
}

export function cors(response: Response): Response {
  const headers = new Headers(response.headers);
  headers.set("access-control-allow-origin", "*");
  headers.set("access-control-allow-methods", "GET,POST,DELETE,OPTIONS");
  headers.set("access-control-allow-headers", "content-type,authorization,x-api-key,x-gooddollar-account");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers
  });
}

/**
 * Credit every account streaming into the vault for the G$ its stream periods carried since its last
 * credit, and sync each account's recorded flow rate. The only path that credits streams.
 *
 * Each account is owed `streamOwedWei(periods, lastStreamCreditAt, asOf)`: every constant-rate period
 * times its overlap with the uncredited window. That prices rate changes, closes and re-opens exactly,
 * with no remembered rate or stream "last update" to go stale. `asOf` is the subgraph's indexed head,
 * not wall-clock time, so the window never runs past what the subgraph has seen.
 *
 * The deposit id is the window's start (`stream:<account>:<creditedThrough>`). Two runs that overlap
 * read the same clock, derive the same id and are funded once: `depositForWithId` dedupes on it.
 *
 * Live streams are gated by a 24h cooldown -- aged from the window's own start, so a first-time or
 * reopened stream is held like any other -- and by `MIN_STREAM_BONUS_WEI`. An account whose streams have
 * all closed is settled in full regardless, so its final tail is paid out and the closed-periods
 * cursor can move past it.
 */
