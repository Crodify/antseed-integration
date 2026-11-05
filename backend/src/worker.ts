import { z } from "zod";
import { AntSeedFundingVaultClient } from "./antseed-funding-vault.js";
import { fetchCeloVaultEvents, fetchCeloVaultEventsForAccount, fetchCurrentGdPrice, fetchGoodIdRoot } from "./celo-events.js";
import { Env, RuntimeConfig, configFromEnv } from "./env.js";
import { KVCreditStore } from "./kv-credit-store.js";
import { fundCredit } from "./funding.js";
import { creditStreams } from "./stream-credits.js";
import { SUPERFLUID_CELO_SUBGRAPH_URL } from "./stream-periods.js";
import { GdCreditEntry } from "./types.js";
import { errorMessage, logError, logInfo, logWarn, redactAddress, redactHash } from "./logging.js";
import { getAnalyticsWindow, runAnalyticsAggregation, KVAnalyticsStore } from "./analytics.js";
import { cors, json, notifySlackOnFailure, notifySlackOnRequestError, parseJson, readRequestBody } from "./http.js";
import {
  ANALYTICS_CRON_BACKFILL_DAYS,
  ANALYTICS_REFRESH_COOLDOWN_MS,
  ANALYTICS_REFRESH_LAST_RUN_KEY,
  backFillAnalytics,
  readAnalyticsRefreshTimestamp
} from "./analytics-cron.js";


const CeloEventsRecordSchema = z
  .object({
    txHash: z
      .string()
      .regex(/^0x[0-9a-fA-F]{64}$/)
      .optional(),
    account: z
      .string()
      .regex(/^0x[0-9a-fA-F]{40}$/)
      .optional(),
    fromBlock: z
      .string()
      .regex(/^(latest|0x[0-9a-fA-F]+|\d+)$/)
      .optional(),
    toBlock: z
      .string()
      .regex(/^(latest|0x[0-9a-fA-F]+|\d+)$/)
      .optional()
  })
  .refine((value) => Boolean(value.txHash || (value.account && value.fromBlock)), {
    message: "provide txHash or account+fromBlock"
  });
const StreamUpdateSchema = z.object({
  account: z.string().min(1),
  rootAccount: z.string().min(1).optional(),
  flowRateWeiPerSecond: z.string().regex(/^\d+$/),
  monthlyGdAmountWei: z.string().regex(/^\d+$/).optional(),
  txHash: z.string().optional(),
  logIndex: z.number().int().nonnegative().optional()
});

const WithdrawPrincipalSchema = z.object({
  amount: z.string().regex(/^\d+$/),
  recipient: z.string().regex(/^0x[0-9a-fA-F]{40}$/),
  nonce: z.string().regex(/^\d+$/),
  signature: z.string().regex(/^0x[0-9a-fA-F]+$/)
});
const OperatorConsentSchema = z.object({
  nonce: z.string().regex(/^\d+$/),
  signature: z.string().regex(/^0x[0-9a-fA-F]+$/)
});
const CreditHistoryQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(100).default(20),
  offset: z.coerce.number().int().min(0).default(0),
  source: z.enum(["deposit", "streamUpdate", "streamRequest", "streamCron"]).optional(),
  fundingStatus: z.enum(["pending", "funded", "failed"]).optional(),
  from: z
    .string()
    .refine((value) => !Number.isNaN(Date.parse(value)), { message: "invalid ISO timestamp" })
    .optional(),
  to: z
    .string()
    .refine((value) => !Number.isNaN(Date.parse(value)), { message: "invalid ISO timestamp" })
    .optional()
});
const ChannelOpSchema = z.object({
  nonce: z.string().regex(/^\d+$/),
  signature: z.string().regex(/^0x[0-9a-fA-F]+$/)
});
const AnalyticsQuerySchema = z.object({
  days: z.coerce.number().int().min(1).max(365).default(30)
});


export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const startedAt = Date.now();
    const url = new URL(request.url);
    const requestClone = request.clone();
    logInfo("request.start", {
      method: request.method,
      path: url.pathname
    });
    try {
      const response = await route(request, env, ctx);
      logInfo("request.end", {
        method: request.method,
        path: url.pathname,
        status: response.status,
        elapsedMs: Date.now() - startedAt
      });
      return response;
    } catch (err) {
      const message = errorMessage(err);
      logError("request.error", {
        method: request.method,
        path: url.pathname,
        elapsedMs: Date.now() - startedAt,
        message
      });
      ctx.waitUntil(
        notifySlackOnRequestError(env, {
          method: request.method,
          path: `${url.pathname}${url.search}`,
          body: await readRequestBody(requestClone),
          message,
          error: err
        })
      );
      return json({ error: message }, 500);
    }
  },
  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const startedAt = Date.now();
    try {
      const cfg = configFromEnv(env);
      const store = new KVCreditStore(env.ANTSEED_KV);
      const antseedFundingVault = new AntSeedFundingVaultClient(cfg);
      logInfo("cron.start", { bridgeEnabled: antseedFundingVault.enabled });
      const summary = await creditStreams(cfg, env.ANTSEED_KV, store, antseedFundingVault, ctx);
      logInfo("cron.summary", { ...summary, elapsedMs: Date.now() - startedAt });
    } catch (error) {
      logError("cron.streams.failed", { message: errorMessage(error) });
      await notifySlackOnFailure(env, { operation: "cron.streams", error });
    }

    try {
      const summaries = await backFillAnalytics(env, ANALYTICS_CRON_BACKFILL_DAYS);
      logInfo("cron.analytics.summary", {
        runs: summaries.length,
        summaries: JSON.stringify(summaries)
      });
    } catch (error) {
      logError("cron.analytics.failed", {
        message: errorMessage(error)
      });
      ctx.waitUntil(
        notifySlackOnFailure(env, {
          operation: "cron.analytics",
          error
        })
      );
    }
  }
};

async function route(request: Request, env: Env, _ctx: ExecutionContext): Promise<Response> {
  const url = new URL(request.url);
  const cfg = configFromEnv(env);
  const requestClone = request.clone();
  let requestBody: Promise<string | undefined> | undefined;
  cfg.reportFailure = async (details) => notifySlackOnFailure(env, {
    ...details,
    request: { method: request.method, path: `${url.pathname}${url.search}`, body: await (requestBody ??= readRequestBody(requestClone)) }
  });
  const store = new KVCreditStore(env.ANTSEED_KV);
  const antseedFundingVault = new AntSeedFundingVaultClient(cfg);

  if (request.method === "OPTIONS") return cors(new Response(null, { status: 204 }));

  if (request.method === "GET" && url.pathname === "/health") {
    return json({
      ok: true,
      service: "gooddollar-antseed-integration",
      runtime: "cloudflare-worker",
      kvEnabled: true,
      bridgeEnabled: antseedFundingVault.enabled
    });
  }

  if (request.method === "GET" && url.pathname === "/config/status") {
    return json({
      ok: true,
      service: "gooddollar-antseed-integration",
      runtime: "cloudflare-worker",
      bridge: {
        celoVaultEvents: true,
        baseBuyerOperatorEnabled: antseedFundingVault.enabled,
        mode: "celo-vault-to-base-buyer-operator"
      },
      celo: {
        rpcConfigured: Boolean(env.CELO_RPC_URL),
        vaultConfigured: Boolean(env.CELO_VAULT_ADDRESS),
        goodIdConfigured: Boolean(env.CELO_GOODID_ADDRESS),
        staticOracleConfigured: Boolean(env.CELO_STATIC_ORACLE_ADDRESS)
      },
      kvEnabled: true
    });
  }

  if (request.method === "GET" && url.pathname === "/config/values") {
    return json({
      ok: true,
      service: "gooddollar-antseed-integration",
      runtime: "cloudflare-worker",
      config: {
        GD_CUSD_PRICE: cfg.GD_CUSD_PRICE,
        MAX_BONUS_CAP_USD: cfg.MAX_BONUS_CAP_USD.toString(),
        REGULAR_BONUS_BPS: cfg.REGULAR_BONUS_BPS.toString(),
        STREAMING_BONUS_BPS: cfg.STREAMING_BONUS_BPS.toString(),
        MIN_STREAM_BONUS_WEI: cfg.MIN_STREAM_BONUS_WEI.toString(),
        CELO_VAULT_ADDRESS: cfg.CELO_VAULT_ADDRESS,
        CELO_GOODID_ADDRESS: cfg.CELO_GOODID_ADDRESS,
        CELO_STATIC_ORACLE_ADDRESS: cfg.CELO_STATIC_ORACLE_ADDRESS,
        CELO_CUSD_ADDRESS: cfg.CELO_CUSD_ADDRESS,
        CELO_GD_SUPERTOKEN_ADDRESS: cfg.CELO_GD_SUPERTOKEN_ADDRESS,
        SUPERFLUID_SUBGRAPH_URL: cfg.SUPERFLUID_SUBGRAPH_URL ?? SUPERFLUID_CELO_SUBGRAPH_URL
      }
    });
  }

  if (request.method === "GET" && url.pathname === "/v1/analytics") {
    const parsed = AnalyticsQuerySchema.safeParse({
      days: url.searchParams.get("days") ?? undefined
    });
    if (!parsed.success) return json({ error: parsed.error.flatten() }, 400);
    const analytics = await getAnalyticsWindow(env, parsed.data.days);
    return json(analytics);
  }

  if (request.method === "POST" && url.pathname === "/v1/analytics/refresh") {
    const lastRefreshAt = await readAnalyticsRefreshTimestamp(env.ANTSEED_KV);
    const now = Date.now();
    if (lastRefreshAt !== undefined && now - lastRefreshAt < ANALYTICS_REFRESH_COOLDOWN_MS) {
      const retryAfterSeconds = Math.ceil((ANALYTICS_REFRESH_COOLDOWN_MS - (now - lastRefreshAt)) / 1000);
      return json(
        {
          error: "analytics refresh is rate-limited",
          retryAfterSeconds
        },
        429
      );
    }
    await env.ANTSEED_KV.put(ANALYTICS_REFRESH_LAST_RUN_KEY, String(now));
    const summaries = await backFillAnalytics(env);
    return json(summaries);
  }

  const accountMatch = url.pathname.match(/^\/v1\/accounts\/([^/]+)\/profile$/);
  if (request.method === "GET" && accountMatch) {
    const account = decodeURIComponent(accountMatch[1]);
    const profile = await store.getUser(account);
    return json({ account: profile.account, profile });
  }

  const creditHistoryMatch = url.pathname.match(/^\/v1\/accounts\/([^/]+)\/credit-history$/);
  if (request.method === "GET" && creditHistoryMatch) {
    const account = decodeURIComponent(creditHistoryMatch[1]);
    const parsed = CreditHistoryQuerySchema.safeParse({
      limit: url.searchParams.get("limit") ?? undefined,
      offset: url.searchParams.get("offset") ?? undefined,
      source: url.searchParams.get("source") ?? undefined,
      fundingStatus: url.searchParams.get("fundingStatus") ?? undefined,
      from: url.searchParams.get("from") ?? undefined,
      to: url.searchParams.get("to") ?? undefined
    });
    if (!parsed.success) return json({ error: parsed.error.flatten() }, 400);
    const history = await store.getGdCreditHistory(account, parsed.data);
    return json({
      account: account.toLowerCase(),
      ...history
    });
  }

  if (request.method === "POST" && url.pathname === "/v1/celo/events/record") {
    const body = await parseJson(request);
    const parsed = CeloEventsRecordSchema.safeParse(body);
    if (!parsed.success) return json({ error: parsed.error.flatten() }, 400);
    logInfo("celo.events.record.start", {
      mode: parsed.data.txHash ? "txHash" : "accountRange",
      txHash: redactHash(parsed.data.txHash),
      account: redactAddress(parsed.data.account),
      fromBlock: parsed.data.fromBlock,
      toBlock: parsed.data.toBlock ?? "latest"
    });
    const events = parsed.data.txHash
      ? await fetchCeloVaultEvents(parsed.data.txHash, cfg)
      : await fetchCeloVaultEventsForAccount(parsed.data.account!, cfg, parsed.data.fromBlock!, parsed.data.toBlock ?? "latest");
    logInfo("celo.events.record.fetched", {
      count: events.length,
      txHash: redactHash(parsed.data.txHash),
      account: redactAddress(parsed.data.account)
    });
    const recorded = [];
    const gdPrice = await fetchCurrentGdPrice(cfg);
    for (const event of events) {
      const rootAccount = await fetchGoodIdRoot(event.account, cfg);

      if (event.kind === "deposit") {
        if (event.gdAmountWei === 0n) {
          logInfo("celo.events.record.deposit.skipped.zero-amount", {
            txHash: redactHash(event.txHash),
            logIndex: event.logIndex,
            account: redactAddress(event.account)
          });
          continue;
        }
        const buyerForOperatorCheck = event.buyer || event.account;
        const hasOperatorConsent = antseedFundingVault.enabled ? (await antseedFundingVault.isBuyerOperator(buyerForOperatorCheck)).isOperator : true;
        const isValidForBonus = !!rootAccount && hasOperatorConsent;
        const depositId = `${event.txHash}:${event.logIndex}`;
        const entry = await store.recordGdCredit({
          id: depositId,
          account: event.account,
          rootAccount,
          source: "deposit",
          gdAmountWei: event.gdAmountWei,
          txHash: event.txHash,
          logIndex: event.logIndex,
          isVerified: isValidForBonus, // if root acccount was found it is whitelisted & operator set
          gdPrice,
          maxBonusCapUsd: cfg.MAX_BONUS_CAP_USD,
          regularBonusBps: cfg.REGULAR_BONUS_BPS,
          streamingBonusBps: cfg.STREAMING_BONUS_BPS,
          buyerAddress: event.buyer
        });
        logInfo("celo.events.record.entry", {
          kind: event.kind,
          entryId: entry.id,
          account: redactAddress(entry.account),
          rootAccount: redactAddress(entry.rootAccount),
          buyer: redactAddress(entry.buyerAddress)
        });
        const res = await fundCredit(entry, store, antseedFundingVault, cfg.reportFailure);
        recorded.push(res);
      } else {
        // Streams are credited by the scheduled run alone, from the subgraph's stream periods.
        // Crediting here too let two paths price the same window from the same stale clock under
        // different deposit ids, so both were paid. The event is still recorded, as a zero-amount
        // acknowledgement: clients wait for an entry carrying their txHash to settle. It has no
        // `streamCreditedThrough`, so it leaves the stream clock alone, and no flow rate -- the
        // scheduled run syncs that from the subgraph.
        const entry = await store.recordGdCredit({
          id: `${event.txHash}:${event.logIndex}`,
          account: event.account,
          rootAccount,
          source: "streamUpdate",
          gdAmountWei: 0n,
          txHash: event.txHash,
          logIndex: event.logIndex,
          isVerified: false,
          gdPrice,
          maxBonusCapUsd: cfg.MAX_BONUS_CAP_USD,
          regularBonusBps: cfg.REGULAR_BONUS_BPS,
          streamingBonusBps: cfg.STREAMING_BONUS_BPS,
          buyerAddress: event.buyer
        });
        logInfo("celo.events.record.stream.acknowledged", {
          entryId: entry.id,
          account: redactAddress(entry.account),
          flowRateWeiPerSecond: event.flowRateWeiPerSecond.toString(),
          reportedTotalFlowWei: event.totalFlowWei.toString()
        });
        if (BigInt(entry.totalCreditUsd) === 0n) {
          recorded.push(await fundCredit(entry, store, antseedFundingVault, cfg.reportFailure));
        } else {
          logWarn("celo.events.record.stream.legacy-entry-skipped", {
            entryId: entry.id,
            account: redactAddress(entry.account),
            fundingStatus: entry.fundingStatus
          });
          recorded.push(entry);
        }
      }
    }
    logInfo("celo.events.record.end", {
      count: recorded.length,
      txHash: redactHash(parsed.data.txHash),
      account: redactAddress(parsed.data.account)
    });
    return json({
      txHash: parsed.data.txHash,
      account: parsed.data.account?.toLowerCase(),
      fromBlock: parsed.data.fromBlock,
      toBlock: parsed.data.toBlock ?? "latest",
      events: recorded
    });
  }

  const outstandingMatch = url.pathname.match(/^\/v1\/accounts\/([^/]+)\/outstanding$/);
  if (request.method === "GET" && outstandingMatch) {
    const account = decodeURIComponent(outstandingMatch[1]);
    const [profile, gdCredits] = await Promise.all([store.getUser(account), store.getGdCredits(account)]);
    const outstandingFundingCredits = gdCredits.filter((entry) => entry.fundingStatus === "failed" || entry.fundingStatus === "pending");
    return json({
      account: profile.account,
      outstandingFundingUsd: profile.totalOutstandingFundingUsd,
      failedFundingCredits: outstandingFundingCredits
    });
  }

  const operatorConsentMatch = url.pathname.match(/^\/v1\/accounts\/([^/]+)\/operator-consent$/);
  if (request.method === "POST" && operatorConsentMatch) {
    const buyer = decodeURIComponent(operatorConsentMatch[1]).toLowerCase();
    const body = await parseJson(request);
    const parsed = OperatorConsentSchema.safeParse(body);
    if (!parsed.success) return json({ error: parsed.error.flatten() }, 400);
    logInfo("operator.consent.request", {
      buyer: redactAddress(buyer),
      nonce: parsed.data.nonce
    });
    const bridge = await antseedFundingVault.acceptBuyerOperator(buyer, BigInt(parsed.data.nonce), parsed.data.signature);
    logInfo("operator.consent.result", {
      buyer: redactAddress(buyer),
      enabled: bridge.enabled,
      txHash: redactHash(bridge.txHash)
    });
    return json({ buyer, bridge });
  }

  const operatorRevokeMatch = url.pathname.match(/^\/v1\/accounts\/([^/]+)\/operator-revoke$/);
  if (request.method === "POST" && operatorRevokeMatch) {
    const buyer = decodeURIComponent(operatorRevokeMatch[1]).toLowerCase();
    const body = await parseJson(request);
    const parsed = OperatorConsentSchema.safeParse(body);
    if (!parsed.success) return json({ error: parsed.error.flatten() }, 400);
    logInfo("operator.revoke.request", {
      buyer: redactAddress(buyer),
      nonce: parsed.data.nonce
    });
    const bridge = await antseedFundingVault.revokeBuyerOperator(buyer, BigInt(parsed.data.nonce), parsed.data.signature);
    logInfo("operator.revoke.result", {
      buyer: redactAddress(buyer),
      enabled: bridge.enabled,
      txHash: redactHash(bridge.txHash)
    });
    return json({ buyer, bridge });
  }

  const withdrawMatch = url.pathname.match(/^\/v1\/accounts\/([^/]+)\/withdraw$/);
  if (request.method === "POST" && withdrawMatch) {
    const account = decodeURIComponent(withdrawMatch[1]).toLowerCase();
    const body = await parseJson(request);
    const parsed = WithdrawPrincipalSchema.safeParse(body);
    if (!parsed.success) return json({ error: parsed.error.flatten() }, 400);
    logInfo("withdraw.request", {
      account: redactAddress(account),
      amountUsd: parsed.data.amount,
      recipient: redactAddress(parsed.data.recipient)
    });
    const bridge = await antseedFundingVault.withdrawPrincipalForBuyer(
      account,
      BigInt(parsed.data.amount),
      parsed.data.recipient,
      BigInt(parsed.data.nonce),
      parsed.data.signature
    );
    logInfo("withdraw.result", {
      account: redactAddress(account),
      enabled: bridge.enabled,
      txHash: redactHash(bridge.txHash)
    });
    return json({ account, amountUsd: parsed.data.amount, bridge });
  }

  const channelOpMatch = url.pathname.match(/^\/v1\/channels\/(0x[0-9a-fA-F]{64})\/(close|withdraw)$/);
  if (request.method === "POST" && channelOpMatch) {
    const channelId = channelOpMatch[1];
    const action = channelOpMatch[2];
    const body = await parseJson(request);
    const parsed = ChannelOpSchema.safeParse(body);
    if (!parsed.success) return json({ error: parsed.error.flatten() }, 400);
    const { nonce, signature } = parsed.data;
    logInfo("channel.request", {
      action,
      channelId: redactHash(channelId),
      hasNonce: true,
      hasSignature: true
    });
    const parsedNonce = BigInt(nonce);
    const bridge =
      action === "close"
        ? await antseedFundingVault.requestClose(channelId, parsedNonce, signature)
        : await antseedFundingVault.withdrawFromChannel(channelId, parsedNonce, signature);
    logInfo("channel.result", {
      action,
      channelId: redactHash(channelId),
      enabled: bridge.enabled,
      txHash: redactHash(bridge.txHash)
    });
    return json({ channelId, action, bridge });
  }

  return json({ error: "not found" }, 404);
}

