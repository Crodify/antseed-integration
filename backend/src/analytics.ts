import { Interface } from "ethers";
import { createPublicClient, fallback, http, toHex, type Address, type Chain, type Hex, type Log, type PublicClient } from "viem";
import { base, celo } from "viem/chains";
import { decodeBuyerFromUserData } from "./celo-events.js";
import { Env } from "./env.js";
import { errorMessage, logError, logInfo, logWarn, redactAddress } from "./logging.js";
import { notifySlackOnFailure, type FailureReporter } from "./http.js";

type KV = Pick<KVNamespace, "get" | "put">;

type StreamSnapshot = {
  sender: string;
  buyerAddress?: string;
  flowRateWeiPerSecond: bigint;
  totalStreamedWei: bigint;
};

type AnalyticsState = {
  finalizedThroughDate?: string;
  updatedAt: string;
};

export type AnalyticsDailyRecord = {
  date: string;
  gdOneTimeDepositsWei: string;
  gdStreamedWei: string;
  gdTotalFlowRateWeiPerSecond: string;
  aiCreditsUsedWei: string;
  uniqueGdBuyers: number;
  uniqueCreditUsers: number;
  updatedAt: string;
  missing?: boolean;
};

export type AnalyticsGlobalTotals = {
  gdOneTimeDepositsWei: string;
  gdStreamedWei: string;
  aiCreditsUsedWei: string;
  gdTotalFlowRateWeiPerSecond: string;
  updatedAt: string;
};

export type AnalyticsLastRun = {
  currentDate: string;
  finalizedThroughDate?: string;
  updatedAt: string;
};

export type AnalyticsResponse = {
  days: number;
  daily: AnalyticsDailyRecord[];
  global: AnalyticsGlobalTotals;
  lastRun: AnalyticsLastRun;
};

export type AnalyticsRunSummary = {
  ok: true;
  currentDate: string;
  finalizedDates: string[];
  celo: { fromTimestamp: number; toTimestamp: number; scanned: number; matched: number };
  base: { fromTimestamp: number; toTimestamp: number; scanned: number; matched: number };
  streams: { senders: number; totalFlowRateWeiPerSecond: string };
};

type DailyAggregate = {
  gdOneTimeDepositsWei: bigint;
  gdStreamedWei: bigint;
  gdTotalFlowRateWeiPerSecond: bigint;
  aiCreditsUsedWei: bigint;
  gdBuyers: Set<string>;
  creditUsers: Set<string>;
};

type AnalyticsConfig = {
  reportFailure: FailureReporter;
  celoBlockscoutUrl: string;
  baseBlockscoutUrl: string;
  celoVaultAddress?: string;
  baseChannelsAddress: string;
  celoSuperTokenAddress?: string;
  celoStreamReceiverAddress?: string;
  superfluidSubgraphUrl: string;
};

const CELO_VAULT_EVENTS = new Interface(["event GdDeposited(address indexed account,address indexed buyer,uint256 gdAmount,bytes data)"]);

const BASE_CHANNEL_EVENTS = new Interface([
  "event Reserved(bytes32 indexed channelId,address indexed buyer,address indexed seller,uint128 maxAmount)",
  "event ChannelSettled(bytes32 indexed channelId,address indexed buyer,address indexed seller,uint128 cumulativeAmount,uint128 delta,uint128 totalSettled,uint256 platformFee,bytes metadata)",
  "event ChannelClosed(bytes32 indexed channelId,address indexed buyer,address indexed seller,uint128 settledAmount,uint128 refund)",
  "event ChannelTopUp(bytes32 indexed channelId,address indexed buyer,address indexed seller,uint128 additionalAmount,uint128 newDeposit)",
  "event ChannelWithdrawn(bytes32 indexed channelId,address indexed buyer,address indexed seller,uint128 refund)",
  "event CloseRequested(bytes32 indexed channelId,address indexed buyer,address indexed seller,uint256 gracePeriodEnd)"
]);

const DAILY_PREFIX = "analytics:daily:";
const GLOBAL_KEY = "analytics:global";
const STATE_KEY = "analytics:state";
const BUYER_REGISTRY_KEY = "analytics:buyers:registry";
const GD_BUYERS_PREFIX = "analytics:buyers:gd:";
const CREDIT_USERS_PREFIX = "analytics:buyers:credits:";
const CELO_CHAIN_ID = 42220;
const BASE_CHAIN_ID = 8453;
const RPCS_CACHE_PREFIX = "analytics:rpcs:";
const RPCS_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const CHAINLIST_RPCS_URL = "https://chainlist.org/rpcs.json";
const LOG_BATCH_BLOCKS = 5000n;
const LOG_BATCH_WORKERS = 5;
const LOG_BATCH_DELAY_MS = 500;
const ANALYTICS_KV_READ_WORKERS = 20;
const BASE_BLOCKS_PER_SECOND = 0.5;
const TIMESTAMP_SEARCH_WINDOW_SECONDS = 6 * 60 * 60;
const CELO_FALLBACK_RPCS = ["https://forno.celo.org", "https://rpc.ankr.com/celo"];
const BASE_FALLBACK_RPCS = ["https://mainnet.base.org", "https://rpc.ankr.com/base"];

type RpcCacheEntry = {
  rpcs: string[];
  fetchedAt: string;
};

const ZERO_DAILY = {
  gdOneTimeDepositsWei: "0",
  gdStreamedWei: "0",
  gdTotalFlowRateWeiPerSecond: "0",
  aiCreditsUsedWei: "0",
  uniqueGdBuyers: 0,
  uniqueCreditUsers: 0
};

export async function runAnalyticsAggregation(env: Env, runAt = new Date()): Promise<AnalyticsRunSummary> {
  const cfg = analyticsConfigFromEnv(env);
  const store = new KVAnalyticsStore(env.ANTSEED_KV);
  const celoClient = await createChainClient(env.ANTSEED_KV, CELO_CHAIN_ID, celo, CELO_FALLBACK_RPCS, cfg.reportFailure);
  const baseClient = await createChainClient(env.ANTSEED_KV, BASE_CHAIN_ID, base, BASE_FALLBACK_RPCS, cfg.reportFailure);
  const runDay = dayFromDate(runAt);
  const state = (await store.getState()) ?? {
    updatedAt: runAt.toISOString()
  };
  const currentDate = resolveRunDate(state, runAt, runDay);
  const windowEnd = currentDate === runDay ? runAt : new Date(`${currentDate}T23:59:59.999Z`);

  const isCurrentDay = currentDate === runDay;
  const dayWindow = getUtcDayWindow(windowEnd, isCurrentDay ? "until-now" : "full-day");
  const aggregate = createDailyAggregate();
  const knownBuyers = await store.getBuyerRegistry();
  const discoveredBuyers = new Set<string>();

  console.log("Collecting celo metrics...");
  const celoMetrics = await collectCeloDayMetrics(cfg, celoClient, dayWindow, aggregate, discoveredBuyers);
  console.log("Collecting stream metrics...");
  const streamMetrics = await collectStreamDayMetrics(cfg, dayWindow, aggregate, windowEnd, discoveredBuyers);
  if (discoveredBuyers.size > 0) {
    await store.addBuyersToRegistry([...discoveredBuyers]);
    for (const buyer of discoveredBuyers) knownBuyers.add(buyer);
  }
  logInfo("getting base metrics....");
  const baseMetrics = await collectBaseDayMetrics(cfg, baseClient, dayWindow, aggregate, knownBuyers);
  logInfo("building dialy reocrd....");

  const dailyRecord = buildDailyRecord(currentDate, aggregate, windowEnd);
  await store.replaceDaily(currentDate, dailyRecord, [...aggregate.gdBuyers], [...aggregate.creditUsers]);
  const latestState = await store.getState();
  await store.putState({
    finalizedThroughDate: latestState?.finalizedThroughDate,
    updatedAt: windowEnd.toISOString()
  });

  const finalizedDates = await finalizeClosedDays(store, state, runDay, runAt);
  logInfo("analytics.sync.end", {
    currentDate,
    finalizedDates,
    aggregate,
    celoMetrics,
    baseMetrics,
    streamMetrics
  });

  return {
    ok: true,
    currentDate,
    finalizedDates,
    celo: celoMetrics,
    base: baseMetrics,
    streams: streamMetrics
  };
}

export function resolveRunDate(state: AnalyticsState, requestedDate: Date, runDay: string): string {
  const requested = dayFromDate(requestedDate);
  if (state.finalizedThroughDate) {
    const next = nextDate(state.finalizedThroughDate);
    if (next && next <= runDay) {
      return next;
    }
  }

  return requested <= runDay ? requested : runDay;
}

export async function getAnalyticsWindow(env: Env, days = 30, now = new Date()): Promise<AnalyticsResponse> {
  const normalizedDays = Math.max(1, Math.min(days, 365));
  const store = new KVAnalyticsStore(env.ANTSEED_KV);
  const currentDate = dayFromDate(now);

  // Oldest to newest, so the last entry is always today's (i = 0) record.
  const dates = Array.from({ length: normalizedDays }, (_, index) => {
    const daysAgo = normalizedDays - 1 - index;
    return dayFromDate(new Date(now.getTime() - daysAgo * 24 * 60 * 60 * 1000));
  });

  // Fetch every day plus the two independent reads concurrently instead of one round-trip at a time.
  const [daily, persistedGlobal, state] = await Promise.all([
    fetchDailyRecordsConcurrently(store, dates),
    store.getGlobal(),
    store.getState()
  ]);

  // The last fetched day is currentDate's record, so no need to re-read it from KV.
  const today = daily[daily.length - 1];
  const global = addDailyToGlobal(persistedGlobal, today, now.toISOString());
  const resolvedState = state ?? { updatedAt: now.toISOString() };

  return {
    days: normalizedDays,
    daily,
    global,
    lastRun: {
      currentDate,
      finalizedThroughDate: resolvedState.finalizedThroughDate,
      updatedAt: resolvedState.updatedAt
    }
  };
}

/** Reads each date's daily record from KV using a bounded-concurrency worker pool. */
async function fetchDailyRecordsConcurrently(store: KVAnalyticsStore, dates: string[]): Promise<AnalyticsDailyRecord[]> {
  const results: AnalyticsDailyRecord[] = new Array(dates.length);
  let nextIndex = 0;

  const worker = async (): Promise<void> => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= dates.length) return;
      results[index] = await store.getDaily(dates[index]);
    }
  };

  const workerCount = Math.min(ANALYTICS_KV_READ_WORKERS, dates.length);
  await Promise.all(new Array(workerCount).fill(null).map(() => worker()));
  return results;
}

export class KVAnalyticsStore {
  constructor(private readonly kv: KV) {}

  async getBuyerRegistry(): Promise<Set<string>> {
    const buyers = (await this.getJson<string[]>(BUYER_REGISTRY_KEY)) ?? [];
    return new Set(buyers.map((buyer) => buyer.toLowerCase()));
  }

  async addBuyersToRegistry(buyers: string[]): Promise<void> {
    if (buyers.length === 0) return;
    const existing = await this.getBuyerRegistry();
    for (const buyer of buyers) {
      existing.add(buyer.toLowerCase());
    }
    await this.putJson(BUYER_REGISTRY_KEY, [...existing].sort());
  }

  async getDaily(date: string): Promise<AnalyticsDailyRecord> {
    const key = `${DAILY_PREFIX}${date}`;
    const value = await this.getJson<AnalyticsDailyRecord>(key);
    if (value) return value;
    return {
      date,
      ...ZERO_DAILY,
      updatedAt: new Date().toISOString(),
      missing: true
    };
  }

  async replaceDaily(date: string, value: AnalyticsDailyRecord, gdBuyers: string[], creditUsers: string[]): Promise<void> {
    await this.putJson(`${DAILY_PREFIX}${date}`, value);
    await this.putJson(`${GD_BUYERS_PREFIX}${date}`, dedupeAccounts(gdBuyers));
    await this.putJson(`${CREDIT_USERS_PREFIX}${date}`, dedupeAccounts(creditUsers));
  }

  async getGlobal(): Promise<AnalyticsGlobalTotals> {
    const value = await this.getJson<AnalyticsGlobalTotals>(GLOBAL_KEY);
    if (value) return value;
    return {
      gdOneTimeDepositsWei: "0",
      gdStreamedWei: "0",
      aiCreditsUsedWei: "0",
      gdTotalFlowRateWeiPerSecond: "0",
      updatedAt: new Date().toISOString()
    };
  }

  async putGlobal(value: AnalyticsGlobalTotals): Promise<void> {
    await this.putJson(GLOBAL_KEY, value);
  }

  async getState(): Promise<AnalyticsState | undefined> {
    return this.getJson<AnalyticsState>(STATE_KEY);
  }

  async putState(value: AnalyticsState): Promise<void> {
    await this.putJson(STATE_KEY, value);
  }

  private async getJson<T>(key: string): Promise<T | undefined> {
    const value = await this.kv.get(key, "json");
    return (value ?? undefined) as T | undefined;
  }

  private async putJson(key: string, value: unknown): Promise<void> {
    await this.kv.put(key, JSON.stringify(value));
  }
}

function analyticsConfigFromEnv(env: Env): AnalyticsConfig {
  return {
    reportFailure: (details) => notifySlackOnFailure(env, details),
    celoBlockscoutUrl: env.CELO_BLOCKSCOUT_API_URL ?? "https://celo.blockscout.com/api",
    baseBlockscoutUrl: env.BASE_BLOCKSCOUT_API_URL ?? "https://base.blockscout.com/api",
    celoVaultAddress: env.CELO_VAULT_ADDRESS,
    baseChannelsAddress: (env.ANTSEED_CHANNELS_ADDRESS ?? "0xba66d3b4fbcf472f6f11d6f9f96aace96516f09d").toLowerCase(),
    celoSuperTokenAddress: env.CELO_GD_SUPERTOKEN_ADDRESS?.toLowerCase(),
    celoStreamReceiverAddress: env.CELO_VAULT_ADDRESS?.toLowerCase(),
    superfluidSubgraphUrl: env.SUPERFLUID_SUBGRAPH_URL ?? "https://celo-mainnet.subgraph.x.superfluid.dev/"
  };
}

async function finalizeClosedDays(store: KVAnalyticsStore, state: AnalyticsState, currentDate: string, now: Date): Promise<string[]> {
  const finalizedDates: string[] = [];
  const yesterday = previousDate(currentDate);
  if (!yesterday) return finalizedDates;

  const startDate = state.finalizedThroughDate ? nextDate(state.finalizedThroughDate) : yesterday;
  if (!startDate) {
    return finalizedDates;
  }

  if (startDate > yesterday) {
    return finalizedDates;
  }

  let global = await store.getGlobal();
  let cursor = startDate;
  while (cursor <= yesterday) {
    const day = await store.getDaily(cursor);
    //dont finalize if a day is missing
    if (day.missing) {
      // log warning
      logWarn("analytics.finalize.skipped", {
        reason: "missing_daily_record",
        date: cursor
      });
      break;
    }
    global = addDailyToGlobal(global, day, now.toISOString());
    finalizedDates.push(cursor);
    const next = nextDate(cursor);
    if (!next) break;
    cursor = next;
  }

  await store.putGlobal(global);
  const finalizedThroughDate = finalizedDates.length > 0 ? finalizedDates[finalizedDates.length - 1] : state.finalizedThroughDate;
  await store.putState({
    finalizedThroughDate,
    updatedAt: now.toISOString()
  });
  return finalizedDates;
}

async function collectCeloDayMetrics(
  cfg: AnalyticsConfig,
  client: PublicClient,
  dayWindow: UtcDayWindow,
  aggregate: DailyAggregate,
  discoveredBuyers: Set<string>
): Promise<{ fromTimestamp: number; toTimestamp: number; scanned: number; matched: number }> {
  if (!cfg.celoVaultAddress) {
    return {
      fromTimestamp: dayWindow.startUnix,
      toTimestamp: dayWindow.endUnix,
      scanned: 0,
      matched: 0
    };
  }

  const range = await getExplorerBlockRange(cfg.celoBlockscoutUrl, dayWindow);

  const logs = await fetchLogsByRange(client, {
    address: cfg.celoVaultAddress as Address,
    topic0: getTopicHash(CELO_VAULT_EVENTS, "GdDeposited") as Hex,
    fromBlock: BigInt(range.fromBlock),
    toBlock: BigInt(range.toBlock)
  });
  logInfo("analytics.celo.scan", {
    range,
    dayWindow,
    foundLogs: logs.length
  });
  let matched = 0;
  for (const log of logs) {
    const decoded = decodeViemEventSafe(CELO_VAULT_EVENTS, log);
    if (!decoded || decoded.name !== "GdDeposited") continue;
    matched += 1;
    aggregate.gdOneTimeDepositsWei += BigInt(decoded.args.gdAmount.toString());
    aggregate.gdBuyers.add(String(decoded.args.account).toLowerCase());
    discoveredBuyers.add(String(decoded.args.buyer).toLowerCase());
  }

  return {
    fromTimestamp: dayWindow.startUnix,
    toTimestamp: dayWindow.endUnix,
    scanned: logs.length,
    matched
  };
}

async function collectBaseDayMetrics(
  cfg: AnalyticsConfig,
  client: PublicClient,
  dayWindow: UtcDayWindow,
  aggregate: DailyAggregate,
  knownBuyers: Set<string>
): Promise<{ fromTimestamp: number; toTimestamp: number; scanned: number; matched: number }> {
  const eventNames = ["Reserved", "ChannelSettled", "ChannelClosed", "ChannelTopUp", "ChannelWithdrawn", "CloseRequested"] as const;

  let scanned = 0;
  let matched = 0;
  const range = await getBlockRangeByTimestamp(client, dayWindow);
  const logRange = {
    fromBlock: range.fromBlock.toString(),
    toBlock: range.toBlock.toString()
  };
  logInfo("base range:", logRange);
  for (const eventName of eventNames) {
    const logs = await fetchLogsByRange(client, {
      address: cfg.baseChannelsAddress as Address,
      topic0: getTopicHash(BASE_CHANNEL_EVENTS, eventName) as Hex,
      fromBlock: range.fromBlock,
      toBlock: range.toBlock
    });
    logInfo("analytics.base.scan", {
      eventName,
      range: logRange,
      dayWindow,
      foundLogs: logs.length
    });
    scanned += logs.length;

    for (const log of logs) {
      const decoded = decodeViemEventSafe(BASE_CHANNEL_EVENTS, log);
      if (!decoded) continue;
      const buyer = String(decoded.args.buyer).toLowerCase();
      if (!knownBuyers.has(buyer)) continue;
      matched += 1;
      aggregate.creditUsers.add(buyer);
      if (decoded.name === "ChannelSettled") {
        aggregate.aiCreditsUsedWei += BigInt(decoded.args.delta.toString());
      }
    }
  }

  return {
    fromTimestamp: dayWindow.startUnix,
    toTimestamp: dayWindow.endUnix,
    scanned,
    matched
  };
}

async function collectStreamDayMetrics(
  cfg: AnalyticsConfig,
  dayWindow: UtcDayWindow,
  aggregate: DailyAggregate,
  now: Date,
  discoveredBuyers: Set<string>
): Promise<{ senders: number; totalFlowRateWeiPerSecond: string }> {
  const snapshots = await fetchStreamSnapshots(cfg, now, dayWindow.startUnix, dayWindow.endUnix);

  for (const snapshot of snapshots) {
    if (snapshot.buyerAddress) {
      discoveredBuyers.add(snapshot.buyerAddress);
    }
    aggregate.gdTotalFlowRateWeiPerSecond += snapshot.flowRateWeiPerSecond;
    if (snapshot.totalStreamedWei > 0n) {
      aggregate.gdStreamedWei += snapshot.totalStreamedWei;
      aggregate.gdBuyers.add(snapshot.sender);
    }
  }

  return {
    senders: snapshots.length,
    totalFlowRateWeiPerSecond: aggregate.gdTotalFlowRateWeiPerSecond.toString()
  };
}

function buildDailyRecord(date: string, aggregate: DailyAggregate, now: Date): AnalyticsDailyRecord {
  return {
    date,
    gdOneTimeDepositsWei: aggregate.gdOneTimeDepositsWei.toString(),
    gdStreamedWei: aggregate.gdStreamedWei.toString(),
    gdTotalFlowRateWeiPerSecond: aggregate.gdTotalFlowRateWeiPerSecond.toString(),
    aiCreditsUsedWei: aggregate.aiCreditsUsedWei.toString(),
    uniqueGdBuyers: aggregate.gdBuyers.size,
    uniqueCreditUsers: aggregate.creditUsers.size,
    updatedAt: now.toISOString()
  };
}

function addDailyToGlobal(global: AnalyticsGlobalTotals, day: AnalyticsDailyRecord, updatedAt: string): AnalyticsGlobalTotals {
  return {
    gdOneTimeDepositsWei: (BigInt(global.gdOneTimeDepositsWei) + BigInt(day.gdOneTimeDepositsWei)).toString(),
    gdStreamedWei: (BigInt(global.gdStreamedWei) + BigInt(day.gdStreamedWei)).toString(),
    aiCreditsUsedWei: (BigInt(global.aiCreditsUsedWei) + BigInt(day.aiCreditsUsedWei)).toString(),
    gdTotalFlowRateWeiPerSecond: BigInt(day.gdTotalFlowRateWeiPerSecond).toString(),
    updatedAt
  };
}

async function fetchStreamSnapshots(cfg: AnalyticsConfig, now: Date, dayStartUnix: number, dayEndUnix: number): Promise<StreamSnapshot[]> {
  if (!cfg.celoSuperTokenAddress || !cfg.celoStreamReceiverAddress) {
    logWarn("analytics.streams.skipped", {
      reason: "missing_config",
      hasToken: Boolean(cfg.celoSuperTokenAddress),
      hasReceiver: Boolean(cfg.celoStreamReceiverAddress)
    });
    return [];
  }

  const snapshotsBySender = new Map<string, { buyerAddress?: string; flowRateWeiPerSecond: bigint; totalStreamedWei: bigint }>();
  const pageSize = 1000;
  let lastId = "";
  const nowUnix = Math.floor(now.getTime() / 1000);

  // Keyset pagination on id_gt to avoid graph-node's hard 5,000-entity cap on skip.
  let body = {};
  while (true) {
    body = {
      query: `
        query StreamPeriodsPage($receiver: String!, $token: String!, $daysago: BigInt!, $until: BigInt!, $lastId: String!, $first: Int!) {
          streamPeriods(
            first: $first
            orderBy: id
            orderDirection: asc
            where: {
              or: [
                { receiver: $receiver, token: $token, id_gt: $lastId, stoppedAtTimestamp: null, startedAtTimestamp_lte: $until },
                { receiver: $receiver, token: $token, id_gt: $lastId, stoppedAtTimestamp_gt: $daysago, startedAtTimestamp_lte: $until }
              ]
            }
          ) {
            id
            sender { id }
            flowRate
            startedAtTimestamp
            stoppedAtTimestamp
            stream {
              userData
            }
          }
        }
      `,
      variables: {
        receiver: cfg.celoStreamReceiverAddress,
        token: cfg.celoSuperTokenAddress,
        daysago: String(dayStartUnix),
        until: String(dayEndUnix),
        lastId,
        first: pageSize
      }
    };

    const response = await retryWithBackoff(
      () =>
        fetch(cfg.superfluidSubgraphUrl, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body)
        }),
      3,
      500
    );

    if (!response.ok) {
      throw new Error(`Superfluid subgraph HTTP ${response.status}`);
    }

    const json = (await response.json()) as {
      errors?: Array<any>;
      data?: {
        streamPeriods?: Array<{
          id?: string;
          sender: { id: string };
          flowRate: string;
          startedAtTimestamp: string;
          stoppedAtTimestamp: string | null;
          stream?: { userData?: string } | null;
        }>;
      };
    };
    if (!json.data || json.errors) {
      logError("analytics.streams.subgraph_error", { url: cfg.superfluidSubgraphUrl, body, response: json });
      throw new Error(`Superfluid subgraph error: ${errorMessage(json)}`);
    }
    const batch = json.data?.streamPeriods ?? [];
    for (const period of batch) {
      const sender = period.sender.id.toLowerCase();
      const flowRate = BigInt(period.flowRate || "0");
      const startedAt = parseNumberish(period.startedAtTimestamp || "0");
      const stoppedAt = period.stoppedAtTimestamp ? parseNumberish(period.stoppedAtTimestamp) : null;
      const buyerAddress = decodeBuyerFromUserData(period.stream?.userData);

      // intersect period with the day window to get seconds streamed today
      const effectiveStart = Math.max(startedAt, dayStartUnix);
      const effectiveEnd = Math.min(stoppedAt ?? nowUnix, nowUnix);
      const activeSeconds = BigInt(Math.max(0, effectiveEnd - effectiveStart));
      const streamedWei = flowRate * activeSeconds;

      const existing = snapshotsBySender.get(sender);
      if (existing) {
        existing.buyerAddress ??= buyerAddress;
        existing.totalStreamedWei += streamedWei;
        if (stoppedAt === null) existing.flowRateWeiPerSecond += flowRate;
      } else {
        snapshotsBySender.set(sender, {
          buyerAddress,
          flowRateWeiPerSecond: stoppedAt === null ? flowRate : 0n,
          totalStreamedWei: streamedWei
        });
      }
    }

    if (batch.length < pageSize) break;
    lastId = batch[batch.length - 1].id ?? "";
    if (!lastId) break;
  }

  return [...snapshotsBySender.entries()].map(([sender, value]) => ({
    sender,
    buyerAddress: value.buyerAddress,
    flowRateWeiPerSecond: value.flowRateWeiPerSecond,
    totalStreamedWei: value.totalStreamedWei
  }));
}

async function fetchLogsByRange(
  client: PublicClient,
  params: {
    address: Address;
    topic0: Hex;
    fromBlock: bigint;
    toBlock: bigint;
  }
): Promise<Log[]> {
  if (params.toBlock < params.fromBlock) {
    return [];
  }

  const batches: Array<{ fromBlock: bigint; toBlock: bigint }> = [];
  for (let fromBlock = params.fromBlock; fromBlock <= params.toBlock; fromBlock += LOG_BATCH_BLOCKS) {
    const toBlock = fromBlock + LOG_BATCH_BLOCKS - 1n;
    batches.push({
      fromBlock,
      toBlock: toBlock <= params.toBlock ? toBlock : params.toBlock
    });
  }

  const results: Log[][] = new Array(batches.length);
  let nextIndex = 0;

  const worker = async (): Promise<void> => {
    let firstRequest = true;
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= batches.length) {
        return;
      }

      if (!firstRequest) {
        await wait(LOG_BATCH_DELAY_MS);
      }
      firstRequest = false;

      const batch = batches[index];
      results[index] = await retryWithBackoff(
        async () => {
          const logs = await client.request({
            method: "eth_getLogs",
            params: [
              {
                address: params.address,
                topics: [params.topic0],
                fromBlock: toHex(batch.fromBlock),
                toBlock: toHex(batch.toBlock)
              }
            ]
          });
          return logs as Log[];
        },
        3,
        500
      );
    }
  };

  await Promise.all(new Array(LOG_BATCH_WORKERS).fill(null).map(() => worker()));
  return results.flat();
}

async function createChainClient(kv: KV, chainId: number, chain: Chain, fallbackRpcs: string[], reportFailure: FailureReporter): Promise<PublicClient> {
  const rpcUrls = await getChainRpcUrls(kv, chainId, fallbackRpcs, reportFailure);
  return createPublicClient({
    chain,
    transport: fallback(rpcUrls.map((rpcUrl) => http(rpcUrl)))
  });
}

async function getChainRpcUrls(kv: KV, chainId: number, fallbackRpcs: string[], reportFailure: FailureReporter): Promise<string[]> {
  const key = `${RPCS_CACHE_PREFIX}${chainId}`;
  const cached = (await kv.get(key, "json")) as RpcCacheEntry | null;
  if (cached?.fetchedAt) {
    const fetchedAtMs = Date.parse(cached.fetchedAt);
    if (!Number.isNaN(fetchedAtMs) && Date.now() - fetchedAtMs < RPCS_CACHE_TTL_MS) {
      const cachedUrls = sanitizeRpcUrls(cached.rpcs);
      if (cachedUrls.length > 0) {
        return cachedUrls;
      }
    }
  }

  try {
    const fetchedUrls = await fetchRpcsFromChainlist(chainId);
    if (fetchedUrls.length > 0) {
      await kv.put(
        key,
        JSON.stringify({
          rpcs: fetchedUrls,
          fetchedAt: new Date().toISOString()
        } satisfies RpcCacheEntry)
      );
      return fetchedUrls;
    }
  } catch (error) {
    await reportFailure({ operation: "analytics.rpc.discovery", chain: chainId === CELO_CHAIN_ID ? "celo" : "base", recovered: true, error });
    logWarn("analytics.rpc.chainlist_failed", {
      chainId,
      message: errorMessage(error)
    });
  }

  return sanitizeRpcUrls(fallbackRpcs);
}

async function fetchRpcsFromChainlist(chainId: number): Promise<string[]> {
  const chainlistUrl = new URL(CHAINLIST_RPCS_URL);

  const response = await retryWithBackoff(() => fetch(chainlistUrl.href), 3, 500);
  if (!response.ok) {
    throw new Error(`Chainlist HTTP ${response.status}`);
  }

  const payload = (await response.json()) as Array<{
    chainId?: number;
    rpc?: Array<string | { url?: string }>;
  }>;

  const chain = payload.find((entry) => entry.chainId === chainId);
  console.log("got rpcs from chainlist", { chainId });

  if (!chain?.rpc) {
    return [];
  }

  const urls = chain.rpc.map((entry) => (typeof entry === "string" ? entry : (entry.url ?? ""))).filter((entry) => Boolean(entry));

  return sanitizeRpcUrls(urls);
}

function sanitizeRpcUrls(urls: string[]): string[] {
  const deduped = new Set<string>();
  for (const url of urls) {
    if (!url.startsWith("https://")) continue;
    if (url.includes("${")) continue;
    deduped.add(url);
  }
  return [...deduped];
}

async function getExplorerBlockRange(apiUrl: string, dayWindow: UtcDayWindow): Promise<{ fromBlock: number; toBlock: number }> {
  const fromBlock = await getBlockByTimestamp(apiUrl, dayWindow.startUnix, "after");
  const toBlock = await getBlockByTimestamp(apiUrl, dayWindow.endUnix, "before");
  return {
    fromBlock,
    toBlock: toBlock >= fromBlock ? toBlock : fromBlock
  };
}

async function getBlockRangeByTimestamp(client: PublicClient, dayWindow: UtcDayWindow): Promise<{ fromBlock: bigint; toBlock: bigint }> {
  const fromBlock = await getBlockByTimestampViem(client, dayWindow.startUnix, "after", BASE_BLOCKS_PER_SECOND);
  const toBlock = await getBlockByTimestampViem(client, dayWindow.endUnix, "before", BASE_BLOCKS_PER_SECOND);
  return {
    fromBlock,
    toBlock: toBlock >= fromBlock ? toBlock : fromBlock
  };
}

async function getBlockByTimestampViem(client: PublicClient, timestamp: number, closest: "before" | "after", blocksPerSecond: number): Promise<bigint> {
  const latest = await retryWithBackoff(() => client.getBlock({ blockTag: "latest" }), 3, 300);
  const latestNumber = latest.number;
  if (latestNumber === null) {
    throw new Error("latest block number missing");
  }

  const latestTimestamp = Number(latest.timestamp);
  if (timestamp >= latestTimestamp) {
    return latestNumber;
  }

  const estimatedBlocksAgo = BigInt(Math.max(0, Math.floor((latestTimestamp - timestamp) * blocksPerSecond)));
  const estimatedBlock = estimatedBlocksAgo >= latestNumber ? 0n : latestNumber - estimatedBlocksAgo;
  const windowBlocks = BigInt(Math.max(1, Math.ceil(blocksPerSecond * TIMESTAMP_SEARCH_WINDOW_SECONDS)));

  let low = estimatedBlock >= windowBlocks ? estimatedBlock - windowBlocks : 0n;
  let high = estimatedBlock + windowBlocks <= latestNumber ? estimatedBlock + windowBlocks : latestNumber;

  let lowTimestamp = Number((await retryWithBackoff(() => client.getBlock({ blockNumber: low }), 3, 300)).timestamp);
  let highTimestamp = Number((await retryWithBackoff(() => client.getBlock({ blockNumber: high }), 3, 300)).timestamp);

  while (timestamp < lowTimestamp && low > 0n) {
    high = low;
    low = low > windowBlocks ? low - windowBlocks : 0n;
    lowTimestamp = Number((await retryWithBackoff(() => client.getBlock({ blockNumber: low }), 3, 300)).timestamp);
  }

  while (timestamp > highTimestamp && high < latestNumber) {
    low = high;
    high = high + windowBlocks <= latestNumber ? high + windowBlocks : latestNumber;
    highTimestamp = Number((await retryWithBackoff(() => client.getBlock({ blockNumber: high }), 3, 300)).timestamp);
  }

  let best = closest === "after" ? latestNumber : 0n;

  while (low <= high) {
    const mid = (low + high) / 2n;
    const block = await retryWithBackoff(() => client.getBlock({ blockNumber: mid }), 3, 300);
    const blockTimestamp = Number(block.timestamp);

    if (blockTimestamp === timestamp) {
      return mid;
    }

    if (blockTimestamp < timestamp) {
      if (closest === "before") best = mid;
      low = mid + 1n;
      continue;
    }

    if (closest === "after") best = mid;
    if (mid === 0n) {
      break;
    }
    high = mid - 1n;
  }

  return best;
}

async function getBlockByTimestamp(apiUrl: string, timestamp: number, closest: "before" | "after"): Promise<number> {
  const url = new URL(apiUrl);
  url.searchParams.set("module", "block");
  url.searchParams.set("action", "getblocknobytime");
  url.searchParams.set("timestamp", String(timestamp));
  url.searchParams.set("closest", closest);

  const response = await retryWithBackoff(() => fetch(url.href), 3, 300);
  if (!response.ok) {
    throw new Error(`Explorer HTTP ${response.status}`);
  }

  const payload = (await response.json()) as {
    status?: string;
    message?: string;
    result?: string | { blockNumber?: string };
  };

  if (payload.result && payload.status !== "0") {
    const blockValue = typeof payload.result === "string" ? payload.result : payload.result.blockNumber;
    if (!blockValue) throw new Error("Explorer getblocknobytime missing blockNumber");
    return parseNumberish(blockValue);
  }

  throw new Error(`Explorer getblocknobytime failed: ${payload.message ?? "unknown"}`);
}

function createDailyAggregate(): DailyAggregate {
  return {
    gdOneTimeDepositsWei: 0n,
    gdStreamedWei: 0n,
    gdTotalFlowRateWeiPerSecond: 0n,
    aiCreditsUsedWei: 0n,
    gdBuyers: new Set<string>(),
    creditUsers: new Set<string>()
  };
}

type UtcDayWindow = {
  startUnix: number;
  endUnix: number;
};

function getUtcDayWindow(date: Date, mode: "full-day" | "until-now" = "full-day"): UtcDayWindow {
  const startMs = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0, 0);
  const endMs = mode === "full-day" ? Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 23, 59, 59, 999) : date.getTime();

  return {
    startUnix: Math.floor(startMs / 1000),
    endUnix: Math.floor(endMs / 1000)
  };
}

function dayFromDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function previousDate(date: string): string | undefined {
  const parsed = new Date(`${date}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) return undefined;
  parsed.setUTCDate(parsed.getUTCDate() - 1);
  return dayFromDate(parsed);
}

function nextDate(date: string): string | undefined {
  const parsed = new Date(`${date}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) return undefined;
  parsed.setUTCDate(parsed.getUTCDate() + 1);
  return dayFromDate(parsed);
}

function dedupeAccounts(accounts: string[]): string[] {
  return [...new Set(accounts.map((account) => account.toLowerCase()))].sort();
}

function getTopicHash(iface: Interface, eventName: string): string {
  const event = iface.getEvent(eventName);
  if (!event) throw new Error(`event not found: ${eventName}`);
  return event.topicHash;
}

function decodeViemEventSafe(iface: Interface, log: Log) {
  try {
    const topics = log.topics.filter((topic): topic is Hex => Boolean(topic));
    if (topics.length === 0) {
      return null;
    }
    return iface.parseLog({
      data: log.data,
      topics
    });
  } catch (error) {
    logWarn("analytics.decode.failed", {
      txHash: log.transactionHash ?? "unknown",
      logIndex: log.logIndex?.toString() ?? "unknown",
      message: errorMessage(error)
    });
    return null;
  }
}

function parseNumberish(value: string | number): number {
  if (typeof value === "number") return value;
  if (value.startsWith("0x")) return Number.parseInt(value, 16);
  return Number.parseInt(value, 10);
}

async function retryWithBackoff<T>(fn: () => Promise<T>, retries: number, baseWaitMs: number): Promise<T> {
  let attempt = 0;
  let lastError: unknown;
  while (attempt <= retries) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      attempt += 1;
      if (attempt > retries) break;
      await wait(baseWaitMs * 2 ** (attempt - 1));
    }
  }
  logError("analytics.retry.failed", {
    retries,
    message: errorMessage(lastError)
  });
  throw lastError instanceof Error ? lastError : new Error("retry failed");
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
