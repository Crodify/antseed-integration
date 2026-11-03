import { AntSeedFundingVaultClient } from "./antseed-funding-vault.js";
import { fetchCurrentGdPrice, fetchGoodIdRoot } from "./celo-events.js";
import { RuntimeConfig } from "./env.js";
import { fundCredit } from "./funding.js";
import { KVCreditStore } from "./kv-credit-store.js";
import { StreamPeriod, earliestUnsettledStop, fetchStreamPeriods, liveFlowRate, nextBuyerWindow } from "./stream-periods.js";
import { UserCreditProfile } from "./types.js";
import { logInfo, logWarn, redactAddress } from "./logging.js";

/** Minimum time between two credits of a live stream. */
const STREAM_CREDIT_COOLDOWN_SECONDS = 24 * 60 * 60;

/**
 * Unix seconds up to which every closed stream period is known to be settled. Periods that stopped at
 * or before it are not fetched again. Absent means 0: the first run reads every period.
 */
const STREAM_CLOSED_PERIODS_CURSOR_KEY = "streams:closed-periods-cursor";

export async function creditStreams(
  cfg: RuntimeConfig,
  kv: KVNamespace,
  store: KVCreditStore,
  antseedFundingVault: AntSeedFundingVaultClient,
  ctx: ExecutionContext
): Promise<Record<string, unknown>> {
  const closedAfter = await readClosedPeriodsCursor(kv);
  const snapshot = await fetchStreamPeriods(cfg, closedAfter);
  // Undefined is "subgraph unavailable", not "nothing streaming": skip, and leave the cursor alone.
  if (!snapshot) return { skipped: "subgraph_unavailable", closedAfter };
  const { asOf, periods } = snapshot;

  const periodsByAccount = new Map<string, StreamPeriod[]>();
  for (const period of periods) {
    const accountPeriods = periodsByAccount.get(period.account);
    if (accountPeriods) accountPeriods.push(period);
    else periodsByAccount.set(period.account, [period]);
  }

  const counts = {
    closedAfter,
    asOf,
    periodCount: periods.length,
    accountCount: periodsByAccount.size,
    inactiveAccounts: 0,
    syncedFlowRates: 0,
    syncedRootFlowRates: 0,
    skippedCooldown: 0,
    skippedMinAmount: 0,
    skippedUnreadableClock: 0,
    retried: 0,
    processed: 0
  };

  // Sync every account's recorded rate on every run, not only when a credit clears the gates. All
  // periods closed sums to 0, which is how the profile stops advertising a stream that has ended.
  // `getSavedUser`: the scheduled run must not create a profile as a side effect; a first-time
  // streamer gets one, with its rate, from its first credit below.
  const profileByAccount = new Map<string, UserCreditProfile>();
  const flowRateByRoot = new Map<string, bigint>();
  for (const [account, accountPeriods] of periodsByAccount) {
    const flowRate = liveFlowRate(accountPeriods);
    if (flowRate === 0n) counts.inactiveAccounts += 1;
    const profile = await store.getSavedUser(account);
    if (!profile) continue;
    if (await store.recordStreamFlowRate(profile, flowRate)) counts.syncedFlowRates += 1;
    profileByAccount.set(account, profile);
    flowRateByRoot.set(profile.rootAccount, (flowRateByRoot.get(profile.rootAccount) ?? 0n) + flowRate);
  }
  // A GoodID root's rate is the sum across its accounts (a rate is absolute, so `updateUser`'s
  // mirroring does not apply). Runs after the per-account pass so it wins for a root that streams.
  for (const [rootAccount, totalFlowRate] of flowRateByRoot) {
    const rootProfile = profileByAccount.get(rootAccount) ?? (await store.getSavedUser(rootAccount));
    if (!rootProfile) continue;
    if (await store.recordStreamFlowRate(rootProfile, totalFlowRate)) counts.syncedRootFlowRates += 1;
  }

  const gdPrice = await fetchCurrentGdPrice(cfg);
  // Bounded by the earliest closed period still owed anything, as read before this run's credits: a
  // credit made now may yet fail, so the next run has to see that period again.
  let nextCursor = asOf;
  const fundings: Promise<{ [key: string]: unknown }>[] = [];

  for (const [account, accountPeriods] of periodsByAccount) {
    const profile = profileByAccount.get(account) ?? (await store.getUser(account));
    const creditedThrough = profile.lastStreamCreditAt ? Math.floor(Date.parse(profile.lastStreamCreditAt) / 1000) : 0;
    if (!Number.isFinite(creditedThrough)) {
      logWarn("stream.credits.unreadable-clock", { account: redactAddress(account), lastStreamCreditAt: profile.lastStreamCreditAt });
      counts.skippedUnreadableClock += 1;
      const unsettled = earliestUnsettledStop(accountPeriods, 0);
      if (unsettled !== undefined) nextCursor = Math.min(nextCursor, unsettled - 1);
      continue;
    }
    const unsettled = earliestUnsettledStop(accountPeriods, creditedThrough);
    if (unsettled !== undefined) nextCursor = Math.min(nextCursor, unsettled - 1);

    // Cut the window at the first buyer change, so this entry pays exactly the buyer that was named
    // while its periods ran. The next run continues from `through` and settles the next segment.
    const window = nextBuyerWindow(accountPeriods, creditedThrough, asOf);
    if (!window) continue;
    const { buyerAddress, owedWei: gdAmountWei, from, through } = window;
    if (gdAmountWei === 0n) continue;
    const flowRate = liveFlowRate(accountPeriods);
    // The cooldown and the minimum hold back an open-ended window on a live stream, to avoid paying
    // out dust every run. They must not apply to a window cut short at a buyer change: that window
    // never grows, so gating it would stall the account's clock permanently and nothing after the
    // change would ever be credited. A cut window is a settlement boundary, like a close.
    if (flowRate > 0n && through === asOf) {
      // Aged from the window's own start, not from the clock. On a first credit the clock is 0, so
      // `asOf - creditedThrough` is the whole Unix epoch and the cooldown always passed; a stream
      // reopened after a long gap inherits a stale clock and passes just as trivially. Either way the
      // gate was decorative for exactly the accounts it exists to cover.
      if (asOf - from < STREAM_CREDIT_COOLDOWN_SECONDS) {
        counts.skippedCooldown += 1;
        continue;
      }
      if (gdAmountWei < cfg.MIN_STREAM_BONUS_WEI) {
        counts.skippedMinAmount += 1;
        continue;
      }
    }

    const rootAccount = await fetchGoodIdRoot(account, cfg);
    const hasOperatorConsent = antseedFundingVault.enabled ? (await antseedFundingVault.isBuyerOperator(buyerAddress || account)).isOperator : true;
    // Returns the stored entry unchanged when this window's id already exists.
    let entry = await store.recordGdCredit({
      id: `stream:${account}:${creditedThrough}`,
      account,
      rootAccount,
      source: "streamCron",
      gdAmountWei,
      flowRate,
      isVerified: !!rootAccount && hasOperatorConsent, // root found => whitelisted; plus operator consent
      gdPrice,
      maxBonusCapUsd: cfg.MAX_BONUS_CAP_USD,
      regularBonusBps: cfg.REGULAR_BONUS_BPS,
      streamingBonusBps: cfg.STREAMING_BONUS_BPS,
      buyerAddress,
      // The cut point, not `asOf`: only this segment has been settled.
      streamCreditedThrough: new Date(through * 1000).toISOString()
    });
    if (entry.fundingStatus === "funded") {
      // Funded but the clock never moved (or a concurrent run just moved it): repair and move on.
      await store.advanceStreamClock(entry);
      continue;
    }
    if (entry.fundingStatus === "failed") {
      entry = await store.retryFailedFunding(entry);
      counts.retried += 1;
    }
    logInfo("streamCron.credits.entry", {
      entryId: entry.id,
      account: redactAddress(entry.account),
      rootAccount: redactAddress(entry.rootAccount),
      buyer: redactAddress(entry.buyerAddress),
      creditedThrough,
      streamCreditedThrough: entry.streamCreditedThrough,
      gdAmountWei: entry.gdAmountWei
    });
    counts.processed += 1;
    const funding = fundCredit(entry, store, antseedFundingVault, cfg.reportFailure).catch(async (error) => {
      await cfg.reportFailure?.({ operation: "funding.background", chain: "base", error, entryId: entry.id, buyer: buyerAddress || account });
      return { fundingStatus: "failed" };
    });
    ctx.waitUntil(funding);
    fundings.push(funding);
  }

  if (nextCursor > closedAfter) await kv.put(STREAM_CLOSED_PERIODS_CURSOR_KEY, nextCursor.toString());
  ctx.waitUntil(
    Promise.all(fundings).then((results) => {
      logInfo("streamCron.funding.summary", {
        funded: results.filter((result) => result.fundingStatus === "funded").length,
        failed: results.filter((result) => result.fundingStatus === "failed").length
      });
    })
  );
  return { ...counts, nextCursor: Math.max(nextCursor, closedAfter) };
}

async function readClosedPeriodsCursor(kv: Pick<KVNamespace, "get">): Promise<number> {
  const raw = await kv.get(STREAM_CLOSED_PERIODS_CURSOR_KEY);
  const parsed = raw ? Number(raw) : 0;
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : 0;
}
