import { z } from "zod";
import { decodeBuyerFromUserData } from "./celo-events.js";
import { RuntimeConfig } from "./env.js";
import { errorMessage, logError, logInfo, logWarn, redactAddress } from "./logging.js";

export const SUPERFLUID_CELO_SUBGRAPH_URL = "https://subgraph-endpoints.superfluid.dev/celo-mainnet/protocol-v1";

const PAGE_SIZE = 1000;

/** A plausible unix-seconds timestamp: positive, and small enough to stay an exact integer. */
const unixSeconds = z
  .string()
  .regex(/^\d+$/)
  .refine((value) => {
    const seconds = Number(value);
    return Number.isSafeInteger(seconds) && seconds > 0;
  }, "timestamp must be a positive safe integer");

const StreamPeriodsResponseSchema = z.object({
  data: z.object({
    _meta: z.object({ block: z.object({ timestamp: z.coerce.number().int().positive() }) }),
    streamPeriods: z.array(
      z.object({
        id: z.string().min(1),
        sender: z.object({ id: z.string().regex(/^0x[0-9a-fA-F]{40}$/) }),
        flowRate: z.string().regex(/^\d+$/),
        // Positive and safe, not merely numeric. A zero or absent start would be read as 1970, and
        // on an account's first credit -- where `creditedThrough` is 0 and nothing clamps the window
        // -- that bills decades at the period's rate. This is the shape that produced a single
        // 6,191,029 G$ entry once already, so the snapshot is rejected rather than priced.
        startedAtTimestamp: unixSeconds,
        stoppedAtTimestamp: unixSeconds.nullable(),
        startedAtEvent: z.object({ userData: z.string() }).nullable()
      })
    )
  })
});

/** One constant-rate segment of a Superfluid stream into the vault. Timestamps are unix seconds. */
export type StreamPeriod = {
  id: string;
  account: string;
  flowRateWeiPerSecond: bigint;
  startedAt: number;
  /** Undefined while the period is still flowing. */
  stoppedAt?: number;
  /** AntSeed buyer from the userData of the flow update that opened this period. */
  buyerAddress?: string;
};

export type StreamPeriodsSnapshot = {
  /** Timestamp of the block the subgraph had indexed; the end of every window credited from this snapshot. */
  asOf: number;
  periods: StreamPeriod[];
};

/**
 * Every stream period into the vault that can still owe credit: all open periods, plus closed ones that
 * stopped after `closedAfter`. Periods that stopped at or before it are known to be settled and are not
 * fetched again.
 *
 * Returns undefined when the subgraph is unreachable or misconfigured, so the caller skips the run
 * instead of reading "no periods" as "nothing is streaming".
 */
export async function fetchStreamPeriods(cfg: RuntimeConfig, closedAfter: number): Promise<StreamPeriodsSnapshot | undefined> {
  if (!cfg.CELO_VAULT_ADDRESS || !cfg.CELO_GD_SUPERTOKEN_ADDRESS) {
    logWarn("superfluid.periods.skipped", {
      reason: "missing_config",
      hasVaultAddress: Boolean(cfg.CELO_VAULT_ADDRESS),
      hasSuperTokenAddress: Boolean(cfg.CELO_GD_SUPERTOKEN_ADDRESS)
    });
    return undefined;
  }

  const endpoint = cfg.SUPERFLUID_SUBGRAPH_URL ?? SUPERFLUID_CELO_SUBGRAPH_URL;
  const receiver = cfg.CELO_VAULT_ADDRESS.toLowerCase();
  const token = cfg.CELO_GD_SUPERTOKEN_ADDRESS.toLowerCase();
  const periods: StreamPeriod[] = [];
  let asOf: number | undefined;
  let lastId = "";
  let pages = 0;

  try {
    while (true) {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          // `or` branches must each carry every condition: the subgraph rejects mixing them with
          // top-level fields. Keyset pagination on `id` rather than `skip`, which graph-node caps.
          query: `
            query VaultStreamPeriods($receiver: String!, $token: String!, $closedAfter: BigInt!, $lastId: String!, $first: Int!) {
              _meta { block { timestamp } }
              streamPeriods(
                first: $first
                orderBy: id
                orderDirection: asc
                where: {
                  or: [
                    { receiver: $receiver, token: $token, id_gt: $lastId, stoppedAtTimestamp: null }
                    { receiver: $receiver, token: $token, id_gt: $lastId, stoppedAtTimestamp_gt: $closedAfter }
                  ]
                }
              ) {
                id
                sender { id }
                flowRate
                startedAtTimestamp
                stoppedAtTimestamp
                startedAtEvent { userData }
              }
            }
          `,
          variables: { receiver, token, closedAfter: closedAfter.toString(), lastId, first: PAGE_SIZE }
        })
      });
      if (!response.ok) throw new Error(`Superfluid subgraph HTTP ${response.status}`);

      const parsed = StreamPeriodsResponseSchema.safeParse(await response.json());
      if (!parsed.success) throw new Error("invalid Superfluid subgraph response shape");

      asOf ??= parsed.data.data._meta.block.timestamp;
      const batch = parsed.data.data.streamPeriods;
      for (const period of batch) {
        periods.push({
          id: period.id,
          account: period.sender.id.toLowerCase(),
          flowRateWeiPerSecond: BigInt(period.flowRate),
          startedAt: Number(period.startedAtTimestamp),
          stoppedAt: period.stoppedAtTimestamp === null ? undefined : Number(period.stoppedAtTimestamp),
          buyerAddress: decodeBuyerFromUserData(period.startedAtEvent?.userData)
        });
      }
      pages += 1;
      if (batch.length < PAGE_SIZE) break;
      lastId = batch[batch.length - 1].id;
    }
  } catch (error) {
    await cfg.reportFailure?.({ operation: "celo.streams.subgraph", chain: "celo", error });
    logError("superfluid.periods.fetch.failed", {
      endpoint,
      receiver: redactAddress(receiver),
      token: redactAddress(token),
      message: errorMessage(error)
    });
    return undefined;
  }

  logInfo("superfluid.periods.fetch.end", { endpoint, closedAfter, asOf, pages, count: periods.length });
  return { asOf: asOf!, periods };
}

/**
 * G$ an account's periods carried in the window `(creditedThrough, asOf]`: each period's rate times
 * the seconds it overlaps that window. Gaps between periods (a closed stream, a later re-open) have no
 * period and so contribute nothing.
 */
export function streamOwedWei(periods: StreamPeriod[], creditedThrough: number, asOf: number): bigint {
  let owed = 0n;
  for (const period of periods) {
    const start = Math.max(period.startedAt, creditedThrough);
    const end = Math.min(period.stoppedAt ?? asOf, asOf);
    if (end > start) owed += period.flowRateWeiPerSecond * BigInt(end - start);
  }
  return owed;
}

/** Combined rate of the account's open periods; 0 once every stream has closed. */
export function liveFlowRate(periods: StreamPeriod[]): bigint {
  return periods.reduce((sum, period) => (period.stoppedAt === undefined ? sum + period.flowRateWeiPerSecond : sum), 0n);
}

/** Case-insensitive buyer comparison; two periods with no buyer count as the same buyer. */
function sameBuyer(a: string | undefined, b: string | undefined): boolean {
  return (a ?? "").toLowerCase() === (b ?? "").toLowerCase();
}

/**
 * The next window to credit, ending at the first buyer change.
 *
 * `userData` on a flow update names the AntSeed buyer, and the vault stores whatever it is given
 * (`streamBuyer[sender] = buyer` runs on update as well as creation), so one account's periods can
 * name different buyers over time. Crediting the whole owed span to a single buyer would pay the
 * earlier periods to whoever happens to be named last.
 *
 * Periods are sequential, so cutting the window at the first change is enough: every period
 * overlapping the returned window shares one buyer. The next run continues from `through` and
 * credits the following segment.
 *
 * `from` is where the window actually begins -- the earliest unpaid period's start, or the clock when
 * the clock is later. On a first credit the clock is 0 and the window begins at the stream, so callers
 * that age the window must measure from `from` rather than from the clock.
 */
export function nextBuyerWindow(
  periods: StreamPeriod[],
  creditedThrough: number,
  asOf: number
): { buyerAddress?: string; owedWei: bigint; from: number; through: number } | undefined {
  const unpaid = periods
    .filter((period) => Math.min(period.stoppedAt ?? asOf, asOf) > Math.max(period.startedAt, creditedThrough))
    .sort((left, right) => left.startedAt - right.startedAt);
  if (unpaid.length === 0) return undefined;

  const buyerAddress = unpaid[0].buyerAddress;
  let through = asOf;
  for (const period of unpaid) {
    if (sameBuyer(period.buyerAddress, buyerAddress)) continue;
    through = Math.max(creditedThrough, period.startedAt);
    break;
  }
  // `unpaid` is sorted, so the earliest start is the window's start. Derived here rather than in the
  // caller: re-deriving it there would duplicate the filter above, and the two drifting apart would
  // age a different window than the one being priced.
  const from = Math.max(unpaid[0].startedAt, creditedThrough);
  return { buyerAddress, owedWei: streamOwedWei(periods, creditedThrough, through), from, through };
}


/**
 * Earliest stop time among closed periods that still owe credit past `creditedThrough`, or undefined
 * when every closed period is settled. Bounds how far the closed-periods cursor may advance.
 */
export function earliestUnsettledStop(periods: StreamPeriod[], creditedThrough: number): number | undefined {
  let earliest: number | undefined;
  for (const period of periods) {
    if (period.stoppedAt === undefined || period.stoppedAt <= Math.max(period.startedAt, creditedThrough)) continue;
    if (earliest === undefined || period.stoppedAt < earliest) earliest = period.stoppedAt;
  }
  return earliest;
}
