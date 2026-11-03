import test from "node:test";
import assert from "node:assert/strict";
import worker from "../src/worker.js";
import { AntSeedFundingVaultClient } from "../src/antseed-funding-vault.js";
import { Env, configFromEnv } from "../src/env.js";
import { KVCreditStore } from "../src/kv-credit-store.js";
import { encodeVaultEventLog } from "../src/celo-events.js";
import { creditStreams } from "../src/stream-credits.js";
import type { FailureDetails } from "../src/http.js";

class MemoryKV {
  private data = new Map<string, string>();
  async get(key: string, type?: "text" | "json") {
    const raw = this.data.get(key) ?? null;
    if (type === "json") return raw ? JSON.parse(raw) : null;
    return raw;
  }
  async put(key: string, value: string) {
    this.data.set(key, value);
  }
}

const VAULT = "0x0000000000000000000000000000000000000def";
const SUPERTOKEN = "0x0000000000000000000000000000000000000fed";
const RATE = 3_433_641_975_308_641n; // 8900 G$/month
const DAY = 86_400;

/** Collects `waitUntil` work so a test can await the funding the scheduled run fires off. */
function trackingContext() {
  const pending: Promise<unknown>[] = [];
  const ctx = {
    waitUntil(promise: Promise<unknown>) {
      pending.push(promise);
    },
    passThroughOnException() {}
  } as unknown as ExecutionContext;
  return { ctx, settle: () => Promise.all(pending) };
}

/**
 * Stands in for `AntseedBuyerOperator.depositForWithId`: one funding per id, as the contract enforces
 * with `DuplicateDepositId`. Every distinct id is real money landing in the buyer's deposits.
 */
function stubOnChainFunding() {
  const original = AntSeedFundingVaultClient.prototype.depositForBuyerWithId;
  const funded = new Map<string, bigint>();
  AntSeedFundingVaultClient.prototype.depositForBuyerWithId = async function (buyer, principalUsd, bonusUsd, id) {
    // Yield like a real RPC round trip, so concurrent requests interleave.
    await new Promise((resolve) => setTimeout(resolve, 50));
    if (!funded.has(id)) funded.set(id, principalUsd + bonusUsd);
    return { enabled: true, buyer, amountUsd: (principalUsd + bonusUsd).toString(), txHash: `0x${funded.size}` };
  };
  return { funded, restore: () => (AntSeedFundingVaultClient.prototype.depositForBuyerWithId = original) };
}

async function seedProfile(kv: MemoryKV, account: string, lastStreamCreditAt: string) {
  await kv.put(
    `user:${account}`,
    JSON.stringify({
      account,
      rootAccount: account,
      createdAt: lastStreamCreditAt,
      updatedAt: lastStreamCreditAt,
      totalGdDepositedWei: "0",
      totalBonusUsd: "0",
      streamFlowRateWeiPerSecond: RATE.toString(),
      totalPrincipalUsd: "0",
      totalGDStreamedWei: "0",
      totalOutstandingFundingUsd: "0",
      lastStreamCreditAt
    })
  );
}

function testEnv(kv: MemoryKV): Env {
  return {
    ANTSEED_KV: kv as never,
    CELO_RPC_URL: "https://celo.rpc.local",
    CELO_VAULT_ADDRESS: VAULT,
    CELO_GD_SUPERTOKEN_ADDRESS: SUPERTOKEN
  } as Env;
}

/**
 * The account streamed for 30 days, was last credited 20 days ago and closed its stream an hour ago.
 * It is owed exactly 20 days minus an hour of G$, once.
 */
function closedStreamScenario() {
  const nowSeconds = Math.floor(Date.now() / 1000);
  const openedAt = nowSeconds - 30 * DAY;
  const closedAt = nowSeconds - 3600;
  const lastCreditAt = new Date((nowSeconds - 20 * DAY) * 1000).toISOString();
  const owedWei = RATE * BigInt(closedAt - (nowSeconds - 20 * DAY));
  return { nowSeconds, openedAt, closedAt, lastCreditAt, owedWei };
}

test("background stream funding failures alert and retain the same retry window", { concurrency: false }, async () => {
  const account = "0x0000000000000000000000000000000000000b99";
  const kv = new MemoryKV();
  const scenario = closedStreamScenario();
  const originalFetch = globalThis.fetch;
  const originalDeposit = AntSeedFundingVaultClient.prototype.depositForBuyerWithId;
  const failures: FailureDetails[] = [];
  const ids: string[] = [];
  try {
    await seedProfile(kv, account, scenario.lastCreditAt);
    globalThis.fetch = subgraphMock(account, scenario, null);
    AntSeedFundingVaultClient.prototype.depositForBuyerWithId = async function (_buyer, _principal, _bonus, id, requireMinimum) {
      assert.equal(requireMinimum, false, "direct-deposit guard must not alter stream policy");
      ids.push(id);
      throw Object.assign(new Error("BelowMinDeposit"), { code: "CALL_EXCEPTION" });
    };
    const cfg = configFromEnv(testEnv(kv));
    cfg.reportFailure = async (details) => { failures.push(details); };
    const store = new KVCreditStore(kv as never);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const { ctx, settle } = trackingContext();
      await creditStreams(cfg, kv as never, store, new AntSeedFundingVaultClient(cfg), ctx);
      await settle();
      const entries = await store.getGdCredits(account);
      assert.equal(entries.length, 1);
      assert.equal(entries[0].fundingStatus, "failed");
      assert.equal((await store.getUser(account)).lastStreamCreditAt, scenario.lastCreditAt);
      assert.equal(failures.filter((failure) => failure.operation === "funding.deposit").length, attempt + 1);
    }
    assert.equal(ids.length, 2);
    assert.equal(ids[0], ids[1]);
  } finally {
    AntSeedFundingVaultClient.prototype.depositForBuyerWithId = originalDeposit;
    globalThis.fetch = originalFetch;
  }
});

function subgraphMock(account: string, scenario: ReturnType<typeof closedStreamScenario>, closeLog: unknown) {
  return (async (_url: string | URL | Request, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body ?? "{}"));
    await new Promise((resolve) => setTimeout(resolve, 1));
    if (typeof body.query === "string") {
      if (body.query.includes("streamPeriods")) {
        return Response.json({
          data: {
            _meta: { block: { timestamp: scenario.nowSeconds } },
            streamPeriods: [
              {
                id: `${account}-period-0`,
                sender: { id: account },
                flowRate: RATE.toString(),
                startedAtTimestamp: scenario.openedAt.toString(),
                stoppedAtTimestamp: scenario.closedAt.toString(),
                startedAtEvent: { userData: "0x" }
              }
            ]
          }
        });
      }
      const activeOnly = body.query.includes("currentFlowRate_gt");
      return Response.json({
        data: {
          streams: activeOnly
            ? []
            : [
                {
                  sender: { id: account },
                  currentFlowRate: "0",
                  updatedAtTimestamp: scenario.closedAt.toString(),
                  flowUpdatedEvents: [{ userData: "0x" }]
                }
              ]
        }
      });
    }
    if (body.method === "eth_call") {
      return Response.json({ jsonrpc: "2.0", id: body.id, result: "0x000000000000000000000000abababababababababababababababababababab" });
    }
    return Response.json({ jsonrpc: "2.0", id: body.id, result: { logs: [closeLog] } });
  }) as typeof fetch;
}

/** A single live period, opened `ageSeconds` ago at `rate`, with no profile: the first-credit shape. */
function liveStreamMock(account: string, nowSeconds: number, openedAt: number, rate: bigint) {
  return (async (_url: string | URL | Request, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body ?? "{}"));
    if (typeof body.query === "string" && body.query.includes("streamPeriods")) {
      return Response.json({
        data: {
          _meta: { block: { timestamp: nowSeconds } },
          streamPeriods: [
            {
              id: `${account}-period-0`,
              sender: { id: account },
              flowRate: rate.toString(),
              startedAtTimestamp: openedAt.toString(),
              stoppedAtTimestamp: null,
              startedAtEvent: { userData: "0x" }
            }
          ]
        }
      });
    }
    if (body.method === "eth_call") {
      return Response.json({ jsonrpc: "2.0", id: body.id, result: "0x000000000000000000000000abababababababababababababababababababab" });
    }
    return Response.json({ data: { streams: [] } });
  }) as typeof fetch;
}

// A first-time streamer has no `lastStreamCreditAt`, so the clock is 0 and `asOf - clock` is the whole
// Unix epoch -- a cooldown aged from the clock always passed. The rate here is deliberately high
// enough (4000 G$ in an hour) to clear MIN_STREAM_BONUS_WEI, so the cooldown is the only gate left.
test("a live stream opened an hour ago is held by the cooldown on its first credit", { concurrency: false }, async () => {
  const account = "0x0000000000000000000000000000000000000b10";
  const nowSeconds = Math.floor(Date.now() / 1000);
  const rate = 2_000_000_000_000_000_000n; // 2 G$/s -> 7200 G$ in an hour, well over the 4000 G$ minimum
  const kv = new MemoryKV();
  const originalFetch = globalThis.fetch;
  const originalLog = console.log;
  const onChain = stubOnChainFunding();
  try {
    console.log = (() => {}) as typeof console.log;
    globalThis.fetch = liveStreamMock(account, nowSeconds, nowSeconds - 3600, rate);
    const { ctx, settle } = trackingContext();
    await worker.scheduled({ scheduledTime: Date.now(), cron: "0 */6 * * *" } as unknown as ScheduledEvent, testEnv(kv), ctx);
    await settle();
    assert.deepEqual(await new KVCreditStore(kv as never).getGdCredits(account), [], "nothing may be credited an hour in");
    assert.equal(onChain.funded.size, 0, "and no deposit may land");
  } finally {
    onChain.restore();
    globalThis.fetch = originalFetch;
    console.log = originalLog;
  }
});

// The same stream past the cooldown must still be credited -- the gate delays a first credit, it does
// not block one. Without this, a stall would look identical to a working cooldown.
test("the same stream is credited once it is older than the cooldown", { concurrency: false }, async () => {
  const account = "0x0000000000000000000000000000000000000b11";
  const nowSeconds = Math.floor(Date.now() / 1000);
  const rate = 2_000_000_000_000_000_000n;
  const openedAt = nowSeconds - 2 * DAY;
  const kv = new MemoryKV();
  const originalFetch = globalThis.fetch;
  const originalLog = console.log;
  const onChain = stubOnChainFunding();
  try {
    console.log = (() => {}) as typeof console.log;
    globalThis.fetch = liveStreamMock(account, nowSeconds, openedAt, rate);
    const { ctx, settle } = trackingContext();
    await worker.scheduled({ scheduledTime: Date.now(), cron: "0 */6 * * *" } as unknown as ScheduledEvent, testEnv(kv), ctx);
    await settle();
    const entries = await new KVCreditStore(kv as never).getGdCredits(account);
    assert.equal(entries.length, 1, "a stream two days old is past the cooldown");
    assert.equal(totalFundedGdWei(entries), rate * BigInt(nowSeconds - openedAt), "and is priced from the stream, not the epoch");
  } finally {
    onChain.restore();
    globalThis.fetch = originalFetch;
    console.log = originalLog;
  }
});

function totalFundedGdWei(entries: Array<{ gdAmountWei: string; fundingStatus: string }>): bigint {
  return entries.filter((entry) => entry.fundingStatus === "funded").reduce((sum, entry) => sum + BigInt(entry.gdAmountWei), 0n);
}

// Race audit. `/stream-credits` keyed its entry `stream:<day>:<account>` and `/v1/celo/events/record`
// keyed a termination `<txHash>:<logIndex>`. Both priced the same window from the same
// `lastStreamCreditAt`, which only moves once funding lands -- so fired together, each saw the stale
// clock and each funded the full tail under its own id. On-chain `DuplicateDepositId` cannot help:
// the ids differ. Both endpoints are unauthenticated, so any streamer could double their credit.
test("concurrent user-triggered requests cannot double-credit a stream window", { concurrency: false }, async () => {
  const account = "0x0000000000000000000000000000000000000b01";
  const scenario = closedStreamScenario();
  const kv = new MemoryKV();
  await seedProfile(kv, account, scenario.lastCreditAt);
  const txHash = `0x${"e".repeat(64)}`;
  const closeLog = encodeVaultEventLog("StreamUpdated", [account, account, 0n, 0n, RATE * BigInt(30 * DAY)], VAULT, txHash, 0);

  const originalFetch = globalThis.fetch;
  const originalLog = console.log;
  const onChain = stubOnChainFunding();
  try {
    console.log = (() => {}) as typeof console.log;
    globalThis.fetch = subgraphMock(account, scenario, closeLog);
    const env = testEnv(kv);
    const { ctx } = trackingContext();

    // The second request lands while the first is still waiting for its Base deposit to confirm.
    await Promise.all([
      worker.fetch(
        new Request("https://worker.test/v1/celo/events/record", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ txHash })
        }),
        env,
        ctx
      ),
      new Promise((resolve) => setTimeout(resolve, 10)).then(() =>
        worker.fetch(new Request(`https://worker.test/v1/accounts/${account}/stream-credits`, { method: "POST" }), env, ctx)
      )
    ]);

    const credited = totalFundedGdWei(await new KVCreditStore(kv as never).getGdCredits(account));
    assert.ok(credited <= scenario.owedWei, `credited ${credited} G$ wei across ${onChain.funded.size} on-chain deposits, owed ${scenario.owedWei}`);
  } finally {
    onChain.restore();
    globalThis.fetch = originalFetch;
    console.log = originalLog;
  }
});

// Two scheduled runs overlapping (a slow run still funding when the next tick fires, or a manual
// trigger) both read the same stale clock. Keying the deposit id on the window's start rather than
// on the calendar day makes them collide on the id, so the contract funds the window once.
test("overlapping scheduled runs fund a stream window once", { concurrency: false }, async () => {
  const account = "0x0000000000000000000000000000000000000b02";
  const scenario = closedStreamScenario();
  const kv = new MemoryKV();
  await seedProfile(kv, account, scenario.lastCreditAt);

  const originalFetch = globalThis.fetch;
  const originalLog = console.log;
  const onChain = stubOnChainFunding();
  try {
    console.log = (() => {}) as typeof console.log;
    globalThis.fetch = subgraphMock(account, scenario, undefined);
    const env = testEnv(kv);
    const first = trackingContext();
    const second = trackingContext();
    const event = { scheduledTime: Date.now(), cron: "0 */6 * * *" } as unknown as ScheduledEvent;

    await Promise.all([worker.scheduled(event, env, first.ctx), worker.scheduled(event, env, second.ctx)]);
    await Promise.all([first.settle(), second.settle()]);

    assert.equal(onChain.funded.size, 1, `expected one on-chain deposit, got ${[...onChain.funded.keys()].join(", ")}`);

    // On-chain dedupe alone is not enough: KV has to agree with what was actually paid, or the
    // profile drifts even though the money is right.
    const store = new KVCreditStore(kv as never);
    const entries = await store.getGdCredits(account);
    const streamEntries = entries.filter((entry) => entry.source === "streamCron");
    assert.equal(streamEntries.length, 1, "one window, one entry");
    assert.equal(streamEntries[0].fundingStatus, "funded");

    const profile = await store.getUser(account);
    assert.equal(profile.totalGDStreamedWei, streamEntries[0].gdAmountWei, "the window is counted once, not twice");
    assert.equal(profile.totalPrincipalUsd, streamEntries[0].principalUsd);
    assert.equal(profile.totalOutstandingFundingUsd, "0", "nothing left owed once the deposit landed");
    assert.equal(profile.lastStreamCreditAt, streamEntries[0].streamCreditedThrough, "the clock advanced to the settled boundary, once");
  } finally {
    onChain.restore();
    globalThis.fetch = originalFetch;
    console.log = originalLog;
  }
});

// A window cut at a buyer change never grows: `through` is pinned to the change, not to `asOf`. So
// if the dust minimum applied to it, a small segment would be skipped on every run, the clock would
// never advance, and nothing after the change would ever be credited. The account would be stuck.
test("a live stream still settles a sub-minimum segment ending at a buyer change", { concurrency: false }, async () => {
  const account = "0x0000000000000000000000000000000000000b03";
  const nowSeconds = Math.floor(Date.now() / 1000);
  const changedAt = nowSeconds - 29 * DAY; // one day after the last credit: a small segment
  const lastCreditAt = new Date((nowSeconds - 30 * DAY) * 1000).toISOString();
  const kv = new MemoryKV();
  await seedProfile(kv, account, lastCreditAt);

  const originalFetch = globalThis.fetch;
  const originalLog = console.log;
  const onChain = stubOnChainFunding();
  try {
    console.log = (() => {}) as typeof console.log;
    globalThis.fetch = (async (_url: string | URL | Request, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body ?? "{}"));
      if (typeof body.query === "string" && body.query.includes("streamPeriods")) {
        return Response.json({
          data: {
            _meta: { block: { timestamp: nowSeconds } },
            streamPeriods: [
              // One day under the old buyer, then 29 days still running under a new one.
              {
                id: `${account}-period-0`,
                sender: { id: account },
                flowRate: RATE.toString(),
                startedAtTimestamp: (nowSeconds - 40 * DAY).toString(),
                stoppedAtTimestamp: changedAt.toString(),
                startedAtEvent: { userData: "0x" }
              },
              {
                id: `${account}-period-1`,
                sender: { id: account },
                flowRate: RATE.toString(),
                startedAtTimestamp: changedAt.toString(),
                stoppedAtTimestamp: null,
                startedAtEvent: { userData: `0x${"00".repeat(12)}${"bb".repeat(20)}` }
              }
            ]
          }
        });
      }
      if (body.method === "eth_call") {
        return Response.json({ jsonrpc: "2.0", id: body.id, result: "0x000000000000000000000000abababababababababababababababababababab" });
      }
      return Response.json({ jsonrpc: "2.0", id: body.id, result: "0x" });
    }) as typeof fetch;

    const env = testEnv(kv);
    const run = trackingContext();
    await worker.scheduled({ scheduledTime: Date.now(), cron: "0 */6 * * *" } as unknown as ScheduledEvent, env, run.ctx);
    await run.settle();

    const store = new KVCreditStore(kv as never);
    const entries = (await store.getGdCredits(account)).filter((entry) => entry.source === "streamCron");
    assert.equal(entries.length, 1, "the short pre-change segment is settled rather than skipped as dust");
    const owed = RATE * BigInt(changedAt - (nowSeconds - 30 * DAY));
    assert.equal(BigInt(entries[0].gdAmountWei), owed);
    assert.ok(owed < 4_000n * 10n ** 18n, "and it really is below the 4000 G$ minimum");
  } finally {
    onChain.restore();
    globalThis.fetch = originalFetch;
    console.log = originalLog;
  }
});
