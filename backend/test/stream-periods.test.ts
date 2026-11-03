import assert from "node:assert/strict";
import test from "node:test";
import { RuntimeConfig } from "../src/env.js";
import { StreamPeriod, earliestUnsettledStop, fetchStreamPeriods, liveFlowRate, nextBuyerWindow, streamOwedWei } from "../src/stream-periods.js";

const period = (flowRate: bigint, startedAt: number, stoppedAt?: number, buyerAddress?: string): StreamPeriod => ({
  id: `p-${startedAt}`,
  account: "0xabc",
  flowRateWeiPerSecond: flowRate,
  startedAt,
  stoppedAt,
  buyerAddress
});

test("streamOwedWei prices each period's overlap with the uncredited window", () => {
  const periods = [period(10n, 0, 100), period(5n, 100)];
  // Window (50, 200]: 50s at 10 + 100s at 5.
  assert.equal(streamOwedWei(periods, 50, 200), 10n * 50n + 5n * 100n);
});

test("streamOwedWei earns nothing in a gap between periods", () => {
  const periods = [period(10n, 0, 100), period(10n, 500)];
  assert.equal(streamOwedWei(periods, 50, 600), 10n * 50n + 10n * 100n);
});

test("streamOwedWei credits a first-time account from the stream's start, never past asOf", () => {
  assert.equal(streamOwedWei([period(7n, 1000)], 0, 1100), 700n);
  assert.equal(streamOwedWei([period(7n, 1000)], 1200, 1100), 0n, "a clock ahead of the head owes nothing");
});

test("liveFlowRate and earliestUnsettledStop", () => {
  const periods = [period(10n, 0, 100, "0xold"), period(3n, 200, 300, "0xmid"), period(4n, 400, undefined, "0xnew")];
  assert.equal(liveFlowRate(periods), 4n);
  assert.equal(earliestUnsettledStop(periods, 50), 100);
  assert.equal(earliestUnsettledStop(periods, 100), 300);
  assert.equal(earliestUnsettledStop(periods, 300), undefined);
});

// A flow update can name a different AntSeed buyer, and the vault stores it
// (`streamBuyer[sender] = buyer` runs on update too). Crediting the whole owed span to one buyer
// would pay the earlier periods to whoever was named last.
test("nextBuyerWindow cuts the window at a buyer change", () => {
  const periods: StreamPeriod[] = [
    { id: "p1", account: "0xabc", flowRateWeiPerSecond: 100n, startedAt: 1_000, stoppedAt: 2_000, buyerAddress: "0xold" },
    { id: "p2", account: "0xabc", flowRateWeiPerSecond: 100n, startedAt: 2_000, buyerAddress: "0xnew" }
  ];

  const first = nextBuyerWindow(periods, 0, 5_000);
  assert.equal(first?.buyerAddress, "0xold", "the earlier periods pay the buyer that was in force");
  assert.equal(first?.through, 2_000, "window ends where the buyer changed");
  assert.equal(first?.owedWei, 100n * 1_000n, "p1 ran from 1000 to 2000");

  // The next run picks up from there and pays the new buyer.
  const second = nextBuyerWindow(periods, 2_000, 5_000);
  assert.equal(second?.buyerAddress, "0xnew");
  assert.equal(second?.through, 5_000);
  assert.equal(second?.owedWei, 100n * 3_000n);
});

test("nextBuyerWindow runs to asOf when the buyer never changes", () => {
  const periods: StreamPeriod[] = [
    { id: "p1", account: "0xabc", flowRateWeiPerSecond: 100n, startedAt: 1_000, stoppedAt: 2_000, buyerAddress: "0xsame" },
    { id: "p2", account: "0xabc", flowRateWeiPerSecond: 200n, startedAt: 2_000, buyerAddress: "0xSAME" } // case differs only
  ];

  const window = nextBuyerWindow(periods, 0, 4_000);
  assert.equal(window?.through, 4_000);
  assert.equal(window?.owedWei, 100n * 1_000n + 200n * 2_000n, "each rate priced for its own span");
});

// The window's start, which is what the cooldown must be aged from. Measuring from the clock instead
// makes `asOf - clock` the whole Unix epoch on a first credit, so a 24h cooldown always passes.
test("nextBuyerWindow reports where the window begins", () => {
  const periods = [period(100n, 1_000, 2_000), period(100n, 2_000)];
  assert.equal(nextBuyerWindow(periods, 0, 5_000)?.from, 1_000, "a first credit begins at the stream, not at the epoch");
  assert.equal(nextBuyerWindow(periods, 3_000, 5_000)?.from, 3_000, "mid-stream it begins at the clock");
  // A stream reopened after a long gap: the stale clock predates the new period, so the window begins
  // at the reopen and a cooldown aged from it still holds.
  assert.equal(nextBuyerWindow([period(100n, 9_000)], 1_000, 9_060)?.from, 9_000, "a reopen begins at the new period");
});

test("nextBuyerWindow returns undefined when nothing is owed", () => {
  const periods: StreamPeriod[] = [{ id: "p1", account: "0xabc", flowRateWeiPerSecond: 100n, startedAt: 1_000, stoppedAt: 2_000 }];
  assert.equal(nextBuyerWindow(periods, 2_000, 5_000), undefined);
});

const periodsCfg = {
  CELO_VAULT_ADDRESS: "0x4Dd0136b9aabD5823cf0F65d89e8fB882C660885",
  CELO_GD_SUPERTOKEN_ADDRESS: "0x62B8B11039FcfE5aB0C56E502b1C372A3d2a9c7A",
  SUPERFLUID_SUBGRAPH_URL: "https://subgraph.test/periods"
} as unknown as RuntimeConfig;

function subgraphResponding(streamPeriods: unknown[], blockTimestamp = 1_800_000_000) {
  return (async () => Response.json({ data: { _meta: { block: { timestamp: blockTimestamp } }, streamPeriods } })) as typeof fetch;
}

const rawPeriod = (overrides: Record<string, unknown> = {}) => ({
  id: "0xstream-0",
  sender: { id: "0x0000000000000000000000000000000000000abc" },
  flowRate: "1000000",
  startedAtTimestamp: "1700000000",
  stoppedAtTimestamp: null,
  startedAtEvent: { userData: "0x" },
  ...overrides
});

test("fetchStreamPeriods reads a well-formed snapshot", async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = subgraphResponding([rawPeriod()]);
    const snapshot = await fetchStreamPeriods(periodsCfg, 0);
    assert.equal(snapshot?.periods.length, 1);
    assert.equal(snapshot?.periods[0].startedAt, 1_700_000_000);
    assert.equal(snapshot?.asOf, 1_800_000_000);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

// A zero start would be read as 1970. On an account's first credit `creditedThrough` is 0, so
// nothing else clamps the window and the period would be priced from the epoch -- the shape that
// produced a 6,191,029 G$ entry once already. The whole snapshot must be refused, not priced.
test("fetchStreamPeriods refuses a snapshot carrying an epoch-zero timestamp", async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = subgraphResponding([rawPeriod({ startedAtTimestamp: "0" })]);
    assert.equal(await fetchStreamPeriods(periodsCfg, 0), undefined);

    globalThis.fetch = subgraphResponding([rawPeriod({ stoppedAtTimestamp: "0" })]);
    assert.equal(await fetchStreamPeriods(periodsCfg, 0), undefined, "a zero stop is equally unusable");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("fetchStreamPeriods refuses a timestamp beyond safe-integer range", async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = subgraphResponding([rawPeriod({ startedAtTimestamp: "99999999999999999999" })]);
    assert.equal(await fetchStreamPeriods(periodsCfg, 0), undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("fetchStreamPeriods only reports exceptions caught by the existing handler", async () => {
  const originalFetch = globalThis.fetch;
  const failures: string[] = [];
  const cfg = { ...periodsCfg, reportFailure: async (details: { operation: string }) => { failures.push(details.operation); } };
  try {
    const responses = [
      async () => new Response("unavailable", { status: 503 }),
      async () => { throw new Error("network offline"); },
      async () => Response.json({ data: {} })
    ];
    for (const response of responses) {
      globalThis.fetch = response as typeof fetch;
      assert.equal(await fetchStreamPeriods(cfg, 42), undefined);
    }
    assert.deepEqual(failures, new Array(3).fill("celo.streams.subgraph"));
    globalThis.fetch = (async () => Response.json({ errors: [{ message: "partial indexing error" }], data: { _meta: { block: { timestamp: 1_800_000_000 } }, streamPeriods: [rawPeriod()] } })) as typeof fetch;
    assert.equal((await fetchStreamPeriods(cfg, 42))?.periods.length, 1, "no new GraphQL validation is introduced");
    globalThis.fetch = subgraphResponding([]);
    assert.deepEqual((await fetchStreamPeriods(cfg, 42))?.periods, []);
    assert.equal(failures.length, 3, "legitimate empty snapshots are quiet");
  } finally {
    globalThis.fetch = originalFetch;
  }
});
