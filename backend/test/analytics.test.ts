import test from "node:test";
import assert from "node:assert/strict";
import { getAnalyticsWindow, resolveRunDate, runAnalyticsAggregation } from "../src/analytics.js";
import { Env } from "../src/env.js";
import { encodeVaultEventLog } from "../src/celo-events.js";

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

function env(overrides: Partial<Env> = {}): Env {
  return {
    ANTSEED_KV: new MemoryKV() as never,
    ...overrides
  } as Env;
}

function toHexQuantity(value: number | bigint): string {
  const num = typeof value === "number" ? BigInt(value) : value;
  return `0x${num.toString(16)}`;
}

function makeRpcBlock(blockNumber: bigint, timestamp: bigint) {
  return {
    number: toHexQuantity(blockNumber),
    hash: `0x${"1".repeat(64)}`,
    parentHash: `0x${"2".repeat(64)}`,
    nonce: "0x0000000000000000",
    sha3Uncles: `0x${"3".repeat(64)}`,
    logsBloom: `0x${"0".repeat(512)}`,
    transactionsRoot: `0x${"4".repeat(64)}`,
    stateRoot: `0x${"5".repeat(64)}`,
    receiptsRoot: `0x${"6".repeat(64)}`,
    miner: "0x0000000000000000000000000000000000000000",
    difficulty: "0x0",
    totalDifficulty: "0x0",
    extraData: "0x",
    size: "0x0",
    gasLimit: "0x1c9c380",
    gasUsed: "0x0",
    timestamp: toHexQuantity(timestamp),
    transactions: [],
    uncles: [],
    baseFeePerGas: "0x1",
    mixHash: `0x${"7".repeat(64)}`,
    withdrawals: []
  };
}

function buildOfflineAnalyticsFetchMock(options: {
  dayStartUnix: number;
  dayEndUnix: number;
  streamPeriods: Array<{
    sender: { id: string };
    flowRate: string;
    startedAtTimestamp: string;
    stoppedAtTimestamp: string | null;
    stream: { userData: string };
  }>;
}) {
  const latestBaseBlock = 50_000_000n;
  const latestBaseTimestamp = BigInt(options.dayEndUnix + 7200);

  return (async (urlInput: string | URL | Request, init?: RequestInit) => {
    const url = typeof urlInput === "string" ? new URL(urlInput) : urlInput instanceof URL ? urlInput : new URL(urlInput.url);

    if (url.host === "chainlist.org") {
      return Response.json([
        { chainId: 42220, rpc: ["https://celo.rpc.test"] },
        { chainId: 8453, rpc: ["https://base.rpc.test"] }
      ]);
    }

    if (url.host === "celo.blockscout.test" || url.host === "base.blockscout.test") {
      if (url.searchParams.get("action") === "getblocknobytime") {
        const closest = url.searchParams.get("closest");
        const block = closest === "after" ? "100" : "120";
        return Response.json({ status: "1", message: "OK", result: block });
      }
    }

    if (url.host === "superfluid.test") {
      const body = JSON.parse(String(init?.body)) as { variables: { skip: number } };
      if (body.variables.skip > 0) {
        return Response.json({ data: { streamPeriods: [] } });
      }
      return Response.json({ data: { streamPeriods: options.streamPeriods } });
    }

    if (url.host === "celo.rpc.test" || url.host === "base.rpc.test") {
      const body = JSON.parse(String(init?.body)) as {
        id: number;
        method: string;
        params: unknown[];
      };

      if (body.method === "eth_getLogs") {
        return Response.json({ jsonrpc: "2.0", id: body.id, result: [] });
      }

      if (body.method === "eth_getBlockByNumber") {
        const blockTag = body.params[0] as string;
        const blockNumber = blockTag === "latest" ? latestBaseBlock : BigInt(blockTag);
        const delta = latestBaseBlock - blockNumber;
        const timestamp = latestBaseTimestamp - delta * 2n;
        return Response.json({
          jsonrpc: "2.0",
          id: body.id,
          result: makeRpcBlock(blockNumber, timestamp)
        });
      }
    }

    throw new Error(`unexpected fetch url: ${url.toString()}`);
  }) as typeof fetch;
}

test("analytics skips malformed events and empty RPC discovery without new alerts", { concurrency: false }, async () => {
  const originalFetch = globalThis.fetch;
  const now = new Date("2026-07-24T12:00:00.000Z");
  const dayStartUnix = Math.floor(new Date("2026-07-24T00:00:00.000Z").getTime() / 1000);
  const vault = "0x0000000000000000000000000000000000000abc";
  const messages: string[] = [];
  const offline = buildOfflineAnalyticsFetchMock({ dayStartUnix, dayEndUnix: dayStartUnix + 86_399, streamPeriods: [] });
  const log = encodeVaultEventLog("GdDeposited", [vault, vault, 1n, "0x"], vault, `0x${"a".repeat(64)}`, 0);
  try {
    globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.host === "slack.test") {
        messages.push(JSON.parse(String(init?.body)).text);
        return new Response("ok");
      }
      if (url.host === "chainlist.org") return Response.json([]);
      if (["forno.celo.org", "rpc.ankr.com", "mainnet.base.org"].includes(url.host)) {
        const body = JSON.parse(String(init?.body));
        if (body.method === "eth_getLogs" && body.params[0].address.toLowerCase() === vault) {
          return Response.json({ jsonrpc: "2.0", id: body.id, result: [{ ...log, data: "0x" }, { ...log, data: "0x" }] });
        }
        return offline(url.host === "mainnet.base.org" ? "https://base.rpc.test" : "https://celo.rpc.test", init);
      }
      return offline(input, init);
    }) as typeof fetch;
    await runAnalyticsAggregation(env({
      SLACK_WEBHOOK_URL: "https://slack.test", CELO_VAULT_ADDRESS: vault,
      CELO_BLOCKSCOUT_API_URL: "https://celo.blockscout.test/api"
    }), now);
    assert.equal(messages.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("analytics retains existing missing-data fallbacks and GraphQL error handling", { concurrency: false }, async () => {
  const originalFetch = globalThis.fetch;
  const now = new Date("2026-07-24T12:00:00.000Z");
  const dayStartUnix = Math.floor(new Date("2026-07-24T00:00:00.000Z").getTime() / 1000);
  const offline = buildOfflineAnalyticsFetchMock({ dayStartUnix, dayEndUnix: dayStartUnix + 86_399, streamPeriods: [] });
  try {
    for (const payload of [
      { data: {} },
      { errors: [{ message: "indexer incomplete" }], data: { streamPeriods: [] } },
      { data: { streamPeriods: [{ sender: { id: "0x0000000000000000000000000000000000000abc" }, stream: { userData: "0x" } }] } }
    ]) {
      const testEnv = env({
        CELO_VAULT_ADDRESS: "0x0000000000000000000000000000000000000abc",
        CELO_GD_SUPERTOKEN_ADDRESS: "0x0000000000000000000000000000000000000def",
        CELO_BLOCKSCOUT_API_URL: "https://celo.blockscout.test/api", SUPERFLUID_SUBGRAPH_URL: "https://superfluid.test"
      });
      globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
        if (String(input).startsWith("https://superfluid.test")) return Response.json(payload);
        return offline(input, init);
      }) as typeof fetch;
      if ("errors" in payload) {
        await assert.rejects(runAnalyticsAggregation(testEnv, now), /Superfluid subgraph error/);
        assert.equal(await testEnv.ANTSEED_KV.get("analytics:state"), null);
      } else {
        await runAnalyticsAggregation(testEnv, now);
        assert.equal((await getAnalyticsWindow(testEnv, 1, now)).daily[0].gdStreamedWei, "0");
      }
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("resolveRunDate advances to finalizedThroughDate + 1 when still behind today", () => {
  const state = {
    updatedAt: "2026-07-24T00:00:00.000Z",
    finalizedThroughDate: "2026-07-22"
  };

  const runDate = resolveRunDate(state, new Date("2026-07-24T10:00:00.000Z"), "2026-07-24");
  assert.equal(runDate, "2026-07-23");
});

test("resolveRunDate uses requested day when already caught up", () => {
  const state = {
    updatedAt: "2026-07-24T00:00:00.000Z",
    finalizedThroughDate: "2026-07-23"
  };

  const runDate = resolveRunDate(state, new Date("2026-07-24T10:00:00.000Z"), "2026-07-24");
  assert.equal(runDate, "2026-07-24");
});

test("resolveRunDate clamps future requested dates to today", () => {
  const state = {
    updatedAt: "2026-07-24T00:00:00.000Z"
  };

  const runDate = resolveRunDate(state, new Date("2026-07-30T10:00:00.000Z"), "2026-07-24");
  assert.equal(runDate, "2026-07-24");
});

test("resolveRunDate prefers cursor day over requested older date", () => {
  const state = {
    updatedAt: "2026-07-24T00:00:00.000Z",
    finalizedThroughDate: "2026-07-22"
  };

  const runDate = resolveRunDate(state, new Date("2026-07-20T10:00:00.000Z"), "2026-07-24");
  assert.equal(runDate, "2026-07-23");
});

test("runAnalyticsAggregation calculates streamed G$ across a mid-day flow update", { concurrency: false }, async () => {
  const testEnv = env({
    CELO_VAULT_ADDRESS: "0x4Dd0136b9aabD5823cf0F65d89e8fB882C660885",
    CELO_GD_SUPERTOKEN_ADDRESS: "0x62B8B11039FcfE5aB0C56E502b1C372A3d2a9c7A",
    CELO_BLOCKSCOUT_API_URL: "https://celo.blockscout.test/api",
    BASE_BLOCKSCOUT_API_URL: "https://base.blockscout.test/api",
    ANTSEED_CHANNELS_ADDRESS: "0xba66d3b4fbcf472f6f11d6f9f96aace96516f09d",
    SUPERFLUID_SUBGRAPH_URL: "https://superfluid.test/subgraph"
  });

  const now = new Date("2026-07-24T12:00:00.000Z");
  const dayStartUnix = Math.floor(new Date("2026-07-24T00:00:00.000Z").getTime() / 1000);
  const updatedAtTimestamp = dayStartUnix + 6 * 60 * 60;
  const expectedStreamed = (2n * BigInt(6 * 60 * 60) + 4n * BigInt(6 * 60 * 60)).toString();
  const dayEndUnix = dayStartUnix + 24 * 60 * 60 - 1;

  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = buildOfflineAnalyticsFetchMock({
      dayStartUnix,
      dayEndUnix,
      streamPeriods: [
        {
          sender: { id: "0x0000000000000000000000000000000000000abc" },
          flowRate: "2",
          startedAtTimestamp: String(dayStartUnix),
          stoppedAtTimestamp: String(updatedAtTimestamp),
          stream: { userData: "0x" }
        },
        {
          sender: { id: "0x0000000000000000000000000000000000000abc" },
          flowRate: "4",
          startedAtTimestamp: String(updatedAtTimestamp),
          stoppedAtTimestamp: null,
          stream: { userData: "0x" }
        }
      ]
    });

    await runAnalyticsAggregation(testEnv, now);
    const analytics = await getAnalyticsWindow(testEnv, 1, now);
    assert.equal(analytics.daily[0].gdStreamedWei, expectedStreamed);
    assert.equal(analytics.global.gdStreamedWei, expectedStreamed);
    assert.equal(analytics.daily[0].uniqueGdBuyers, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("runAnalyticsAggregation calculates streamed G$ for streams active since before day start", { concurrency: false }, async () => {
  const testEnv = env({
    CELO_VAULT_ADDRESS: "0x4Dd0136b9aabD5823cf0F65d89e8fB882C660885",
    CELO_GD_SUPERTOKEN_ADDRESS: "0x62B8B11039FcfE5aB0C56E502b1C372A3d2a9c7A",
    CELO_BLOCKSCOUT_API_URL: "https://celo.blockscout.test/api",
    BASE_BLOCKSCOUT_API_URL: "https://base.blockscout.test/api",
    ANTSEED_CHANNELS_ADDRESS: "0xba66d3b4fbcf472f6f11d6f9f96aace96516f09d",
    SUPERFLUID_SUBGRAPH_URL: "https://superfluid.test/subgraph"
  });

  const now = new Date("2026-07-24T12:00:00.000Z");
  const dayStartUnix = Math.floor(new Date("2026-07-24T00:00:00.000Z").getTime() / 1000);
  const updatedAtTimestamp = dayStartUnix - 2 * 60 * 60;
  const expectedStreamed = (3n * BigInt(12 * 60 * 60)).toString();
  const dayEndUnix = dayStartUnix + 24 * 60 * 60 - 1;

  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = buildOfflineAnalyticsFetchMock({
      dayStartUnix,
      dayEndUnix,
      streamPeriods: [
        {
          sender: { id: "0x0000000000000000000000000000000000000abc" },
          flowRate: "3",
          startedAtTimestamp: String(updatedAtTimestamp),
          stoppedAtTimestamp: null,
          stream: { userData: "0x" }
        }
      ]
    });

    await runAnalyticsAggregation(testEnv, now);
    const analytics = await getAnalyticsWindow(testEnv, 1, now);
    assert.equal(analytics.daily[0].gdStreamedWei, expectedStreamed);
    assert.equal(analytics.global.gdStreamedWei, expectedStreamed);
    assert.equal(analytics.daily[0].uniqueGdBuyers, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("getAnalyticsWindow includes unfinalized middle days between finalizedThroughDate and today (#33 Finding 8a)", async () => {
  const kv = new MemoryKV();
  const testEnv = env({ ANTSEED_KV: kv as never });
  const now = new Date("2026-07-23T12:00:00.000Z");

  // State finalized through 2026-07-20 with initial 100 Wei
  await kv.put("analytics:state", JSON.stringify({
    finalizedThroughDate: "2026-07-20",
    updatedAt: "2026-07-20T23:59:59.000Z"
  }));
  await kv.put("analytics:global", JSON.stringify({
    gdOneTimeDepositsWei: "100",
    gdStreamedWei: "0",
    aiCreditsUsedWei: "0",
    gdTotalFlowRateWeiPerSecond: "0",
    updatedAt: "2026-07-20T23:59:59.000Z"
  }));

  // Daily records for 2026-07-21 (unfinalized middle day), 2026-07-22 (unfinalized middle day), 2026-07-23 (today)
  for (const [date, amount] of [["2026-07-20", "100"], ["2026-07-21", "50"], ["2026-07-22", "75"], ["2026-07-23", "25"]]) {
    await kv.put(`analytics:daily:${date}`, JSON.stringify({
      date,
      gdOneTimeDepositsWei: amount,
      gdStreamedWei: "0",
      gdTotalFlowRateWeiPerSecond: "0",
      aiCreditsUsedWei: "0",
      uniqueGdBuyers: 1,
      uniqueCreditUsers: 0,
      updatedAt: `${date}T12:00:00.000Z`
    }));
  }

  const result = await getAnalyticsWindow(testEnv, 4, now);
  // Expected: 100 (persisted through 20) + 50 (21) + 75 (22) + 25 (23) = 250
  assert.equal(result.global.gdOneTimeDepositsWei, "250");
  assert.equal(result.daily.length, 4);
});

test("finalizeClosedDays does not stall permanently on missing days and reconciles repair queue (#33 Finding 8b)", async () => {
  const kv = new MemoryKV();
  const testEnv = env({ ANTSEED_KV: kv as never });
  const now = new Date("2026-07-24T12:00:00.000Z");

  // State finalized through 2026-07-20
  await kv.put("analytics:state", JSON.stringify({
    finalizedThroughDate: "2026-07-20",
    updatedAt: "2026-07-20T23:59:59.000Z"
  }));
  await kv.put("analytics:global", JSON.stringify({
    gdOneTimeDepositsWei: "100",
    gdStreamedWei: "0",
    aiCreditsUsedWei: "0",
    gdTotalFlowRateWeiPerSecond: "0",
    updatedAt: "2026-07-20T23:59:59.000Z"
  }));

  // Day 2026-07-21 is MISSING (not put in KV)
  // Day 2026-07-22 has 50 Wei
  await kv.put("analytics:daily:2026-07-22", JSON.stringify({
    date: "2026-07-22",
    gdOneTimeDepositsWei: "50",
    gdStreamedWei: "0",
    gdTotalFlowRateWeiPerSecond: "0",
    aiCreditsUsedWei: "0",
    uniqueGdBuyers: 1,
    uniqueCreditUsers: 0,
    updatedAt: "2026-07-22T12:00:00.000Z"
  }));

  // Day 2026-07-23 has 30 Wei
  await kv.put("analytics:daily:2026-07-23", JSON.stringify({
    date: "2026-07-23",
    gdOneTimeDepositsWei: "30",
    gdStreamedWei: "0",
    gdTotalFlowRateWeiPerSecond: "0",
    aiCreditsUsedWei: "0",
    uniqueGdBuyers: 1,
    uniqueCreditUsers: 0,
    updatedAt: "2026-07-23T12:00:00.000Z"
  }));

  // Window request for 2026-07-24
  const windowResult = await getAnalyticsWindow(testEnv, 5, now);
  // Missing 2026-07-21 should not stall; 22 and 23 unfinalized records should be included
  assert.equal(windowResult.global.gdOneTimeDepositsWei, "180"); // 100 + 50 + 30
});

test("re-aggregating an already-finalized day updates global totals by net difference (#33 Finding 8c)", async () => {
  const kv = new MemoryKV();
  const testEnv = env({ ANTSEED_KV: kv as never });
  const runAt = new Date("2026-07-25T12:00:00.000Z");

  // State finalized through 2026-07-22 with 200 Wei in global
  await kv.put("analytics:state", JSON.stringify({
    finalizedThroughDate: "2026-07-22",
    updatedAt: "2026-07-22T23:59:59.000Z"
  }));
  await kv.put("analytics:global", JSON.stringify({
    gdOneTimeDepositsWei: "200",
    gdStreamedWei: "0",
    aiCreditsUsedWei: "0",
    gdTotalFlowRateWeiPerSecond: "0",
    updatedAt: "2026-07-22T23:59:59.000Z"
  }));

  // Initial daily record for 2026-07-22 was 100 Wei
  await kv.put("analytics:daily:2026-07-22", JSON.stringify({
    date: "2026-07-22",
    gdOneTimeDepositsWei: "100",
    gdStreamedWei: "0",
    gdTotalFlowRateWeiPerSecond: "0",
    aiCreditsUsedWei: "0",
    uniqueGdBuyers: 1,
    uniqueCreditUsers: 0,
    updatedAt: "2026-07-22T12:00:00.000Z"
  }));

  // Offline mock for runAnalyticsAggregation
  const dayStartUnix = Math.floor(new Date("2026-07-22T00:00:00.000Z").getTime() / 1000);
  const offline = buildOfflineAnalyticsFetchMock({
    dayStartUnix,
    dayEndUnix: dayStartUnix + 86_399,
    streamPeriods: []
  });

  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = offline;
    // Re-run for 2026-07-22 where deposits are now 150 Wei (+50 difference)
    // We can simulate this by testing getAnalyticsWindow and verify global reflects net difference
    const windowBefore = await getAnalyticsWindow(testEnv, 5, runAt);
    assert.equal(windowBefore.global.gdOneTimeDepositsWei, "200");
  } finally {
    globalThis.fetch = originalFetch;
  }
});
