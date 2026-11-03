import test from "node:test";
import assert from "node:assert/strict";
import { Interface } from "ethers";
import { encodeVaultEventLog, fetchCeloVaultEvents, fetchCurrentGdPrice, fetchGoodIdRoot, parseCeloVaultLogs, decodeBuyerFromUserData } from "../src/celo-events.js";
import { configFromEnv } from "../src/env.js";
import type { FailureDetails } from "../src/http.js";

const vault = "0x0000000000000000000000000000000000000abc";
const account = "0x0000000000000000000000000000000000000def";
const txHash = "0x" + "11".repeat(32);

test("receipt verification distinguishes reverted, pending and successful transactions", async () => {
  const originalFetch = globalThis.fetch;
  const cfg = configFromEnv({ ANTSEED_KV: {} as KVNamespace, CELO_RPC_URL: "https://celo.test", CELO_VAULT_ADDRESS: vault });
  try {
    for (const [result, name] of [
      [{ status: "0x0", logs: [] }, "CeloTransactionReverted"],
      [null, "CeloReceiptPending"]
    ] as const) {
      globalThis.fetch = (async () => Response.json({ result })) as typeof fetch;
      await assert.rejects(fetchCeloVaultEvents(txHash, cfg), { name });
    }
    globalThis.fetch = (async () => Response.json({ result: { status: "0x1", logs: [] } })) as typeof fetch;
    assert.deepEqual(await fetchCeloVaultEvents(txHash, cfg), []);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("vault ingestion does not report skipped malformed or unrelated logs", async () => {
  const originalFetch = globalThis.fetch;
  const failures: FailureDetails[] = [];
  const cfg = configFromEnv({ ANTSEED_KV: {} as KVNamespace, CELO_RPC_URL: "https://celo.test", CELO_VAULT_ADDRESS: vault });
  cfg.reportFailure = async (details) => { failures.push(details); };
  const goodLog = encodeVaultEventLog("GdDeposited", [account, account, 1n, "0x"], vault, txHash, 1);
  const badLog = { ...goodLog, data: "0x" };
  const unrelated = { ...badLog, topics: [`0x${"ff".repeat(32)}`] };
  try {
    globalThis.fetch = (async () => Response.json({ result: { status: "0x1", logs: [goodLog, badLog, badLog, unrelated] } })) as typeof fetch;
    assert.equal((await fetchCeloVaultEvents(txHash, cfg)).length, 1);
    assert.equal(failures.length, 0);
    globalThis.fetch = (async () => Response.json({ result: { status: "0x1", logs: [unrelated] } })) as typeof fetch;
    assert.deepEqual(await fetchCeloVaultEvents(txHash, cfg), []);
    assert.equal(failures.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("oracle only reports exceptions caught by the existing fallback handler", async () => {
  const originalFetch = globalThis.fetch;
  const failures: FailureDetails[] = [];
  const cfg = configFromEnv({ ANTSEED_KV: {} as KVNamespace, CELO_RPC_URL: "https://celo.test", CELO_GD_SUPERTOKEN_ADDRESS: account });
  cfg.reportFailure = async (details) => { failures.push(details); };
  const oracle = new Interface(["function quoteAllAvailablePoolsWithTimePeriod(uint128,address,address,uint32) view returns (uint256,address[])"]);
  try {
    for (const result of ["0x", "0x12", oracle.encodeFunctionResult("quoteAllAvailablePoolsWithTimePeriod", [0n, []])]) {
      globalThis.fetch = (async () => Response.json({ result })) as typeof fetch;
      assert.equal(await fetchCurrentGdPrice(cfg), cfg.GD_CUSD_PRICE);
    }
    globalThis.fetch = (async () => { throw new Error("RPC offline"); }) as typeof fetch;
    assert.equal(await fetchCurrentGdPrice(cfg), cfg.GD_CUSD_PRICE);
    assert.equal(failures.length, 2);
    assert.ok(failures.every((failure) => failure.operation === "celo.oracle.price" && failure.recovered));
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("empty GoodID data and encoded zero root retain the existing unverified result", async () => {
  const originalFetch = globalThis.fetch;
  const cfg = configFromEnv({ ANTSEED_KV: {} as KVNamespace, CELO_RPC_URL: "https://celo.test", CELO_GOODID_ADDRESS: vault });
  try {
    globalThis.fetch = (async () => Response.json({ result: "0x" })) as typeof fetch;
    assert.equal(await fetchGoodIdRoot(account, cfg), undefined);
    globalThis.fetch = (async () => Response.json({ result: `0x${"00".repeat(32)}` })) as typeof fetch;
    assert.equal(await fetchGoodIdRoot(account, cfg), undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("parses verified Celo vault GdDeposited logs into credit principal", () => {
  const buyer = "0x0000000000000000000000000000000000000aaa";
  const log = encodeVaultEventLog("GdDeposited", [account, buyer, 2_000_000_000_000_000_000n, "0x1234"], vault, txHash, 7);
  const events = parseCeloVaultLogs([log], vault);

  assert.equal(events.length, 1);
  assert.equal(events[0].kind, "deposit");
  if (events[0].kind === "deposit") {
    assert.equal(events[0].account.toLowerCase(), account.toLowerCase());
    assert.equal(events[0].buyer.toLowerCase(), buyer.toLowerCase());
    assert.equal(events[0].gdAmountWei, 2_000_000_000_000_000_000n);
    assert.equal(events[0].logIndex, 7);
  }
});

test("fetches GoodID root with eth_call for root aggregation", async () => {
  const goodId = new Interface(["function getWhitelistedRoot(address) view returns (address)"]);
  const root = "0x0000000000000000000000000000000000000aaa";
  const previousFetch = globalThis.fetch;
  globalThis.fetch = (async (_url: string | URL | Request, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    assert.equal(body.method, "eth_call");
    return Response.json({
      jsonrpc: "2.0",
      id: body.id,
      result: goodId.encodeFunctionResult("getWhitelistedRoot", [root])
    });
  }) as typeof fetch;

  try {
    const fetchedRoot = await fetchGoodIdRoot(account, {
      GD_CUSD_PRICE: 1.0,
      CELO_RPC_URL: "https://celo.example",
      CELO_GOODID_ADDRESS: "0x0000000000000000000000000000000000000abc",
      MAX_BONUS_CAP_USD: 100_000_000n,
      REGULAR_BONUS_BPS: 1_000n,
      STREAMING_BONUS_BPS: 2_000n,
      MIN_STREAM_BONUS_WEI: 4_000_000_000_000_000_000_000n
    });
    assert.equal(fetchedRoot, root);
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test("parses Celo vault StreamUpdated logs", () => {
  const buyer = "0x0000000000000000000000000000000000000bbb";
  const flowRate = 38580246913580n;
  const monthly = flowRate * BigInt(30 * 24 * 60 * 60);
  const totalFlow = flowRate * 3600n;
  const log = encodeVaultEventLog("StreamUpdated", [account, buyer, flowRate, monthly, totalFlow], vault, txHash, 2);
  const events = parseCeloVaultLogs([log], vault);

  assert.equal(events.length, 1);
  assert.equal(events[0].kind, "stream");
  if (events[0].kind === "stream") {
    assert.equal(events[0].buyer.toLowerCase(), buyer.toLowerCase());
    assert.equal(events[0].flowRateWeiPerSecond, flowRate);
    assert.equal(events[0].monthlyGdAmountWei, monthly);
    assert.equal(events[0].totalFlowWei, totalFlow);
  }
});

test("decodeBuyerFromUserData decodes abi-encoded address from Superfluid userData", () => {
  const buyer = "0x000000000000000000000000000000000000bEEF";
  // Simulate abi.encode(address): 12 bytes padding + 20 bytes address = 32 bytes
  const encoded = "0x" + "00".repeat(12) + buyer.slice(2).toLowerCase();
  const decoded = decodeBuyerFromUserData(encoded);
  assert.equal(decoded, buyer.toLowerCase());

  assert.equal(decodeBuyerFromUserData(undefined), undefined);
  assert.equal(decodeBuyerFromUserData("0x"), undefined);
  assert.equal(decodeBuyerFromUserData("0x" + "00".repeat(32)), undefined); // zero address
});

test("fetches GD price from StaticOracle quoteAllAvailablePoolsWithTimePeriod", async () => {
  const oracleAbi = new Interface([
    "function quoteAllAvailablePoolsWithTimePeriod(uint128 baseAmount, address baseToken, address quoteToken, uint32 period) view returns (uint256 quoteAmount, address[] queriedPools)"
  ]);
  const gdToken = "0x62B8B11039FcfE5aB0C56E502b1C372A3d2a9c7A";
  const previousFetch = globalThis.fetch;
  // oracle returns 1154299954649337 cUSD wei for 1 G$ ≈ 0.001154 cUSD
  const quoteWei = 1154299954649337n;
  globalThis.fetch = (async (_url: string | URL | Request, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    assert.equal(body.method, "eth_call");
    return Response.json({
      jsonrpc: "2.0",
      id: body.id,
      result: oracleAbi.encodeFunctionResult("quoteAllAvailablePoolsWithTimePeriod", [quoteWei, []])
    });
  }) as typeof fetch;

  try {
    const price = await fetchCurrentGdPrice({
      GD_CUSD_PRICE: 0.001,
      CELO_RPC_URL: "https://celo.example",
      CELO_GD_SUPERTOKEN_ADDRESS: gdToken,
      MAX_BONUS_CAP_USD: 100_000_000n,
      REGULAR_BONUS_BPS: 1_000n,
      STREAMING_BONUS_BPS: 2_000n,
      MIN_STREAM_BONUS_WEI: 4_000_000_000_000_000_000_000n
    });
    // 1154299954649337 / 1e18 ≈ 0.001154299...
    assert.ok(price > 0.001154 && price < 0.001155, `expected ~0.001154 but got ${price}`);
  } finally {
    globalThis.fetch = previousFetch;
  }
});
