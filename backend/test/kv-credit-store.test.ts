import test from "node:test";
import assert from "node:assert/strict";
import { KVCreditStore } from "../src/kv-credit-store.js";

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

const GD_PRICE = 1.0; // 1 G$ = $1.00 cUSD

test("recordGdCredit persists entry and updates user profile", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "deposit:1",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 10_000_000_000_000_000_000n, // 10 G$
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  assert.equal(entry.account, "0xabc");
  assert.equal(entry.source, "deposit");
  assert.equal(entry.principalUsd, "10000000");
  assert.equal(entry.bonusUsd, "1000000"); // 10% bonus for deposit
  assert.equal(entry.totalCreditUsd, "11000000");
  assert.equal(entry.fundingStatus, "pending");

  // Lifetime G$ only moves once funding lands; until then the credit is merely outstanding.
  const pending = await store.getUser("0xABC");
  assert.equal(pending.totalGdDepositedWei, "0");
  assert.equal(pending.totalOutstandingFundingUsd, "11000000");

  await store.markFundingResult(entry, { funded: true, txHash: "0xfunded" });
  const user = await store.getUser("0xABC");
  assert.equal(user.totalGdDepositedWei, "10000000000000000000");
  assert.equal(user.totalOutstandingFundingUsd, "0");
});

test("recordGdCredit is idempotent on duplicate id", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const first = await store.recordGdCredit({
    id: "deposit:dup",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });
  const second = await store.recordGdCredit({
    id: "deposit:dup",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  assert.deepEqual(first, second);

  const credits = await store.getGdCredits("0xABC");
  assert.equal(credits.length, 1);
});

test("recordGdCredit gives streaming bonus (20%) for stream sources", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "stream:2026-05-28:0xabc",
    account: "0xABC",
    source: "streamRequest",
    gdAmountWei: 1_000_000_000_000_000_000n, // 1 G$
    flowRate: 385_802_469_136n,
    rootAccount: "0xROOT",
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  assert.equal(entry.principalUsd, "1000000");
  assert.equal(entry.bonusUsd, "200000"); // 20% streaming bonus
  assert.equal(entry.totalCreditUsd, "1200000");

  const pending = await store.getUser("0xABC");
  assert.equal(pending.totalGDStreamedWei, "0");
  assert.equal(pending.streamFlowRateWeiPerSecond, "385802469136"); // current rate, not an accrual

  await store.markFundingResult(entry, { funded: true, txHash: "0xfunded" });
  const user = await store.getUser("0xABC");
  assert.equal(user.totalGDStreamedWei, "1000000000000000000");
  assert.equal(user.streamFlowRateWeiPerSecond, "385802469136");
});

test("recordGdCredit gives streaming bonus (20%) for streamUpdate source", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "streamUpdate:2026-05:0xabc",
    account: "0xABC",
    source: "streamUpdate",
    gdAmountWei: 5_000_000_000_000_000_000n, // 5 G$
    flowRate: 1_929_012_345_679n,
    rootAccount: "0xROOT",
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  assert.equal(entry.principalUsd, "5000000");
  assert.equal(entry.bonusUsd, "1000000"); // 20% streaming bonus
  assert.equal(entry.totalCreditUsd, "6000000");

  await store.markFundingResult(entry, { funded: true, txHash: "0xfunded" });
  const user = await store.getUser("0xABC");
  assert.equal(user.totalGDStreamedWei, "5000000000000000000");
  assert.equal(user.streamFlowRateWeiPerSecond, "1929012345679");
});

test("recordGdCredit gives no bonus for unverified accounts", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "deposit:unverified",
    account: "0xABC",
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: false,
    maxBonusCapUsd: 100_000_000n
  });

  assert.equal(entry.principalUsd, "1000000");
  assert.equal(entry.bonusUsd, "0");
  assert.equal(entry.totalCreditUsd, "1000000");
});

test("recordGdCredit enforces monthly bonus cap per root account", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const cap = 500_000n; // $0.50 cap

  // First deposit: gets full bonus ($0.10 on $1)
  const first = await store.recordGdCredit({
    id: "deposit:cap1",
    account: "0xAAA",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: cap
  });
  assert.equal(first.bonusUsd, "100000"); // full 10%

  // Second deposit from different wallet same root: bonus clamped
  const second = await store.recordGdCredit({
    id: "deposit:cap2",
    account: "0xBBB",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 10_000_000_000_000_000_000n, // 10 G$ → would be $1 bonus
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: cap
  });
  assert.equal(second.bonusUsd, "400000"); // capped to remaining $0.40

  // Third deposit: no bonus left
  const third = await store.recordGdCredit({
    id: "deposit:cap3",
    account: "0xCCC",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: cap
  });
  assert.equal(third.bonusUsd, "0");
});

test("markFundingResult updates entry status and user profile on success", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "deposit:fund1",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  assert.equal(entry.fundingStatus, "pending");

  const funded = await store.markFundingResult(entry, { funded: true, txHash: "0xfund123" });
  assert.equal(funded.fundingStatus, "funded");
  assert.equal(funded.fundingTxHash, "0xfund123");

  const user = await store.getUser("0xABC");
  assert.equal(user.totalPrincipalUsd, "1000000");
  assert.equal(user.totalBonusUsd, "100000");
  assert.equal(user.totalOutstandingFundingUsd, "0");
});

test("markFundingResult records failure without crediting totals and clears outstanding", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "deposit:fail1",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  const failed = await store.markFundingResult(entry, { funded: false, error: "tx reverted" });
  assert.equal(failed.fundingStatus, "failed");
  assert.equal(failed.fundingError, "tx reverted");

  const user = await store.getUser("0xABC");
  assert.equal(user.totalPrincipalUsd, "0"); // not updated on failure
  assert.equal(user.totalGdDepositedWei, "0"); // a failed credit must not inflate deposited G$
  assert.equal(user.totalOutstandingFundingUsd, "0"); // terminal, never retried, so no longer owed
});

test("markFundingResult is idempotent for already-funded entries", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "deposit:idem",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  const funded = await store.markFundingResult(entry, { funded: true, txHash: "0xfirst" });
  const again = await store.markFundingResult(funded, { funded: true, txHash: "0xsecond" });
  assert.equal(again.fundingTxHash, "0xfirst"); // unchanged
});

test("recordGdCredit tracks credits under both wallet and root account", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "deposit:root1",
    account: "0xWALLET",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });
  await store.markFundingResult(entry, { funded: true, txHash: "0xfunded" });

  const walletCredits = await store.getGdCredits("0xWALLET");
  const rootCredits = await store.getGdCredits("0xROOT");
  assert.equal(walletCredits.length, 1);
  assert.equal(rootCredits.length, 1);
  assert.equal(walletCredits[0].account, "0xwallet");
  assert.equal(walletCredits[0].rootAccount, "0xroot");

  const walletProfile = await store.getUser("0xWALLET");
  const rootProfile = await store.getUser("0xROOT");
  assert.equal(walletProfile.rootAccount, "0xroot");
  assert.equal(rootProfile.rootAccount, "0xroot");
  assert.equal(walletProfile.totalGdDepositedWei, "1000000000000000000");
  assert.equal(rootProfile.totalGdDepositedWei, "1000000000000000000");
});

test("getUser returns default profile for unknown account", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const user = await store.getUser("0xNOBODY");
  assert.equal(user.account, "0xnobody");
  assert.equal(user.totalGdDepositedWei, "0");
  assert.equal(user.totalPrincipalUsd, "0");
  assert.equal(user.totalBonusUsd, "0");
  assert.equal(user.totalOutstandingFundingUsd, "0");
  assert.equal(user.streamFlowRateWeiPerSecond, "0");
});

test("markFundingResult updates lastStreamCreditAt for stream sources", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "stream:2026-05-28:0xabc",
    account: "0xABC",
    source: "streamCron",
    gdAmountWei: 1_000_000_000_000_000_000n,
    flowRate: 385_802_469_136n,
    rootAccount: "0xROOT",
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  assert.equal(entry.fundingStatus, "pending");
  const before = (await store.getUser("0xABC")).createdAt;
  await new Promise((r) => setTimeout(r, 5));

  await store.markFundingResult(entry, { funded: true, txHash: "0xstream" });
  const after = await store.getUser("0xABC");

  // lastStreamCreditAt should be updated for stream sources
  assert.notEqual(after.lastStreamCreditAt, before);
});

test("updateUser sets updatedAt on recordGdCredit and changes it after markFundingResult", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "deposit:upd1",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  const afterRecord = await store.getUser("0xABC");
  assert.ok(afterRecord.updatedAt, "updatedAt should be set after recordGdCredit");

  // Small delay so the timestamp can differ
  await new Promise((r) => setTimeout(r, 5));

  await store.markFundingResult(entry, { funded: true, txHash: "0xupd" });
  const afterFund = await store.getUser("0xABC");
  assert.ok(
    new Date(afterFund.updatedAt).getTime() >= new Date(afterRecord.updatedAt).getTime(),
    "updatedAt should be updated after markFundingResult"
  );
});

test("updateUser applies the same mutation to both wallet and root account profiles", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const entry = await store.recordGdCredit({
    id: "deposit:upd2",
    account: "0xWALLET",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 5_000_000_000_000_000_000n, // 5 G$ → $5 principal + $0.50 bonus
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  await store.markFundingResult(entry, { funded: true, txHash: "0xupd2" });

  const wallet = await store.getUser("0xWALLET");
  const root = await store.getUser("0xROOT");

  // Both profiles should reflect the same funded totals
  assert.equal(wallet.totalPrincipalUsd, root.totalPrincipalUsd);
  assert.equal(wallet.totalBonusUsd, root.totalBonusUsd);
  assert.equal(wallet.totalOutstandingFundingUsd, root.totalOutstandingFundingUsd);
  assert.equal(wallet.totalOutstandingFundingUsd, "0");

  // Root profile's account field should be the root address
  assert.equal(root.account, "0xroot");
  assert.equal(root.rootAccount, "0xroot");
});

test("updateUser accumulates totals correctly across multiple deposits", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);

  const e1 = await store.recordGdCredit({
    id: "deposit:acc1",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 2_000_000_000_000_000_000n, // 2 G$ → $2 principal + $0.20 bonus
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });
  const e2 = await store.recordGdCredit({
    id: "deposit:acc2",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "deposit",
    gdAmountWei: 3_000_000_000_000_000_000n, // 3 G$ → $3 principal + $0.30 bonus
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });

  // Both pending → outstanding = $2.20 + $3.30 = $5.50
  const pending = await store.getUser("0xABC");
  assert.equal(pending.totalOutstandingFundingUsd, "5500000");

  await store.markFundingResult(e1, { funded: true, txHash: "0xa1" });
  await store.markFundingResult(e2, { funded: true, txHash: "0xa2" });

  const done = await store.getUser("0xABC");
  assert.equal(done.totalPrincipalUsd, "5000000");
  assert.equal(done.totalBonusUsd, "500000");
  assert.equal(done.totalOutstandingFundingUsd, "0");
});

test("getGdCreditHistory paginates filters and sorts newest first", async () => {
  const kv = new MemoryKV();
  const store = new KVCreditStore(kv as never);
  const account = "0xabc";

  await store.recordGdCredit({
    id: "a-old",
    account,
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: false,
    maxBonusCapUsd: 100_000_000n
  });
  await store.recordGdCredit({
    id: "b-mid",
    account,
    source: "streamUpdate",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: false,
    maxBonusCapUsd: 100_000_000n
  });
  await store.recordGdCredit({
    id: "c-new",
    account,
    source: "deposit",
    gdAmountWei: 1_000_000_000_000_000_000n,
    gdPrice: GD_PRICE,
    isVerified: false,
    maxBonusCapUsd: 100_000_000n
  });

  const credits = await store.getGdCredits(account);
  for (const [index, entry] of credits.entries()) {
    entry.createdAt = new Date(Date.UTC(2026, 0, index + 1)).toISOString();
    if (entry.id === "b-mid") {
      entry.fundingStatus = "failed";
      entry.fundingError = "boom";
    } else {
      entry.fundingStatus = "funded";
    }
    await kv.put(`gd-credit:${entry.id}`, JSON.stringify(entry));
  }

  const page = await store.getGdCreditHistory(account, { limit: 1, offset: 0 });
  assert.equal(page.total, 3);
  assert.equal(page.items.length, 1);
  assert.equal(page.items[0].id, "c-new");
  assert.equal(page.hasMore, true);

  const page2 = await store.getGdCreditHistory(account, { limit: 1, offset: 1 });
  assert.equal(page2.items[0].id, "b-mid");
  assert.equal(page2.hasMore, true);

  const deposits = await store.getGdCreditHistory(account, { limit: 20, offset: 0, source: "deposit" });
  assert.equal(deposits.total, 2);
  assert.equal(deposits.items.map((item) => item.id).join(","), "c-new,a-old");

  const failed = await store.getGdCreditHistory(account, { limit: 20, offset: 0, fundingStatus: "failed" });
  assert.equal(failed.total, 1);
  assert.equal(failed.items[0].id, "b-mid");

  const ranged = await store.getGdCreditHistory(account, {
    limit: 20,
    offset: 0,
    from: "2026-01-02T00:00:00.000Z",
    to: "2026-01-02T23:59:59.999Z"
  });
  assert.equal(ranged.total, 1);
  assert.equal(ranged.items[0].id, "b-mid");
});

// A terminated stream reports flowRate 0. The old truthiness check discarded it, leaving the closed
// stream's rate on the profile, which is what let a later stream start bill the dormant gap.
test("recordGdCredit records a zero flow rate on stream termination", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);

  const opened = await store.recordGdCredit({
    id: "stream:open",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "streamUpdate",
    gdAmountWei: 1_000_000_000_000_000_000n,
    flowRate: 385_802_469_136n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });
  await store.markFundingResult(opened, { funded: true, txHash: "0xopen" });
  assert.equal((await store.getUser("0xABC")).streamFlowRateWeiPerSecond, "385802469136");

  const closed = await store.recordGdCredit({
    id: "stream:close",
    account: "0xABC",
    rootAccount: "0xROOT",
    source: "streamUpdate",
    gdAmountWei: 500_000_000_000_000_000n,
    flowRate: 0n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });
  await store.markFundingResult(closed, { funded: true, txHash: "0xclose" });

  const after = await store.getUser("0xABC");
  assert.equal(after.streamFlowRateWeiPerSecond, "0", "a closed stream must not keep its old rate");
  // Which makes the next stream's owed window worth nothing, re-baselining instead of billing it.
  assert.equal(BigInt(after.streamFlowRateWeiPerSecond) * 5_000_000n, 0n);
});

// Totals mirror onto the GoodID root because they accumulate. A flow rate is absolute, so mirroring
// it would leave the root holding whichever sub-account wrote last; the cron sums it there instead.
test("recordStreamFlowRate does not mirror the rate onto the root account", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);

  const entry = await store.recordGdCredit({
    id: "stream:no-mirror",
    account: "0xWALLET",
    rootAccount: "0xROOT",
    source: "streamCron",
    gdAmountWei: 1_000_000_000_000_000_000n,
    flowRate: 385_802_469_136n,
    gdPrice: GD_PRICE,
    isVerified: true,
    maxBonusCapUsd: 100_000_000n
  });
  await store.markFundingResult(entry, { funded: true, txHash: "0xfunded" });

  assert.equal((await store.getUser("0xWALLET")).streamFlowRateWeiPerSecond, "385802469136");
  assert.equal((await store.getUser("0xROOT")).streamFlowRateWeiPerSecond, "0", "the root's rate is the cron's sum, not a mirror");
  // The totals still aggregate on the root, which is what mirroring is for.
  assert.equal((await store.getUser("0xROOT")).totalGDStreamedWei, "1000000000000000000");
});

test("credit-id index retains all entries beyond 500 without truncation (#33 Finding 7)", async () => {
  const store = new KVCreditStore(new MemoryKV() as never);
  const account = "0xHIGHVOLUME";

  // Record 520 credits to exceed the former 500-entry truncation threshold
  for (let i = 0; i < 520; i++) {
    await store.recordGdCredit({
      id: `deposit:${i}`,
      account,
      source: "deposit",
      gdAmountWei: 1_000_000_000_000_000_000n,
      gdPrice: GD_PRICE,
      isVerified: false,
      maxBonusCapUsd: 100_000_000n
    });
  }

  const allCredits = await store.getGdCredits(account);
  assert.equal(allCredits.length, 520, "must retain all 520 entries without dropping oldest");
  assert.equal(allCredits[0].id, "deposit:0", "oldest entry must be preserved");
  assert.equal(allCredits[519].id, "deposit:519", "newest entry must be preserved");

  const history = await store.getGdCreditHistory(account, { limit: 100, offset: 0 });
  assert.equal(history.total, 520, "history total must report full count");
  assert.equal(history.items.length, 100);
  assert.equal(history.hasMore, true);

  const tailPage = await store.getGdCreditHistory(account, { limit: 100, offset: 500 });
  assert.equal(tailPage.items.length, 20, "tail entries beyond 500 must remain accessible");
  assert.equal(tailPage.hasMore, false);
});
