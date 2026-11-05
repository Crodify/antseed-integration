import { calculateCreditWithBonus, monthKey } from "./credit-bonus.js";
import { GdCreditEntry, UserCreditProfile } from "./types.js";
import { logInfo, logWarn, redactAddress } from "./logging.js";

type KV = Pick<KVNamespace, "get" | "put">;

const USER_PREFIX = "user:";
const GD_CREDIT_PREFIX = "gd-credit:";
const USER_GD_CREDITS_PREFIX = "user-gd-credits:";
const MONTHLY_BONUS_PREFIX = "monthly-bonus:";

export class KVCreditStore {
  constructor(private readonly kv: KV) {}

  async recordGdCredit(input: {
    id: string;
    account: string;
    rootAccount?: string;
    isVerified: boolean;
    source: GdCreditEntry["source"];
    gdAmountWei: bigint;
    txHash?: string;
    logIndex?: number;
    date?: Date;
    gdPrice: number;
    flowRate?: bigint;
    maxBonusCapUsd: bigint;
    regularBonusBps?: bigint;
    streamingBonusBps?: bigint;
    buyerAddress?: string;
    streamCreditedThrough?: string;
  }): Promise<GdCreditEntry> {
    const account = normalizeAccount(input.account);
    const rootAccount = normalizeAccount(input.rootAccount ?? input.account);
    const entryId = input.id;
    const existing = await this.getJson<GdCreditEntry>(`${GD_CREDIT_PREFIX}${entryId}`);
    if (existing) {
      logWarn("kv.credit.idempotent-hit", {
        entryId,
        account: redactAddress(account),
        source: input.source,
        existingStatus: existing.fundingStatus
      });
      return existing;
    }
    const month = monthKey(input.date ?? new Date());
    const bonus = calculateCreditWithBonus(input.gdAmountWei, input.source, input.isVerified, input.gdPrice, input.regularBonusBps, input.streamingBonusBps);

    // Enforce per-root-account monthly bonus cap
    let effectiveBonusUsd = bonus.bonusUsd;
    if (effectiveBonusUsd > 0n && input.maxBonusCapUsd > 0n) {
      const monthlyBonusUsed = await this.getMonthlyBonusUsed(rootAccount, month);
      const remainingCap = input.maxBonusCapUsd > monthlyBonusUsed ? input.maxBonusCapUsd - monthlyBonusUsed : 0n;
      if (effectiveBonusUsd > remainingCap) {
        logInfo("kv.credit.bonus-capped", {
          entryId,
          rootAccount: redactAddress(rootAccount),
          requestedBonusUsd: effectiveBonusUsd.toString(),
          remainingCapUsd: remainingCap.toString()
        });
        effectiveBonusUsd = remainingCap;
      }
    }

    const now = new Date().toISOString();
    const entry: GdCreditEntry = {
      id: entryId,
      account,
      rootAccount,
      source: input.source,
      gdAmountWei: input.gdAmountWei.toString(),
      principalUsd: bonus.principalUsd.toString(),
      bonusUsd: effectiveBonusUsd.toString(),
      totalCreditUsd: (bonus.principalUsd + effectiveBonusUsd).toString(),
      streamUpdateMonth: month,
      ...(input.txHash !== undefined && { txHash: input.txHash }),
      ...(input.logIndex !== undefined && { logIndex: input.logIndex }),
      fundingStatus: "pending",
      createdAt: now,
      ...(input.buyerAddress && {
        buyerAddress: input.buyerAddress.toLowerCase()
      }),
      ...(input.streamCreditedThrough && { streamCreditedThrough: input.streamCreditedThrough })
    };

    await this.putJson(`${GD_CREDIT_PREFIX}${entry.id}`, entry);
    await this.addGdCreditToAccount(account, entry.id);
    if (rootAccount && rootAccount !== account) {
      await this.addGdCreditToAccount(rootAccount, entry.id);
    }
    if (effectiveBonusUsd > 0n) {
      await this.addMonthlyBonusUsed(rootAccount, month, effectiveBonusUsd);
    }
    // The G$ and USD totals are credited in `markFundingResult`, once funding actually lands. Only
    // the outstanding balance moves here, because the entry is at this point owed but not yet paid.
    await this.updateUser(account, rootAccount, (current) => ({
      ...current,
      rootAccount: rootAccount,
      createdAt: current.createdAt ?? now,
      updatedAt: now,
      totalOutstandingFundingUsd: addDecimalStrings(current.totalOutstandingFundingUsd, entry.totalCreditUsd)
    }));

    // Written separately from the mirrored update above, because the flow rate belongs to this
    // account alone -- see `recordStreamFlowRate`. `!== undefined` rather than a truthiness check:
    // a terminated stream reports 0, and reading that as "no value given" would leave the closed
    // stream's rate on the profile forever.
    if (input.flowRate !== undefined) {
      await this.recordStreamFlowRate(await this.getUser(account), input.flowRate);
    }

    logInfo("kv.credit.recorded", {
      entryId: entry.id,
      account: redactAddress(entry.account),
      rootAccount: redactAddress(entry.rootAccount),
      source: entry.source,
      principalUsd: entry.principalUsd,
      bonusUsd: entry.bonusUsd,
      totalCreditUsd: entry.totalCreditUsd,
      buyer: entry.buyerAddress,
      input
    });

    return entry;
  }

  /**
   * Sync a profile's recorded flow rate to what the subgraph reports, including 0 for a stream that
   * has closed. The subgraph is the source of truth here: the termination event that would
   * otherwise write the 0 is pushed in by `POST /v1/celo/events/record` and may never arrive.
   *
   * Deliberately leaves `lastStreamCreditAt` and every total alone -- this corrects current state,
   * it does not settle anything. The caller must pass a freshly read profile: it is both the basis
   * for the comparison and the record written back. Updates that object in place to match what was
   * stored, and returns whether a write was needed.
   */
  async recordStreamFlowRate(profile: UserCreditProfile, flowRate: bigint): Promise<boolean> {
    const next = flowRate.toString();
    if (profile.streamFlowRateWeiPerSecond === next) return false;
    const now = new Date().toISOString();
    // Writes this profile only -- deliberately not through `updateUser`, which mirrors every change
    // onto the GoodID root. The totals mirror correctly because they accumulate; a flow rate is an
    // absolute value, so mirroring it would leave the root holding whichever sub-account wrote last.
    // The root's rate is maintained separately, as the sum across the identity's accounts.
    const previousFlowRateWeiPerSecond = profile.streamFlowRateWeiPerSecond;
    // Keep the caller's copy in step with what was stored, so a profile held across a loop and
    // logged later does not report a stale rate or timestamp.
    profile.streamFlowRateWeiPerSecond = next;
    profile.updatedAt = now;
    await this.putJson(`${USER_PREFIX}${profile.account}`, profile);
    logInfo("kv.stream.flow-rate-synced", {
      account: redactAddress(profile.account),
      previousFlowRateWeiPerSecond,
      flowRateWeiPerSecond: next
    });
    return true;
  }

  async markFundingResult(
    entry: GdCreditEntry,
    result: { funded: boolean; id?: string; txHash?: string; error?: string; credited?: boolean }
  ): Promise<GdCreditEntry> {
    // Re-read: a concurrent request for the same id may have settled it while this one was funding,
    // and its result must not be overwritten (funded -> failed) or counted twice.
    const stored = await this.getJson<GdCreditEntry>(`${GD_CREDIT_PREFIX}${entry.id}`);
    const settled = entry.fundingStatus !== "pending" ? entry : stored && stored.fundingStatus !== "pending" ? stored : undefined;
    if (settled) {
      logWarn("kv.funding.already-terminal", {
        entryId: entry.id,
        account: redactAddress(entry.account),
        fundingStatus: settled.fundingStatus
      });
      return settled;
    }

    entry.fundingStatus = result.funded ? "funded" : "failed";
    entry.fundingTxHash = result.txHash;
    entry.fundingError = result.error;
    await this.putJson(`${GD_CREDIT_PREFIX}${entry.id}`, entry);

    // `funded` and `failed` both end this attempt (a failed stream window is retried only through
    // `retryFailedFunding`), so either way the entry stops being outstanding. Only a funded entry
    // adds to the lifetime totals; a failed one must leave them untouched, or the G$ counters drift
    // above the credit actually granted (what produced a 6.2M G$ "deposited" profile against a $4
    // balance).
    const now = new Date().toISOString();
    const credited = result.funded && result.credited !== false;
    await this.updateUser(entry.account, entry.rootAccount, (current) => {
      const outstanding = BigInt(current.totalOutstandingFundingUsd);
      const creditAmount = BigInt(entry.totalCreditUsd);
      // The stream clock belongs to the streaming account alone; mirroring it onto the GoodID root
      // would skip the root's own stream window.
      const advancesStreamClock = result.funded && entry.streamCreditedThrough !== undefined && current.account === entry.account;
      return {
        ...current,
        updatedAt: now,
        lastStreamCreditAt: advancesStreamClock ? laterIso(current.lastStreamCreditAt, entry.streamCreditedThrough!) : current.lastStreamCreditAt,
        totalGdDepositedWei: credited ? addDecimalStrings(current.totalGdDepositedWei, entry.gdAmountWei) : current.totalGdDepositedWei,
        totalGDStreamedWei:
          credited && entry.source.startsWith("stream") ? addDecimalStrings(current.totalGDStreamedWei, entry.gdAmountWei) : current.totalGDStreamedWei,
        totalPrincipalUsd: (BigInt(current.totalPrincipalUsd) + (credited ? BigInt(entry.principalUsd) : 0n)).toString(),
        totalBonusUsd: (BigInt(current.totalBonusUsd) + (credited ? BigInt(entry.bonusUsd) : 0n)).toString(),
        totalOutstandingFundingUsd: (outstanding > creditAmount ? outstanding - creditAmount : 0n).toString()
      };
    });
    logInfo("kv.funding.result", {
      entryId: entry.id,
      account: redactAddress(entry.account),
      source: entry.source,
      fundingStatus: entry.fundingStatus,
      txHash: result.txHash,
      error: result.error
    });
    return entry;
  }

  /**
   * Put a failed entry back to `pending` so it can be funded again under the same id. Safe because the
   * id is what `depositForWithId` dedupes on: if the failed attempt actually landed, the retry is
   * recognised as already funded rather than paid twice.
   */
  async retryFailedFunding(entry: GdCreditEntry): Promise<GdCreditEntry> {
    if (entry.fundingStatus !== "failed") return entry;
    const now = new Date().toISOString();
    const retry: GdCreditEntry = { ...entry, fundingStatus: "pending", fundingError: undefined };
    await this.putJson(`${GD_CREDIT_PREFIX}${entry.id}`, retry);
    await this.updateUser(entry.account, entry.rootAccount, (current) => ({
      ...current,
      updatedAt: now,
      totalOutstandingFundingUsd: addDecimalStrings(current.totalOutstandingFundingUsd, entry.totalCreditUsd)
    }));
    logInfo("kv.funding.retry", { entryId: entry.id, account: redactAddress(entry.account), previousError: entry.fundingError });
    return retry;
  }

  /**
   * Move the account's stream clock up to a funded entry's window end, if it is not there already.
   * Repairs the case where the entry was marked funded but the profile write that follows was lost;
   * without it the account would hit the same window id on every run and never be credited again.
   * Returns whether a write was needed.
   */
  async advanceStreamClock(entry: GdCreditEntry): Promise<boolean> {
    if (entry.fundingStatus !== "funded" || !entry.streamCreditedThrough) return false;
    const profile = await this.getUser(entry.account);
    const next = laterIso(profile.lastStreamCreditAt, entry.streamCreditedThrough);
    if (next === profile.lastStreamCreditAt) return false;
    await this.putJson(`${USER_PREFIX}${profile.account}`, { ...profile, lastStreamCreditAt: next, updatedAt: new Date().toISOString() });
    logWarn("kv.stream.clock-repaired", { entryId: entry.id, account: redactAddress(entry.account), lastStreamCreditAt: next });
    return true;
  }

  async getGdCredits(account: string): Promise<GdCreditEntry[]> {
    const normalized = normalizeAccount(account);
    const ids = (await this.getJson<string[]>(`${USER_GD_CREDITS_PREFIX}${normalized}`)) ?? [];
    const entries = await Promise.all(ids.map((id) => this.getJson<GdCreditEntry>(`${GD_CREDIT_PREFIX}${id}`)));
    return entries.filter((item): item is GdCreditEntry => Boolean(item));
  }

  async getGdCreditHistory(
    account: string,
    options: {
      limit: number;
      offset: number;
      source?: GdCreditEntry["source"];
      fundingStatus?: GdCreditEntry["fundingStatus"];
      from?: string;
      to?: string;
    }
  ): Promise<{ items: GdCreditEntry[]; total: number; limit: number; offset: number; hasMore: boolean }> {
    let entries = await this.getGdCredits(account);
    entries = [...entries].sort((a, b) => {
      const byCreated = b.createdAt.localeCompare(a.createdAt);
      if (byCreated !== 0) return byCreated;
      return b.id.localeCompare(a.id);
    });
    if (options.source) {
      entries = entries.filter((entry) => entry.source === options.source);
    }
    if (options.fundingStatus) {
      entries = entries.filter((entry) => entry.fundingStatus === options.fundingStatus);
    }
    if (options.from) {
      const fromMs = Date.parse(options.from);
      entries = entries.filter((entry) => Date.parse(entry.createdAt) >= fromMs);
    }
    if (options.to) {
      const toMs = Date.parse(options.to);
      entries = entries.filter((entry) => Date.parse(entry.createdAt) <= toMs);
    }
    const total = entries.length;
    const items = entries.slice(options.offset, options.offset + options.limit);
    return {
      items,
      total,
      limit: options.limit,
      offset: options.offset,
      hasMore: options.offset + options.limit < total
    };
  }

  /**
   * The stored profile, or undefined when the account has none yet. `getUser` synthesizes a default
   * for unknown accounts, which cannot distinguish "never seen" from "seen, all zero" -- callers
   * that must not create a profile as a side effect need this instead.
   */
  async getSavedUser(account: string): Promise<UserCreditProfile | undefined> {
    const normalized = normalizeAccount(account);
    const saved = await this.getJson<Partial<UserCreditProfile>>(`${USER_PREFIX}${normalized}`);
    return saved ? normalizeProfile(saved, normalized) : undefined;
  }

  async getUser(account: string): Promise<UserCreditProfile> {
    const normalized = normalizeAccount(account);
    const saved = await this.getJson<Partial<UserCreditProfile>>(`${USER_PREFIX}${normalized}`);
    return normalizeProfile(saved, normalized);
  }

  private async addGdCreditToAccount(account: string, entryId: string): Promise<void> {
    const key = `${USER_GD_CREDITS_PREFIX}${account}`;
    const ids = (await this.getJson<string[]>(key)) ?? [];
    if (!ids.includes(entryId)) ids.push(entryId);
    await this.putJson(key, ids);
  }

  private async getMonthlyBonusUsed(rootAccount: string, month: string): Promise<bigint> {
    const key = `${MONTHLY_BONUS_PREFIX}${rootAccount}:${month}`;
    const value = await this.getJson<string>(key);
    return value ? BigInt(value) : 0n;
  }

  private async addMonthlyBonusUsed(rootAccount: string, month: string, amount: bigint): Promise<void> {
    const key = `${MONTHLY_BONUS_PREFIX}${rootAccount}:${month}`;
    const current = await this.getMonthlyBonusUsed(rootAccount, month);
    await this.putJson(key, (current + amount).toString());
  }

  private async updateUser(account: string, rootAccount: string | undefined, mutate: (profile: UserCreditProfile) => UserCreditProfile): Promise<void> {
    const normalized = normalizeAccount(account);
    const normalizedRoot = normalizeAccount(rootAccount ?? account);
    const current = await this.getUser(normalized);
    const next = mutate({ ...current, rootAccount: normalizedRoot });
    await this.putJson(`${USER_PREFIX}${normalized}`, next);

    if (normalizedRoot !== normalized) {
      const rootCurrent = await this.getUser(normalizedRoot);
      const rootNext = mutate({
        ...rootCurrent,
        account: normalizedRoot,
        rootAccount: normalizedRoot
      });
      await this.putJson(`${USER_PREFIX}${normalizedRoot}`, rootNext);
    }
  }

  private async getJson<T>(key: string): Promise<T | undefined> {
    const value = await this.kv.get(key, "json");
    return (value ?? undefined) as T | undefined;
  }

  private async putJson(key: string, value: unknown): Promise<void> {
    await this.kv.put(key, JSON.stringify(value));
  }
}

function normalizeProfile(saved: Partial<UserCreditProfile> | undefined, account: string): UserCreditProfile {
  const createdAt = saved?.createdAt ?? new Date().toISOString();
  return {
    account,
    rootAccount: saved?.rootAccount ?? account,
    createdAt,
    updatedAt: saved?.updatedAt ?? createdAt,
    totalGdDepositedWei: saved?.totalGdDepositedWei ?? "0",
    totalBonusUsd: saved?.totalBonusUsd ?? "0",
    streamFlowRateWeiPerSecond: saved?.streamFlowRateWeiPerSecond ?? "0",
    totalPrincipalUsd: saved?.totalPrincipalUsd ?? "0",
    totalGDStreamedWei: saved?.totalGDStreamedWei ?? "0",
    totalOutstandingFundingUsd: saved?.totalOutstandingFundingUsd ?? "0",
    lastStreamCreditAt: saved?.lastStreamCreditAt
  };
}

function normalizeAccount(account: string): string {
  return account.toLowerCase();
}

function laterIso(current: string | undefined, next: string): string {
  return current && Date.parse(current) > Date.parse(next) ? current : next;
}

function addDecimalStrings(a: string, b: string): string {
  return (BigInt(a) + BigInt(b)).toString();
}
