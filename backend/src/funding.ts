import { AntSeedFundingVaultClient } from "./antseed-funding-vault.js";
import { KVCreditStore } from "./kv-credit-store.js";
import { GdCreditEntry } from "./types.js";
import { errorMessage, logError, logInfo, logWarn, redactAddress, redactHash } from "./logging.js";
import type { FailureReporter } from "./http.js";

export async function fundCredit(
  entry: GdCreditEntry,
  store: KVCreditStore,
  antseedFundingVault: AntSeedFundingVaultClient,
  reportFailure?: FailureReporter
): Promise<{ [key: string]: unknown }> {
  const buyer = entry.buyerAddress || entry.account;
  if (entry.fundingStatus === "funded") {
    logInfo("funding.already-funded", {
      entryId: entry.id,
      source: entry.source,
      account: redactAddress(entry.account),
      buyer: redactAddress(buyer),
      fundingTxHash: redactHash(entry.fundingTxHash)
    });
    return {
      ...entry,
      bridge: {
        enabled: antseedFundingVault.enabled,
        buyer,
        amountUsd: entry.totalCreditUsd,
        txHash: entry.fundingTxHash,
        alreadyFunded: true
      }
    };
  }
  if (entry.fundingStatus === "failed") {
    entry = await store.retryFailedFunding(entry);
  }
  const principalUsd = BigInt(entry.principalUsd);
  const bonusUsd = BigInt(entry.bonusUsd);
  logInfo("funding.start", {
    entryId: entry.id,
    source: entry.source,
    account: redactAddress(entry.account),
    buyer: redactAddress(buyer),
    principalUsd: entry.principalUsd,
    bonusUsd: entry.bonusUsd,
    totalCreditUsd: entry.totalCreditUsd
  });
  try {
    const shouldCredit = principalUsd + bonusUsd > 0n;
    const isAlreadyUsed = shouldCredit && antseedFundingVault.enabled && typeof antseedFundingVault.isDepositUsed === "function"
      ? await antseedFundingVault.isDepositUsed(entry.id)
      : false;

    if (shouldCredit && !isAlreadyUsed && antseedFundingVault.enabled && typeof antseedFundingVault.isBuyerOperator === "function") {
      const operatorStatus = await antseedFundingVault.isBuyerOperator(buyer);
      if (!operatorStatus.isOperator) {
        logWarn("funding.deposit.skipped.not-operator", {
          entryId: entry.id,
          source: entry.source,
          account: redactAddress(entry.account),
          buyer: redactAddress(buyer)
        });
        const updated = await store.markFundingResult(entry, {
          funded: false,
          error: "service_not_buyer_operator"
        });
        return {
          ...updated,
          depositId: entry.id,
          bridge: {
            enabled: true,
            buyer,
            amountUsd: entry.totalCreditUsd,
            error: "service_not_buyer_operator"
          }
        };
      }
    }
    const bridge = shouldCredit
      ? await antseedFundingVault.depositForBuyerWithId(buyer, principalUsd, bonusUsd, entry.id, entry.source === "deposit")
      : { enabled: antseedFundingVault.enabled, buyer, amountUsd: "0" };
    if (!shouldCredit) {
      logInfo("funding.skipped.zero-amount", {
        entryId: entry.id,
        source: entry.source,
        account: redactAddress(entry.account),
        buyer: redactAddress(buyer)
      });
    } else if (!bridge.enabled) {
      logWarn("funding.bridge.disabled", {
        entryId: entry.id,
        source: entry.source,
        buyer: redactAddress(buyer)
      });
    }
    const updated = await store.markFundingResult(entry, {
      funded: true,
      txHash: bridge.txHash,
      error: undefined,
      credited: shouldCredit
    });
    logInfo("funding.success", {
      entryId: entry.id,
      source: entry.source,
      buyer: redactAddress(buyer),
      txHash: redactHash(bridge.txHash),
      bridgeEnabled: bridge.enabled
    });
    return { ...updated, bridge };
  } catch (error) {
    await reportFailure?.({ operation: "funding.deposit", chain: "base", error, entryId: entry.id, buyer });
    const message = errorMessage(error);
    logError("funding.failed", {
      entryId: entry.id,
      source: entry.source,
      account: redactAddress(entry.account),
      buyer: redactAddress(buyer),
      message
    });
    let updated: GdCreditEntry;
    try {
      updated = await store.markFundingResult(entry, { funded: false, error: message });
    } catch (persistenceError) {
      logError("funding.failure-persistence.failed", { entryId: entry.id, message: errorMessage(persistenceError) });
      throw error;
    }
    return {
      ...updated,
      depositId: entry.id,
      bridge: {
        enabled: antseedFundingVault.enabled,
        buyer,
        amountUsd: entry.totalCreditUsd,
        error: message
      }
    };
  }
}
