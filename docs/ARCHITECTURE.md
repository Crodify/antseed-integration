# Architecture

## Boundaries

This repo is only the AntSeed credit/vault integration. It does not depend on GoodDollar L2, does not index L2 events, and does not include frontend code. Celo G$ deposits and Superfluid stream updates are handled directly through the standalone Celo vault and Worker tx-log ingestion.

## Components

### AntseedBuyerOperator

A UUPS-upgradeable contract deployed on Base. It acts as the on-chain operator for the AntSeed deposits contract, bridging G$-denominated principal and bonus credits into the USDC-backed AntSeed buyer deposit.

- accepts the operator role for a buyer via `acceptBuyerOperator(buyer, nonce, sig)` → `deposits.setOperator`
- `depositFor(buyer, principal, bonus)` — pulls USDC from contract balance, calls `deposits.deposit(buyer, principal + bonus)`
- `depositForWithId(buyer, principal, bonus, id)` — same but idempotent via `usedDepositIds[keccak256(id)]`; duplicate IDs are silently skipped
- tracks `totalPrincipalDeposited[buyer]` and `totalBonusDeposited[buyer]` separately; `withdrawablePrincipal(buyer) = totalPrincipalDeposited - totalWithdrawn`
- `withdrawPrincipal(buyer, amount, recipient, nonce, buyerSig)` — buyer EIP-712 signed principal-only withdrawal using the per-address `usedNonces[buyer]` counter; bonus is not withdrawable by buyer
- `withdrawDepositedFor(buyer, amount, recipient)` — owner-only full withdrawal from deposits
- `requestClose(channelId)` / `withdrawChannel(channelId)` — callable by owner or the channel's buyer
- `sweepToken(token, recipient, amount)` — owner-only token rescue
- `approveCurrentDeposits()` — re-approves USDC to deposits contract (used if deposits contract address changes)

### CeloGdAntSeedVault

A Celo-side G$ vault for credit issuance without a bridge. It supports:

- ERC677/ERC667 `onTokenTransfer` and `tokenFallback` single-transaction deposits
- ERC777 `tokensReceived` single-transaction deposits
- classic ERC-20 `deposit` fallback
- GoodID check (backend only): the backend calls `getWhitelistedRoot(account)` on `CELO_GOODID_ADDRESS`; a non-zero root enables bonus credits and root-account aggregation; GoodID is **not** enforced at the contract level
- minimum USD thresholds enforced on-chain: `minFirstDepositUsd` (first deposit) and `minMonthlyStreamUsd` (stream rate); converted using `reservePriceOracle.currentPrice(bytes32)` or a configurable `fallbackGdUsdPerToken`
- Superfluid SuperApp `afterAgreementCreated`, `afterAgreementUpdated`, and `afterAgreementTerminated` callbacks
- stream state events for Worker-side bonus accounting

**Buyer address requirement.** Every deposit and stream creation/update must specify the AntSeed buyer account that receives the funded credits:

- **Deposits** — the caller encodes the buyer address in the `data` / `userData` field as `abi.encode(buyerAddress)`. The vault decodes it and emits `GdDeposited(account, buyer, gdAmount, data)`. Missing or zero buyer causes a `MissingBuyerAddress` revert.
- **Streams** — the stream creator passes `userData = abi.encode(buyerAddress)` when calling `createFlow` / `updateFlow` on the Superfluid host. The vault's SuperApp callbacks call `host.decodeCtx(ctx).userData` to extract and validate the buyer. The buyer is stored on-chain in `streamBuyer[sender]` and re-used on stream termination (terminator need not re-supply it). The vault emits `StreamUpdated(account, buyer, flowRate, monthlyGdAmountWei, totalFlowWei)`.

The backend reads `buyer` directly from on-chain events (log topics) for deposit ingestion. For stream subgraph polling it reads the `userData` field on the most recent `FlowUpdatedEvent` and decodes the buyer from it. Credits are always funded to `buyerAddress`, not to the depositor wallet.

### Backend credit service

The backend is a Cloudflare Worker managed by Wrangler. Its current scope is G$ credit ingestion, accounting, and AntSeed deposit funding. It does **not** proxy AI requests.

**Event ingestion** (`POST /v1/celo/events/record`):

- fetches a Celo transaction receipt by `txHash` (or a log range by `account + fromBlock`)
- parses `GdDeposited` and `StreamUpdated` events from `CeloGdAntSeedVault`
- resolves `getWhitelistedRoot(account)` to determine GoodID verification and root-account aggregation
- records a `GdCreditEntry` in KV and calls `fundCredit` immediately
- for `StreamUpdated`, the endpoint records a **zero-amount acknowledgement** entry (`source: "streamUpdate"`,
  id `txHash:logIndex`) so callers polling for the tx see a terminal entry. It never credits: all
  stream crediting happens in the cron (below), so repeated or concurrent calls cannot pay twice

**Cron** (`0 */6 * * *` — every 6 hours, per `wrangler.toml`) is the **only** stream crediting path:

- fetches Superfluid `StreamPeriod`s for the vault: every open period, plus closed periods stopped after
  the persisted `streams:closed-periods-cursor`. Closed periods at or before the cursor are fully settled
  and are not fetched again. The cursor only advances to just before the earliest closed period that
  some account has not yet been credited through, so a held-back account keeps its periods in view
- per account, the amount owed is the sum over its periods of `flowRate × overlap(period, (lastStreamCreditAt, asOf])`,
  where `asOf` is the subgraph's indexed block timestamp. Gaps between periods earn nothing and each
  rate is priced only for the time it was actually in force
- an account with a live stream is held by the 24-hour cooldown and the `MIN_GD_STREAMED_FOR_BONUS`
  minimum; an account whose streams are all closed is settled for its final window regardless
- the deposit id is `stream:<account>:<lastStreamCreditAt>` (the window start). Concurrent or repeated
  runs over the same window derive the same id, which `AntseedBuyerOperator.usedDepositIds` dedupes
  on-chain. `lastStreamCreditAt` advances to `asOf` only once that entry is **funded**; a failed entry
  is retried with the same id on the next run
- syncs `streamFlowRateWeiPerSecond` for every account from that result before crediting, so the rate
  is refreshed on every run rather than only when a credit clears the cooldown and the 4000 G$ minimum
  (which can be a fortnight apart). The rate is **summed per account**: one account can hold a closed
  revision alongside its replacement, and a closed row must not clobber the live one. All-closed sums
  to 0, which is how a profile stops advertising a stream that no longer exists. This writes current
  state only — `lastStreamCreditAt` and the lifetime totals are settlement and are left untouched
- a GoodID root profile then gets the **sum across its identity's accounts**. The lifetime totals reach
  the root by mirroring in `updateUser`, which is correct because they accumulate; a flow rate is an
  absolute value, so mirroring it would leave the root holding whichever sub-account wrote last. The
  rate is therefore excluded from that mirror — `recordStreamFlowRate` writes one profile only — and
  summed onto the root here. A root that streams itself is part of its own sum
- then issues and funds the stream credits described above

**Profile** (`GET /v1/accounts/:account/profile`):

- returns the user's `UserCreditProfile` only

**Credit history** (`GET /v1/accounts/:account/credit-history`):

- returns paginated `GdCreditEntry` records newest-first
- query params: `limit` (default 20, max 100), `offset` (default 0), optional `source`, `fundingStatus`, `from` / `to` (ISO `createdAt` range, inclusive)
- response: `{ account, items, total, limit, offset, hasMore }`

**Outstanding funding** (`GET /v1/accounts/:account/outstanding`):

- returns `totalOutstandingFundingUsd` and all `GdCreditEntry` records with `fundingStatus = "pending"` or `"failed"`

**Principal withdraw** (`POST /v1/accounts/:account/withdraw`):

- body: `amount` (USDC micro-units), `recipient`, `nonce`, buyer EIP-712 `signature`
- calls `AntseedBuyerOperator.withdrawPrincipal(buyer, amount, recipient, nonce, buyerSig)`

**Operator revoke** (`POST /v1/accounts/:account/operator-revoke`):

- body: `nonce`, buyer EIP-712 `signature`
- calls `AntseedBuyerOperator.revokeOperator(buyer, nonce, buyerSig)`

**Channel close** (`POST /v1/channels/:channelId/close`):

- buyer EIP-712 `RequestClose` (`nonce`, `signature`)
- calls `AntseedBuyerOperator.requestClose(channelId, nonce, buyerSig)`

**Channel withdraw** (`POST /v1/channels/:channelId/withdraw`):

- buyer EIP-712 `WithdrawChannel` (`nonce`, `signature`)
- calls `AntseedBuyerOperator.withdrawChannel(channelId, nonce, buyerSig)`

**Funding path** (`fundCredit`):

- calls `AntSeedFundingVaultClient.depositForBuyerWithId(buyer, principal, bonus, id)` — uses the `buyer` from the credit entry, or falls back to `account`
- on success: marks entry `funded`, credits the profile's lifetime totals, decrements `totalOutstandingFundingUsd`
- on failure: marks entry `failed`, preserves `fundingError`, leaves the lifetime totals untouched, and
  decrements `totalOutstandingFundingUsd` — both statuses are terminal, so a failed entry is never retried

### AntSeed payment boundary

The `AntseedBuyerOperator` contract (Base) is the on-chain operator for the backend's AntSeed buyer. The Worker's `AntSeedFundingVaultClient` calls `depositForWithId` to move principal + bonus credits into the AntSeed deposits contract. The AntSeed network then settles providers from the deposit balance using buyer-signed EIP-712 authorization.

Future payment mechanisms (sponsorships, org budgets, subscriptions, multi-buyer routing) should be added as adapter/router layers above this boundary.

## Accounting model

- G$ amounts are converted to micro-USD principal using the on-chain reserve oracle price (`currentPrice(bytes32)`) or the fallback `GD_USD_PER_TOKEN`
- regular bonus = `principal * 10%` (deposit and non-stream sources)
- streaming bonus = `principal * 20%` (sources: `streamUpdate`, `streamRequest`, `streamCron`)
- unverified accounts (no GoodID root): bonus = 0
- monthly bonus cap: the effective bonus is capped to `MAX_BONUS_CAP_USD - monthlyBonusUsed` for the root account; cap is tracked in `monthly-bonus:<rootAccount>:YYYY-MM`
- total credit = `principalUsd + effectiveBonusUsd`
- `totalOutstandingFundingUsd` tracks credit not yet funded to `AntseedBuyerOperator`; incremented when an entry is recorded and decremented when `fundingStatus` leaves `"pending"`, whether it lands on `"funded"` or `"failed"`
- the profile's lifetime totals (`totalGdDepositedWei`, `totalGDStreamedWei`, `totalPrincipalUsd`, `totalBonusUsd`) only move when an entry is actually funded — a recorded-but-unfunded entry must never contribute, or the G$ counters drift above the credit granted. `streamFlowRateWeiPerSecond` is the exception: it is current state, not an accrual, so it is written at record time

## Non-goals

- no marketplace/indexer
- no GoodDollar L2 block processing
- no wallet UI
- no model hosting logic

## Cloudflare KV persistence

The Worker binds `ANTSEED_KV` and persists:

- `user:<account>` — `UserCreditProfile` aggregate, written for both the depositor wallet and the GoodID root wallet when they differ; tracks `totalGdDepositedWei`, `totalPrincipalUsd`, `totalBonusUsd`, `totalGDStreamedWei`, `totalOutstandingFundingUsd`, `streamFlowRateWeiPerSecond`, `lastStreamCreditAt`
- `user-gd-credits:<account>` — bounded list (last 500) of `gd-credit` entry IDs for the account
- `gd-credit:<id>` — individual `GdCreditEntry`: source, amounts, `fundingStatus` (`pending` → `funded` or `failed`), `fundingTxHash`, `fundingError`, `buyerAddress`
- `monthly-bonus:<rootAccount>:YYYY-MM` — cumulative bonus issued to the root account in that calendar month; used to enforce `MAX_BONUS_CAP_USD`

KV is used for long-term user data. High-concurrency balance enforcement for the AntSeed buyer deposit is handled on-chain by `AntseedBuyerOperator`; KV is eventually consistent.
