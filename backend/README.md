# Backend Worker

Cloudflare Worker for GoodDollar Celo-vault credit accounting and Celo → Base bridge funding.

## Runtime

- Wrangler Cloudflare Worker (`src/worker.ts`) only
- KV namespace binding: `ANTSEED_KV`
- Celo `CeloGdAntSeedVault` tx-log ingestion for G$ deposits and Superfluid stream updates
- Optional Base `AntseedBuyerOperator` bridge client that calls `depositForWithId(buyer, principal, bonus, id)`
- Cron trigger every 6 hours for stream bonus settlement checks and analytics aggregation

## Endpoints

- `GET /health`
- `GET /config/status`
- `GET /config/values`
- `GET /v1/analytics`
- `GET /v1/accounts/:account/profile`
- `GET /v1/accounts/:account/credit-history`
- `GET /v1/accounts/:account/outstanding`
- `POST /v1/analytics/refresh`
- `POST /v1/accounts/:account/operator-consent`
- `POST /v1/accounts/:account/operator-revoke`
- `POST /v1/accounts/:account/withdraw`
- `POST /v1/celo/events/record`
- `POST /v1/channels/:channelId/close`
- `POST /v1/channels/:channelId/withdraw`

`GET /v1/accounts/:account/profile` returns the wallet `UserCreditProfile` only.

`GET /v1/accounts/:account/credit-history` returns paginated `GdCreditEntry` history (newest first):

- query: `limit` (default `20`, max `100`), `offset` (default `0`)
- filters: `source`, `fundingStatus`, `from` / `to` (ISO timestamps on `createdAt`, inclusive)
- response: `{ account, items, total, limit, offset, hasMore }`

`POST /v1/celo/events/record` accepts either:

- `{ "txHash": "0x..." }`
- `{ "account": "0x...", "fromBlock": "0x...", "toBlock": "latest" }`

`GET /v1/analytics` returns daily analytics (last 30 days by default).

- query: `days` (default `30`, max `365`)
- response: `{ days, daily, global, lastRun }`
- `daily` — one record per UTC day, newest last; the current day is always recomputed from midnight to now on each refresh and its snapshot is overwritten
- `global` — all-time totals composed as persisted closed-day totals plus the current day's live snapshot
- `lastRun.finalizedThroughDate` — the most recent closed day whose values have been merged into persisted globals
- Base usage (`aiCreditsUsedWei`, `uniqueCreditUsers`) is filtered to known buyers only; known buyers are learned from Celo vault `buyer` fields.

`POST /v1/analytics/refresh` recomputes the current UTC day from midnight to now, overwrites the day snapshot, finalizes any closed days into persisted globals, and returns a summary. This endpoint is rate-limited to one invocation per hour.

## Setup

```bash
npm install
npm run typecheck
npm test
npm run build
```

Optional secrets/config:

- `SLACK_WEBHOOK_URL` - receives notifications for caught exceptions and blockchain transaction failures. HTTP notifications include the request body. See Failure Reporting below.
- `MAX_BONUS_CAP_USD` - monthly per-root-account bonus cap (token units, 18 decimals), defaults to `100`.
- `REGULAR_BONUS_BPS` - bonus basis points for deposits/non-stream sources, defaults to `1000` (10%).
- `STREAMING_BONUS_BPS` - bonus basis points for stream sources, defaults to `2000` (20%).
- `MIN_GD_STREAMED_FOR_BONUS` - minimum stream amount in G$ to issue stream credits, defaults to `4000`.
- `CELO_BLOCKSCOUT_API_URL` - Blockscout API URL for Celo logs (defaults to `https://celo.blockscout.com/api`).
- `BASE_BLOCKSCOUT_API_URL` - Blockscout API URL for Base logs (defaults to `https://base.blockscout.com/api`).
- `ANTSEED_CHANNELS_ADDRESS` - Antseed Channels contract address on Base used for AI credit usage metrics.

## Failure Reporting

With `SLACK_WEBHOOK_URL` configured, the Worker reports caught Base funding failures as well as request exceptions. This includes idempotency reads, minimum preflight reads, transaction submission/confirmation, cron stream processing, and background funding failures. Explicitly reverted Celo receipts are rejected and reported rather than accepted as an empty event list; missing receipts are classified separately as pending/not found.

Exceptions caught by the existing oracle, subgraph, and analytics RPC-discovery handlers are also reported. Recovered exceptions are labeled `recovered_with_fallback`, not as confirmed transaction failures. No new parsing or validation rules are added for reporting: skipped malformed events, empty oracle results, non-positive oracle quotes, empty GoodID returns, and empty RPC-discovery results retain their prior behavior without alerts. Legitimate empty scans, unverified GoodID roots, missing operator consent, stream cooldowns, and duplicate deposit recovery do not generate alerts.

Notifications contain operation, chain when known, outcome, an allowlisted error category, and validated credit entry/buyer/transaction identifiers when available. HTTP exception and funding-failure notifications also include the request method, full path, and body (truncated to 3,000 characters, as before). Request bodies may contain sensitive fields such as signatures, so restrict access to the Slack destination. Arbitrary remote error payloads are not forwarded. Propagated errors are deduplicated by error identity so nested handlers do not alert again. Separate funding attempts can each alert.

Delivery is best effort with a five-second timeout. Failed webhook delivery is logged without recursion or a retry queue. A confirmation timeout is not proof of an on-chain revert. Wallet-only failures and user cancellations that never reach the Worker are not observable here.

## Buyer Deposit Minimum

Before submitting a new Base funding transaction for a direct G$ deposit, the Worker resolves the current AntSeed deposits contract through the funding operator's registry and reads the selected buyer's balance and `MIN_BUYER_DEPOSIT`. If available plus reserved balance is zero, the deposit **principal alone** must meet that minimum (currently $1 / 1,000,000 micro-USDC); bonus does not count. A payer's earlier deposits to another buyer do not waive this requirement. Buyers whose balance has been emptied need the minimum again; reserved-only positive balances still allow smaller top-ups.

Insufficient principal or failed preflight reads prevent Base submission, retain the entry as failed funding, and generate an alert. Already-used deposit IDs bypass this guard so recovery never resubmits funding. Disabled bridging, zero-credit acknowledgements, and the existing stream funding policy are unchanged.

This is a backend funding guard, **not a Celo payment preflight**: ingestion happens after the user has deposited G$ on Celo, so the Worker cannot prevent or undo that transfer. AntSeed remains authoritative if buyer balance or minimum changes between the read and Base execution. No contract or widget behavior changes are included.

## Config And Constants

The Worker reads constants from Wrangler env vars and derives runtime values in `src/env.ts`.

- `GD_CUSD_PRICE`
  - Decimal cUSD price per 1 G$ used as fallback pricing.
  - Default: `0.0001`.
- `MAX_BONUS_CAP_USD`
  - Monthly bonus cap per GoodID root account.
  - Input units: token units with 18 decimals (e.g. `100`).
  - Runtime representation: `MAX_BONUS_CAP_USD` bigint (18 decimals).
- `REGULAR_BONUS_BPS`
  - Bonus rate for non-stream sources (`deposit`).
  - Basis points where `10000 = 100%`.
  - Default: `1000` (10%).
- `STREAMING_BONUS_BPS`
  - Bonus rate for stream sources (`streamUpdate`, `streamRequest`, `streamCron`).
  - Basis points where `10000 = 100%`.
  - Default: `2000` (20%).
- `MIN_GD_STREAMED_FOR_BONUS`
  - Minimum streamed G$ amount required before stream credits are issued.
  - Input units: whole-token G$ string parsed to wei.
  - Runtime representation: `MIN_STREAM_BONUS_WEI` bigint.
  - Default: `4000` G$.

### Inspect Runtime Values

Use `GET /config/values` to inspect the effective non-secret config values currently used by the Worker.

```bash
curl "$GOODDOLLAR_ANTSEED_API/config/values"
```

Important response fields:

- `config.GD_CUSD_PRICE`
- `config.MAX_BONUS_CAP_USD`
- `config.REGULAR_BONUS_BPS`
- `config.STREAMING_BONUS_BPS`
- `config.MIN_STREAM_BONUS_WEI` (derived from `MIN_GD_STREAMED_FOR_BONUS`)

For local dev:

```bash
cp .dev.vars.example .dev.vars
npm run dev
```
