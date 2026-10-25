# Signal trading dashboard V1

Implementation: SPEC-SIGNAL-DASH-001. This is a parallel Signal workstream with a dedicated observation process. Pulseforge core routing, outbound and research state machines are unchanged.

## Operator surfaces

- `/signal-v1/positions`: watched positions and operational live signals.
- `/signal-v1/positions/<Solana mint>`: persisted position value, realized cash, P&L, observed peaks/drawdown, distribution evidence, chart and timeline.
- The UI polls cached database snapshots every 10 seconds; it retries with bounded exponential backoff and resumes on reconnect. No page refresh is required. API polling does not call providers.
- BUY/SELL markers use exact blockchain timestamps, not candle bucket timestamps. Marker risk/research state uses evidence known by that transaction time; absent historical evidence stays UNKNOWN.
- Notes are separate, append-only operator context. No note alters a chain transaction or research event.

## Configuration

Default off. Apply only `migrations/2026-10-25-signal-dashboard.sql` explicitly before activation. It creates two isolated tables and does not touch existing Signal research or Pulseforge business tables.

Web service and dedicated worker both require:

```
SIGNAL_DASHBOARD_ENABLED=1
SIGNAL_DASHBOARD_TENANT_ID=<trusted session workspace>
SIGNAL_DASHBOARD_OWNER_USER_ID=<trusted operator user id>
SIGNAL_DASHBOARD_PUBLIC_WALLET=<public FOMO Solana wallet>
```

Run the separate process with `npm run signal:dashboard-worker`. It uses the existing database connection, never starts the application server, and never applies migrations. Configure up to three watched tokens as JSON in `SIGNAL_DASHBOARD_WATCH_TOKENS`, each with `tokenAddress`, optional `symbol` and optional verified `poolAddress`. When no pool is specified, the DEX adapter picks the most liquid eligible base-token pool once and pins it durably. Changing pools requires an explicit new watch configuration and comparison reset; there is no silent pool substitution.

Provider credentials, when available, belong only in server configuration:

- `SIGNAL_DASHBOARD_HELIUS_API_KEY`: enhanced finalized public-address history with token-account activity. Optional `SIGNAL_DASHBOARD_RPC_URL` selects an HTTPS finalized balance-read endpoint. The adapter exposes only GET history and `getTokenAccountsByOwner`; signing and transaction RPC methods are rejected.
- `SIGNAL_DASHBOARD_BIRDEYE_API_KEY`: historical preceding minute SOL/USDC prices for USD conversion. Native consideration remains exact; minute USD estimates are PARTIAL. Missing/late quotes remain UNKNOWN. USDC quantity is not silently equated with USD.
- `SIGNAL_DASHBOARD_COINGECKO_PRO_KEY`: independent paid-quota OHLCV bootstrap, with no empty-interval padding. Public GeckoTerminal is supported only when an existing shared durable request budget is injected; the standalone process does not create an unbudgeted public Gecko consumer.
- `SIGNAL_DASHBOARD_FEED_URL`: the existing private `http://<service>.railway.internal/feed`, using `SIGNAL_OPERATOR_FEED_TOKEN`. No Telegram credentials are copied to this worker. Existing empirical raw evidence and strict prospective FIRST_CALLER / INDEPENDENT_CONVERGENCE observations are read through an allowlist bridge. No cohort outcome/performance tables are queried.

Backfill is limited to 50 pages of 100 wallet-history rows by default (`SIGNAL_DASHBOARD_MAX_HISTORY_PAGES`, 1–1000). Only watched mints are persisted/enriched. Reaching this limit stays PARTIAL; increasing it is an explicit configuration choice. Provider and observation quotas remain enforced across replicas.

No wallet private key, seed, signing material, signing SDK, buy/sell API or trade execution controls exist in this feature.

## Data interpretation

Every displayed market/accounting metric has a provenance object. DEX Screener does not supply a source update timestamp in this response; frequent receipt timestamps are therefore PARTIAL, not verified LIVE. Missing market cap does not fall back to FDV. Unavailable values are null, not zero. Pool age does not substitute for token age.

Sampled candles aggregate only actually observed prices and are explicitly PARTIAL / SAMPLED_OBSERVATIONS_ONLY. They do not imply complete OHLCV coverage. Provider candles preserve original timestamps, returned prices/volume and acquisition time. No gaps are padded or candles interpolated. Provider-candle freshness remains unknown unless independently established.

The ledger uses fixed-point integer FIFO, preserves raw transaction evidence and lots, and separates transfers from trades. Unknown basis, quote value, fees, incomplete history, unknown same-slot ordering or reconciliation failure prevents complete P&L claims. Wallet balance is displayed independently of ledger reconciliation. Exhausting an indexer endpoint is observed coverage, not proof of complete blockchain history. Network fees are separately observable; all-in platform fee coverage still depends on parser/provider evidence. Trade analytics is not tax accounting.

Promotion intensity is descriptive, counts source messages once and retains same-cluster activity. Promotional traffic never creates independent convergence; only the existing strict research engine can provide that category. Divergence is evidence for distribution review, never an automatic exit. No alert does not mean safe.

Peaks are computed from all persisted actual observations after entry/call boundaries, including an append-only chronology. They never use future ATH or retroactively treat bootstrap candle highs as contemporaneously known peaks.

## Verification

`npm run test:signal-dashboard` covers accounting, observer transport/classification, provenance, market/candle chronology, cluster behavior, risk evidence, owner authorization, cache, startup, polling recovery, actual browser interactions and native PostgreSQL persistence/locking/quota. Native PostgreSQL and a Chromium executable are required; missing prerequisites fail rather than silently skip. `PUPPETEER_EXECUTABLE_PATH` selects the browser. CI uses Ubuntu's Google Chrome.

Synthetic fixtures explicitly identify themselves as tests. They are not REMUS reconstruction or live provider evidence.

## REMUS live acceptance — pending

Supply and verify the public FOMO wallet and exact REMUS mint. Confirm provider credentials/coverage, configure the dedicated worker and watched mint, then:

1. Backfill finalized token-account-inclusive history until exhausted; inspect missing/ambiguous transactions and historical USD coverage.
2. Reconcile remaining tokens against a finalized wallet balance at a slot covering all imported trades. Check chronological slot order where multiple transactions share a slot.
3. Verify initial entry, principal recovery, re-entry, staged sales/additions, later realization and remaining runner from signatures. Screenshots are validation only; none of their values are hard-coded.
4. Verify live price/cap/liquidity and real provider candle coverage/freshness; retain UNKNOWN where a source cannot support the claim. Confirm ten-second receipt updates on the phone.
5. Verify empirical Telegram/promotion overlays, risk explanations, exact-timestamp markers and automatic recovery after a network interruption.

V1 is not accepted as a complete live REMUS dashboard until these checks pass. No deployment, migration, provider call, wallet import or live money result is implied by local tests.
