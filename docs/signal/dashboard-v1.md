# Signal trading dashboard V1

Implementation: SPEC-SIGNAL-DASH-001. This is a parallel Signal workstream with a dedicated observation process. Pulseforge core routing, outbound and research state machines are unchanged.

## Operator surfaces

- `/signal-v1/positions`: watched positions and operational live signals.
- `/signal-v1/positions/<Solana mint>`: persisted position value, realized cash, P&L, observed peaks/drawdown, distribution evidence, chart and timeline.
- The UI polls cached database snapshots every 10 seconds; it retries with bounded exponential backoff and resumes on reconnect. No page refresh is required. API polling does not call providers.
- BUY/SELL markers use exact blockchain timestamps, not candle bucket timestamps. Marker risk/research state uses evidence known by that transaction time; absent historical evidence stays UNKNOWN.
- Notes are separate, append-only operator context. No note alters a chain transaction or research event.

## Configuration

Default off. Apply the Signal-only migrations through the explicit standalone migration command before activation. They create isolated dashboard, authentication and append-only zone-research tables without changing existing Signal research or Pulseforge business tables.

Web service and dedicated worker both require:

```
SIGNAL_DASHBOARD_ENABLED=1
SIGNAL_DASHBOARD_TENANT_ID=<trusted session workspace>
SIGNAL_DASHBOARD_OWNER_USER_ID=<trusted operator user id>
SIGNAL_DASHBOARD_PUBLIC_WALLET=<public FOMO Solana wallet>
```

Run the separate process with `npm run signal:dashboard-worker`. It uses the existing database connection, never starts the application server, and never applies migrations. Configure up to three watched tokens as JSON in `SIGNAL_DASHBOARD_WATCH_TOKENS`, each with `tokenAddress`, optional `symbol` and optional verified `poolAddress`. When no pool is specified, the DEX adapter picks the most liquid eligible base-token pool once and pins it durably. Changing pools requires an explicit new watch configuration and comparison reset; there is no silent pool substitution.

Provider credentials, when available, belong only in server configuration:

- Public Solana RPC is the default wallet observer when no Helius key is configured. It reads finalized v1 transactions, wallet signatures and current owned token accounts, verifies PumpSwap instruction discriminators against the official IDL, and preserves token-account/closed-account coverage gaps. Head cursors acknowledge a signature only after its transaction response is processed.
- `SIGNAL_DASHBOARD_HELIUS_API_KEY`: enhanced finalized public-address history with token-account activity. Optional `SIGNAL_DASHBOARD_RPC_URL` selects an HTTPS finalized balance-read endpoint. The adapter exposes only GET history and `getTokenAccountsByOwner`; signing and transaction RPC methods are rejected.
- `SIGNAL_DASHBOARD_BIRDEYE_API_KEY`: historical preceding minute SOL/USDC prices for USD conversion. Native consideration remains exact; minute USD estimates are PARTIAL. Missing/late quotes remain UNKNOWN. USDC quantity is not silently equated with USD.
- `SIGNAL_DASHBOARD_COINGECKO_PRO_KEY`: optional independent paid-quota OHLCV bootstrap. By default, the standalone service and dedicated worker use public GeckoTerminal behind the existing shared durable Signal reservation, capped to one dashboard request per minute and six total reserved requests per minute. Missing shared quota tables or a provider cooldown blocks history collection without blocking live market or wallet updates. No distribution activation, scheduler or outbound sender is started. Historical candles are fetched on a separate lane, starting with 15-minute history, then the other intervals. No empty intervals are padded.
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

The public FOMO wallet and exact REMUS mint are verified and the privately retained chain reconstruction reconciles to a finalized balance. The isolated service has reached Railway SUCCESS and passed public health/sign-in/unauthenticated-API checks. Full acceptance still requires:

1. Backfill finalized token-account-inclusive history until exhausted; inspect missing/ambiguous transactions and historical USD coverage.
2. Reconcile remaining tokens against a finalized wallet balance at a slot covering all imported trades. Check chronological slot order where multiple transactions share a slot.
3. Verify initial entry, principal recovery, re-entry, staged sales/additions, later realization and remaining runner from signatures. Screenshots are validation only; none of their values are hard-coded.
4. Verify live price/cap/liquidity and real provider candle coverage/freshness; retain UNKNOWN where a source cannot support the claim. Confirm ten-second receipt updates on the phone.
5. Verify empirical Telegram/promotion overlays, risk explanations, exact-timestamp markers and automatic recovery after a network interruption.

V1 is not accepted as a complete live REMUS dashboard until these checks pass. No deployment, migration, provider call, wallet import or live money result is implied by local tests.

## Isolated service rollout

`standalone/signal-dashboard/Dockerfile` starts only Signal's authenticated web app and independent market/wallet collectors. It does not start `server.js`, Max, outbound agents, the Signal research scheduler, or message senders. Its login verifies the configured existing Pulseforge owner against the existing read-only users table, including active/role/workspace/password-change status. Signal sessions and rate limits use new Signal-only tables; password/role revocations are checked on every protected read.

Pre-deploy: `node standalone/signal-dashboard/migrate.js`, requiring `SIGNAL_DASHBOARD_SCHEMA_APPROVAL=SPEC-SIGNAL-DASH-001`. This explicitly creates only the dashboard, Signal-only session/login-attempt and append-only zone-research tables. No schema action occurs during normal app startup.

Configure the existing database by reference, a dedicated server session secret, `SIGNAL_DASHBOARD_OWNER_EMAIL` referencing the existing configured admin identity, the verified public wallet and mint/pool watch JSON, and the existing private caller feed/bearer by reference. The standalone app resolves the owner's database ID and trusted workspace at boot. Provider secrets and wallet signing material are unnecessary for the default public RPC / DEX Screener mode. Paid candle/history adapters remain optional. Public historical candles share the existing durable Gecko quota; sampled observations remain clearly marked as partial coverage.

Native quote accounting is shown explicitly in USDC. Net observed wallet USDC changes include embedded token fees; network fees paid by a separate sponsor are recorded separately. A current observed USDC/USD market quote can value the remaining tokens in USDC without assuming a $1 peg. Historical USD conversion and complete fee itemization remain UNKNOWN unless observed.

A separate Railway service uses a pinned reviewed commit, one replica limited to 0.25 CPU / 0.5 GB, `/health`, and the explicit pre-deploy command. Set these service settings directly with the Railway API; new services cannot opt into the deprecated `railway.json` configuration format. Never commit unrelated environment patches when rolling out Signal: create/configure only the new service directly if the shared environment already contains pending Pulseforge work. Existing Pulseforge service source, variables, deployment and switches remain unchanged. Private observation-health logs expose availability, chronology, reconciliation status and marker count, never financial values, signatures or wallet identities. An activated preview is not full V1 acceptance until live market, chain accounting, source overlays, risk chronology and mobile refresh have been checked.

## Plain-language dashboard

The positions home summarizes value still held, money received back, total profit/loss and original money not yet paid back. Search and holding/sold filters work on configured watches. The position page keeps money, drawdown and explained risk above the chart, with optional trade/call/risk overlays, a full-history view, readable activity filters and separate notes. Technical reason codes, provider provenance and raw evidence stay behind expandable details. Risk driven only by missing evidence is explicitly distinguished from an observed selling warning.

USDC cash is never silently displayed as USD. Native quote analytics can establish recovered-principal milestones without inventing historical dollar conversion prices; raw transaction values remain unchanged. Historical candles acquired now are never treated as evidence known at an earlier trade, nor used to rewrite peak chronology.

## TradingView chart and prospective watch areas

The chart uses the official vendored TradingView Lightweight Charts 5.2.1 standalone production build. Candles/line, touch pan/pinch, keyboard pan/zoom, expansion, interval selection, full history and trade/call/risk toggles remain read-only. Exact-time DOM trade markers interpolate screen coordinates between bar times; they are not snapped to a candle boundary. Whitespace time rows contain no OHLC. Disconnected line segments preserve gaps. Actual observations may update only the current open provider candle; its volume remains the original provider volume with its own timestamp. Completed candles are not rewritten by sampled prices. Reference lines use observed peaks and known USD average acquisition prices only.

The exploratory method `pullback-watch-v1` is frozen in `services/signalDashboard/zones.js`. Its rules are recorded with every setup. It examines 24 completed five-minute empirical provider bars, requires two lows within 1.5%, at least three bars apart, a 3% rebound between them, and a current observed price within 5% of the area. One-minute rollup requires all five actual complete bars from the same pool. Open bars, sampled-only bars, future knowledge, test/procedural evidence, missing recent history, stale current observations, unknown risk and high/critical risk block discovery. A RE_ENTRY label also requires an observed earlier sell; it is not a trade recommendation.

A watch area is first recorded at discovery time, never at an earlier pivot. CONDITIONS_OBSERVED needs two entirely new completed bars: a test with a close above the area followed by another close above, current nearby price and no more than a 10% decline in observed pool value. Missing inputs block that transition. A new completed close 1.5% below the lower bound, a 20% pool-value decline, pool change, high/critical risk or two-hour expiry cancels the watch. Terminal setups never resurrect; discovery has a 30-minute cooldown. Changes to these rules require a new method version.

The independent 30-second evaluator writes only append-only `signal_dashboard_zone_events`, scoped by tenant, public wallet and mint. Each change preserves input snapshot, rule version, discovery/knowledge times, input hash, pool, risk and reasons. Its lock/collector lane does not delay wallet or market collection. GET `/api/v1/signal/positions/:ca/zones` gives a bounded summary; GET `/zones/:zoneId` returns the saved evidence. No endpoint creates a setup, chain transaction or order. Initial mobile detail includes at most six summaries and omits full input snapshots; history is loaded on demand, and expanded evidence survives polling.

Paper comparisons start from an actual current observed price when conditions are recorded. They do not assume an executable fill. Fifteen-minute and one-hour checkpoints use only actual observations within their two-minute windows; missed windows remain UNKNOWN. Invalidated setups retain comparisons, including negative observations. Net returns, fee impact and slippage remain UNKNOWN; there is no win rate, performance ranking or predictive confidence claim. These isolated records do not read outcome cohorts, overwrite Signal research state, alter distribution doctrine or create independent confirmations from promotion.

If the evaluator is delayed, saved areas are labeled historical rather than current. No qualifying area is a valid result. The USDC what-if form only estimates the change in original capital not yet recovered from current matched wallet evidence; it does not choose a size or create an order. All remaining tokens can lose value. Detail-page last-verified cash/P&L may remain visible for at most five minutes during wallet-head checks, explicitly labeled DELAYED; authoritative API accounting, risk, zones and what-if inputs never use that display cache.

### Third-party attribution

`public/signal-positions-vendor.js` is copied unmodified from npm `lightweight-charts@5.2.1`, `dist/lightweight-charts.standalone.production.js`. SHA-256: `e21cc5caa0226ef30bd8549c50b9ef926615f2a4ee6b4e486353477a55f598cf`. The accompanying `signal-positions-chart-LICENSE.txt` contains its Apache 2.0 license and TradingView NOTICE. The chart retains its built-in attribution logo and links to TradingView and the license. No external chart script or trading integration is loaded.
