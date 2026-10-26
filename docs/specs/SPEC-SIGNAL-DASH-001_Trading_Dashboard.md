# SPEC-SIGNAL-DASH-001 — Signal trading dashboard V1

Status: approved implementation in progress (operator specification, 2026-10-25).

## Objective and scope
Parallel Signal workstream: mobile position dashboard, real candlesticks, public wallet observation, deterministic FIFO trade analytics, empirical event overlays, distribution evidence, provenance and automatic updates. No transaction execution, signing material, outbound/orchestration changes or research unblinding.

## Contract
Routes: `/signal-v1/positions`, `/signal-v1/positions/:ca`; authenticated `/api/v1/signal/positions` and detail/candles/events/transactions/risk, `/api/v1/signal/stream/:ca`. A separate annotation endpoint writes operator notes only.
Every metric carries LIVE/DELAYED/PARTIAL/UNKNOWN with source and observation times. Missing data is null, never zero. FIFO is trade analytics, not tax accounting. Preserve raw lots. Transfer-in basis is unknown; transfer-out is not a sale. No screenshot-derived transactions. Market observations and blockchain timestamps are separate from knowledge time. Observed peaks are chronological, post-entry/post-call, never a future ATH.

## Architecture and dependencies
See [ADR-SIGNAL-DASH-001](../adr/ADR-SIGNAL-DASH-001_Read_Only_Position_Ledger.md). Dedicated opt-in collector process, additive PostgreSQL tables, session/role/owner scope, cached reads with bounded polling. Helius enhanced history observes all wallet token accounts; Solana RPC reads finalized balances. MarketProvider and WalletWatchProvider interfaces isolate provider changes. DEX Screener observations do not claim source freshness when its timestamp is unavailable. GeckoTerminal public bootstrap shares the existing request budget; paid CoinGecko can use a separate quota. No interpolation or empty-interval candles. Exploratory promotion intensity never establishes independent convergence or automatic exits.

## Acceptance
All 30 sections of the supplied operator spec remain the V1 contract. Required local tests: read-only wallet transport, dedupe, buy/sell/transfer classification, FIFO/partial sells/reentry/fees, reconciliation, P&L, timestamp alignment, knowledge-time/no-future guards, chronological peaks, unknown metrics, cluster behavior, explained distribution, polling recovery, mobile payload, startup and PostgreSQL persistence. Live REMUS acceptance requires a verified public wallet, mint, provider coverage, balance reconciliation and observed automatic updates. Local fixtures are synthetic tests only and cannot satisfy live acceptance.

## Rollout
Default off; explicit additive migration and worker configuration. No automatic production DDL. See `docs/signal/dashboard-v1.md` for configuration, verification and remaining acceptance gaps. Pulseforge P0 priorities remain unchanged.
