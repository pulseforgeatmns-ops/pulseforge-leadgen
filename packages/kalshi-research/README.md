# Kalshi Research Platform

> **Monorepo home:** `packages/kalshi-research` inside Pulseforge (`@pulseforge/kalshi-research`).
> **Isolation ([ADR-033](../../docs/adr/ADR-033_Kalshi_Research_Stays_Isolated.md) / [SPEC-049](../../docs/specs/SPEC-049_Kalshi_Research_Package.md)):** research only.
> Not imported by production Node services. Not deployed. No live trading. No order placement.

A deterministic, paper-trading-first foundation for researching short-duration BTC event markets. It can ingest market/price snapshots, generate strategy decisions, simulate fills, enforce risk limits, and retain a complete audit trail.

## Safety boundary

This package does **not** send live orders. `ExecutionMode` only permits `paper` or `replay`; order submission is represented exclusively by the in-process paper execution engine. Do not add a live order endpoint, trading credentials for submission, production route wiring, or a Railway/deploy entrypoint. Do not require this package from `server.js`, cron, Mission Engine, or the Capability Registry.

## Quick start

Run from this package directory (`packages/kalshi-research`):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev,dashboard]'
cp .env.example .env
python -m kalshi_research.cli demo
python -m kalshi_research.cli init-db
# Lists BTC/Bitcoin series only (one public API request).
python -m kalshi_research.cli list-btc-series
# Public market data only; no credentials or order submission required.
python -m kalshi_research.cli collect-once
# Continuous paper-data collection; Ctrl+C stops it safely.
python -m kalshi_research.cli collect-loop
# Local snapshot coverage and paper-intent health report.
python -m kalshi_research.cli report
# Read-only data-quality/feature-readiness diagnostics: density, gaps, missing bid/ask,
# price distribution, and fee drag by price band. Tells you whether the dataset is ready
# for feature research or needs another capture pass.
python -m kalshi_research.cli diagnose-data
# Deterministic feature distributions for resolved markets, split by train/test and
# YES vs NO outcomes. Local/read-only — no network, no model training.
python -m kalshi_research.cli feature-report --train-fraction 0.5
# List immutable hypothesis/rule candidates, then evaluate H-005 (entry midpoint above).
python -m kalshi_research.cli list-hypotheses
python -m kalshi_research.cli evaluate-hypothesis --hypothesis-id H-005 --train-fraction 0.5
# Resolve completed paper trades from Kalshi's public market data, then view P/L.
python -m kalshi_research.cli resolve-settlements
# Resolve outcomes for every captured market, then replay the threshold rule.
python -m kalshi_research.cli resolve-outcomes
python -m kalshi_research.cli replay --max-yes-ask 35
# Compare threshold variants on already captured/resolved markets.
python -m kalshi_research.cli replay-sweep --min-threshold 1 --max-threshold 99 --top 10
# Pick a threshold on earlier markets, then test it on later markets.
python -m kalshi_research.cli replay-split --min-threshold 1 --max-threshold 99 --train-fraction 0.5
# Same commands accept --fee-rate and --min-edge-cents to see fee-adjusted results.
python -m kalshi_research.cli replay --max-yes-ask 35 --fee-rate 0.07 --min-edge-cents 0
pytest
streamlit run dashboard.py
```

The demo uses synthetic snapshots and writes structured logs. The default SQLite URL is portable for local research; set `DATABASE_URL` to a PostgreSQL URL when a server is available.

`collect-once` makes only public `GET` requests to Kalshi and Coinbase, then writes local snapshots and simulated orders to `kalshi_research.db`. It deliberately has no live-order code path. It queries only the series listed in `KALSHI_RESEARCH_BTC_SERIES_TICKERS` (default: `KXBTC15M`, Kalshi's Bitcoin price up/down fifteen-minute series), avoiding broad discovery bursts. Use `list-btc-series` to identify another series if needed. The API key you created should remain local and is not required for this public-data collector.

`collect-loop` repeats the same safe collection pass every 10 seconds by default. Temporary network or DNS failures retry automatically with exponential backoff up to 60 seconds. Stop it with Ctrl+C; it does not cancel, submit, or modify any Kalshi order.

The illustrative threshold strategy is capped at one virtual contract per market by default. After it opens a paper position, later signals for that same direction/market are ignored rather than recorded as duplicate rejected orders. It skips zero-cent/unavailable quotes and is solely for exercising the research pipeline—not a trading recommendation.

`resolve-settlements` reads the public market result for each filled paper trade and calculates a fee-excluded payout. It caches results for duplicate ticker entries and never submits orders. Re-run it as new markets resolve, then use `report` to see the resolved count and P/L. The report also includes a clean one-entry-per-market view, excluding any earlier duplicate paper fills.

`replay-sweep` runs a deterministic threshold comparison over local snapshots whose outcomes have already been resolved. It is useful for spotting whether a single replay threshold is unusually sensitive to parameter choice. It is still in-sample research and excludes fees.

`replay-split` is the first out-of-sample validation check. It selects the best threshold on the earlier portion of resolved markets, then scores that same threshold on the later portion without re-optimizing.

`diagnose-data` is a read-only, local-only data-quality report. It shows resolved/unresolved market counts, train/test coverage at a given split, snapshot density per market, within-market collection gaps, missing bid/ask fields, and fee drag by first-seen yes-ask price band, then prints an explicit verdict: whether the dataset looks ready for feature research or whether another capture pass is recommended first. It makes no network calls and writes nothing back to the database. Accepts `--fee-rate` and `--train-fraction` to match whatever `replay-split` configuration you're about to run.

`feature-report` is the first feature-research command. For every resolved market it extracts deterministic features from stored snapshots only (first yes ask/bid, spread, midpoint, time-to-close when available, snapshot count, ask move first→last, and BTC spot move over the same window when ticks exist), then prints mean/median/stdev/min/max distributions split by train vs test and by YES vs NO outcomes. It is read-only and local-only: no network calls, no writes, and no ML training. Use the YES−NO mean gaps to shortlist which features deserve a deterministic strategy-rule test next. Accepts `--train-fraction` (same chronological split as `replay-split` / `diagnose-data`).

`evaluate-hypothesis` runs an immutable rule candidate end-to-end. **H-005 / BuyWhenEntryMidpointAbove** buys YES at `first_yes_ask` when entry midpoint is strictly above a threshold. Thresholds are swept on **train fee-adjusted P/L only** (candidate range 50–65c), then the selected cutoff is scored on the untouched test window, expanding-window walk-forward folds, and a sensitivity grid around the selected threshold. The report prints trades, win rate, gross P/L, fees, net P/L, max drawdown, and sign stability, then emits an explicit **promoted** or **probably_noise** verdict. Entry rules must not use path features (`ask_move_cents`, `btc_move_usd`). **H-001** (midpoint below 40) is retired and must not be retuned. Still paper/replay only — no ML and no live trading path.

`replay`, `replay-sweep`, and `replay-split` all model fees using Kalshi's general taker-fee formula, `fee = ceil(rate * price * (1 - price))`, with a conservative default rate of 7% (`--fee-rate`, override per run). Every result reports P/L both fee-excluded and fee-adjusted so a barely-positive pre-fee result can be checked against a more realistic, fee-adjusted one; `replay-split` selects and reports thresholds using the fee-adjusted number. `--min-edge-cents` adds an optional, purely price-derived filter: an entry is only taken if its fee-adjusted best case (contract resolves YES) beats its fee-adjusted worst case (contract resolves NO) by at least that many cents. It defaults to 0 (disabled) and never invents a probability estimate — it only tightens the existing price threshold using numbers already in the trade. Fee-adjusted output is still in-sample or first-pass out-of-sample research, not evidence of a durable edge.

## Architecture

- `data/kalshi.py`: read-only Kalshi REST abstraction.
- `data/btc.py`: BTC price-feed interfaces plus a REST implementation.
- `strategies/`: deterministic strategy protocol, illustrative threshold strategy, and H-005 midpoint-above rule.
- `hypotheses/`: immutable hypothesis registry plus fee-aware train/test, walk-forward, and sensitivity evaluation.
- `engine/`: orchestration, virtual portfolio, paper fill simulation, and risk controls.
- `storage/`: PostgreSQL-ready models and repository for market snapshots, BTC ticks, and paper trades.
- `reporting/`: portfolio metrics, data-health, and data-quality/feature-readiness diagnostics.
- `features.py`: deterministic per-market feature extraction and win/lose distribution reports.

## Assumptions to validate before relying on results

Paper fills are deliberately conservative but simplified: marketable orders fill at the current best price; non-marketable orders stay open. Historical/replay data must include book snapshots suitable for modelling fill probability, fees, partial fills, and latency. Strategy output is an intent, not an instruction to bypass risk controls.
