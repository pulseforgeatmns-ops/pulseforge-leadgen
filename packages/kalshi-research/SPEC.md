# Kalshi BTC Research Platform — Product Specification

## Status

**Phase 2: in progress.** The platform is a research and simulation tool. It must not place, route, or authorize live orders.

**Monorepo:** lives at `packages/kalshi-research` in Pulseforge ([SPEC-049](../../docs/specs/SPEC-049_Kalshi_Research_Package.md) / [ADR-033](../../docs/adr/ADR-033_Kalshi_Research_Stays_Isolated.md)). Isolated from production execution — not deployed, not imported by Node services.

This file is the canonical package research specification and should be updated whenever the agreed scope or a design decision changes.

## Objective

Evaluate whether deterministic, risk-controlled strategies can find repeatable edge in short-duration BTC event markets, using live-data paper trading and historical replay before any consideration of real-money execution.

## Safety and scope boundaries

- Supported execution modes: `paper` and `replay` only.
- No live-order client, credentials, order endpoint, or live-trading configuration may be added in Phase 1–3.
- Every strategy is deterministic code: the same input snapshot produces the same decision.
- The system records the inputs, signal rationale, risk decision, simulated fill, settlement, and P/L for each trade.
- An LLM may help research and engineering, but it is not an execution decision-maker.

## Users and primary workflows

### Researcher

1. Collect BTC market snapshots and underlying BTC spot prices.
2. Replay stored data through one or more strategy versions.
3. Compare performance, calibration, drawdown, and fill assumptions.

### Paper-trading operator

1. Start the market watcher in `paper` mode.
2. Observe strategy decisions, risk rejections, simulated fills, portfolio exposure, and daily P/L.
3. Review a daily report; stop or adjust a strategy only through explicit configuration/code changes.

## Architecture

| Layer | Responsibility | Current implementation |
| --- | --- | --- |
| Data adapters | Read-only market and BTC price retrieval | `data/kalshi.py`, `data/btc.py` |
| Domain | Typed snapshots, signals, orders, statuses | `domain.py` |
| Strategies | Pure deterministic decisions from supplied inputs | `strategies/` |
| Risk | Exposure, cash, open-order, and loss limits | `engine/risk.py` |
| Simulation | Conservative simulated order/fill behavior | `engine/paper.py` |
| Portfolio | Virtual cash, positions, P/L | `engine/portfolio.py` |
| Storage | PostgreSQL-compatible snapshot/trade tables | `storage/models.py` |
| Reporting | Metrics, paper-only dashboard, and data-quality diagnostics | `reporting/`, `dashboard.py` |

## Required data

For every observed market snapshot, retain:

- Market ticker, observation time, bid, ask, last price, and volume.
- Underlying BTC spot price, observation time, and source.
- Market metadata: title, close/expiration time, event rules, and eventual settlement result.
- Strategy version/configuration identifier.

For every trade intent, retain:

- Signal ID, ticker, side, quantity, limit, strategy, rationale, and creation time.
- Risk decision and rejection reason, if any.
- Fill assumption, fill price/time, fees, and later settlement/P&L.

## Paper-fill model

Initial model:

- A marketable limit order fills at the contemporaneous best displayed price.
- A non-marketable limit order remains open.
- No partial fills, queue position, cancellation lifecycle, latency, or fees are assumed yet.

The model must be versioned and progressively made more conservative. Results must always display the fill-model version used.

## Risk controls

- Max per-order/position exposure (cents)
- Max virtual contracts per market (default: 1)
- Max daily realized loss (cents)
- Max open orders
- Available virtual cash check
- Fail closed: unavailable or stale market inputs produce no new order

Future controls: per-market exposure, correlated-position exposure, stale-data threshold, strategy kill switch, and circuit-breaker reporting.

## Delivery plan

### Phase 1 — Foundation (complete)

- Modular project, configuration, logging, core domain types
- Read-only data abstractions
- Deterministic strategy contract
- Virtual portfolio, paper executor, risk manager
- PostgreSQL-ready schema and dashboard/reporting scaffold

### Phase 2 — Data capture and persistence (next)

- Discover/filter active BTC markets — **implemented as a public, keyword-filtered single collection pass**
- Targeted initial series: `KXBTC15M` — Kalshi's Bitcoin price up/down, fifteen-minute series.
- Capture market + BTC ticks on a configurable interval or websocket feed — **single-pass and scheduled polling collection implemented; websocket capture remains**
- Persist all snapshots, signals, decisions, and paper orders — **implemented for one collection pass**
- Add idempotent repositories, database initialization, and retention-safe logging — **database initialization and idempotent trade persistence implemented**
- Persist market metadata and resolution/settlement state

**Acceptance:** a multi-hour paper run can be restarted safely and leaves a queryable, timestamped audit trail with no live API writes.

### Phase 3 — Replay and settlement

- Replay stored snapshots in timestamp order with a deterministic clock — **implemented for the baseline threshold rule**
- Apply market resolution to close positions and calculate final P/L — **implemented for filled paper trades; replay clock remains**
- Create performance report: trade count, win rate, P/L, max drawdown, exposure, and fill rate
- Compare baseline strategy parameters with deterministic replay sweeps — **implemented for YES ask thresholds**
- Add simple out-of-sample train/test replay validation — **implemented for chronological threshold splits**
- Model trading fees and report fee-adjusted P/L alongside fee-excluded P/L — **implemented: `fee = ceil(rate * price * (1 - price))`, Kalshi's general taker-fee formula, conservative default rate 7%, configurable via `--fee-rate`; `replay-split` selects thresholds on the fee-adjusted number**
- Add a configurable minimum-edge filter so entries require a fee-adjusted margin between best and worst case — **implemented as `--min-edge-cents` (default 0/disabled); price-derived only, does not invent a probability signal**
- Version strategies and fill models in every result

**Acceptance:** the same dataset + strategy version reproduces the same trades and report.

### Phase 4 — Research dashboard and validation

- Build a read-only dashboard for runs, positions, trades, performance, and data health
- Initial local data-health report — **implemented as `kalshi-research report`**
- Data-quality and feature-readiness diagnostics (density, within-market gaps, missing bid/ask,
  price distribution, fee drag by price band, explicit ready/not-ready verdict) — **implemented as
  `kalshi-research diagnose-data`, read-only and local-only**
- Deterministic feature extraction + win/lose distribution report over train/test splits —
  **implemented as `kalshi-research feature-report` (`kalshi_research/features.py`); no ML training**
- Immutable hypothesis/rule candidates with fee-aware train selection, untouched test scoring,
  walk-forward folds, sensitivity, and promote / probably_noise verdict —
  **implemented as `kalshi-research evaluate-hypothesis` (`kalshi_research/hypotheses/`);
  first entry-time candidate is H-005 BuyWhenEntryMidpointAbove (midpoint > train-selected
  threshold in 50–65c; entry at first_yes_ask). Path features (`ask_move_cents`, `btc_move_usd`)
  are forbidden in entry rules. Retired H-001 (midpoint < 40) must not be retuned.**
- Compare strategies against holdout periods and baseline/no-trade controls
- Add calibration and regime breakdowns once sufficient resolved data exists

**Acceptance:** decisions and outcomes can be inspected end-to-end without reading logs.

## Explicit non-goals

- Live trading or funding an account
- Claiming a strategy is profitable from a small sample
- Using an LLM to autonomously make execution decisions
- Optimizing strategies on the same data used for validation

## Operating memory

- `SPEC.md` is the canonical product spec and must be updated whenever scope, safety boundaries, architecture, or research methodology changes.
- `NEXT_STEP.md` is the canonical handoff file and must be updated whenever the recommended next action changes.
- Every Codex handoff/final answer for this project must include a usable next-action package, not just a label. Include:
  - a Cursor/Codex prompt Jake can paste into another coding agent when implementation is next,
  - the relevant files/context that agent needs,
  - current facts/results that should constrain the work,
  - terminal command(s) only when Jake actually needs to run something.

## Current next implementation task

Dataset readiness cleared for feature research. `feature-report` showed consistent entry-time
midpoint separation (train YES mean ≈52.60 vs NO ≈46.60, Δ≈+6.00c; test YES ≈52.24 vs NO ≈45.96,
Δ≈+6.28c) on the chronological split, with similar gaps for first_yes_ask / first_yes_bid.

**H-005 / BuyWhenEntryMidpointAbove** is implemented as an immutable candidate:
buy YES at `first_yes_ask` when entry midpoint > threshold; sweep 50–65c on train fee-adjusted
P/L only; score the selected cutoff on untouched test, walk-forward folds, and sensitivity.
Verdict is `promoted` or `probably_noise`. Entry rules must not use `ask_move_cents` or
`btc_move_usd`. **H-001** (midpoint < 40) remains retired and must not be retuned.

A distributional reconstruction from the published midpoint means (256/257, fee rate 7%)
selected threshold **50c** and returned **promoted** (test net +$22.17; 4/4 walk-forward folds
positive; sensitivity sign-stable). That reconstruction is **not** a substitute for the operator
local `kalshi_research.db`. Next step: run `evaluate-hypothesis --hypothesis-id H-005` on the
real resolved DB and treat that printout as authoritative. If the real run is `probably_noise`,
shortlist a different entry-time feature under a new hypothesis ID. Still paper/replay-only —
no ML training and no live trading.
