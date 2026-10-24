# Next Step

## Handoff Prompt For Cursor Or Codex

The Kalshi research prototype now lives in the Pulseforge monorepo at `packages/kalshi-research`
([ADR-033](../../docs/adr/ADR-033_Kalshi_Research_Stays_Isolated.md) / [SPEC-049](../../docs/specs/SPEC-049_Kalshi_Research_Package.md)).
Feature research scaffolding is in place. The dataset is ready, the fee-aware threshold baseline
was not durable, and `feature-report` now prints deterministic win/lose feature distributions
over the chronological train/test split. The next task is to turn the strongest feature gaps
into explicit strategy rules — still paper/replay-only, still no ML, still not deployed.

Context:

- Work inside `packages/kalshi-research` in the Pulseforge git repo. Do not create a standalone
  repository. Do not wire this package into `server.js`, cron, Mission Engine, or Railway deploy.
- This package is paper/replay-only. Do not add live trading, live order submission, credentials
  for order placement, or authenticated trading paths.
- Dataset readiness (`diagnose-data`): 289 markets observed, 288 resolved, 1 unresolved;
  144 train / 144 test at 50% split; 0 missing bid/ask; mean ~71.9 snapshots/market;
  16 markets with >60s gaps (acceptable under the gap threshold). Verdict: ready.
- Fee-aware threshold baseline (`replay-split`) did **not** hold up on fee-adjusted test P/L —
  do not keep tuning thresholds as the primary research path.
- `kalshi_research/features.py` implements `extract_market_features()` /
  `inspect_feature_report()` / `format_feature_report()`, wired through `kalshi_research/cli.py`
  as `feature-report`. Read-only and local-only: no network calls, no writes, no model training.
- Features currently reported (per resolved market):
  - first yes_ask / first yes_bid
  - bid/ask spread and midpoint
  - seconds from first observation to close_time (when available)
  - snapshot count
  - ask move (last − first)
  - BTC spot move over the same observation window (when ticks exist)
- Report layout: train and test blocks; within each, YES vs NO outcome groups with
  mean/median/stdev/min/max plus Δmean (YES − NO).

Build:

- Run `feature-report` and shortlist 1–2 features whose YES/NO separation is large in train
  **and** directionally consistent in test (ignore fragile one-sided gaps).
- Encode each shortlisted feature as a deterministic strategy rule (same style as
  `BuyBelowThreshold`: pure function of snapshot inputs → order intents).
- Add fee-aware replay coverage for the new rule(s) using the existing train/test split
  machinery; do not optimize on the test window.
- Do not train an ML model. Do not add live trading.
- Update `README.md`, `SPEC.md`, and this `NEXT_STEP.md` when the first feature-based rule
  lands with train/test fee-adjusted results.

Acceptance:

- `pytest` passes.
- New rule(s) are deterministic and replayable from stored snapshots only.
- Fee-adjusted train/test results are reported (no claim of durable edge from one sample).
- `feature-report`, `diagnose-data`, and existing replay commands still work.
- No live trading path exists.

## Relevant Files

- `packages/kalshi-research/kalshi_research/features.py`
- `packages/kalshi-research/kalshi_research/cli.py`
- `packages/kalshi-research/kalshi_research/strategies/threshold.py`
- `packages/kalshi-research/kalshi_research/strategies/base.py`
- `packages/kalshi-research/kalshi_research/replay.py`
- `packages/kalshi-research/tests/test_features.py`
- `docs/adr/ADR-033_Kalshi_Research_Stays_Isolated.md`
- `docs/specs/SPEC-049_Kalshi_Research_Package.md`
- `packages/kalshi-research/README.md`
- `packages/kalshi-research/SPEC.md`
- `packages/kalshi-research/NEXT_STEP.md`

## Optional Baseline Commands

```bash
cd packages/kalshi-research
.venv/bin/python -m kalshi_research.cli diagnose-data
.venv/bin/python -m kalshi_research.cli feature-report --train-fraction 0.5
.venv/bin/python -m kalshi_research.cli replay-split --train-fraction 0.5
```
