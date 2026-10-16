# Next Step

## Handoff Prompt For Cursor Or Codex

The Kalshi research prototype lives in the Pulseforge monorepo at `packages/kalshi-research`
([ADR-033](../../docs/adr/ADR-033_Kalshi_Research_Stays_Isolated.md) / [SPEC-049](../../docs/specs/SPEC-049_Kalshi_Research_Package.md)).
Feature research and the first entry-time midpoint hypothesis are in place. Run the H-005
evaluation against the local resolved DB, record the promote / probably_noise verdict, and
only then consider a different entry-time feature — still paper/replay-only, still no ML,
still not deployed.

Context:

- Work inside `packages/kalshi-research` in the Pulseforge git repo. Do not create a standalone
  repository. Do not wire this package into `server.js`, cron, Mission Engine, or Railway deploy.
- This package is paper/replay-only. Do not add live trading, live order submission, credentials
  for order placement, or authenticated trading paths.
- Dataset context from feature-report: ~513 resolved markets (256 train / 257 test at 50% split)
  when the local research DB is present. Midpoint YES/NO separation was consistent
  (train Δ≈+6.00c, test Δ≈+6.28c). Similar gaps exist for first_yes_ask and first_yes_bid.
- Do **not** use `ask_move_cents` or `btc_move_usd` in first-entry rules (post-entry / path leak).
- **H-001** (midpoint < 40) failed robustness and is **retired** — do not retune it.
- **H-005 / BuyWhenEntryMidpointAbove** is implemented:
  - Entry side: YES
  - Entry price: `first_yes_ask`
  - Condition: entry midpoint > threshold
  - Train sweep: 50–65c, select on fee-adjusted train P/L only
  - Untouched test + walk-forward + sensitivity → `promoted` or `probably_noise`
- Strategy module: `kalshi_research/strategies/midpoint.py`
- Registry + evaluator: `kalshi_research/hypotheses/`
- CLI: `list-hypotheses`, `evaluate-hypothesis`
- Synthetic reconstruction from published midpoint means (no local DB in cloud): selected
  **50c**, verdict **promoted** (test net +$22.17 after 7% fees; 4/4 folds +; sensitivity stable).
  Confirm on operator `kalshi_research.db` before treating as authoritative.

Build / next actions:

- With the local `kalshi_research.db` present, run:
  `python -m kalshi_research.cli evaluate-hypothesis --hypothesis-id H-005 --train-fraction 0.5`
- Persist the printed verdict (trades, win rate, gross/fees/net P/L, max drawdown, sign
  stability) into research notes. If `probably_noise`, shortlist a *different* entry-time
  feature (e.g. first_yes_ask band) as a **new** hypothesis ID — do not retune H-001 or mutate
  H-005's definition in place.
- Do not train an ML model. Do not add live trading.
- Keep `README.md`, `SPEC.md`, and this `NEXT_STEP.md` aligned with the latest verdict.

Acceptance:

- `pytest` passes.
- H-005 remains deterministic and replayable from stored snapshots only.
- Entry rule source does not reference `ask_move_cents` or `btc_move_usd`.
- Evaluation report emits an explicit promote / probably_noise verdict.
- No live trading path exists.

## Relevant Files

- `packages/kalshi-research/kalshi_research/strategies/midpoint.py`
- `packages/kalshi-research/kalshi_research/hypotheses/registry.py`
- `packages/kalshi-research/kalshi_research/hypotheses/evaluate.py`
- `packages/kalshi-research/kalshi_research/cli.py`
- `packages/kalshi-research/tests/test_hypothesis_midpoint.py`
- `packages/kalshi-research/kalshi_research/features.py`
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
.venv/bin/python -m kalshi_research.cli list-hypotheses
.venv/bin/python -m kalshi_research.cli evaluate-hypothesis --hypothesis-id H-005 --train-fraction 0.5
.venv/bin/pytest -q
```
