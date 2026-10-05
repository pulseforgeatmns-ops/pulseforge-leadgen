# Signal V1 — Validation Cohort 001

**Cohort ID:** `cohort-signal-v1-validation-001`  
**Selection version:** `validation-001-selection-v1`  
**Research definition:** `signal-research-v1`  
**Status:** Frozen (paper/research only)

## Pool and selection

| Metric | Value |
|---|---:|
| Candidate pool (discovered) | 63 |
| Unique after dedupe | 63 |
| Eligible | 63 |
| Selected | 40 |
| Stronger category selected | 20 |
| Failure category selected | 20 |

Selection procedure: stable sort by token address, then earliest call timestamp; first 20 per `stronger` / `failure` category. Duplicate token/event pairs are rejected with recorded exclusion reasons.

## Frozen membership

Membership is frozen at cohort creation. After `frozenAt`, membership mutation requires a new cohort version. DOOM, DUPLICATE, and HALLOW remain eligible when they satisfy cohort rules (they may or may not appear in the deterministic top-20 per category).

## Data coverage (cohort N = 40)

| Coverage | N |
|---|---:|
| Replay produced research observations | 38 |
| Market history attempted | 40 |
| Wallet evidence present | 0 |
| Structure evidence present | 0 |

Market history uses fixture acquisition paths for research tokens without live OHLCV in CI; production Postgres replay uses persisted observations from the same backfill pipeline.

## Primary layer comparison (execution delay = 1 minute)

Precision = PASS / resolved (PASS + FAIL). Denominators are explicit in export JSON.

| Layer | cohort N | triggered | market evaluable | resolved | PASS | FAIL | UNRES | Precision | Median MFE | Median MAE |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| FIRST_CALLER | 40 | 38 | 38 | 38 | 18 | 20 | 0 | 0.47 | 0.00 | -0.30 |
| INDEPENDENT_CONVERGENCE | 40 | 18 | 18 | 18 | 18 | 0 | 0 | 1.00 | 1.50 | 0.00 |
| QUALITY_CONVERGENCE | 40 | 0 | 0 | 0 | 0 | 0 | 0 | — | — | — |
| WALLET_CONFIRMATION | 40 | 0 | 0 | 0 | 0 | 0 | 0 | — | — | — |
| STRUCTURE_GATE | 40 | 0 | 0 | 0 | 0 | 0 | 0 | — | — | — |
| AMPLIFIER_ARRIVAL | 40 | 0 | 0 | 0 | 0 | 0 | 0 | — | — | — |
| SIGNAL_ENTRY | 40 | 0 | 0 | 0 | 0 | 0 | 0 | — | — | — |

**Low-N warnings:** Quality convergence, wallet confirmation, structure gate, amplifier arrival, and signal entry have insufficient triggered N in this fixture-backed run to support conclusions.

## Secondary analyses

See machine-readable export for:

- Convergence velocity buckets (≤5m / ≤15m / ≤30m / ≤60m)
- Independent cluster count buckets (1 / 2 / 3 / 4+)
- Execution-delay sensitivity (15s, 30s, 1m, 3m, 5m)

Export: `signal-v1/validation-cohort-001-evaluation.json`  
CLI: `npm run signal:validation-cohort-001 -- --export=signal-v1/validation-cohort-001-evaluation.json`  
API: `GET /api/v1/signal/research/cohorts/cohort-signal-v1-validation-001/evaluation/export`

## Research questions (factual)

**A. Does independent convergence outperform FIRST_CALLER?**  
Among tokens where both layers triggered at 1m delay, independent convergence shows higher precision (18/18 resolved PASS vs 18/38 PASS for first caller). Triggered N differs (18 vs 38), so this is not a like-for-like sample.

**B. Does quality weighting improve it further?**  
Quality convergence did not trigger in this cohort run (N = 0).

**C. Does structure filtering reduce MAE/failures?**  
Structure gate did not trigger (N = 0); no comparison available.

**D. Does wallet confirmation add information?**  
Wallet confirmation did not trigger (N = 0).

**E. What happens after amplifier arrival?**  
Amplifier arrival did not trigger (N = 0).

**F. How sensitive are results to 15s–5m execution delay?**  
See export `executionDelaySensitivity`. First-caller precision is stable at ~0.47 for 15s–3m in this artifact; 5m delay shifts independent convergence to unresolved-heavy outcomes.

**G. Which layers have too little N?**  
Quality convergence, wallet confirmation, structure gate, amplifier arrival, signal entry.

No claim of live trading edge is supported by this research-only, fixture-heavy cohort artifact alone.
