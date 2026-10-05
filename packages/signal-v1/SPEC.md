# SIGNAL-V1

Implementation of the attention-driven Solana market intelligence engine (paper-only).

See repository issue/spec for full doctrine. V1 explicitly excludes real-money execution paths.

## Ground truth (SIGNAL-V1-002)

- Historical market observations persist in `signal_market_observations` (Postgres in production).
- Default provider: **GeckoTerminal** public OHLCV (`aggregate=1` → 1-minute candles when trades exist; gaps are not interpolated).
- Operator commands: `npm run signal:ingest-history`, `npm run signal:replay`.
- PASS/FAIL/UNRESOLVED uses achievable entry (15s–5m delays) from observed prices only.

## Validation cohort 001 (SIGNAL-V1-003 Phase B)

- Cohort ID: `cohort-signal-v1-validation-001` (frozen, selection version `validation-001-selection-v1`).
- Acquisition pipeline: `packages/signal-v1/acquisition/` (providers, eligibility, deterministic selection, backfill, replay).
- Operator report: `signal-v1/VALIDATION_COHORT_001_REPORT.md`.
- Export: `npm run signal:validation-cohort-001` or `GET .../evaluation/export`.

## Empirical validation cohort 003 (SIGNAL-V1-005)

- Cohort ID: `cohort-signal-v1-validation-003` (`validation-003-selection-v1`, `dataClass: EMPIRICAL`).
- Natural chronological selection from `pulseforge-historical-caller-catalog-v1` (no outcome balancing).
- Hard empirical guard: `assertEmpiricalCohort()` fail-closes on procedural caller/market evidence.
- Export: `npm run signal:validation-cohort-003` → `signal-v1/validation-cohort-003-evaluation.json`.

## Convergence integrity audit (SIGNAL-V1-004)

- Holdout cohort: `cohort-signal-v1-validation-002` (`validation-002-selection-v1`), built only from `procedural-holdout-catalog-v2` with evidence patterns decoupled from selection labels.
- Audit runner: `npm run signal:convergence-audit` → full audit table, provenance counts, Wilson intervals, negative controls, cross-cohort comparison.
- Regression tests: `packages/signal-v1/tests/convergenceIntegrityAudit.test.js`.
