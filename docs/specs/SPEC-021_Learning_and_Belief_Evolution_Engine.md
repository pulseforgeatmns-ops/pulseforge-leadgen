# SPEC-021 — Learning & Belief Evolution Engine

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v1.0.1 |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Completed** | 2026-07-27 |
| **Depends on** | SPEC-018, SPEC-019, SPEC-020 |
| **Blocks** | Research calibration UI; strategy accuracy dashboards |

## Objective

Allow the Evidence Platform to learn from historical outcomes by adjusting belief strength based on observed accuracy.

The engine updates confidence overlays **only after reality is known**.

Success: given a history of Claims and Outcomes, the engine reports accuracy, precision, recall (where applicable), calibration, and confidence adjustments — without mutating history, replay, or runtime confidence.

**No machine learning. No neural networks. No black-box optimization.**

## Vision References

- [ADR-009 Evidence Platform Architecture](../adr/ADR-009_Evidence_Platform_Architecture.md)
- [SPEC-018 Deterministic Replay](SPEC-018_Deterministic_Replay_and_Temporal_Reasoning_Engine.md)
- [SPEC-019 Evidence Laboratory](SPEC-019_Evidence_Laboratory.md)
- [SPEC-020 Evidence Query Language](SPEC-020_Evidence_Query_Language.md)

## Problem

Replay regenerates reasoning; the Laboratory explores counterfactuals; EQL queries the graph. None of these calibrate belief against outcomes.

Without a learning layer:

- Claim confidence never earns or loses trust from reality
- Strategy packs have no historical accuracy surface
- Calibration cannot be explained separately from runtime confidence

## Scope

- Package `packages/learning/`
  - `LearningEngine.js` — façade (consumes Claims / Evidence / Outcomes)
  - `BeliefTracker.js` — claim occurrence / correct / incorrect / accuracy
  - `CalibrationEngine.js` — confidence vs historical calibration vs adjusted confidence
  - `OutcomeEvaluator.js` — correct / incorrect / partially_correct / unresolved
  - `LearningSession.js` — isolated copy-on-write learning run
  - `types.js` — rules, verdicts, typedefs
  - `index.js` — public API (`createLearningEngine`)
- EQL integration: `SHOW Calibration FOR Claim("…")`, `SHOW Accuracy FOR StrategyPack("…")`
- Laboratory integration: `lab.compareCalibration(...)`, `lab.replayWithCalibration(...)`
- Unit tests covering acceptance criteria
- Spec index + CHANGELOG / CURRENT_STATE updates

## Out of Scope

- Machine learning / neural nets / gradient optimization
- Mutating stored claim confidence or replay step confidence
- Persisting learning aggregates to Postgres (future)
- Cross-tenant learning
- UI for calibration dashboards
- Changing Outcome Intelligence (SPEC-013) Max recommendation outcomes

## Dependencies

- ✅ SPEC-018 Deterministic Replay
- ✅ SPEC-019 Evidence Laboratory
- ✅ SPEC-020 Evidence Query Language

## Architecture

```text
Observations
      │
      ▼
   Claims
      │
      ▼
Outcome Recorded
      │
      ▼
Learning Engine
      │
      ▼
Belief Calibration
```

### Guiding principle

> Evidence earns trust. Outcomes calibrate trust.

### Learning rules

| Rule | Meaning |
|---|---|
| 1 | Confidence overlays update only after reality is known |
| 2 | Never mutate history |
| 3 | Never mutate replay |
| 4 | Never mutate runtime confidence |
| 5 | No ML / neural nets / black-box optimization |
| 6 | Every calibration is explainable |

### Components

**OutcomeEvaluator** — classifies claim vs outcome as `correct` | `incorrect` | `partially_correct` | `unresolved`. Records credit. Does not invent correctness when reality is missing.

**BeliefTracker** — aggregates every Claim over time:

| Field | Example |
|---|---|
| Claim | Momentum Continuation |
| Occurrences | 127 |
| Correct | 91 |
| Incorrect | 36 |
| Accuracy | 71.65% |

**CalibrationEngine** — keeps calibration separate from confidence:

| Field | Example |
|---|---|
| Confidence | 82% |
| Historical Calibration | 67% |
| Adjusted Confidence | 74% |

Deterministic blend (default equal weight):

```text
adjusted = w · confidence + (1 − w) · historicalCalibration
```

**LearningEngine** — consumes Claims · Evidence · Outcomes; produces calibration updates, historical accuracy, confidence adjustments.

**LearningSession** — isolated session; copy-on-write outcomes; never writes production.

### Explainability

Every calibration explains:

- observations considered
- outcome
- historical statistics
- confidence before
- confidence after

### EQL Integration

```eql
SHOW Calibration
FOR Claim("momentum_continuation")

SHOW Accuracy
FOR StrategyPack("market")
```

### Laboratory Integration

```js
lab.compareCalibration({ left, right })
lab.replayWithCalibration({ experiment, outcomes })
```

## Data Model

No new durable tables. Learning state is process-local.

### Evaluation record

```js
{
  id, claimId, claimType, subjectId, strategyPack,
  confidenceBefore, verdict, credit,
  outcomeId, observedAt, observationsConsidered, explanation,
  mutatesHistory: false, mutatesReplay: false, mutatesRuntime: false,
}
```

### Calibration result

```js
{
  claimId, claimType,
  confidence,               // runtime belief (read-only overlay input)
  historicalCalibration,    // historical performance
  adjustedConfidence,       // blended overlay (not written back)
  blendWeight, stats, explanation,
  mutatesHistory: false, mutatesReplay: false, mutatesRuntime: false,
}
```

## Implementation Plan

1. Land `packages/learning/` with evaluator, tracker, calibration, session, engine
2. Extend EQL targets (`calibrations`, `accuracies`, `strategy_packs`) + `SHOW … FOR`
3. Wire `lab.compareCalibration` / `lab.replayWithCalibration`
4. Unit tests; index SPEC-021; update CHANGELOG + CURRENT_STATE

## Migration Strategy

None. Pure additive package. No schema changes. Production evaluate / dual-write / replay paths unchanged. Runtime claim confidence is never written by this package.

## Testing

```bash
npm run test:learning
npm run test:eql
npm run test:laboratory
```

Coverage:

- Outcome verdicts: correct / incorrect / partial / unresolved
- BeliefTracker accuracy for the 91/36 example (71.65%)
- Calibration blend 82% × 67% → ~74% with full explanation
- LearningEngine reports accuracy, precision, recall (n/a without FN), calibration, adjustments
- Inputs frozen (no history mutation)
- EQL `SHOW Calibration FOR Claim` / `SHOW Accuracy FOR StrategyPack`
- Lab `compareCalibration` / `replayWithCalibration` isolation flags

## Acceptance Criteria

- [x] `packages/learning/` delivers LearningEngine, BeliefTracker, CalibrationEngine, OutcomeEvaluator, LearningSession, types, index
- [x] Given Claims + Outcomes, engine reports accuracy, precision, recall (where applicable), calibration, confidence adjustments
- [x] No history mutation
- [x] No replay mutation
- [x] No runtime mutation
- [x] Every calibration explains observations, outcome, historical stats, confidence before/after
- [x] EQL supports `SHOW Calibration FOR Claim("…")` and `SHOW Accuracy FOR StrategyPack("…")`
- [x] Laboratory supports `compareCalibration` and `replayWithCalibration`
- [x] Spec documented at `docs/specs/SPEC-021_Learning_and_Belief_Evolution_Engine.md`

## Future Work

- Durable belief aggregates (Postgres) with tenant scoping
- Explicit false-negative ingest for recall across claim types
- Calibration UI / operator review surface
- Optional ADR naming Learning as an Evidence Platform subsystem
- Time-decayed calibration windows
