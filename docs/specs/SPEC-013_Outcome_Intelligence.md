# SPEC-013 — Outcome Intelligence

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v1.0.0 |
| **Priority** | Critical |
| **Owner** | TBD |
| **Created** | 2026-07-26 |
| **Completed** | 2026-07-26 |
| **Version** | v1.0.0 |

## Objective

Measure whether Pulseforge's intelligence actually produces better business outcomes. Outcome Intelligence evaluates reasoning after the fact — it never changes reasoning. It answers a different question from Operator Intelligence: not “why did the operator ignore this?” but “was the recommendation itself right?”

## Vision References

- `docs/vision/Product_Constitution.md`
- `docs/vision/Intelligence_Architecture.md`
- `docs/vision/Product_Experience.md`
- [SPEC-002](SPEC-002_Max_Reasoning_Engine.md) — strategies + confidence
- [SPEC-011](SPEC-011_Live_Intelligence_Loop.md) — Live Intelligence
- [SPEC-012](SPEC-012_Operator_Intelligence.md) — Operator Intelligence (adjacent; different question)
- [ADR-008](../adr/ADR-008_Outcome_Intelligence.md)

## Problem

Operator Intelligence learns presentation and engagement. It cannot tell whether a dismissed recommendation was poorly presented or actually wrong. There is no empirical loop from executed recommendations → observed business results → strategy accuracy / confidence calibration / drift. Without that loop, confidence remains “the engine believes this,” not “historically this band succeeds X% of the time.”

## Philosophy

```text
Knowledge
        │
Reasoning
        │
Memory
        │
Briefing
        │
Policy
        │
Live Intelligence
        │
Operator Intelligence
──────────────────────────
Outcome Intelligence
──────────────────────────
Command Deck
```

Outcome Intelligence never changes reasoning. It evaluates reasoning.

## Scope

- `RecommendationOutcome` record + richer lifecycle than approve/dismiss
- Strategy-level performance metrics (internal only)
- Confidence calibration reports (empirical success by confidence band)
- Drift detection (strategy underperformance, false positives, acceptance drops)
- Internal Intelligence Review dashboard (Pulseforge engineering command deck)
- HTTP APIs to record outcomes / read calibration / review
- Observe Generated recommendations from compose (additive; no model mutation)

## Out of Scope

- Rewriting history, altering reasoning, or mutating recommendation confidence
- Changing recommendations shown to customers
- Customer-facing accuracy analytics
- Durable Postgres outcome log (process-scoped in v1, same as Operator / Live)
- Autonomous strategy reweighting from outcomes
- Cross-tenant outcome learning

## Dependencies

- ✅ SPEC-002 Reasoning Engine (strategies + confidence to attribute)
- ✅ SPEC-007 / SPEC-008 Command Deck compose + UI
- ✅ SPEC-012 Operator Intelligence (adjacent lifecycle; not subsumed)

## Architecture

```text
Command Deck compose / Operator executed / CRM observation
        │
        ▼
POST /api/v1/outcome/records | lifecycle
        │
        ▼
OutcomeEngine
        ├── OutcomeStore (append-only RecommendationOutcome)
        ├── LifecycleTracker (Generated → … → terminal)
        ├── CalibrationReport (confidence bands)
        ├── StrategyPerformance (per-strategy metrics)
        ├── DriftDetector (internal alerts)
        └── ReviewDashboard (internal engineering view)
                │
                ▼
GET /api/v1/outcome/review | calibration | strategies | drift
```

### Hard rules

Outcome Intelligence **may**:

- evaluate
- measure
- calibrate
- report

It **may never**:

- rewrite history
- alter reasoning
- manipulate confidence retroactively
- change recommendations

The deterministic stack remains authoritative.

## Data Model

No new Postgres tables in v1. Process-scoped stores.

### RecommendationOutcome

```text
RecommendationOutcome {
  recommendationId
  tenantId
  strategyId?
  executed
  outcome                 // successful | unsuccessful | inconclusive | null (not yet)
  lifecycle               // generated → … → terminal
  observedAt
  confidenceAtRecommendation
  confidenceAtOutcome?
  notes?
  generatedAt
  reviewedAt?
  approvedAt?
  executedAt?
  evidenceSourceIds?
}
```

### Lifecycle

```text
Generated → Reviewed → Approved → Executed → Observed
                                              ↓
                                    Successful | Unsuccessful | Inconclusive
```

Terminal business results are distinct from Operator Intelligence engagement outcomes (`dismissed` / `expired` / trust). Operator may explain *why* something was ignored; Outcome explains *whether intelligence was right*.

### Confidence bands (calibration)

```text
90+ · 80–89 · 70–79 · 60–69 · <60
```

Report: for each band, success rate among observed outcomes — never writes back onto recommendation.confidence.

## Implementation Plan

1. Spec + ADR-008 + heartbeat docs
2. `packages/max/outcome/` — types, store, lifecycle, calibration, strategy metrics, drift, review, engine
3. Wire into `createMaxReasoningRuntime` (observeGenerated on compose; no customer-facing mutation)
4. HTTP: records, lifecycle, calibration, strategies, drift, review
5. Tests

## Migration Strategy

- Additive only; no schema migration
- Decks unchanged for customers
- Rollback: stop recording / ignore outcome APIs

## Testing

- Unit: outcome build, lifecycle transitions, calibration bands, strategy metrics, drift alerts, review dashboard
- Unit: OutcomeEngine observe → record → report
- Unit: runtime compose does not mutate confidence / cards from outcome layer

## Acceptance Criteria

- [x] Recommendation outcome model implemented
- [x] Strategy-level performance metrics tracked
- [x] Confidence calibration reports generated
- [x] Drift detection implemented
- [x] Internal engineering dashboard available
- [x] No customer-facing reasoning changes
- [x] No deterministic logic modified

## Future Work

- Durable outcome log when knowledge dual-write ships
- CRM close / booking signals → automatic Observed → Successful
- Optional strategy weight proposals (still require human ADR — never auto-apply)
- Evidence-source quality scoring from outcome correlation
