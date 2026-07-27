# ADR-008 — Outcome Intelligence

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-26 |
| **Spec** | [SPEC-013](../specs/SPEC-013_Outcome_Intelligence.md) |
| **Supersedes** | — |
| **Related** | [ADR-002](ADR-002_Explainable_AI.md), [ADR-006](ADR-006_Live_Intelligence_Evolution.md), [ADR-007](ADR-007_Operator_Intelligence.md) |

## Context

Operator Intelligence (ADR-007) learns how humans engage with recommendations — presentation, dismissals, Max habits. That does not answer whether the intelligence itself was correct. Folding empirical outcome learning into Reasoning or Policy would blur explainability (ADR-002) and risk retroactive confidence manipulation. Live Intelligence (ADR-006) asks “is this still true?” Outcome Intelligence asks “was this recommendation right for the business?”

## Decision

**Outcome Intelligence is a separate evaluation layer above Operator Intelligence and below / beside Command Deck.**

1. Every recommendation may become a measurable `RecommendationOutcome` with an explicit lifecycle: Generated → Reviewed → Approved → Executed → Observed → Successful | Unsuccessful | Inconclusive.
2. Strategy-level performance metrics and confidence calibration reports are computed for internal engineering use only.
3. Drift detection alerts Pulseforge operators (engineers), never customers.
4. Calibration never rewrites `recommendation.confidence`, never alters strategy scores, and never changes cards shown on the Command Deck.
5. Observing Generated recommendations from compose is additive registration only — the deck model is not mutated for Outcome Intelligence.
6. v1 storage is process-scoped (same posture as LiveLoop EventStore and Operator InteractionStore).

## Consequences

### Positive

- Clear separation: presentation learning vs intelligence accuracy
- Empirically grounded calibration reports without corrupting the deterministic stack
- Early warning when strategies or evidence sources degrade

### Negative / tradeoffs

- Process-scoped outcomes reset on deploy
- Manual / API observation required until CRM outcome signals are wired
- Calibration quality depends on volume of Observed → terminal outcomes

### Follow-ups

- SPEC-013 OutcomeEngine + internal review APIs
- Durable outcome log when dual-write ships
- CRM booking/close → automatic terminal outcomes
