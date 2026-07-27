# ADR-007 — Operator Intelligence

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-26 |
| **Spec** | [SPEC-012](../specs/SPEC-012_Operator_Intelligence.md) |
| **Supersedes** | — |
| **Related** | [ADR-002](ADR-002_Explainable_AI.md), [ADR-005](ADR-005_LLM_Presentation_Engine.md), [ADR-006](ADR-006_Live_Intelligence_Evolution.md) |

## Context

Live Intelligence (ADR-006) answers “is this still true?” Operator experience still treats every tenant and every morning the same: identical section emphasis, identical Max suggestion chips, no memory of what was approved, ignored, or investigated. Folding that learning into Reasoning or Policy would corrupt the deterministic stack and blur explainability (ADR-002).

## Decision

**Operator Intelligence is a separate presentation layer above Live Intelligence and below Command Deck / Max.**

1. Every meaningful operator action is an append-only `InteractionEvent` (behavior, not analytics productization).
2. Per-recommendation `RecommendationLearning` and an explicit outcome lifecycle track engagement and decisions.
3. An internal trust/usefulness signal may be computed from outcomes — it never replaces confidence, score, or policy.
4. Adaptive presentation may reorder sections and change visual dominance; it must never hide intelligence, alter evidence, rewrite reasoning, or override policy.
5. Max suggestion chips may be personalized from tenant conversational preferences while remaining deterministic and grounded in verified context (ADR-005).
6. The Intelligence Quality Dashboard is internal-only.

v1 storage is process-scoped (same posture as LiveLoop EventStore and Workspace SessionStore).

## Consequences

### Positive

- Presentation improves without threatening the authoritative intelligence stack
- Clear boundary: market reasoning vs operator interaction learning
- Quality metrics give a path to improve Pulseforge itself

### Negative / tradeoffs

- Process-scoped learning resets on deploy
- Adaptive order can surprise operators until habits stabilize
- Trust signal must be carefully labeled so it is never mistaken for confidence

### Follow-ups

- SPEC-012 OperatorEngine + Command Deck / Max wiring
- Durable operator event log when dual-write ships
- Optional per-operator (vs tenant) preference profiles
