# ADR-086 — Every Decision Must Teach

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-166](../specs/SPEC-166_Outcome_Learning_Engine.md) |

## Context

ADR-085 introduced Strategic Decision — allocating finite effort toward the best business outcome. That ADR answers **what should the business do today?** but does not close the loop when outcomes arrive.

Intelligence without feedback eventually becomes opinion. A decision has no lasting value unless its outcome improves the next decision.

## Decision

Introduce **Outcome Learning** after Execution and Observe:

```
Decision → Execution → Outcome → Learning → Future Decisions
```

Principles:

1. **Every recommendation becomes a prediction** — expected outcome, probability, business value
2. **Every prediction resolves** — compared against observed business outcome (walkthrough, lost, no answer, etc.)
3. **Every evaluation teaches** — root cause, heuristic calibration, strategy updates, organizational knowledge
4. **Never auto-apply** — learnings are durable records for operator review; live missions and MIR confidence are not mutated retroactively (ADR-008)
5. **No recommendation disappears** — pending predictions remain tracked until resolved or marked inconclusive

## Consequences

- Mission Intelligence Report gains `outcomeReview` section (SPEC-166)
- Scout discovery contributions auto-capture predictions from MIR recommendations
- Terminal AMO outcomes (`walkthrough_booked`, `lost`, etc.) auto-evaluate pending predictions
- Heuristic `learnFromOutcome()` (SPEC-162) is wired into the learning pipeline
- Operator Q&A supports "What have we learned this month?" via mission inspection
- New persistence tables for predictions, evaluations, and outcome learnings

## Relationship to Prior ADRs

- **ADR-079** — Understanding before recommendation
- **ADR-082** — Business judgment through reusable heuristics (calibrated by outcomes)
- **ADR-084** — Opportunity intelligence optimizes what matters
- **ADR-085** — Strategic decision optimizes allocation of finite effort
- **ADR-086** — Outcome learning closes the prediction loop

## Principle

PulseForge exists to compound business intelligence. Every completed mission should leave the organization measurably smarter than it was before.

A decision has no lasting value unless its outcome improves the next decision.
