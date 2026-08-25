# ADR-083 — Investigate What Reduces Uncertainty Most

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-163](../specs/SPEC-163_Investigative_Strategy_Engine.md) |

## Context

Scout possesses market definition, universe estimation, investigative reasoning, evidence synthesis, market memory, and business heuristics. However, investigation selection still lacked an explicit strategy for maximizing information gain per unit of effort.

## Decision

An investigation is not a sequence of searches. It is a sequence of decisions that progressively reduce uncertainty.

Scout shall select investigations based on their expected contribution to understanding, not on a fixed source order.

The Investigative Strategy Engine:
- Maintains explicit knowns, unknowns, assumptions, and hypotheses
- Generates candidate investigations per unknown with scored expected information gain
- Consumes activated business heuristics (SPEC-162) to shift priorities
- Recalculates dynamically when evidence resolves unknowns
- Documents expected information gain for every candidate investigation

## Consequences

- Mission Intelligence Report adds an `investigativeStrategy` section
- `explainInvestigationChoice()` provides operator traceability
- `nextQuestions` originate from strategy queue, not static defaults
- Every investigation must have documented expected information gain

## Relationship to Prior ADRs

- **ADR-079** — Understanding before recommendation
- **ADR-080** — Understanding emerges from evidence
- **ADR-081** — Markets are living systems (Market Memory)
- **ADR-082** — Business judgment through reusable heuristics
- **ADR-083** — Investigate what reduces uncertainty most
