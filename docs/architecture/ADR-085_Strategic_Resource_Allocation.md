# ADR-085 — Allocate Finite Effort Toward the Best Business Outcome

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-165](../specs/SPEC-165_Strategic_Decision_Engine.md) |

## Context

Opportunity Intelligence (SPEC-164 / ADR-084) ranks what matters. Ranking is necessary and insufficient. Operators have finite hours, finite AOs, and competing work. Recommending the highest-ranked opportunity as if the rest of the day were free is analyst thinking, not COO thinking.

## Decision

Introduce **Strategic Decision** between Opportunity Intelligence and the operator.

- Scout answers: **What is true?**
- Opportunity Intelligence answers: **What matters?**
- Strategic Decision answers: **What should the business actually do today?**

Max allocates capacity. Every daily recommendation is a resource mix with explicit tradeoffs, expected business outcome, and confidence. Activities are instruments — never ends.

## Consequences

- Mission Intelligence Report adds a `strategicDecision` section (today's allocation, deferred work, tradeoffs)
- Every Max recommendation includes `basedOnStrategicDecision` with allocation + tradeoffs
- "Why phone for two hours?" is answered by mission-objective contribution, not channel preference
- Concentrating on ABC versus spreading the day is an explicit comparison, not an implicit default

## Relationship to Prior ADRs

- **ADR-079** — Understanding before recommendation
- **ADR-082** — Business judgment through reusable heuristics
- **ADR-083** — Investigative strategy optimizes information gain
- **ADR-084** — Opportunity intelligence optimizes what matters
- **ADR-085** — Strategic decision optimizes allocation of finite effort

## Principle

Businesses do not grow by completing activities. They grow by allocating finite effort toward the highest-value opportunities.

Scout understands reality. Opportunity Intelligence ranks what matters. Max decides how the business spends today.
