# ADR-084 — Businesses Grow by Pursuing Opportunities

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-164](../specs/SPEC-164_Opportunity_Intelligence_Engine.md) |

## Context

Scout's intelligence stack (SPEC-157–163) produces rich market and business understanding. Operators need executive judgment — which opportunities deserve finite effort now, and why — not ranked lead lists.

## Decision

Introduce **Opportunity Intelligence** between Business Judgment and operator recommendations.

- Scout answers: **What is true?**
- Opportunity Intelligence answers: **What matters most?**
- Max translates opportunities into **business decisions** with explicit reasoning

Opportunities are evaluated on independent dimensions (business value, timing, strategic fit, reachability, probability, learning value). Rankings use multidimensional reasoning — never a numeric lead score alone.

## Consequences

- Mission Intelligence Report replaces "Qualified Prospects" presentation with **Top Opportunities**
- Every Max recommendation includes `opportunityReasoning`
- Comparative operator questions ("Why ABC before XYZ?") have first-class explainability
- Opportunity timeline tracks evolution: Monitor → Developing → Immediate → Active → Won/Lost

## Relationship to Prior ADRs

- **ADR-079** — Understanding before recommendation
- **ADR-082** — Business judgment through reusable heuristics
- **ADR-083** — Investigative strategy optimizes information gain
- **ADR-084** — Opportunity intelligence optimizes business outcomes

## Principle

Businesses do not grow by completing activities. They grow by allocating finite effort toward the highest-value opportunities.

Scout's responsibility is to understand reality. Max's responsibility is to determine where the business should act next.
