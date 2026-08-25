# ADR-082 — Business Judgment Through Reusable Heuristics

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-162](../specs/SPEC-162_Business_Heuristics_Engine.md) |

## Context

Scout synthesizes evidence into business understanding (SPEC-160). Operators need judgment — reusable patterns that explain business implications and buying likelihood — not just facts and assertions.

## Decision

Recommendations originate from **activated business heuristics**, not directly from evidence or synthesized understanding.

Heuristics are:
- Reusable across investigations
- Scored and explainable
- Calibrated by outcomes (`learnFromOutcome`)
- Allowed to contradict each other with reduced overall confidence

## Consequences

- Mission Intelligence Report adds a `businessJudgment` section distinct from `businessUnderstanding`
- `explainJudgment()` provides operator traceability
- SPEC-163 will consume activated heuristics for investigative prioritization

## Relationship to Prior ADRs

- **ADR-079** — Understanding before recommendation
- **ADR-080** — Understanding emerges from evidence
- **ADR-081** — Markets are living systems (Market Memory)
- **ADR-082** — Business judgment through reusable heuristics
