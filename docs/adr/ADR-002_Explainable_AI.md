# ADR-002 — Explainable AI

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-26 |
| **Spec** | SPEC-000 (policy); SPEC-002 (implementation) |
| **Supersedes** | — |

## Context

Operators will not trust black-box scores for revenue-critical follow-up. Max orchestration already records explanation components for warmth scoring in shadow mode—this must become a product-wide rule, not a one-off.

## Decision

Any **material** automated score, recommendation, or state transition that influences operator action **must** persist an auditable explanation: ordered components, evidence references, and stable decision identity where applicable.

“Material” includes: ICP-influencing recalculations exposed to operators, warmth/orchestration scores, Max recommendations, and auto-applied workflow transitions.

## Consequences

### Positive

- Debuggability, training, and compliance-friendly audits
- Enables conversation answers of the form “because these events…”

### Negative / tradeoffs

- More storage and schema care
- Slightly slower iteration on throwaway experiments (use shadow + explicit experimental flags)

### Follow-ups

- SPEC-002 recommendation schema
- Keep Max shadow explanations as the reference pattern
