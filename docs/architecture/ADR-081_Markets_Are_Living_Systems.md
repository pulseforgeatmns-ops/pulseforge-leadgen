# ADR-081 — Markets Are Living Systems

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-161](../specs/SPEC-161_Market_Memory.md) |

## Context

Markets are not static datasets. Businesses change, terminology drifts, operators enter and exit, and buying signals evolve. Scout previously treated each mission as an isolated investigation, rediscovering what it already knew.

## Decision

Markets are **living systems** that evolve over time. Scout maintains **Market Memory** — durable, synthesized understanding that compounds across missions.

Scout's responsibility is not simply to rediscover businesses, but to maintain an increasingly accurate understanding of evolving markets.

## Consequences

- Every discovery pipeline run recalls Market Memory before investigation (`DiscoveryPipeline.js`).
- Business understanding is archived on revision, never silently overwritten.
- Mission Intelligence Reports include **Market Changes Since Last Investigation**.
- Confidence evolves longitudinally; stable markets reinforce prior understanding instead of resetting.
- Duplicate investigation work is avoided when no meaningful changes are detected.

## Relationship to Prior ADRs

- **ADR-079** — Understanding before recommendation (investigation output is understanding)
- **ADR-080** — Understanding emerges from evidence (synthesis feeds Market Memory)
- **ADR-081** — Markets evolve; Scout remembers and measures change
