# ADR-037 — Reason About Businesses, Not Companies

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-053](../specs/SPEC-053_Business_Intelligence_Engine.md) |
| **Supersedes** | — (narrows descriptive posture of [ADR-017](ADR-017_Intelligence_Before_Execution.md) Company Intelligence packages) |
| **Related** | [ADR-017](ADR-017_Intelligence_Before_Execution.md), [ADR-032](ADR-032_Strategy_Before_Language.md), [ADR-002](ADR-002_Explainable_AI.md), [ADR-005](ADR-005_LLM_Presentation_Engine.md), [ADR-031](ADR-031_Review_Must_Be_Evidence_First.md) |

## Context

Company Intelligence answered *what exists?* — industry, headcount, contacts, website. Sales Intelligence ([ADR-032](ADR-032_Strategy_Before_Language.md)) needs answers to *why does this business behave this way?* Descriptive directories do not explain revenue mechanics, operational constraints, or buying psychology. Operators reviewed language before business reasoning was explicit.

## Decision

1. **Pulseforge shall model the operation of a business rather than summarize observable facts about a company.**
2. Facts are evidence. **Business Intelligence is the interpretation of that evidence.**
3. **Sales Intelligence derives strategy from Business Intelligence.**
4. Language generation derives communication from Sales Intelligence.
5. The structured artifact is `BusinessIntelligenceProfile` (`business_intelligence_profile`).
6. When required reasoning questions cannot be answered confidently, **uncertainty must be explicit rather than guessed.**
7. Review Workspace presents Business Intelligence before Sales Intelligence, Messaging Strategy, and Mail Package.
8. Implementing contract: [SPEC-053 Business Intelligence Engine](../specs/SPEC-053_Business_Intelligence_Engine.md).

## Rationale

Sales decisions are based on business dynamics, not directories. Facts become understanding. Understanding becomes strategy. Strategy becomes messaging.

## Consequences

### Positive

- Operators inspect business reasoning before sell strategy and language
- Sales Intelligence consumes analytical inputs (constraints, KPIs, buying triggers) instead of thin firmographics alone
- Confidence and uncertainty are first-class on the profile

### Negative / tradeoffs

- Adds a pipeline stage between Ranking and Sales Intelligence
- Thin Level-1 facts (no website research / knowledge graph) yield lower confidence and explicit uncertainty lists
- Enrichment stub remains for contact backfill; it is no longer the operator-facing “intelligence” artifact

### Follow-ups

- [x] SPEC-053 v1 capability + Sales Intelligence consumer + Review Workspace surface
- [ ] Deeper Knowledge Graph / research providers for Level-1 facts
- [ ] Full SPEC-030 Company Enrichment as richer evidence input (contacts without fabrication)
- [ ] Multi-channel generators cite BI fields in provenance
