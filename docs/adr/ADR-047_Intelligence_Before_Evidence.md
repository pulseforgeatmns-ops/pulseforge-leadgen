# ADR-047 — Intelligence Before Evidence

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-18 |
| **Spec** | [SPEC-110](../specs/SPEC-110_Business_Intelligence_Synthesis.md) |
| **Related** | [ADR-045](ADR-045_Evidence_Before_Reasoning.md), [ADR-046](ADR-046_Intent_Determines_Response_Structure.md), [ADR-032](ADR-032_Strategy_Before_Language.md), [ADR-017](ADR-017_Intelligence_Before_Execution.md), [ADR-005](ADR-005_LLM_Presentation_Engine.md) |

## Context

Max can retrieve and ground operating evidence, then still present that evidence as an inventory. Operators receive true counts (prospects, Scout companies, AO leads, touchpoints) and must perform the reasoning themselves.

ADR-045 already separates evidence collection from reasoning. ADR-046 already binds response *structure* to intent. This decision separates **intelligence from evidence presentation**: Max may retrieve and ground internally, but the operator-facing lead is a synthesized conclusion, not a database recital.

A paragraph of prose is not enough. Rex, dashboards, daily briefings, and Cal otherwise invent their own summaries. Intelligence must be a reusable object.

## Decision

1. **Business Intelligence is a first-class object.** Fields: `finding`, `category` (`bottleneck` \| `momentum` \| `risk` \| `readiness` \| `unknown`), `confidence`, `supporting_claims`, `unknowns`, `operator_impact`.
2. **Intelligence precedes evidence** on Retrieval, Summary, and Recommendation contracts. Evidence remains fully available and attributable after the intelligence section.
3. **Synthesis is a bounded transformation of grounded claims.** It may identify bottlenecks, progress, missing evidence, operating state, goal gaps, and uncertainty. It may not speculate, invent causes, or forecast. Insufficient evidence yields `unknown`, never inference.
4. **No finding without supporting claims.** Downstream consumers (Max, Rex, dashboards, briefings, Cal) read the same objects instead of re-summarizing inventories.
5. **Conclusions remain ephemeral.** Intelligence objects are composed at response time. They are not persisted as operating fact (ADR-045).

## Consequences

### Positive

- Operators receive "here's what matters" before the inventory that supports it
- Traceability is preserved: every finding cites grounded claims
- One synthesis layer can be reused across Max, Rex, briefings, and coaching
- Channel-effectiveness questions fail closed instead of guessing

### Negative / tradeoffs

- Synthesis must stay conservative; "pipeline is declining" is not justified without a prior baseline
- Contract headings grow by one required section; SPEC-109 inventory phrasing (`What I can verify`) is preserved after intelligence
- SPEC-053 prospect `BusinessIntelligenceProfile` remains a different object for ranked prospects

### Follow-ups

- [x] SPEC-110 implementation
- [ ] Workspace UI rendering of intelligence objects
- [ ] Rex / briefing / Cal consumers of the same object
