# ADR-050 — Single Governed Reasoning Pipeline

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-18 |
| **Spec** | [SPEC-113](../specs/SPEC-113_Reasoning_Pipeline_Conformance.md) |
| **Related** | [ADR-048](ADR-048_Intent_Selects_Analysis_Mode.md), [ADR-046](ADR-046_Intent_Determines_Response_Structure.md), [ADR-047](ADR-047_Intelligence_Before_Evidence.md), [ADR-045](ADR-045_Evidence_Before_Reasoning.md) |

## Context

Pilot 0 shipped intent-bound contracts, claim grounding, and business intelligence. Operator-facing answers could still skip that stack. Client Intelligence Blueprint advisory continued to answer "what should we focus on first?" with a commercial acquisition essay whenever operating evidence was thin.

Two pipelines cannot be the product. Advice from a Blueprint is not the same competency as evidence-grounded analysis.

## Decision

1. **One entry.** `bindGovernedReasoning` classifies intent, selects analysis mode, and binds a response contract before retrieval.
2. **Unknown fails closed to Retrieval.** Unclassified and planning questions do not default to Recommendation or Blueprint Advisory.
3. **Blueprints are evidence.** Approved understanding may fill goals and desired-state facts. It may not compose operator recommendations.
4. **Specialists are providers.** Scout investigation intelligence is composed through the Investigation contract. CIE, Paige, and other specialists do not assemble final reasoning responses.
5. **One composer.** `ResponseContract` is the only operator-facing reasoning composer. Pipeline logs make routing bugs visible.

## Consequences

### Positive

- Operator experience is consistent across summary, diagnosis, unknown analysis, risk, retrieval, investigation, and recommendation
- Routing bugs show up in `pipelineLog` (intent, mode, contract, evidence, claims, composer)
- Thin operating evidence still uses the governed recommendation path instead of a Blueprint essay

### Negative / tradeoffs

- SPEC-103 conversational Blueprint essays are no longer operator-facing; follow-ups explain the prior governed recommendation
- CIE remains a workflow handler (execution clarify, plan preparation) and an evidence loader, not a responder

### Follow-ups

- [x] SPEC-113 implementation
- [ ] Surface pipeline log in workspace UI for operator debugging
