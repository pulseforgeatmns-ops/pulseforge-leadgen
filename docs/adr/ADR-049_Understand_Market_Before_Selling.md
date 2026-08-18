# ADR-049 — Understand the Market Before Selling Into It

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-18 |
| **Spec** | [SPEC-112](../specs/SPEC-112_Acquisition_Intelligence_Model.md) |
| **Related** | [ADR-017](ADR-017_Intelligence_Before_Execution.md), [ADR-032](ADR-032_Strategy_Before_Language.md), [ADR-037](ADR-037_Reason_About_Businesses_Not_Companies.md), [ADR-039](ADR-039_Separate_Understanding_from_Execution.md), [ADR-045](ADR-045_Evidence_Before_Reasoning.md) |

## Context

PulseForge already has Client Intelligence (who the client is), Business Intelligence (how a prospect operates), Sales Intelligence (how to sell), and Scout acquisition loops (what to investigate). None of those artifacts is a **market-understanding model** that Scout can reason over *before* searching.

Without that model, Scout searches against demographics and vertical labels. Paige starts copy from a blank prompt. Qualification collapses to a single ICP score. Client expertise — mission, transformation, pain ontology, language — never becomes structured intelligence.

The product brief called this SPEC-110. Repository SPEC-110 is Business Intelligence Synthesis (operator-facing Max conclusions). This decision is numbered **SPEC-112**.

## Decision

1. **PulseForge shall understand a market before it attempts to sell into it.**
2. **The Acquisition Intelligence Model (AIM) is the first-class artifact** that holds a client's mission, reasoning-based ICP, desired transformation, pain ontology, qualification dimensions, captured knowledge, and messaging intelligence.
3. **Scout consumes AIM to reason, not merely to search.** Observable signals are matched to pain categories. Fit is not a directory lookup.
4. **Every prospect receives a six-dimension qualification:** ICP Fit, Pain Match, Evidence Quality, Buying Readiness, Confidence, Overall Recommendation. Each score is explainable.
5. **Knowledge captured from the market is stored** (definition, evidence, objections, language, messaging, discovery questions, case studies). Empty case studies stay empty. Do not invent proof.
6. **Paige never starts from zero when AIM qualification exists.** A Founder Dependency score of 92% already implies likely pain, language, proof, and CTA.
7. **AIM is not operating fact.** It is client-expertise intelligence plus observed-signal matching. SPEC-108 claim grounding and SPEC-110 operator intelligence objects remain separate.

## Consequences

### Positive

- New clients (starting with Fedir) can teach PulseForge a market before Scout runs
- Scout and Paige share one pain ontology instead of inventing parallel ones
- Qualification is inspectable on six dimensions instead of a single ICP number
- Messaging is grounded in captured knowledge, not a blank generation step

### Negative / tradeoffs

- A client without an AIM still uses existing Scout fit (commercial-cleaning defaults). That is correct, not a regression.
- Geography, size, and case studies stay unknown until the client supplies them. Qualification must fail closed on invention.

### Follow-ups

- [x] SPEC-112 v1 engine + Fedir seed
- [x] Scout attaches AIM qualification when an AIM is present
- [x] Paige consumes AIM briefing when qualification is present
- [x] SPEC-113 compiler from market documents (ADR-050)
- [ ] Live Fedir client row + 50-prospect pilot (operational, not this ADR)
- [ ] Operator UI for editing AIM knowledge
