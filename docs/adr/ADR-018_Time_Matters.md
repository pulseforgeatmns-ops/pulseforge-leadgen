# ADR-018 — Time Matters

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-031](../specs/SPEC-031_Business_Signals_Capability.md) |
| **Related** | [ADR-002](ADR-002_Explainable_AI.md), [ADR-005](ADR-005_LLM_Presentation_Engine.md), [ADR-017](ADR-017_Intelligence_Before_Execution.md), [SPEC-003](../specs/SPEC-003_Temporal_Intelligence_Memory.md), [SPEC-030](../specs/SPEC-030_Company_Intelligence_Capability.md) |

## Context

Company intelligence is incomplete if it only answers *who* a company is. Operators need *what is happening now* — hiring, expansion, renovations, leadership changes — because timing changes how you sell.

Static firmographics treat every prospect equally across time. A company that opened a second office last week and one that has been stable for three years should not rank or brief the same way when the evidence differs.

[SPEC-030](../specs/SPEC-030_Company_Intelligence_Capability.md) already includes a Business Signals category, but without a temporal contract those fields become another static list. [SPEC-003](../specs/SPEC-003_Temporal_Intelligence_Memory.md) taught Max to remember transitions; Business Signals must apply the same discipline to outreach timing: recent verified observations increase relevance; expired ones gradually lose influence.

Fabricating “urgency” without evidence violates [ADR-017](ADR-017_Intelligence_Before_Execution.md) and [ADR-005](ADR-005_LLM_Presentation_Engine.md). Time-aware intelligence must still be evidence-backed and explainable ([ADR-002](ADR-002_Explainable_AI.md)).

## Decision

1. **Business intelligence is not static.** Pulseforge evaluates companies based on their current operating context rather than treating all prospects equally forever.
2. **Recent, verified signals increase relevance.** Ranking, Opportunity Briefs, Campaign messaging, and Proposal narratives prefer Active signals with high confidence and recent `observedAt`.
3. **Expired signals gradually lose influence.** Every signal has a lifecycle (Detected → Verified → Active → Decays → Archived) and an influence weight that decays over time; Archived signals do not affect priority.
4. **Signals are observations, not conclusions.** They answer “why contact now?” with evidenced events — they do not assert purchase intent.
5. **One Active set for all consumers.** Ranking, Briefs, Campaign Builder, Proposal Generator, Knowledge, and the operator UI consume the same Active (non-expired) signal set — no parallel invent path.
6. **No fabricated timing.** Missing evidence means no signal and no artificial urgency.

## Consequences

### Positive

- Recommendations stay aligned with what is happening in the market today
- Opportunity Briefs become immediately useful (“recently expanded… hiring admin staff”) instead of generic fit copy
- Ranking Buying Signals / timing factors become honest and explainable
- Campaign and Proposal messaging can key off real signal types without inventing hooks
- Aligns Company Intelligence with Temporal Memory’s “change over time” posture

### Negative / tradeoffs

- Thin markets will show fewer Active signals — correct, not a product failure
- TTL defaults will need calibration (SPEC-021 later); wrong TTLs either over-weight stale news or under-weight sticky operational facts
- Collectors and verification add build cost beyond static firmographics

### Follow-ups

- [ ] Implement [SPEC-031](../specs/SPEC-031_Business_Signals_Capability.md) (collect → verify → lifecycle → consumers)
- [ ] Wire Active signals into SPEC-030 packages and SPEC-026 Ranking / Brief paths
- [ ] Document Campaign Builder + Proposal Generator signal contracts; land adapters
- [ ] Operator Active Business Signals UI on prospect / Company Intelligence surfaces
- [ ] Calibrate TTLs and confidence from outcomes (SPEC-021) after live signal volume exists
