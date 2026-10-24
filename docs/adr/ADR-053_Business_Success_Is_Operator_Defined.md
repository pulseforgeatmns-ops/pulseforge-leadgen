# ADR-053 — Business Success Is Operator-Defined

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-19 |
| **Spec** | [SPEC-116](../specs/SPEC-116_Operator_Scorecard_Intelligence.md) |
| **Related** | [ADR-003](ADR-003_Human_Approval.md), [ADR-007](ADR-007_Operator_Intelligence.md), [ADR-008](ADR-008_Outcome_Intelligence.md), [ADR-021](ADR-021_Human_Approval_Before_Execution.md), [ADR-047](ADR-047_Intelligence_Before_Evidence.md) |

## Context

Traditional CRMs ask operators which metrics they want to track, then store and report those numbers. That puts metric design on the operator.

PulseForge already understands the business through Client Intelligence, the Business Blueprint, the published Acquisition Intelligence Model, Outcome Intelligence, and operator objectives. Max can reason from that understanding and recommend the metrics most likely to predict success.

Two businesses with identical revenue may need completely different scorecards depending on business model, growth stage, acquisition strategy, operational constraints, and stated objectives. PulseForge should not treat a Max draft as the definition of success, and it should not auto-adopt recommendations.

## Decision

**Business success is not determined by PulseForge. Business success is defined collaboratively.**

1. **Max recommends.** Max reasons from the Business Blueprint, Acquisition Intelligence, Outcome Intelligence, business stage, revenue model, and operator objectives before proposing metrics. Every recommendation includes why it matters, which business outcome it supports, and why Max believes it belongs on the scorecard.
2. **The operator decides.** Recommendations are never automatically adopted. Only the operator may accept, modify, remove, reorder, or add metrics.
3. **The approved scorecard is authoritative.** Once approved, the Operator Scorecard is the canonical definition of success for that tenant until the operator chooses to revise it.
4. **Drafts are not runtime.** A draft scorecard is never used for Daily Briefings, Executive Business Briefs as approved truth, Outcome Intelligence measurement, Scout prioritization, or campaign evaluation.
5. **Feedback becomes learning.** Scorecard modifications, including optional removal reasons, become Operator Intelligence and adjust future recommendations. They do not rewrite history or mutate prior reasoning.
6. **Evolution is advisory.** Max may periodically recommend scorecard updates as the business matures. Nothing changes automatically.

## Consequences

### Positive

- Operators keep authority over what success means
- Max's recommendations stay explainable and grounded in business understanding
- Runtime systems share one tenant-scoped definition of success
- Executive Business Briefs can distinguish Max recommendations from operator-approved metrics

### Negative / tradeoffs

- A tenant without an approved scorecard has no operational definition of success until the operator reviews Max's draft
- Operator-stated interview metrics are inputs to reasoning, not an approved scorecard
- v1 evolution is deterministic stage-shift detection, not a full longitudinal model

### Follow-ups

- [x] SPEC-116 Operator Scorecard Intelligence (v1 thin slice)
- [ ] Wire Scout prioritization and campaign evaluation to the approved scorecard in a later slice
- [ ] LLM-polished recommendation copy that still consumes only deterministic reasoning
