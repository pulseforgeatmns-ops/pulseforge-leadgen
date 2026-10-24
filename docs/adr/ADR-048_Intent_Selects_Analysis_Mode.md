# ADR-048 — Intent Selects Analysis Mode

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-18 |
| **Spec** | [SPEC-111](../specs/SPEC-111_Operator_Intent_Taxonomy.md) |
| **Related** | [ADR-046](ADR-046_Intent_Determines_Response_Structure.md), [ADR-047](ADR-047_Intelligence_Before_Evidence.md), [ADR-045](ADR-045_Evidence_Before_Reasoning.md), [ADR-039](ADR-039_Separate_Understanding_from_Execution.md) |

## Context

ADR-046 binds response *structure* to intent. ADR-047 synthesizes business intelligence before evidence. Classification still collapsed several operator questions into Recommendation or a Scout acquisition shortcut.

"What's preventing us from growing faster?" is not a request for the next action. "What don't we know yet that matters?" is not a request to find more prospects. Those are different forms of thinking: diagnosis and unknown analysis.

If Max reasons before recognizing the analytical mode, the response contract cannot save the answer. Advice, retrieval, and investigation will leak into questions that asked for a constraint, a gap, a risk, or a progress measure.

## Decision

1. **Operator intent is an explicit registry.** Retrieval, Summary, Recommendation, Diagnosis, Unknown Analysis, Risk, Progress, Challenge, Investigation.
2. **Intent selects analysis mode before reasoning.** Analysis mode selects the response contract. Retrieval, grounding, and business intelligence fill that contract.
3. **Diagnosis explains why, not what to do.** Optional recommendations may follow. Generic Blueprint strategy is forbidden.
4. **Unknown analysis does not speculate.** It reports evidence gaps and why they matter.
5. **Business Intelligence objects are reused.** Diagnosis consumes bottleneck / readiness / momentum. Unknown analysis consumes unknown findings. Risk consumes risk findings. Reasoning is not duplicated per mode.

## Consequences

### Positive

- Diagnostic, uncertainty, risk, and progress questions keep their own form of intelligence
- Scout and CIE cannot swallow unknown-analysis or diagnosis as acquisition or Blueprint advice
- Recommendation remains recommendation-first when the operator asks what to do next

### Negative / tradeoffs

- Classification must distinguish "where should we focus" (recommendation) from "what's the bottleneck" (diagnosis)
- "What have we completed recently?" remains retrieval; progress measures movement against goals

### Follow-ups

- [x] SPEC-111 implementation
- [ ] Workspace UI section rendering for diagnosis / unknown / risk / progress headings
