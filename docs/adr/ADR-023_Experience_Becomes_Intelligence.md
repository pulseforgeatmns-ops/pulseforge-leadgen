# ADR-023 — Experience Becomes Intelligence

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-036](../specs/SPEC-036_Outcome_Intelligence.md) |
| **Supersedes** | — |
| **Related** | [ADR-008](ADR-008_Outcome_Intelligence.md), [ADR-002](ADR-002_Explainable_AI.md), [ADR-003](ADR-003_Human_Approval.md), [ADR-015](ADR-015_Strategy_Lives_in_the_Playbook.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [SPEC-013](../specs/SPEC-013_Outcome_Intelligence.md), [SPEC-035](../specs/SPEC-035_Direct_Mail_Execution.md), [SPEC-028](../specs/SPEC-028_Client_Playbook_Capability.md), [SPEC-026](../specs/SPEC-026_Opportunity_Ranking_Capability.md) |

## Context

Pulseforge runs campaigns (Direct Mail Execution, future channels) and records operational results — responses, walkthroughs, wins, losses. Without a hard rule separating **observed outcomes** from **strategy mutation**, the system risks:

1. Promoting anecdotal patterns as playbook truth
2. Silently rewriting ranking weights or discovery strategy from thin samples
3. Conflating Max recommendation evaluation ([ADR-008](ADR-008_Outcome_Intelligence.md) / SPEC-013) with campaign experience learning

Operators need a clear path: capture → evidence-backed learning → pending recommendation → human approval → strategy update.

## Decision

1. **Pulseforge improves through observed outcomes, not assumptions.**
2. **Operational experience becomes structured intelligence only after evidence has been collected and operator approval has been granted.**
3. **Learning Engine may propose evidence-backed conclusions** (minimum sample size + measurable lift vs baseline). Assumptions and under-powered segments stay as candidates — never promoted.
4. **Recommendations remain pending** until an operator approves or rejects them. Approval is required before updating Client Playbook, Ranking Weights, Discovery Strategy, or Campaign Templates.
5. **SPEC-036 is distinct from SPEC-013 / ADR-008.** SPEC-013 evaluates whether Max recommendations were right (never changes reasoning). SPEC-036 converts campaign/mission results into reusable strategy *after* approval.

## Consequences

### Positive

- Clear separation: capture → learn → recommend → approve → apply
- Playbooks and ranking stay explainable and operator-owned
- Thin or anecdotal results cannot silently mutate strategy
- Aligns campaign learning with ADR-003 / ADR-015

### Negative / tradeoffs

- Learning loops are slower (human gate on every strategy mutation)
- Under-powered campaigns produce candidates, not promotions — operators may feel "nothing learned"
- Cost / ROI metrics may be incomplete until finance inputs exist

### Follow-ups

- [x] File [SPEC-036](../specs/SPEC-036_Outcome_Intelligence.md) thin slice
- [ ] Command Deck Outcome Intelligence UI
- [ ] Persist outcomes / learnings / recommendations
- [ ] Apply approved recommendations into live playbook versions
- Update CURRENT_STATE when Outcome Intelligence ships
