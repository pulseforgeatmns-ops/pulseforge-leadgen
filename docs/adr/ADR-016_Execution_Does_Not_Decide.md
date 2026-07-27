# ADR-016 — Execution Does Not Decide

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-029](../specs/SPEC-029_Execution_Engine.md) |
| **Supersedes** | — |
| **Related** | [ADR-003](ADR-003_Human_Approval.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [ADR-015](ADR-015_Strategy_Lives_in_the_Playbook.md), [ADR-002](ADR-002_Explainable_AI.md), [ADR-008](ADR-008_Outcome_Intelligence.md) |

## Context

Upstream capabilities now produce explainable strategy:

- Discovery Profiles decide *who* ([SPEC-024](../specs/SPEC-024_Prospect_Discovery_Capability.md))
- Client Playbooks decide *how* ([SPEC-028](../specs/SPEC-028_Client_Playbook_Capability.md) / [ADR-015](ADR-015_Strategy_Lives_in_the_Playbook.md))
- Opportunity Ranking decides *priority* ([SPEC-026](../specs/SPEC-026_Opportunity_Ranking_Capability.md))
- Campaign Builder / Proposal Generator assemble reviewable artifacts

If the system that *sends* outreach also invents timing, channel swaps, retry counts, or contact data, strategy fragments again — this time inside send adapters. That makes execution non-deterministic, hard to audit, and unsafe relative to [ADR-003](ADR-003_Human_Approval.md).

Mission Engine already separates planning from capability invocation ([ADR-010](ADR-010_Mission_Engine.md)). Execution needs the same hard boundary: carry out approved strategy; never create it.

## Decision

1. **The Execution Engine never creates strategy.** It only executes approved strategy.
2. Strategy inputs are **pinned** at launch: approved campaign artifact + Client Playbook version (+ discovery/ranking provenance already on the campaign).
3. Timing, retries, channel order, constraints, and offers are **read from the Playbook / approved plan** — never hardcoded in the engine or channel adapters as product strategy.
4. Execution is **fail-closed**: no unapproved outreach, no skipped required reviews, no execution outside client constraints, no invented contact data.
5. When human action is required, execution **pauses** (`Waiting`) and resumes only after explicit completion — it does not invent a workaround.
6. Outcomes are **recorded and may trigger Missions**; they do not authorize the engine to rewrite targeting, ranking, or playbook strategy.
7. Reasoning stays centralized in Mission / Ranking / Playbook / Policy layers; execution stays deterministic, auditable, and safe.

## Consequences

### Positive

- Clear separation: decide upstream, do downstream
- Deterministic, replayable touch histories
- Playbook changes do not require engine code changes
- Aligns with human approval (ADR-003) and explainability (ADR-002)
- Channel adapters stay swappable behind the Capability Framework (ADR-011)

### Negative / tradeoffs

- Incomplete Playbooks block or thin-out execution until operators fill schedule/retry fields
- Manual channels (mail, phone) require operator UX for Waiting / resume
- Legacy agents that embed their own sequencing must be demoted to adapters or gated off the mission path

### Follow-ups

- [ ] Implement [SPEC-029](../specs/SPEC-029_Execution_Engine.md) (plan builder → runner → evidence → outcomes)
- [ ] Additive Playbook schedule/retry fields as needed (still Playbook-owned)
- [ ] Wire post-Approve Mission hook to `execution_engine` capability
- [ ] Operator Dashboard live touch status on Command Deck Operations
- [ ] Feature flag `EXECUTION_ENGINE` until thin slice proven
