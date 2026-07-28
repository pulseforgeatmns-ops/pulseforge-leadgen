# ADR-024 — Human Work Is Coordinated Through the Operator Inbox

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-037](../specs/SPEC-037_Operator_Inbox.md) |
| **Supersedes** | — |
| **Related** | [ADR-003](ADR-003_Human_Approval.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [ADR-021](ADR-021_Human_Approval_Before_Execution.md), [ADR-023](ADR-023_Experience_Becomes_Intelligence.md), [SPEC-032](../specs/SPEC-032_Mission_Memory.md), [SPEC-034](../specs/SPEC-034_Campaign_Review_Workspace.md), [SPEC-035](../specs/SPEC-035_Direct_Mail_Execution.md), [SPEC-036](../specs/SPEC-036_Outcome_Intelligence.md) |

## Context

Pulseforge capabilities each produce human-required work: campaign approvals ([SPEC-034](../specs/SPEC-034_Campaign_Review_Workspace.md)), print/assemble/mail actions ([SPEC-035](../specs/SPEC-035_Direct_Mail_Execution.md)), recommendation decisions ([SPEC-036](../specs/SPEC-036_Outcome_Intelligence.md)), and validation fixes. Without a unified coordination layer:

1. Operators juggle multiple capability UIs with no authoritative outstanding list
2. Duplicate work items appear when several capabilities ask for the same action
3. Completing work may not update Mission Memory / audit consistently
4. Capabilities risk absorbing inbox concerns (sorting, snooze, assign) into workflow code

## Decision

1. **Human work is coordinated through a unified Operator Inbox.**
2. **Capabilities generate work items.** The Operator Inbox organizes those work items.
3. **Operators review, approve, and complete work** via inbox actions (open / review / approve / reject / complete / snooze / assign / archive).
4. **Completed work updates Mission Memory, Audit History, and may notify Outcome Intelligence** — coordination signals only.
5. **The Operator Inbox is a coordination layer for business workflows and does not perform workflow processing itself.** It never runs Campaign Review, Mail Package Generation, Direct Mail Execution, or Outcome Intelligence logic.

## Consequences

### Positive

- Single authoritative list of outstanding human work
- Deterministic priority + deduplication across capabilities
- Clear separation: generate work (capabilities) vs organize work (inbox) vs do work (operator in originating workspace)
- Auditable completion path into Mission Memory

### Negative / tradeoffs

- Operators must deep-link into originating workspaces to actually process workflows
- Inbox actions are coordination signals — workflow capabilities remain the processors
- Without a live event bus, ingest depends on capability outputs / explicit events

### Follow-ups

- [x] File [SPEC-037](../specs/SPEC-037_Operator_Inbox.md) thin slice
- [ ] Command Deck Operator Inbox UI
- [ ] Live capability → inbox event bus
- [ ] Persist inbox items + audit
- Update CURRENT_STATE when Operator Inbox ships
