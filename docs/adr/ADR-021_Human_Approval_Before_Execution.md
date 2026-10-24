# ADR-021 — Human Approval Before Execution

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-034](../specs/SPEC-034_Campaign_Review_Workspace.md) |
| **Supersedes** | — |
| **Related** | [ADR-003](ADR-003_Human_Approval.md), [ADR-016](ADR-016_Execution_Does_Not_Decide.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [ADR-019](ADR-019_Missions_Are_Conversations.md), [SPEC-029](../specs/SPEC-029_Execution_Engine.md), [SPEC-033](../specs/SPEC-033_Mail_Package_Generator.md) |

## Context

[ADR-003](ADR-003_Human_Approval.md) requires explicit human approval for customer-visible actions. [ADR-016](ADR-016_Execution_Does_Not_Decide.md) forbids the Execution Engine from inventing strategy. Upstream capabilities now generate rich artifacts — campaigns, mail packages, proposals — but generation alone must never authorize outreach.

Without a hard gate between **artifact generation** and **execution**, operators can confuse “mail packages exist” with “campaign is approved to print / send,” and Execution may consume a draft or superseded revision.

## Decision

1. **Every outbound campaign requires explicit operator approval** via the Campaign Review Workspace ([SPEC-034](../specs/SPEC-034_Campaign_Review_Workspace.md)).
2. **Generation produces artifacts.** Campaign Builder, Mail Package Generator, and related capabilities create reviewable outputs only.
3. **Review validates artifacts.** Validation failures block prospect and campaign approval (fail closed).
4. **Approval authorizes execution.** Only an approved campaign revision may transition to Ready to Print / executable state.
5. **Execution may only consume the latest approved campaign revision.** Draft, rejected, skipped, or superseded revisions are not executable inputs ([ADR-016](ADR-016_Execution_Does_Not_Decide.md)).

## Consequences

### Positive

- Clear separation: generate → review → approve → execute
- Aligns brand safety (ADR-003) with execution boundaries (ADR-016)
- Single checkpoint for Direct Mail and future multi-channel campaigns
- Audit trail via revision history and Mission Decisions

### Negative / tradeoffs

- Extra operator step before print / send
- Incomplete mail packages or low-confidence personalization block Ready to Print until fixed
- UI must make approval state unmistakable

### Follow-ups

- [x] File [SPEC-034](../specs/SPEC-034_Campaign_Review_Workspace.md) thin slice
- [ ] Command Deck Campaign Review UI
- [x] Direct Mail Execution consumes latest approved revision ([SPEC-035](../specs/SPEC-035_Direct_Mail_Execution.md) / [ADR-022](ADR-022_Execution_Consumes_Approved_Artifacts.md))
- [ ] Wire multi-channel Execution Engine ([SPEC-029](../specs/SPEC-029_Execution_Engine.md)) to latest approved revision only
- [ ] Mission Memory ([SPEC-032](../specs/SPEC-032_Mission_Memory.md)) pin of decisions / revisions
- Update CURRENT_STATE when Campaign Review ships
