# ADR-022 — Execution Consumes Approved Artifacts

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-035](../specs/SPEC-035_Direct_Mail_Execution.md) |
| **Supersedes** | — |
| **Related** | [ADR-016](ADR-016_Execution_Does_Not_Decide.md), [ADR-021](ADR-021_Human_Approval_Before_Execution.md), [ADR-003](ADR-003_Human_Approval.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [SPEC-029](../specs/SPEC-029_Execution_Engine.md), [SPEC-033](../specs/SPEC-033_Mail_Package_Generator.md), [SPEC-034](../specs/SPEC-034_Campaign_Review_Workspace.md) |

## Context

[ADR-016](ADR-016_Execution_Does_Not_Decide.md) forbids the Execution Engine from inventing strategy. [ADR-021](ADR-021_Human_Approval_Before_Execution.md) requires an approved campaign revision before any outbound work. Direct Mail Execution ([SPEC-035](../specs/SPEC-035_Direct_Mail_Execution.md)) is the first channel that physically acts on those artifacts — print, assemble, mail, track responses.

Without a hard rule that **execution only consumes what was approved**, operators or future adapters may regenerate letters mid-print, swap packages after assembly starts, or silently diverge from the reviewed revision — breaking auditability and brand safety.

## Decision

1. **Execution never generates content.** Letters, envelopes, inserts, prospect lists, and strategy come from upstream capabilities only (Campaign Builder, Mail Package Generator, Campaign Review).
2. **Execution consumes previously approved artifacts exactly as reviewed** — latest approved campaign revision, pinned mail package batch, and execution package.
3. **Once execution begins (Printing), campaign artifacts are immutable.** Revision, mail batch, and execution package are locked for that run.
4. **Any modification requires a new campaign revision and a new approval cycle** ([SPEC-034](../specs/SPEC-034_Campaign_Review_Workspace.md) / [ADR-021](ADR-021_Human_Approval_Before_Execution.md)).

## Consequences

### Positive

- Clear separation: generate → review → approve → execute
- Print / assemble / mail runs are replayable against pinned artifacts
- Audit log can prove what was physically mailed matched what was approved
- Aligns Direct Mail channel with ADR-016 / ADR-021

### Negative / tradeoffs

- Mid-run letter fixes require aborting (or completing) the current run and starting a new approved revision
- Operators must treat Ready-to-Print as a hard freeze once Printing starts
- Stores must pin artifact snapshots, not live mutable references

### Follow-ups

- [x] File [SPEC-035](../specs/SPEC-035_Direct_Mail_Execution.md) thin slice
- [ ] Command Deck Direct Mail Execution UI
- [ ] Wire multi-channel Execution Engine ([SPEC-029](../specs/SPEC-029_Execution_Engine.md)) to the same consume-only rule
- [ ] Mission Memory ([SPEC-032](../specs/SPEC-032_Mission_Memory.md)) pin of locked artifacts
- Update CURRENT_STATE when Direct Mail Execution ships
