# SPEC-035 — Direct Mail Execution

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-029 (Execution Engine consumes this channel), SPEC-033 (Mail Package Batch), SPEC-034 (Approved Campaign Revision / Execution Package), SPEC-032 (Mission Memory — timeline shapes), ADR-003, ADR-010, ADR-011, ADR-016, ADR-021, ADR-022 |
| **Consumed by** | Mission Engine, Command Deck Operations, Max Workspace, Execution Engine (Direct Mail channel), Outcome Intelligence (SPEC-036) |

## Objective

Execute **approved** direct mail campaigns through a deterministic state machine with full audit history.

Execution never generates content ([ADR-022](../adr/ADR-022_Execution_Consumes_Approved_Artifacts.md)). It consumes the latest approved campaign revision, pinned mail package batch, and execution package exactly as reviewed.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-022](../adr/ADR-022_Execution_Consumes_Approved_Artifacts.md) — execution consumes approved artifacts
- [ADR-021](../adr/ADR-021_Human_Approval_Before_Execution.md) — approved revision required
- [ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md) — execution does not decide
- [ADR-003](../adr/ADR-003_Human_Approval.md) — human approval before outbound
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [SPEC-029](SPEC-029_Execution_Engine.md)
- [SPEC-033](SPEC-033_Mail_Package_Generator.md)
- [SPEC-034](SPEC-034_Campaign_Review_Workspace.md)
- [SPEC-032](SPEC-032_Mission_Memory.md)

## Problem

Campaign Review (SPEC-034) produces Ready-to-Print revisions and execution packages, and Mail Package Generator (SPEC-033) produces print-ready batches — but operators lack a durable system that:

1. Walks an approved campaign through print → assemble → mail → response
2. Locks campaign artifacts once Printing begins
3. Tracks per-prospect assembly and response outcomes
4. Records immutable audit transitions
5. Updates Mission timeline / campaign status in real time

Without Direct Mail Execution, approved packages stall after review, or operators invent ad-hoc tracking outside the Mission.

## Scope

- Direct Mail Execution capability (`direct_mail_execution`) as a first-class Mission capability
- Mission type `direct_mail_execution`
- Inputs: Approved Mission, Approved Campaign Revision, Execution Package, Mail Package Batch
- Deterministic execution state machine (campaign-level)
- Campaign lock after Printing begins (revision + mail batch + execution package pinned)
- Print Session records
- Per-prospect assembly tracking (letter / envelope / inserts / sealed / postage)
- Mailing actions (selected / all) with date, operator, optional USPS batch ID, notes
- Per-prospect response tracking
- Campaign metrics summary
- Immutable audit log of every state transition
- Mission Memory shapes: execution events, timeline entries, campaign status

## Out of Scope

- Generating letters, envelopes, or strategy (upstream only — ADR-022)
- Autonomous postage purchase / carrier API beyond optional USPS Batch ID capture
- Full Command Deck HTML UI chrome (v1 returns execution view model; UI binds later)
- Full Mission Memory Postgres tables (SPEC-032) — local event shapes mirror the contract
- Multi-channel Execution Engine orchestration beyond Direct Mail ([SPEC-029](SPEC-029_Execution_Engine.md))
- Live Outcome Intelligence publish beyond stub shapes (SPEC-036 consumes execution outputs)

## Dependencies

| Dependency | Role |
|---|---|
| SPEC-023 Capability Framework | Capability contract + registry |
| SPEC-034 Campaign Review | Latest **approved** campaign revision + execution package |
| SPEC-033 Mail Package Generator | Mail Package Batch (Ready-to-Print packages) |
| SPEC-029 Execution Engine | Direct Mail channel adapter consumes this capability |
| SPEC-032 Mission Memory | Append execution events / timeline |
| ADR-021 / ADR-022 | Approved revision required; artifacts immutable once printing |

## Architecture

```text
Approved Campaign Revision (SPEC-034)
Execution Package · Mail Package Batch (SPEC-033)
      ↓
Direct Mail Execution
      ↓
Validate approved revision present
      ↓
State machine: Draft → … → Completed
      ↓
Print Session · Assembly · Mailing · Responses
      ↓
Immutable audit log + metrics
      ↓
Mission Memory events / campaign status (SPEC-032 shape)
```

### Design rules

1. **Approved revision required** — no execution without a Ready-to-Print / approved campaign revision ([ADR-021](../adr/ADR-021_Human_Approval_Before_Execution.md)).
2. **Consume, don’t create** — never invent letter content, recipients, or strategy ([ADR-022](../adr/ADR-022_Execution_Consumes_Approved_Artifacts.md)).
3. **Deterministic transitions** — only allowed edges; illegal transitions fail closed.
4. **Lock on Printing** — once Printing begins, campaign revision, mail package batch, and execution package are pinned; changes require a new campaign revision + approval cycle.
5. **Immutable audit** — every transition appends; never mutate prior audit rows.
6. **Mission-first** — execution events append to Mission Memory shapes; campaign status updates with each stage.

### Execution states

```text
Draft
  ↓
Ready to Print
  ↓
Printing
  ↓
Printed
  ↓
Assembling
  ↓
Ready to Mail
  ↓
Mailed
  ↓
Delivered (optional)
  ↓
Responded
  ↓
Completed
```

### Campaign lock

Once **Printing** begins:

- Campaign Revision locked
- Mail Package Batch pinned
- Execution Package pinned

Any content or prospect-list change requires a **new campaign revision** and a new approval cycle (SPEC-034 / ADR-021 / ADR-022).

### Print Session

| Field | Description |
|---|---|
| Campaign | Campaign id / name |
| Revision | Locked approved revision number |
| Operator | Who started the session |
| Timestamp | Session start |
| Prospect Count | Packages in session |
| Print Status | Session status |

### Assembly tracking (per prospect)

| Checklist | |
|---|---|
| Letter inserted | |
| Envelope addressed | |
| Inserts added | |
| Sealed | |
| Postage applied | |

Actions: **Complete** · **Skip** · **Reopen**

### Mailing

- Mark Selected Mailed
- Mark All Mailed

Capture: Date · Operator · Optional USPS Batch ID · Notes

### Response tracking (per prospect)

| Status |
|---|
| No Response |
| Returned Mail |
| Called |
| Emailed |
| Walkthrough Scheduled |
| Proposal Sent |
| Closed Won |
| Closed Lost |

### Metrics (campaign summary)

Printed · Assembled · Mailed · Responses · Meetings · Proposals · Wins · Response Rate

### Audit log

Every transition records: Previous State · New State · Timestamp · Operator · Notes — **immutable**.

## Data Model

```text
packages/capabilities/directMailExecution/
  types.js
  transitions.js
  validate.js
  assemble.js
  actions.js
  DirectMailExecutionStore.js
  DirectMailExecution.js
  index.js
```

### Execution status

```ts
type ExecutionStatus =
  | 'draft'
  | 'ready_to_print'
  | 'printing'
  | 'printed'
  | 'assembling'
  | 'ready_to_mail'
  | 'mailed'
  | 'delivered'
  | 'responded'
  | 'completed'
```

### Response status

```ts
type ResponseStatus =
  | 'no_response'
  | 'returned_mail'
  | 'called'
  | 'emailed'
  | 'walkthrough_scheduled'
  | 'proposal_sent'
  | 'closed_won'
  | 'closed_lost'
```

## Implementation Plan

1. File SPEC-035 + ADR-022 + types / transitions / validate / assemble / actions / store
2. Direct Mail Execution capability + register builtin
3. Mission type `direct_mail_execution`; IntentRouter patterns; playbook pin optional
4. Tests: approved-revision gate, lock on printing, assembly, mailing, responses, audit immutability, metrics
5. Later: Command Deck UI, Mission Memory live attach
6. Outcome Intelligence (SPEC-036) consumes execution responses / metrics

## Migration Strategy

- No required migration in v1 (in-memory store, like CampaignReviewStore)
- Forward: `direct_mail_executions` + `direct_mail_audit_log` tables when durability is required
- SPEC-032: map execution events → Mission Memory timeline; pin locked artifacts on `activeArtifacts`

## Testing

- Unit: transition matrix; lock enforcement; metrics calculation
- Capability: start from approved revision; print session; assembly complete/skip/reopen; mark mailed; response tracking; audit append-only
- Mission: IntentRouter + planner chain for `direct_mail_execution`
- Manual: inspect execution view model JSON + metrics summary

## Acceptance Criteria

- [x] Approved revision required
- [x] Deterministic state transitions
- [x] Campaign revision locked after printing
- [x] Per-prospect execution tracking
- [x] Immutable audit history
- [x] Response tracking integrated
- [x] Mission timeline updated automatically (event shapes)

## Future Work

- Command Deck Direct Mail Execution UI
- Mission Memory live attach (SPEC-032)
- Auto-ingest response events into Outcome Intelligence ([SPEC-036](SPEC-036_Outcome_Intelligence.md))
- Publish execution events to Business Signals / Client Analytics
- Carrier / postage API integration
- Multi-channel Execution Engine orchestration (SPEC-029)
