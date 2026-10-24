# SPEC-037 — Operator Inbox

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-032, SPEC-034, SPEC-035, SPEC-036, ADR-003, ADR-010, ADR-011, ADR-021, ADR-023, ADR-024 |
| **Consumed by** | Command Deck Operations, Max Workspace, Mission Engine |

## Objective

Provide a single operational workspace where all business workflow items requiring human attention are surfaced, prioritized, and completed.

The Operator Inbox **coordinates** work across the platform. It does **not** perform business workflows itself ([ADR-024](../adr/ADR-024_Human_Work_Is_Coordinated_Through_the_Operator_Inbox.md)).

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-024](../adr/ADR-024_Human_Work_Is_Coordinated_Through_the_Operator_Inbox.md) — human work coordinated through Operator Inbox
- [ADR-003](../adr/ADR-003_Human_Approval.md) — human approval
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [ADR-021](../adr/ADR-021_Human_Approval_Before_Execution.md) — approval before execution
- [ADR-023](../adr/ADR-023_Experience_Becomes_Intelligence.md) — experience becomes intelligence
- [SPEC-032](SPEC-032_Mission_Memory.md) — Mission Memory
- [SPEC-034](SPEC-034_Campaign_Review_Workspace.md) — Campaign Review
- [SPEC-035](SPEC-035_Direct_Mail_Execution.md) — Direct Mail Execution
- [SPEC-036](SPEC-036_Outcome_Intelligence.md) — Outcome Intelligence

## Problem

Campaign Review, Direct Mail Execution, Outcome Intelligence, and validation gates each surface human-required work independently. Without a unified inbox:

1. Operators miss Critical approvals buried in capability-specific UIs
2. Duplicate work items appear when multiple capabilities request the same action
3. Priority is ad hoc rather than deterministic
4. Completing work does not reliably update Mission Memory / audit / completion events
5. There is no authoritative list of outstanding human work

## Scope

- Operator Inbox capability (`operator_inbox`) as a first-class Mission capability
- Mission type `operator_inbox`
- Inputs: Mission Memory, Campaign Review, Direct Mail Workflow, Outcome Intelligence, Capability Events, Validation Results
- Inbox categories: Approval Required · Review Required · Action Required · Decision Required · Completed
- Inbox items with id, title, category, priority, source, mission, client, dates, status
- Deterministic priority (Critical / High / Normal / Low)
- Actions: Open · Review · Approve · Reject · Complete · Snooze · Assign · Archive
- Deduplication — same operator action → single inbox item
- Deep links to originating workspaces
- Completion → Mission Memory event + audit + remove active item + completion event
- Coordination only — never runs Campaign Review / Print / Mail / Outcome processing

## Out of Scope

- Performing Campaign Review, Mail Package Generation, Direct Mail Execution, or Outcome Intelligence (upstream capabilities only)
- Full Command Deck HTML UI chrome (v1 returns inbox view model; UI binds later)
- Full Mission Memory Postgres tables (SPEC-032) — local event shapes mirror the contract
- Push notifications / email digests
- Multi-tenant SLA calendars beyond due-date fields

## Dependencies

| Dependency | Role |
|---|---|
| SPEC-023 Capability Framework | Capability contract + registry |
| SPEC-034 Campaign Review | Approval / validation work items |
| SPEC-035 Direct Mail Execution | Print / assemble / mail action items |
| SPEC-036 Outcome Intelligence | Decision items (apply recommendation) |
| SPEC-032 Mission Memory | Append completion / audit events |
| ADR-024 | Inbox coordinates; capabilities generate; inbox does not process |

## Architecture

```text
Capability Events · Validation Results
Campaign Review · Direct Mail · Outcome Intelligence · Mission Memory
      ↓
Operator Inbox (coordination only)
      ↓
Ingest → Deduplicate → Prioritize
      ↓
Active Inbox (authoritative outstanding work)
      ↓
Operator actions (open / review / approve / reject / complete / snooze / assign / archive)
      ↓
Mission Memory events · Audit · Completion Event
Deep link → originating workspace
```

### Design rules

1. **Coordinate, don’t process** — inbox never runs workflow capabilities ([ADR-024](../adr/ADR-024_Human_Work_Is_Coordinated_Through_the_Operator_Inbox.md)).
2. **Capabilities generate work items** — inbox organizes them.
3. **Deterministic prioritization** — same inputs → same priority.
4. **Deduplicate** — multiple sources requesting the same action produce one item.
5. **Authoritative outstanding list** — active inbox is the source of truth for human work.
6. **Completion is auditable** — Mission Memory + audit event + completion event; item leaves active list.
7. **Deep links** — every item points at its originating workspace.

### Inbox categories

**Approval Required**

| Kind |
|---|
| Campaign Approval |
| Proposal Approval |
| Mail Package Approval |

**Review Required**

| Kind |
|---|
| Low Confidence Intelligence |
| Missing Recipient |
| Missing Address |
| Validation Issues |

**Action Required**

| Kind |
|---|
| Print Campaign |
| Assemble Mail |
| Mail Campaign |
| Call Prospect |
| Send Proposal |
| Follow Up |

**Decision Required**

| Kind |
|---|
| Apply Recommendation |
| Update Client Playbook |
| Apply Ranking Changes |
| Resolve Duplicate Companies |

**Completed**

| Kind |
|---|
| Campaign Completed |
| Outcome Summary Available |
| Workflow Completed |

### Inbox item

| Field | Description |
|---|---|
| Identifier | Stable id |
| Title | Operator-facing title |
| Category | Approval / Review / Action / Decision / Completed |
| Kind | Specific work kind |
| Priority | Critical · High · Normal · Low |
| Source Capability | Originating capability id |
| Related Mission | Mission id |
| Related Client | Client id |
| Created Date | When surfaced |
| Due Date | Optional due |
| Current Status | open · in_progress · snoozed · completed · archived · rejected |
| Deep Link | Workspace target |
| Dedupe Key | Canonical key for uniqueness |

### Priority levels

| Level | Typical rules |
|---|---|
| Critical | Blocking approval for in-flight execution; overdue Critical due date |
| High | Approval Required; Decision Required with evidence-backed recommendations |
| Normal | Action Required (print / assemble / mail / follow-up) |
| Low | Completed notifications; informational Outcome Summary Available |

Priority is determined using deterministic business rules (see `priority.js`).

### Available actions

| Action | Effect |
|---|---|
| Open | Mark in progress; record audit |
| Review | Open + deep-link intent |
| Approve | Complete as approved (coordination signal only) |
| Reject | Mark rejected; leave active list |
| Complete | Complete item; Mission Memory + audit + completion event |
| Snooze | Defer until until-date |
| Assign | Set assignee |
| Archive | Remove from active without completion event (informational) |

### Mission integration

Each inbox item links to its originating workspace:

- Campaign Review
- Mail Package
- Company Intelligence
- Direct Mail Execution
- Outcome Summary

### Completion

Completing an inbox item:

1. Updates Mission Memory (timeline + event shapes)
2. Records an Audit Event
3. Removes the active inbox item
4. Publishes a Completion Event

### Deduplication

Multiple capabilities requesting the same operator action produce a **single** inbox item keyed by `(clientId, missionId|campaignId, kind, subjectId?)`.

### Business workflows coordinated (not performed)

- Campaign Planning
- Campaign Review
- Mail Package Generation
- Direct Mail Workflow
- Outcome Intelligence

## Data Model

```text
packages/capabilities/operatorInbox/
  types.js
  priority.js
  dedupe.js
  ingest.js
  validate.js
  actions.js
  assemble.js
  OperatorInboxStore.js
  OperatorInbox.js
  index.js
```

### Item status

```ts
type InboxItemStatus =
  | 'open'
  | 'in_progress'
  | 'snoozed'
  | 'completed'
  | 'rejected'
  | 'archived'
```

### Priority

```ts
type InboxPriority = 'critical' | 'high' | 'normal' | 'low'
```

## Implementation Plan

1. File SPEC-037 + ADR-024 + types / priority / dedupe / ingest / validate / actions / assemble / store
2. Operator Inbox capability + register builtin
3. Mission type `operator_inbox`; IntentRouter patterns
4. Tests: ingest + dedupe, deterministic priority, actions, completion → Mission Memory shapes, no workflow processing
5. Later: Command Deck Inbox UI, live event bus from capabilities

## Migration Strategy

- No required migration in v1 (in-memory store)
- Forward: `operator_inbox_items` + `operator_inbox_audit` tables when durability is required
- SPEC-032: map completion / audit events → Mission Memory timeline

## Testing

- Unit: priority rules; dedupe key stability; action transitions
- Capability: ingest from Campaign Review / Direct Mail / Outcome events; no duplicates; complete updates Mission Memory shapes
- Mission: IntentRouter + planner chain for `operator_inbox`
- Manual: inspect inbox view model JSON sorted by priority

## Acceptance Criteria

- [x] Single operational inbox
- [x] Deterministic prioritization
- [x] No duplicate work items
- [x] Every human-required action represented (from ingest sources)
- [x] Completion updates Mission Memory (event shapes)
- [x] Deep links to originating workspace
- [x] Fully auditable action history

## Future Work

- Command Deck Operator Inbox UI
- Live capability event bus → automatic ingest
- Persist inbox + audit in Postgres
- SLA / due-date cron for Critical escalation
- Assign / team routing UI
