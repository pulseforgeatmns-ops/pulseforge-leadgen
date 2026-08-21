# SPEC-131 — Transactional Mission Execution (TME)

**Status:** Implemented  
**Depends on:** SPEC-118 (Acquisition Mission Orchestration), SPEC-128 (Operator Approval), SPEC-130 (Mission Planning Engine)  
**ADR:** [ADR-057](../adr/ADR-057_Transactional_Mission_Execution.md)

## Purpose

Guarantee that every mission stage executes atomically.

A mission stage either:

- completes successfully and commits all state changes, or
- fails and leaves the mission in its previous consistent state.

Partial execution is prohibited.

## Philosophy

Mission state represents reality.  
Mission state must never represent work that failed.

## Current Behavior (before this spec)

```text
Operator approves
        ↓
Mission updated
        ↓
Pending approval removed
        ↓
Status changed
        ↓
Scout executes
        ↓
Exception
        ↓
Mission now claims execution started
```

The mission no longer reflects reality.

## Desired Behavior

```text
Operator approves
        ↓
Validate Preconditions
        ↓
Execute Specialist
        ↓
Persist Contributions
        ↓
Commit Mission State
        ↓
Present Response
```

If execution fails:

```text
Operator approves
        ↓
Validate Preconditions
        ↓
Exception
        ↓
Rollback
        ↓
Mission unchanged
```

## Core Invariant

No mission state mutation may become durable until the stage has completed successfully.

## Transaction Boundary

Every stage executes inside one logical transaction.

```text
BEGIN
  Validate
  Execute
  Persist
  Commit
END
```

Any failure aborts the transaction.

## Execution Lifecycle

### Phase 1 — Preconditions

Validate (no mutation):

- mission exists
- mission active
- mission locked
- structured plan approved
- specialist available
- required evidence present

Specialists never verify these. The Execution Engine validates, then the specialist runs and assumes valid inputs.

### Phase 2 — Specialist Execution

Example:

```text
Scout → Discovery → Candidate Ranking → Evidence → Discovery Contribution
```

Nothing is committed yet. `executing` exists only inside the transaction working set.

### Phase 3 — Validation

Validate outputs. Examples:

- contribution exists
- confidence valid
- evidence attached
- contract satisfied

### Phase 4 — Commit

Only after successful validation, persist together:

- contribution
- mission state
- approval consumption
- progress
- audit events

Everything commits together.

### Phase 5 — Presentation

Generate the workspace response. Presentation failures must not invalidate committed execution. Presentation may retry independently.

## Rollback Rules

Rollback if:

- precondition failure
- specialist exception
- persistence failure
- validation failure

Mission returns to its previous state. No manual repair required.

## Atomic Objects

Commit together, never partially:

- Mission
- Contributions
- Audit events
- Progress
- Confidence
- Pending decisions
- Operator approvals

## Approval Consumption

**Before:** Approve → Approval removed → Scout crashes  

**After:** Approve → Scout succeeds → Approval consumed  

Approval is part of the commit.

## Error Classes

| Class | Meaning | Mission |
|---|---|---|
| Planning Error | Mission Plan missing. No execution attempted. | Unchanged |
| Specialist Error | Scout (or other specialist) exception. | Unchanged |
| Persistence Error | Database / store write failure. | Unchanged |
| Presentation Error | Mission already committed. Presentation may retry. | Committed |

## Mission State Machine

```text
Planned → Approved → Executing → Completed
```

`Executing` exists only inside the transaction. It is never persisted unless execution succeeds. Successful commits persist the completed stage, not `executing`.

## Audit Guarantees

Every execution records:

- Transaction ID
- Mission Version
- Preconditions
- Specialist
- Duration
- Commit Status
- Rollback Reason
- Exception

This makes every failure reproducible. Rollback audits are stored outside mission state so they do not themselves mutate the mission.

## Recovery

After rollback the operator sees:

```text
Discovery could not execute.
Mission remains unchanged.
Resolve the blocker and retry.
```

## Relationship to SPEC-130

SPEC-130 answers: **What should specialists execute?**  
SPEC-131 answers: **How should specialists execute it safely?**

Both are required.

AMO-only persistence of acquisition campaign types (Command Deck Operations) is complementary store-selection work. This spec governs transactional stage execution, not which runtime owns the mission.

## Implementation

| Module | Role |
|---|---|
| `packages/acquisition-mission/TransactionalExecution.js` | Stage transaction: preconditions → execute → validate → commit → present |
| `packages/acquisition-mission/ExecutionErrors.js` | Planning / specialist / persistence / presentation error classes |
| `packages/acquisition-mission/ExecutionAudit.js` | In-memory execution audit (including rollbacks) |
| `packages/acquisition-mission/Store.js` | Snapshot / restore for the logical transaction |
| `services/acquisitionMissionPersistence.js` | `persistStageCommit` — one SQL transaction for durable objects |
| `packages/max/workspace/AmoOperatorApproval.js` | Discovery and plan-lock stages run inside TME |
| `migrations/2026-08-21-transactional-mission-execution.sql` | Durable execution audit table |

## Tests

- `packages/acquisition-mission/tests/spec131.test.js`
- `packages/max/workspace/tests/spec131Tme.test.js`

## Acceptance Criteria

- A mission may never exist in a state where approval is consumed and execution is not completed.
- A specialist exception must never leave orphaned contributions, a partially updated mission status, inconsistent progress, or a missing pending decision.
- Every execution is either fully committed or fully rolled back.
