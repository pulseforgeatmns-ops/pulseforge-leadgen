# ADR-057 — Transactional Mission Execution Is Atomic

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-21 |
| **Spec** | [SPEC-131](../specs/SPEC-131_Transactional_Mission_Execution.md) |
| **Related** | [ADR-055](ADR-055_Max_Manages_Missions.md), [ADR-056](ADR-056_Mission_Planning_Engine_Is_The_Single_Interpreter.md), [ADR-003](ADR-003_Human_Approval.md), [ADR-016](ADR-016_Execution_Does_Not_Decide.md) |

## Context

SPEC-130 locked interpretation behind a structured Mission Plan. SPEC-128 consumed operator approval and then executed the specialist. Approval, pending-decision removal, and status were written **before** Scout (or any specialist) finished.

When Scout threw, the mission claimed execution had started: approval gone, pending decision gone, status “Discovery Executing”, no contribution. Mission state no longer represented reality.

## Decision

1. Every mission stage runs inside one logical transaction: validate → execute → persist → commit.
2. No mission mutation is durable until the stage succeeds. Approval consumption is part of that commit.
3. `executing` exists only in the transaction working set. It is never the durable status of a failed stage.
4. Planning, specialist, validation, and persistence failures roll back to the prior consistent mission.
5. Presentation runs after commit. Presentation failure does not undo a successful stage.
6. Every attempt records a transaction id, mission version, preconditions, specialist, duration, commit status, and (on failure) rollback reason plus exception.

## Consequences

### Positive

- Failed Discovery (or any stage) leaves the mission inspectable and retryable with no manual repair.
- Operators cannot observe “approved and executing” for work that did not complete.
- Failures are reproducible from the execution audit.

### Negative / tradeoffs

- Specialists must return a contribution payload; they must not assume prior in-transaction mission writes are already durable.
- Side effects outside the mission store (for example Scout prospect inserts) remain the specialist’s own persistence boundary. TME v1 atomically commits the mission objects listed in SPEC-131.
