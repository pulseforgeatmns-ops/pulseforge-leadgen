# SPEC-136 — Pending Operator Decision Consistency

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-22 |
| **Depends on** | [SPEC-130](SPEC-130_Structured_Mission_Planning.md), [SPEC-131](SPEC-131_Transactional_Mission_Execution.md), [SPEC-134](SPEC-134_Preserve_AMO_Presentation_Contract.md), SPEC-128 Operator Approval, SPEC-135 Mission Planning Gate |
| **ADR** | [ADR-058](../adr/ADR-058_Pending_Operator_Decision_Matches_Execution.md) |

## Objective

The pending operator decision must always be consistent with the executable mission state.

An operator approval must never become "unmatchable."

## Problem

Current invalid states:

```text
Mission
    ↓
structuredMissionApproved = true
pendingOperatorDecision.kind = plan_approval
```

or

```text
Mission
    ↓
pendingOperatorDecision = discovery_approval
    ↓
hasPendingDiscoveryApproval() == false
```

Both advertise an operator decision that the execution engine refuses to consume.

## Invariant

At every mission mutation, `pendingOperatorDecision` must satisfy the execution predicates.

If the UI renders **Approve Mission Plan**, then `hasPendingPlanApproval() == true`.

If the UI renders **Approve Discovery**, then `hasPendingDiscoveryApproval() == true`.

These values may never diverge.

## State Transition Rules

### Plan Approved

Before:

- `structuredMissionApproved = false`
- `pendingOperatorDecision = plan_approval`

After approval (atomic):

- `structuredMissionApproved = true`
- `pendingOperatorDecision = discovery_approval`

### Discovery Approved

Before:

- `pendingOperatorDecision = discovery_approval`

After consumption:

- `pendingOperatorDecision = null`

Then execute Discovery.

## Validation

After every mission mutation, validate:

- `pendingOperatorDecision.kind`
- `mission.stage`
- `structuredMissionApproved`
- execution predicates (`hasPendingPlanApproval`, `hasPendingDiscoveryApproval`, `hasPendingPlanClarification`)

If any disagree:

- Fail immediately with `MISSION_STATE_INCONSISTENT`
- Do not render the mission

## Presentation Contract

The UI must never present a decision that cannot currently be consumed by the execution engine.

Presentation derives from executable mission state (`presentableOperatorDecision` / `executableDecision`).

It must never become an independent source of truth.

A generic `operator_approved` fallback must not execute while a valid pending decision exists.

## Acceptance Criteria

- [x] Mission Plan → Approve → Discovery Approval appears → approved → pending consumed → Scout executes → pending cleared
- [x] A stale `plan_approval` cannot remain after the plan is approved
- [x] A displayed approval always matches the execution predicates
- [x] Generic `operator_approved` fallback does not execute while a valid pending decision exists
- [x] Inconsistent missions fail with `MISSION_STATE_INCONSISTENT` and are not rendered
