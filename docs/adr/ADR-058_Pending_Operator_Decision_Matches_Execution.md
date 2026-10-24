# ADR-058 — Pending Operator Decision Must Match Execution

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-22 |
| **Spec** | [SPEC-136](../specs/SPEC-136_Pending_Operator_Decision_Consistency.md) |
| **Related** | [ADR-003](ADR-003_Human_Approval.md), [ADR-021](ADR-021_Human_Approval_Before_Execution.md), [ADR-055](ADR-055_Max_Manages_Missions.md), [ADR-057](ADR-057_Transactional_Mission_Execution.md) |

## Context

Acquisition missions advertise a `pendingOperatorDecision` for the operator UI and separately compute execution predicates (`hasPendingPlanApproval`, `hasPendingDiscoveryApproval`). Those two sources could diverge: the plan could be locked while `plan_approval` remained pending, or discovery approval could be advertised while the engine refused to consume it.

An advertised decision the engine cannot consume is unmatchable. Generic `operator_approved` then became the fallback, which consumed the wrong approval.

## Decision

1. `pendingOperatorDecision` is not an independent source of truth. It must satisfy the execution predicates after every mission mutation.
2. Plan approval atomically sets `structuredMissionApproved = true` and `pendingOperatorDecision = discovery_approval`.
3. Discovery approval consumption sets `pendingOperatorDecision = null` in the same commit as Scout execution.
4. If kind, stage, `structuredMissionApproved`, and the predicates disagree, fail immediately with `MISSION_STATE_INCONSISTENT` and do not render the mission.
5. Presentation (`Approve mission plan?` / `Approve discovery?`) derives from executable predicates only.
6. Generic `operator_approved` must not execute while a consumable pending decision exists.

## Consequences

### Positive

- The operator never sees an Approve button the engine will not honor.
- Stale plan approval cannot survive plan lock.
- Discovery cannot be advertised before the mission plan is locked.

### Negative / tradeoffs

- Persisted missions that already diverge fail closed on write and inspect. Hydration skips them rather than silently repairing state.

### Follow-ups

- [SPEC-136](../specs/SPEC-136_Pending_Operator_Decision_Consistency.md)
- Tests in `packages/acquisition-mission/tests/spec136.test.js`
