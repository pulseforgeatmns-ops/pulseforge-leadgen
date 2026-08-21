# SPEC-129 — Remove Legacy Mission Runtime Preemption

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-21 |
| **Depends on** | [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md), [SPEC-118](SPEC-118_Acquisition_Mission_Orchestration.md), SPEC-127, SPEC-128, AUDIT-007 |
| **ADR** | [ADR-025 Active Missions Take Precedence](../adr/ADR-025_Active_Missions_Take_Precedence.md), [ADR-055 Max Manages Missions](../adr/ADR-055_Max_Manages_Missions.md) |

## Objective

Workspace ownership of an active Mission is not enough. After the owner is `active_mission`, Workspace must **select a Mission type** and **dispatch to that type's runtime**. Evaluation order must not decide ownership.

## Problem

`WorkspaceEngine.ask()` invoked the SPEC-022 Mission-first handler and returned when it produced a result. `maybeHandleAcquisitionMissionExecution()` never ran. A pending AMO operator decision (`Approve discovery?`) was ignored. Scout did not execute.

```text
WorkspaceEngine.ask()
        ↓
maybeHandleMissionFirstTurn()   ← SPEC-022
        ↓
return
```

AUDIT-007 located the breakpoint at the early return from `maybeHandleMissionFirstTurn`.

## Ownership rule

```text
Owner → Mission Type → Mission Runtime
```

Mission ownership alone is insufficient. The runtime must also dispatch to the correct Mission implementation.

## Required order

```text
WorkspaceEngine.ask()
        ↓
Active Mission Resolver (owner = active_mission)
        ↓
Resolve Mission (AMO and SPEC-022 independently)
        ↓
Determine Runtime
        ↓
Dispatch
        ↓
Return
```

### Dispatch table

| Mission Type | Runtime | Handler |
|---|---|---|
| Acquisition (AMO) | `AMO` | `maybeHandleAcquisitionMissionExecution` |
| Legacy SPEC-022 | `SPEC-022` | `maybeHandleMissionFirstTurn` |

No implicit precedence. Neither runtime is tried “first” as a fall-through.

### When both missions exist

AMO pending operator approval plus an execution/approval utterance (`approved`, `Approved. Begin Discovery.`, …) selects **AMO**. Legacy resume must not intercept AMO approvals.

A bound SPEC-022 Mission with **no** AMO pending approval selects **SPEC-022**.

## Logging

Every dispatch emits:

```text
MISSION_RUNTIME_SELECTED
runtime: AMO | SPEC-022
```

Never ambiguous. Never omitted when a runtime is selected.

## Acceptance

- AMO approval reaches `maybeHandleAcquisitionMissionExecution()`.
- `MISSION_APPROVAL_MATCHED` emits.
- Discovery executes. Scout runs.
- Legacy runtime never intercepts AMO approvals.
- Legacy runtime continues to resume and execute SPEC-022 missions when no AMO approval is pending.

## Out of scope

- Merging SPEC-022 and SPEC-118 stores.
- Changing `classifyMessage` EXECUTE_STAGE patterns.
- Changing Scout discovery strategy (AUDIT-006).
