# SPEC-171 — Canonical Execution Router

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Max |
| **Created** | 2026-08-25 |
| **Depends on** | [SPEC-118](SPEC-118_Acquisition_Mission_Orchestration.md), [SPEC-131](SPEC-131_Transactional_Mission_Execution.md), [SPEC-132](SPEC-132_Specialist_Execution_Contract.md), [SPEC-170](SPEC-170_Mission_Runtime_Ownership_Boundaries.md) |
| **ADR** | [ADR-090](../adr/ADR-090_Canonical_Execution_Routing.md) |

## Objective

Introduce a Canonical Execution Request (CER) and a single Execution Router so every execution-capable surface produces the same immutable request and only the router may dispatch specialists or Transactional Mission Execution (TME).

## Problem

Chat, Mission Workspace, approval buttons, Command Deck, and REST each contain fragments of intent detection, permission checks, runtime ownership, and specialist dispatch. The same operator intent executes differently depending on origin (AUDIT-046 through AUDIT-050).

## Architecture

```text
Surface (resolve intent)
        ↓
createExecutionRequest()     ← immutable CER
        ↓
routeExecutionRequest()      ← sole dispatcher
        ↓
MissionRuntimeOwnership      ← SPEC-170, mandatory
        ↓
executeMissionStage()        ← SPEC-131, router-owned
        ↓
Specialist (Scout / Max / …) ← CER + mission context
        ↓
Presentation (surface)
```

## Canonical Execution Request

```text
ExecutionRequest {
    id              cer_<uuid>
    source          chat | workspace | voice | api | approval_button | command_deck
    missionId
    operatorId
    intent          APPROVE_PLAN | APPROVE_DISCOVERY | START_DISCOVERY | …
    stage
    executionMode
    approval
    permissions
    runtimeOwner    amo | mission_engine
    objective
    payload
    metadata
}
```

`createExecutionRequest()` freezes the object. Mutation after create fails closed.

Chat `approve` and the Workspace **Approve discovery** button produce the same `intent`. Only `source` and `id` differ.

## Execution Router

`routeExecutionRequest(request, context)` owns:

| Responsibility | Behavior |
|---|---|
| Permission validation | `permissions.canExecute !== false`; mutating intents require `operatorId` unless autonomous |
| Execution eligibility | Known intent; mission present; not read-only / execution-disabled |
| Runtime ownership | `assertMissionRuntimeBoundary` before dispatch (SPEC-170) |
| TME dispatch | Calls existing stage handlers; surfaces never call `executeMissionStage` |
| Specialist selection | Intent → specialist (discovery → Scout, plan lock → Max, …) |
| Rollback | Rolled-back TME errors become a router result, not a surface branch |
| Auditing | One `SPEC-171` audit row per request id |

## Intents

| Intent | Specialist | Handler |
|---|---|---|
| `APPROVE_PLAN` | Max | `advancePlanAfterApproval` |
| `APPROVE_DISCOVERY` | Scout | `advanceDiscoveryAfterApproval` |
| `START_DISCOVERY` | Scout | same as `APPROVE_DISCOVERY` |
| `APPROVE_PRIORITIZATION` | Max | `advancePrioritizationAfterApproval` |
| `CLARIFY_PLAN` | Max | `advancePlanClarification` |
| `CANCEL_PLAN` | Max | `cancelMissionPlan` |
| `EDIT_PLAN` | Max | `beginPlanEdit` |
| `APPLY_PLAN_EDITS` | Max | `applyPlanEdits` |
| `AUTONOMOUS_PROGRESSION` | Max | `runAutonomousProgression` |
| `OPERATOR_APPROVED` | Operator | generic approval contribution |

`START_DISCOVERY` is the voice/API alias of `APPROVE_DISCOVERY`. Dispatch is identical.

Autonomous execution policy may mint a child CER (`APPROVE_DISCOVERY`, `metadata.autoConsumed: true`) after a successful plan lock. That child still enters through the router.

## Surfaces

| Surface | Adapter | Source |
|---|---|---|
| Workspace chat | `maybeHandleAcquisitionMissionExecution` | `chat` |
| Approval button | Mission Workspace operator panel | `approval_button` |
| REST | `POST /api/v1/amo/missions/:id/execute` | `api` |
| Command Deck / Voice | `createExecutionRequest({ source, intent })` | `command_deck` / `voice` |

Surfaces resolve intent, create a CER, submit to the router, and render. They do not call `advanceDiscoveryAfterApproval` or TME.

## Error Contract

| Code | When |
|---|---|
| `cer_invalid` | Missing or unfrozen request |
| `cer_unknown_intent` | Intent not in the catalog |
| `cer_permission_denied` | `canExecute === false` or missing operator for a mutating intent |
| `cer_policy_blocked` | Read-only or execution-disabled policy |
| `cer_runtime_owner_required` | Router cannot resolve runtime owner |
| `MISSION_RUNTIME_BOUNDARY_VIOLATION` | SPEC-170 crossover |

## Implementation

| Component | Change |
|---|---|
| `ExecutionRequest.js` | CER model, freeze, intent catalog, pending-decision → intent |
| `ExecutionRouter.js` | Permission, ownership, dispatch, audit, replay |
| `AcquisitionMissionExecution.js` | Chat becomes a CER producer |
| `routes/acquisitionMissions.js` | `POST /api/v1/amo/missions/:id/execute` |
| `public/acquisition-missions.html` | Approval buttons submit CER intents |

## Verification

`packages/acquisition-mission/tests/spec171.test.js` and `packages/max/workspace/tests/spec171ExecutionSurface.test.js`:

- CER is immutable and uniquely identified
- Chat and approval button produce the same intent
- Voice `START_DISCOVERY` dispatches the same handler as `APPROVE_DISCOVERY`
- Router validates runtime ownership before specialist dispatch
- Replay reuses the same request id and is auditable
- Chat execution returns the routed CER

## Acceptance Criteria

- [x] Every executable AMO action enters through `routeExecutionRequest`
- [x] Chat, API, and approval button produce Canonical Execution Requests
- [x] Specialists are not invoked from UI handlers
- [x] SPEC-170 runtime ownership is validated before dispatch
- [x] TME remains the commit path; the router owns when it runs
- [x] Every request has a unique id, is replayable, and is audited once
