# SPEC-157 — Autonomous Discovery Approval Policy

## Objective

When `executionPolicy = autonomous`, Discovery Approval shall be automatically consumed unless the mission contract explicitly marks Discovery as requiring operator judgment.

## Models

### Model A — Human-gated Discovery (normal policy)

```
Plan Approved → Pause → Approve Discovery → Scout
```

### Model B — Autonomous Discovery (autonomous policy)

```
Plan Approved → Discovery Approval auto-consumed → Scout dispatched
```

Discovery Review (`prioritization_approval`) remains human-gated. The mission stage contract sets `requiresHumanDecision: true` for `discovery_review`.

## Runtime guarantee

The runtime evaluates:

1. Session `executionPolicy`
2. Mission contract (`MISSION_STAGE_CONTRACTS` + optional `structuredMission.execution.requireDiscoveryApproval`)
3. Pending operator decision kind

If autonomous and Discovery does not require human judgment, `maybeAutoAdvanceDiscoveryAfterPlan()` consumes discovery approval and dispatches Scout in the same plan-approval turn.

## Implementation

| Module | Role |
|---|---|
| `OperatorDecisionPolicy.js` | Policy predicates |
| `AcquisitionMissionExecution.js` | Chains discovery after plan approval |
| `MissionProgression.js` | Autonomous progression (SPEC-147) for explicit autonomous commands |
| `WorkspaceMode.js` | Execution-state workspace modes (ADR-074) |

## Acceptance tests

`packages/acquisition-mission/tests/spec157AutonomousDiscoveryPolicy.test.js`

## Related

- ADR-074 — Workspace Modes Reflect Execution State
- SPEC-136 — Pending Operator Decision Consistency
- SPEC-147 — Autonomous Mission Progression
