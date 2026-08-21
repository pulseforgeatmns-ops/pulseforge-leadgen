# AUDIT-007 — Operator Approval Routing

| Field | Value |
|---|---|
| **Status** | Completed |
| **Date** | 2026-08-21 |
| **Related** | [SPEC-118](../specs/SPEC-118_Acquisition_Mission_Orchestration.md), SPEC-127, SPEC-128, AUDIT-003, AUDIT-006 |
| **Scope** | Trace the utterance `approved` through Workspace while an Acquisition Mission is waiting on operator discovery approval |
| **Breakpoint** | `WorkspaceEngine.ask` returns from `maybeHandleMissionFirstTurn` before `maybeHandleAcquisitionMissionExecution` |

## Executive summary

Operator approval does **not** fail because no Mission exists or because no pending decision exists. Both are present.

The utterance `approved` is classified as a Mission execution command (`isMissionExecutionCommand` → true) and, against an AMO Discover mission with `pendingOperatorDecision`, as `discovery_approved`.

Ownership nevertheless leaves the Acquisition Mission runtime at **one line**:

`packages/max/workspace/WorkspaceEngine.js:494` — `return { ...missionFirstTurn }`

Gated by `if (missionFirstTurn)` at line 470. `maybeHandleMissionFirstTurn` (SPEC-022 / AUDIT-003) is invoked **before** `maybeHandleAcquisitionMissionExecution` (SPEC-128). When a session-bound SPEC-022 Mission exists, that handler returns a result. `maybeHandleAcquisitionMissionExecution` at line 507 is never called. `MISSION_APPROVAL_MATCHED` is not emitted. Scout does not run. The AMO `pendingOperatorDecision` remains.

The runtime that claims `approved` is the **legacy SPEC-022 Mission Engine** (`ActiveMissionResolver.resumeMission`). `classifyMessage('approved')` is `resume` / `default_resume_active`, not `execute_stage`, so `executeCurrentStage` is not invoked and **RecommendationEngineExecutor is not entered** for this utterance. The operator-visible result is advisory continuation: *"Continuing with the active Mission (no new Mission created)."*

RecommendationEngineExecutor (AUDIT-003) is the sibling fallback on that same legacy pipeline when classification **is** `execute_stage` and no stage executor is registered. Bare `approved` never reaches that branch.

No routing change is made in this audit.

---

## Capture trace — utterance `approved`

### Path A — AMO only (no SPEC-022 binding)

| Event | Result |
|---|---|
| **WORKSPACE_ENTRY** | `question=approved` |
| **ACTIVE_MISSION_FOUND** | **Yes.** `source=amo`, runtime `AcquisitionMission`, stage `discover` |
| **MISSION_PENDING_DECISION** | **Yes.** `prompt=Approve discovery?`, stage `discover` |
| **APPROVAL_CLASSIFIER** | `missionExecutionCommand=true`, `executeStage=false` (`default_resume_active`), `amoAction=discovery_approved`, `returnsTrue=true` |
| **OWNER_SELECTED** | `active_mission` / `active_mission_execution_command` |
| **PIPELINE_SELECTED** | `AcquisitionMission` via `maybeHandleAcquisitionMissionExecution` |
| **MISSION_APPROVAL_MATCH** | **Yes** (`MISSION_APPROVAL_MATCHED`) |
| **MISSION_STAGE_EXECUTION** | **Yes** — Scout, outcome `completed` |
| **FALLBACK_REASON** | none |

This is the expected SPEC-128 chain. Verified by `AUDIT-007` test *AMO-only workspace consumes approved*.

### Path B — AMO pending **and** SPEC-022 Mission bound to the session (observed failure)

| Event | Result |
|---|---|
| **WORKSPACE_ENTRY** | `question=approved` |
| **ACTIVE_MISSION_FOUND** | **Yes.** `source=legacy` (legacy is preferred over AMO), runtime `MissionEngine` |
| **MISSION_PENDING_DECISION** | **Yes.** AMO still has `Approve discovery?` |
| **APPROVAL_CLASSIFIER** | `missionExecutionCommand=true`, `executeStage=false`, `amoAction=discovery_approved` |
| **OWNER_SELECTED** | `active_mission` / `active_mission_execution_command` |
| **PIPELINE_SELECTED** | `MissionEngine` via `maybeHandleMissionFirstTurn` |
| **MISSION_APPROVAL_MATCH** | **No** — AMO handler never reached |
| **MISSION_STAGE_EXECUTION** | **No** — resume, not execute |
| **FALLBACK_REASON** | `legacy_mission_first_preempted_amo` |

Breakpoint: `WorkspaceEngine.ask:maybeHandleMissionFirstTurn return`.

Verified by `AUDIT-007` test *legacy Mission-first return is the routing breakpoint*.

---

## Acceptance criteria answers

| Question | Answer |
|---|---|
| Was an active Mission found? | **Yes.** Path A: AMO Discover mission. Path B: SPEC-022 Mission is selected first; AMO remains active in parallel. |
| Was a pending operator decision found? | **Yes.** `pendingOperatorDecision.prompt = "Approve discovery?"` on the AMO mission. |
| Did approval classifier return true? | **Split.** Workspace execution-command classifier: **true**. AMO `detectExecutionAction`: **`discovery_approved`**. Legacy `classifyMessage` EXECUTE_STAGE: **false** (`resume` / `default_resume_active`). |
| Which runtime claimed `"approved"`? | Path A: Acquisition Mission execution (`maybeHandleAcquisitionMissionExecution`). Path B: **legacy SPEC-022 Mission Engine** (`maybeHandleMissionFirstTurn` → `ActiveMissionResolver.resumeMission`). |
| Why wasn't Mission selected? | Mission **was** selected as owner (`active_mission`). The **AMO** Mission was not the pipeline that ran, because Mission-first returned first. |
| Why did Recommendation own the response? | The claiming pipeline is the SPEC-022 Mission Engine, the AUDIT-003 home of RecommendationEngine. For bare `"approved"` the action is **`resumed`**, not `stage_fallback`. RecommendationEngineExecutor is **not** invoked. The response is advisory resume copy, not Scout. |

---

## One routing breakpoint

```470:506:packages/max/workspace/WorkspaceEngine.js
      if (missionFirstTurn) {
        const legacyAction =
          missionFirstTurn.resolution && missionFirstTurn.resolution.action;
        logPipelineSelected({
          pipeline:
            legacyAction === 'stage_fallback'
              ? APPROVAL_ROUTING_PIPELINES.RECOMMENDATION_ENGINE
              : APPROVAL_ROUTING_PIPELINES.MISSION_ENGINE,
          claimedBy: 'maybeHandleMissionFirstTurn',
          // ...
        });
        // FALLBACK_REASON: legacy_mission_first_preempted_amo
        return {
          ...missionFirstTurn,
          // ...
        };
      }

      const amoExecutionTurn = await maybeHandleAcquisitionMissionExecution({
```

**Exact line where ownership leaves the AMO Mission runtime:** the `return` at line 494, inside `if (missionFirstTurn)` (line 470). The AMO call at line 507 is skipped.

Upstream preference that makes Mission-first able to return a result: `resolveActiveMissionLock` checks SPEC-022 **before** AMO.

```96:112:packages/max/workspace/ActiveMissionGuard.js
    const legacy = await input.missionEngine.activeMissionResolver.resolveActiveMission(
      input.session.id
    );
    if (legacy && isActiveMissionStatus(legacy.status)) {
      return {
        active: true,
        mission: legacy,
        source: 'legacy',
        // ...
      };
    }

  const amoMission = resolveAcquisitionActiveMission(input);
```

---

## Classifier detail

| Classifier | File | `"approved"` |
|---|---|---|
| `isMissionExecutionCommand` | `ExecutionLanguageDetection.js` `/\bapprov(e\|al\|ed)\b/i` | **true** |
| `detectExecutionAction` | `AcquisitionMissionExecution.js` 246–254 (pending Discover + `approv(e\|al\|ed)`) | **`discovery_approved`** |
| `classifyMessage` EXECUTE_STAGE | `classifyMessage.js` 44–52 (requires `approved` **and** `begin`) | **false** |
| `classifyMessage` default | `classifyMessage.js` 170 | **`resume` / `default_resume_active`** |
| `CONTINUATION_SIGNALS` | `MissionFirstRouting.js` 32 `/\bapprov(e\|al)\b/i` | **does not match** `"approved"` (no word boundary after `approve`) |
| Continuation still fires | `evaluateMissionContinuation` default resume confidence 0.78 ≥ 0.7 | **true** |

EXECUTE_STAGE patterns (none match the bare utterance):

```44:52:packages/mission-engine/classifyMessage.js
const EXECUTE_STAGE = [
  /\bapprov(ed|al)\b.*\bbegin\b/i,
  /\bbegin\s+scout\s+discover(y|ing)?\b/i,
  /\bexecute\s+(the\s+)?(discovery|scout)\b/i,
  /\bstart\s+scout\s+discover(y|ing)?\b/i,
  /\brun\s+scout\s+discover(y|ing)?\b/i,
  /\bexecute\s+stage\b/i,
  /\bapproved\.?\s*begin\b/i,
];
```

SPEC-128 tests and SPEC-127 WorkspaceEngine tests use `"Approved. Begin Discovery."`, which **does** match EXECUTE_STAGE and the AMO `begin` pattern. The operator utterance in this audit is the pending-decision reply `"approved"` alone.

---

## Why RecommendationEngine is adjacent but not entered

`RecommendationEngineExecutor` is selected only from `StageExecutionOrchestrator.executeCurrentStage` when `selectExecutorForStage` returns no executor:

```150:162:packages/mission-engine/StageExecutionOrchestrator.js
  if (!executorId) {
    logMissionExecutorFallback({
      // ...
      selected: EXECUTOR_IDS.RECOMMENDATION_ENGINE,
    });
    const fallback = await executeRecommendationFallback({ ... });
```

`executeCurrentStage` is called only on `MESSAGE_CLASS.EXECUTE_STAGE` (`ActiveMissionResolver.js` ~396). Bare `"approved"` is `RESUME`, which calls `resumeMission` and returns `action: 'resumed'` (`ActiveMissionResolver.js` ~447–469). Stage audit log is empty. ScoutDiscoveryExecutor is not invoked. RecommendationEngineExecutor is not invoked.

The observed “Legacy Recommendation Engine” label names that SPEC-022 pipeline (AUDIT-003), which owned the turn instead of AMO Scout.

---

## Failure matrix

| ID | Condition | Status |
|---|---|---|
| A | No active Mission | **Pass** — AMO mission is present |
| B | No pending operator decision | **Pass** — `Approve discovery?` is set at AMO create |
| C | Approval classifier false | **Partial** — workspace/AMO classifiers true; legacy EXECUTE_STAGE false |
| D | Mission-first returns before AMO execution | **Fail** — breakpoint at `WorkspaceEngine.js:494` |
| E | RecommendationEngineExecutor selected | **Not entered** for `"approved"` (resume, not execute_stage) |

---

## Instrumentation

Implemented in `packages/max/workspace/audit/OperatorApprovalRoutingAudit.js`, wired from `WorkspaceEngine.ask`.

| Event | Status |
|---|---|
| `WORKSPACE_ENTRY` | Implemented |
| `ACTIVE_MISSION_FOUND` | Implemented |
| `MISSION_PENDING_DECISION` | Implemented |
| `APPROVAL_CLASSIFIER` | Implemented |
| `OWNER_SELECTED` | Implemented |
| `PIPELINE_SELECTED` | Implemented |
| `MISSION_APPROVAL_MATCH` | Implemented (AMO path) |
| `MISSION_STAGE_EXECUTION` | Implemented (AMO path) |
| `FALLBACK_REASON` | Implemented (`legacy_mission_first_preempted_amo`) |

Tests: `packages/max/workspace/tests/audit007OperatorApprovalRouting.test.js`

---

## Architectural invariant (observed, not changed)

> An AMO pending operator decision is not consumed unless `maybeHandleAcquisitionMissionExecution` runs.

Today, a session-bound SPEC-022 Mission causes `maybeHandleMissionFirstTurn` to return first. The pending AMO decision is then invisible to routing.

This audit does not change that order.
