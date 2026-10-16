# AUDIT-050 — Scout Execution Completion

| Field | Value |
|---|---|
| **Status** | Completed (Read-Only) |
| **Date** | 2026-08-25 |
| **Related** | [AUDIT-049](./AUDIT-049_Mission_Runtime_Ownership_Crossover.md), [ADR-089](../adr/ADR-089_Mission_Ownership_Shall_Not_Cross_Runtime_Boundaries.md), [SPEC-170](../specs/SPEC-170_Mission_Runtime_Ownership_Boundaries.md), [SPEC-131](../specs/SPEC-131_Transactional_Mission_Execution.md), [SPEC-118](../specs/SPEC-118_Acquisition_Mission_Orchestration.md) |
| **Scope** | Trace Discovery execution from operator approval through Scout until the first architectural point execution ceases. Stop at first divergence. |

## Symptom

After Mission Plan approval and Discovery approval, the mission displays:

```
Status: Discovering
Current Blocker: Waiting for Scout
```

The mission never advances to Discovery Review (`prioritization_approval`) or Understanding.

## Executive Summary

**The mission remains at "Waiting for Scout" because no Scout `discovery` contribution is ever committed to the AMO store.** Blockers derive from `scoutComplete === false` (`Blockers.js:50-54`, `Lifecycle.js:70`). That flag only becomes true when a `scout`/`discovery` contribution exists.

Two distinct first-divergence classes were verified:

| Class | When it applies | First stop |
|---|---|---|
| **A — Approval never dispatches Scout** | Operator approves through AMO inspect/ask UI or non-execution workspace turns | Before `advanceDiscoveryAfterApproval()` |
| **B — TME rolls back after Scout throws** | `advanceDiscoveryAfterApproval()` runs but Scout or persistence fails | After Scout begins, before `commitDiscoveryStage()` durably commits |

Post–SPEC-170, class **B** no longer throws `Unknown mission:` on the default AMO path (`runScoutForAmoMission` passes `missionEngine: null`). Class **A** remains the dominant production failure mode when approval is not routed through `maybeHandleAcquisitionMissionExecution`.

---

## Reproduction Baseline (verified in-process)

```text
Create mission → advancePlanAfterApproval → pendingOperatorDecision = discovery_approval
→ status = Discovering, blocker = Waiting for Scout, scoutComplete = false
```

This reproduces the stuck surface **before** Discovery execution is dispatched.

---

## 1. Discovery Approval

### Trace

```text
Approve Discovery
  → maybeHandleAcquisitionMissionExecution()          [Command Deck only]
  → detectExecutionAction() → 'discovery_approved'
  → shouldExecuteDiscovery() → true (when pending discovery_approval)
  → advanceDiscoveryAfterApproval()
```

Alternate (non-executing) path:

```text
POST /api/v1/amo/ask { question: "approved" }
  → services/acquisitionMission.answerOperator()
  → Engine.answerOperator()                           [inspection only — STOPS HERE]
```

### Capture — successful Command Deck path (fixture-enabled test run)

| Field | Value |
|---|---|
| mission id | `mission_392fa53a-dc6c-4296-92d8-2b82a2133285` (example) |
| transaction id | `tme_0704cd98-8661-4f92-8dca-e86b34e1e99d` |
| runtime | `amo` |
| stage | `discover` |
| mission version | `1 → 2` (after commit) |
| approval contribution | `contrib_f99d2624-…` (`operator`/`approval`, `action: discovery_approved`) |

### Questions

| Question | Answer |
|---|---|
| Does Discovery approval execute? | **Only when routed through `maybeHandleAcquisitionMissionExecution` → `advanceDiscoveryAfterApproval`.** `/api/v1/amo/ask` and read-only cognition turns do **not** execute. |
| Does Discovery TME begin? | **Yes**, when `advanceDiscoveryAfterApproval` is entered (`executeMissionStage`, `AmoOperatorApproval.js:809`). |
| Does Discovery TME commit? | **Yes** on successful Scout return (even `blocked` outcome). **No** on specialist/persistence rollback. |

---

## 2. Scout Launch

### Trace

```text
advanceDiscoveryAfterApproval()
  → executeMissionStage({ specialist: 'scout', stage: 'discover' })
  → execute: runScoutForAmoMission()
```

### Capture — default AMO dispatch (`runScoutForAmoMission`)

| Field | Value |
|---|---|
| invoked | `true` (when TME `execute` runs) |
| runtime owner | `amo` (`opts.runtimeOwner: 'amo'`, `missionEngine: null`) |
| mission id | AMO `mission_<uuid>` |
| execution context | `buildMissionExecutionContext()` — native AMO, `suppressSideEffects: true` |
| worker id | **None** — synchronous in-process await |

### Questions

| Question | Answer |
|---|---|
| Is Scout invoked? | **Only inside TME `execute` callback.** Not invoked from `Engine.answerOperator` or acquisition-missions.html. |
| Does the function return? | **Yes** — `Scout.discover()` is awaited synchronously (~22ms in local run). |
| Does it throw? | **Can throw** — pre-SPEC-170 crossover; post-SPEC-170 boundary guard; validation/persistence failures roll back TME. |
| Is execution asynchronous? | **No.** No worker/queue/cron is scheduled for AMO discovery. |
| Is a worker scheduled? | **No.** |

---

## 3. Scout Discovery (`Scout.discover`)

### Trace

```text
runScoutForAmoMission()
  → Scout.discover({ mission, missionEngine: null, opts: { executionContext, runtimeOwner: 'amo' } })
  → runDiscoveryPipeline()
```

### Capture — local production-like run (`allowFixtureFallback: false`)

| Field | Value |
|---|---|
| entered | `true` (`SCOUT_DISCOVERY_STARTED` emitted) |
| returned | `true` (outcome `DISCOVERY_BLOCKED`, 0 prospects) |
| exception | none on default AMO path post-SPEC-170 |
| execution duration | ~22ms (TME audit for scout stage) |

### Questions

| Question | Answer |
|---|---|
| Does Scout begin Discovery? | **Yes**, when TME dispatch occurs. |
| Does Discovery complete? | **Yes** — pipeline returns (may be `blocked`). Failure to commit is downstream in TME/persistence, not Scout hang. |
| If not, where does execution stop? | **Before TME dispatch** (class A) or **TME rollback** after Scout throws (class B). |

---

## 4. Investigative Reasoning Loop

### Trace

```text
Scout.discover()
  → runDiscoveryPipeline()
  → runInvestigativeReasoningLoop()          [DiscoveryPipeline.js:400-421]
```

### Capture — stage execution (local run, no external API keys)

| Stage | Executed |
|---|---|
| Market Definition | **Yes** (`UNDERSTAND_MARKET`) |
| Coverage Planning | **Yes** (`BUILD_INVESTIGATION_PLAN`) |
| Evidence Collection | **Partial** — `External Search attempted: false` |
| Evidence Synthesis | **Yes** (loop ran; empty evidence graph) |
| Business Understanding | **Yes** (0 entities) |
| Business Heuristics | **Yes** ("Insufficient business signals…") |
| Prediction | **Yes** (strategic decision / outcome review generated) |
| Mission Report | **Yes** (`missionIntelligenceReport` attached) |

### Questions

| Question | Answer |
|---|---|
| Which stage is the final completed stage? | **Mission Report** — loop reaches `phase: 'complete'`. When stuck at "Waiting for Scout", the loop **never ran** because TME dispatch did not commit. |

---

## 5. Coverage Engine

### Trace

```text
DiscoveryPipeline
  → buildDiscoveryPlan()
  → runScoutAcquisitionIntelligence({ useCoverageEngine: true })
  → coverage metrics in payload
```

### Capture

| Field | Value |
|---|---|
| workloads planned | Coverage plan built (universe estimate expected: 63) |
| workloads executed | `0` candidates discovered; `External Search attempted: false` |
| coverage % | `null` / incomplete (`discoveryStatus: 'incomplete'`) |
| blocked | **Yes** — `Discovery produced no verified prospects.` |
| reason | No live search providers/API keys in dev; repository stores empty |

### Questions

| Question | Answer |
|---|---|
| Does Coverage finish? | **Yes** — returns blocked/incomplete, not hung. |
| If not—where? | N/A for stuck missions; stuck missions never reach Coverage because TME never commits Scout output. |

---

## 6. Discovery Contribution

### Trace

```text
Scout result
  → discoveryPayloadFromScoutResult()
  → validateDiscoveryOutput()
  → commitDiscoveryStage()
  → engine.contribute({ specialist: 'scout', kind: 'discovery', payload })
```

### Capture — after successful TME commit

| Field | Value |
|---|---|
| contribution created | **Yes** — `contrib_f5bff255-…` |
| persisted | In-memory yes; durable only if `persistDurable` + pool succeed |
| attached to mission | **Yes** — `scoutComplete` becomes true |

### Capture — stuck mission (no dispatch)

| Field | Value |
|---|---|
| contribution created | **No** |
| persisted | **No** |
| attached to mission | **No** |

---

## 7. Discovery Commit

### Trace

```text
commitDiscoveryStage()
  → operator approval contribution (consumed)
  → scout discovery contribution
  → pendingOperatorDecision = prioritization_approval
  → EXECUTION_COMMITTED event
```

### Questions

| Question | Answer |
|---|---|
| Does Discovery commit? | **Yes** when TME completes (verified: `commitStatus: committed`). |
| If not—why? | **TME rollback** — Scout throw, validation failure, or `persistDurable` failure restores pre-transaction snapshot (`TransactionalExecution.js:382-405`). |

---

## 8. Runtime State (stuck vs healthy)

### Stuck (Waiting for Scout) — plan approved, discovery not dispatched

| Field | Value |
|---|---|
| mission stage | `discover` |
| status | `Discovering` |
| pending decision | `discovery_approval` |
| waitingOn | `Waiting for Scout` |
| current blocker | `waiting_for_scout` |
| scout status | `waiting` (workspace specialist state) |

### Healthy — after successful discovery commit

| Field | Value |
|---|---|
| mission stage | `discover` |
| status | `Discovering` |
| pending decision | `prioritization_approval` |
| waitingOn | null (or evidence review if blocked) |
| current blocker | none |
| scout status | `complete` |

### Question

**Why does the mission remain "Waiting for Scout" instead of advancing?**

Because **`scoutComplete` is false**: no `scout`/`discovery` contribution exists. Advancement to Understanding requires `canEnter(UNDERSTAND)` which demands scout discovery (`Lifecycle.js:22-23`, `Engine.js:277-278`). The UI label "Waiting for Scout" is inferred whenever stage is `discover` and scout has not completed (`Blockers.js:50-54`).

---

## 9. Worker Lifecycle

| Event | Timestamp (example run) |
|---|---|
| Scout started | `2026-08-25T17:50:05.765Z` (`SCOUT_DISCOVERY_STARTED`) |
| Scout finished | `2026-08-25T17:50:05.781Z` (`SCOUT_DISCOVERY_COMPLETED`) |
| Contribution written | `2026-08-25T17:50:05.785Z` (TME commit) |
| Commit completed | `2026-08-25T17:50:05.785Z` (`MISSION_STAGE_EXECUTION_COMPLETED`) |

### Questions

| Question | Answer |
|---|---|
| Does Scout ever signal completion? | **Yes**, when dispatched — observability events through pipeline completion. |
| Does AMO observe completion? | **Only if TME commits.** Rollback removes contribution; stuck missions show no completion. |

---

## 10. First Divergence (STOP)

Two verified first divergences depending on entry path:

### Divergence A — Primary (approval accepted, Scout never dispatched)

| Field | Value |
|---|---|
| **File** | `packages/acquisition-mission/Engine.js` |
| **Function** | `answerOperator` |
| **Line** | **554–676** |
| **Expected** | Operator Discovery approval should invoke `advanceDiscoveryAfterApproval()` and dispatch Scout inside TME. |
| **Actual** | `answerOperator` classifies the message as inspection/workspace prose and **returns without calling** `maybeHandleAcquisitionMissionExecution` or `advanceDiscoveryAfterApproval`. |
| **Reason** | `/api/v1/amo/ask` (`routes/acquisitionMissions.js:301-317`) routes to `Engine.answerOperator`, not the workspace execution router. `public/acquisition-missions.html` displays `Approve discovery?` but has **no approval action** wired—only `Advance stage` → `progressMission`, which cannot pass the Understand gate without scout discovery. |

**Secondary gates on the same class:**

| File | Function | Line | Reason |
|---|---|---|---|
| `packages/max/workspace/AcquisitionMissionExecution.js` | `maybeHandleAcquisitionMissionExecution` | **795–797** | Returns `null` when `!isMissionExecutionCommand(question)` — approval text that does not match execution verbs never dispatches Scout. |
| `packages/max/workspace/AcquisitionMissionExecution.js` | `maybeHandleAcquisitionMissionExecution` | **762–764** | Read-only cognition (`!mayMutateMission`) blocks all mission mutation including discovery dispatch. |
| `packages/max/workspace/AcquisitionMissionExecution.js` | `shouldExecuteDiscovery` | **706–708** | Returns `false` when `!hasPendingDiscoveryApproval` — orphan/inconsistent pending prevents re-dispatch. |

### Divergence B — When TME dispatch occurs but commit fails (AUDIT-049 lineage)

| Field | Value |
|---|---|
| **File** | `packages/scout/Discovery.js` |
| **Function** | `syncMissionFromPipeline` → `missionEngine.store.update` |
| **Line** | **182–194** |
| **Expected** | AMO-native discovery should commit only through AMO TME; Scout must not sync `mission_` ids into Mission Engine. |
| **Actual** | Pre-SPEC-170: `Unknown mission: mission_<uuid>`. Post-SPEC-170 default path avoids this (`missionEngine: null`). Legacy crossover now throws `MISSION_RUNTIME_BOUNDARY_VIOLATION`. TME **rolls back** — no scout contribution, mission stays `Waiting for Scout`. |
| **Reason** | Runtime ownership crossover (ADR-089). Fixed at dispatch (`AmoOperatorApproval.js:517-518`) but any custom `runScout` that re-injects `missionEngine` reproduces rollback. |

**Stop.** Downstream prioritization, Understanding, and outreach systems were not analyzed per audit scope.

---

## Acceptance Criteria

| Criterion | Met |
|---|---|
| Discovery approval traced through TME | ✓ |
| Scout launch verified | ✓ |
| Discovery execution traced stage-by-stage | ✓ |
| Investigative Reasoning Loop verified | ✓ |
| Coverage engine completion verified | ✓ |
| Discovery contribution lifecycle verified | ✓ |
| Discovery commit verified | ✓ |
| Worker lifecycle verified (synchronous, no worker) | ✓ |
| Runtime state captured | ✓ |
| First architectural divergence identified with file/function/line | ✓ |

---

## Conclusion

The mission stays at **"Waiting for Scout"** because **Scout discovery is never durably attached to the AMO mission**. The canonical execution path (`advanceDiscoveryAfterApproval` → TME → `runScoutForAmoMission` → `Scout.discover` → `commitDiscoveryStage`) works when invoked from the Command Deck workspace router.

The **first architectural divergence** for the reported production symptom is that **operator Discovery approval is not guaranteed to enter that path**. Approvals through `/api/v1/amo/ask`, the acquisition-missions HTML surface, or read-only cognition turns terminate before Scout dispatch, leaving the mission in `discover` with `scoutComplete = false` and blocker `waiting_for_scout` indefinitely.

When the execution path **is** entered but fails, the first divergence is **TME rollback before `commitDiscoveryStage` persists the scout contribution** (historically Mission Engine crossover at `Discovery.js:182-194`; now boundary-enforced with the same stuck surface).
