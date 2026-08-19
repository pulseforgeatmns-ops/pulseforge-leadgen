# AUDIT-002 — Mission Continuation & Delegation

| Field | Value |
|---|---|
| **Status** | Observational audit — no routing change |
| **Date** | 2026-08-19 |
| **Related** | [SPEC-118](../specs/SPEC-118_Acquisition_Mission_Orchestration.md), [ADR-055](../adr/ADR-055_Max_Manages_Missions.md), [ADR-025](../adr/ADR-025_Active_Missions_Take_Precedence.md), [SPEC-039](../specs/SPEC-039_Active_Mission_Resolver.md) |
| **Instrumentation** | `packages/max/workspace/MissionContinuationAudit.js` |
| **Regression** | `packages/max/workspace/tests/missionContinuationAudit.test.js` |

## Purpose

Identify exactly where an active Acquisition Mission stops progressing after explicit operator approval. This audit does not change ask() routing. Every checkpoint below is a structured log (`[AUDIT-002] <KIND> {…}`) that includes Mission ID, workspace, stage, selected capability, timestamp, and outcome.

## Operator message under test

```text
Approved. Begin the mission...
```

## Checkpoint results

### Step 1 — Mission persistence

**Was the Mission created? Persisted?**

| Evidence | Result |
|---|---|
| `services/acquisitionMission.createMission` → `Engine.create` | Yes. Mission object has `id`, `stage`, `createdAt`, `tenantId`, `owner`. |
| In-memory store `engine.get(id)` | Yes. Same id and stage (`discover`). |
| Postgres `acquisition_missions` | Written when `persist !== false` via `persistMission`. Tests use `{ persist: false }`. |
| Log | `MISSION_CREATED` |

**Association with workspace/session:** `WorkspaceEngine.ask()` does not bind `session.context.missionId` on create. Association is only present when the caller supplies `context.missionId` / `acquisitionMissionId`. Log field `sessionBound` records that.

Default stage at create is `discover` (`packages/acquisition-mission/Mission.js`). There is no stored stage named `awaiting_approval` or `running_discovery`.

### Step 2 — Mission retrieval on the next Workspace ask

**Was an active Mission found?**

`evaluateWorkspaceMissionContinuation()` now runs at the start of `WorkspaceEngine.ask()`, before `classifyCognitiveMode` and retrieval.

| Condition | Result |
|---|---|
| `context.missionId` + in-memory/engine store | `MISSION_LOADED` — id and stage observed |
| Tenant list via `activeMissionFor` | Loaded if a non-`improve` mission exists |
| No store row and no explicit id | `ACTIVE_MISSION_FOUND` outcome `not_found`, reason `no_mission_in_store` |

**If no Mission is loaded, why?** Logged `loadReason`: `no_tenant` or `no_mission_in_store` or explicit-id failure. Conversational “mission created” prose from Client Intelligence does not create a SPEC-118 row (`packages/max/workspace/tests/clientContextActiveReasoning.test.js` TEST H: plan acceptance leaves `result.mission === null`).

### Step 3 — Approval detection

Existing classifiers on this utterance:

| Classifier | Result | Location |
|---|---|---|
| `looksLikeAcquisitionMissionQuestion` | **false** (gate is why/health/workspace only) | `AcquisitionMissionTurn.js` |
| `classifyCognitiveMode` | **`unclassified`** (`EXECUTION_RE` requires `approve this/the …`, not `Approved. Begin the mission`) | `CognitiveMode.js` |
| `looksLikeAcquisitionQuestion` | **false** | `NeedAssessment.js` |
| `selectExecutionDomain` | **`general_conversation`**, routeKind `intelligence`, confidence 0.2 | `ExecutionDomain.js` |
| AUDIT-002 `classifyOperatorIntent` | **Operator Approval**, confidence ≥ 0.9, reason `operator_approved_and_requested_mission_start` | `MissionContinuationAudit.js` |

Log: `MISSION_APPROVAL` `{ intent, confidence, reasoning, outcome: "operator_approval" }`.

Approval **is** detected by the audit classifier. It was **not** detected by the handlers that can advance a Mission.

### Step 4 — Mission state transition

Expected by this audit: awaiting approval → running discovery.  
Implemented SPEC-118 stages: `discover` → `understand` (only after Scout discovery evidence; `Lifecycle.js` `PREREQUISITES`).

On `ask("Approved. Begin the mission...")` with a loaded discover Mission:

| Field | Observed |
|---|---|
| Previous stage | `discover` |
| Next stage (recommended) | `understand` |
| Transition reason (recommended) | `operator_approval` |
| `progressMission` called | **No** |
| Log `MISSION_TRANSITION` | **Absent** |
| Stored stage after ask | still `discover` |

`stageAdvanced: false` on the continuation snapshot.

### Step 5 — Delegation decision

Continuation evaluation (recommended, not executed):

| Field | Value |
|---|---|
| Capability | `scout` |
| Reason | `operator_approved_and_requested_mission_start` |
| Mission context | `{ missionId, objective, targetSegment, campaign, stage, constraints }` |
| Delegation payload | same object, logged on `MISSION_DELEGATE` |

### Step 6 — Specialist invocation

`maybeHandleScoutAcquisitionTurn` requires `looksLikeAcquisitionQuestion` (`NeedAssessment.js`). `"Approved. Begin the mission..."` does not match investigation verbs or `ACQUISITION_NEED_RE`.

| Field | Observed |
|---|---|
| Invocation attempted? | **No** |
| Log | `MISSION_DELEGATE` `{ capability: "scout", outcome: "not_attempted", invoked: false }` |
| `result.scoutLoop` | absent |

This is **Failure D** if a Mission was loaded and approval was recognized: stage would advance in the evaluator’s recommendation, but Scout is never invoked.

### Step 7 — Result handling

`attachScoutDiscovery` logs `MISSION_RESULT`. Without a `missionId` it returns `null` and logs `attached: false`. Workspace ask never calls it on this utterance, so Scout output is not attached.

### Step 8 — Response composition

| Expected | Observed |
|---|---|
| Mission + Scout results + business intelligence | **No** |
| Fresh / general reasoning | **Yes** |

`MISSION_RESPONSE` `{ composedFrom: "fresh_reasoning", actualPipeline: "general_reasoning" }`.  
`result.route` is `intelligence`. `responseFromMission` is false.

`composeMissionResponse` / `composeActiveMissionResponse` are not used: SPEC-022 mission domain is not selected, and SPEC-118 ask handling does not match this question.

## Continuation routing

Required decision (before reasoning):

```text
Load Active Mission → exists? → Continue Mission Evaluation → Mission Engine | General Reasoning
```

Instrumentation now evaluates that decision on every ask. It does **not** divert the pipeline (audit constraint).

For `"Approved. Begin the mission..."` with a loaded Mission:

| Field | Recommended | Actual ask() pipeline |
|---|---|---|
| Continuation | `continue` | n/a (not wired) |
| Pipeline | `mission_engine` | `general_reasoning` |
| Bypass justification | `continuation_recommended_ask_does_not_divert` | — |

If General Reasoning is selected despite an active Mission, the log states whether that was (a) explicit new-objective language, (b) no Mission loaded, or (c) continuation recommended but ask() not diverted.

## Failure matrix (this utterance)

| Failure | Applies? | Observable evidence |
|---|---|---|
| **A — No Mission loaded** | Only when no SPEC-118 row / no `missionId` | `ACTIVE_MISSION_FOUND` outcome `not_found` |
| **B — Approval not detected** | **Yes, in production handlers.** Audit classifier detects it. | `looksLikeAcquisitionMissionQuestion` false; cognitive mode not execution; `MISSION_APPROVAL` still `operator_approval` |
| **C — Stage never advances** | **Yes** when a Mission is loaded | no `MISSION_TRANSITION`; store stage unchanged |
| **D — Scout never invoked** | **Yes** when a Mission is loaded | `MISSION_DELEGATE` `not_attempted` |
| **E — Scout results ignored** | N/A (Scout not invoked) | no `MISSION_RESULT` from ask() |
| **F — Response rebuilt from scratch** | **Yes** | `MISSION_RESPONSE` `fresh_reasoning` |

## Where progression stops

The stop is **orchestration routing in `WorkspaceEngine.ask()`**, not Scout reasoning and not SPEC-118 `Engine.progress`.

Observed ask() order after this audit:

1. Session + operator message
2. **AUDIT-002 load + continuation evaluation (new, observational)**
3. Specialist interrogation
4. SPEC-118 Max Ask **only** if the question matches why/health/workspace
5. `classifyCognitiveMode` + retrieval (general reasoning pipeline)
6. Operating update → Scout loop (lexical acquisition questions only) → CIE → SPEC-022 domain routing

`"Approved. Begin the mission..."` fails step 4’s regex, is not an execution cognitive mode, is not a Scout acquisition question, and is not a SPEC-022 mission-domain utterance. Ask() therefore answers from intelligence/reasoning and never calls `progressMission` or Scout.

SPEC-039 `ActiveMissionResolver` cannot save this path: it runs only after those handlers, and only when `isMissionDomain(domainDecision)` is true.

SPEC-103D plan acceptance is a separate conversational plan in session memory. It explicitly does not create a Mission.

## Log kinds

`MISSION_CREATED` · `ACTIVE_MISSION_FOUND` · `MISSION_LOADED` · `MISSION_STAGE` · `MISSION_APPROVAL` · `MISSION_TRANSITION` · `MISSION_DELEGATE` · `MISSION_RESULT` · `MISSION_RESPONSE`

Every row includes: `audit: "AUDIT-002"`, `missionId`, `workspace`, `stage`, `selectedCapability`, `timestamp`, `outcome`.
