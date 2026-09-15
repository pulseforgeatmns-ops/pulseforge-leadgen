# AUDIT-049 — Mission Identity Continuity

| Field | Value |
|---|---|
| **Date** | 2026-08-25 |
| **Status** | Proposed |
| **Type** | Architectural Audit (Read-Only) |
| **Priority** | Critical |
| **Related** | [ADR-075](../adr/ADR-075_Transactional_Persistence_Exclusivity.md), [ADR-087](../adr/ADR-087_Operator_Objective_Takes_Precedence.md), [SPEC-168](../specs/SPEC-168_Canonical_Objective_Resolution.md) (referenced), [SPEC-169](../specs/SPEC-169_Canonical_Mission_Verification.md). **ADR-077 is not present in this tree.** |

## Finding

Discovery reports `Unknown mission: mission_<uuid>` after successful Acquisition Mission creation and successful mission-plan approval because **Discovery execution looks up the SPEC-118 Acquisition Mission ID in the SPEC-022 Mission Engine store**.

The ID string does not change. The lookup source does.

Creation, persistence, plan approval, and TME preconditions all resolve the mission from the Acquisition Mission (AMO) engine / `acquisition_missions`. Scout Discovery then calls `missionEngine.store.update({ id: amoMission.id })` against SPEC-022 (`missions` / in-memory MissionStore), which has never seen that ID.

## Reproduction (operator)

```text
Create a production acquisition mission.
Objective: Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.
→ Mission successfully created.

approve
→ Mission plan approved.

approve
→ Discovery could not execute.
   Unknown mission: mission_c1b6003f-8425-4939-b421-1350f5bd3790
```

In-process reproduction of the same sequence (create → `advancePlanAfterApproval` → `advanceDiscoveryAfterApproval` with production `missionEngine`) throws:

```text
SpecialistError: Unknown mission: mission_<uuid>
  caused by InMemoryMissionStore.update (MissionStore.js:45)
  called from syncMissionFromPipeline (Discovery.js:173)
  called from Scout.discover (Discovery.js:262)
  called from runScoutForAmoMission (AmoOperatorApproval.js:506)
  called from executeMissionStage (TransactionalExecution.js:264)
```

TME records `commitStatus: rolled_back`, `errorClass: specialist`, `rollbackReason: Unknown mission: mission_<uuid>`. Presentation wraps this as “Discovery could not execute” with that message as evidence status.

Production uses `PostgresMissionStore` (`utils/missionRuntime.js`) when a pool is available. The throw text and lookup key are identical; only the store implementation changes (`SELECT * FROM missions WHERE id = $1` instead of an in-memory Map).

---

## 1. Mission Creation

Factory: `packages/acquisition-mission/Mission.js` `createMission()` → `newId('mission')` → `mission_<uuid>`.

Production path: `AcquisitionOwnership` → `Runtime.create()` → `engine.create()`.

| Field | Value |
|---|---|
| **mission id** | `mission_c1b6003f-8425-4939-b421-1350f5bd3790` (operator). Format is AMO `newId('mission')`, not SPEC-022 `newId('msn')`. |
| **runtime instance** | Process singleton `getAcquisitionMissionRuntime()` / `bootAcquisitionMissionRuntime()`. ID pattern `amo_rt_<16 hex>`. SPEC-140: one AMO runtime per Node process. |
| **engine instance** | `Runtime.current().engine()` — AMO engine, ID pattern `amo_eng_<16 hex>`. Not the SPEC-022 `MissionEngine`. |
| **transaction id** | `null` at create. `lastTransactionId` is unset until a TME stage commits. Create persist is `persistMissionState` → `persistStageCommit`, not `executeMissionStage`. |
| **persisted** | Yes (operator creation succeeded). `runtime.create()` calls `persistMissionState(mission.id)` unless `autonomous === true`. |
| **version** | `0` |

`orchestrationMissionId` is `null`. No SPEC-022 mission is created or bound.

---

## 2. Mission Persistence (immediately after creation)

Authoritative table: `acquisition_missions` (`services/acquisitionMissionPersistence.js`). SPEC-022 table `missions` is not written.

| Field | Value |
|---|---|
| **exists in Postgres** | Yes — `acquisition_missions`. No — `missions`. |
| **primary key** | `acquisition_missions.id` = `mission_<uuid>` |
| **payload id** | JSONB `payload` includes the full mission; `missionFromRow()` overwrites `id` with `row.id`. PK and payload id match. |
| **version** | `0` |
| **stage** | `discover` |
| **status** | `Discovering` |

Load path: `loadTenantMissions` → `missionFromRow`. There is no `lookupMission()`. Closest: `loadMissionSnapshot(missionId, tenantId)` (used for post-commit verification, not Discovery lookup).

---

## 3. Mission Plan Approval

```text
approve
  → maybeHandleAcquisitionMissionExecution
  → advancePlanAfterApproval()
  → executeMissionStage({ missionId: mission.id, engine: AMO, stage: 'plan_lock' })
```

| Field | Value |
|---|---|
| **mission id entering TME** | Same AMO id (`mission.id`) |
| **mission id committed** | Same AMO id |
| **version before** | `0` |
| **version after** | `1` (`bumpMissionVersion` in plan-lock commit) |
| **transaction id** | `tme_<uuid>` (`newId('tme')`) |

AMO `engine.get(missionId)` succeeds. Durable write is `persistStageCommit` into `acquisition_missions` (+ contributions/events). SPEC-139/169 verification compares AMO memory to `acquisition_missions`, not to SPEC-022 `missions`.

Plan approval does not lose identity.

---

## 4. Discovery Approval

```text
approve
  → maybeHandleAcquisitionMissionExecution
  → advanceDiscoveryAfterApproval()
  → executeMissionStage({ missionId: mission.id, engine: AMO, stage: 'discover' })
       execute: runScoutForAmoMission(current, { missionEngine: input.missionEngine })
```

| Field | Value |
|---|---|
| **mission id received** | Same AMO id (session `missionId` / `acquisitionMissionId`, or `engine.list(tenantId)`) |
| **mission id passed to executeMissionStage** | Same AMO id (`mission.id`) |
| **runtime instance** | Same AMO `Runtime.current()` |
| **engine instance** | AMO engine for TME. **Additionally** WorkspaceEngine passes SPEC-022 `this._missionEngine` as `missionEngine`. |

TME preconditions still resolve the AMO mission (`engine.get` / `validateDiscoveryPreconditions` → `missionExists: true`). Identity is intact at this boundary.

---

## 5. Mission Lookup

There is no `lookupMission()`. Actual functions:

### AMO chain (succeeds through plan approval and TME begin)

| Function | Mission ID | Result |
|---|---|---|
| `resolveAcquisitionActiveMission` / `engine.list` + session bind | `mission_<uuid>` | AMO mission object |
| `engine.inspect(id)` / `engine.get(id, tenantId)` | `mission_<uuid>` | mission |
| `store.getMission(id)` | `mission_<uuid>` | mission |
| `loadMissionSnapshot(id, tenantId)` | `mission_<uuid>` | Used only after TME persist, not during Discovery specialist execute. Would hit `acquisition_missions`. |
| `SELECT * FROM acquisition_missions` | `mission_<uuid>` | row |

### SPEC-022 chain (first miss)

| Function | Mission ID | Result |
|---|---|---|
| `Scout.discover({ mission, missionEngine })` | AMO `mission.id` | proceeds to pipeline, then sync |
| `syncMissionFromPipeline` → `missionEngine.store.update({ id })` | AMO `mission.id` | throws |
| `PostgresMissionStore.get` / `InMemoryMissionStore.get` | AMO `mission.id` | **`null`** |
| `PostgresMissionStore.update` / `InMemoryMissionStore.update` | AMO `mission.id` | **`Unknown mission: mission_<uuid>`** |
| `SELECT * FROM missions WHERE id = $1` | AMO `mission.id` | **no row** |

`executeMissionStage` itself does not miss. The miss is inside the specialist `execute` callback after AMO lookup already succeeded.

---

## 6. Unknown Mission

First function returning `null` / throwing `Unknown mission`:

| Field | Value |
|---|---|
| **File** | `packages/mission-engine/MissionStore.js` (in-memory) / `packages/mission-engine/PostgresMissionStore.js` (production) |
| **Function** | `get` returns `null`; `update` throws |
| **Line** | `get`: MissionStore.js:34–36 / PostgresMissionStore.js:117–123. `update` throw: MissionStore.js:45 / PostgresMissionStore.js:130 |
| **Mission ID** | `mission_c1b6003f-8425-4939-b421-1350f5bd3790` |
| **Lookup Source** | SPEC-022 MissionStore (`missions` table or in-memory Map). Not AMO store. Not `acquisition_missions`. |
| **Expected** | The mission created and plan-approved by AMO |
| **Actual** | `null`, then `Error: Unknown mission: mission_<uuid>` |

Call site that selects this store:

| Field | Value |
|---|---|
| **File** | `packages/scout/Discovery.js` |
| **Function** | `syncMissionFromPipeline` |
| **Line** | 173 (`missionEngine.store.update({ id: mission.id, ... })`) |
| **Invoked from** | `discover` line 261–267 when `missionEngine` is truthy |

Wiring that supplies the wrong engine:

| Field | Value |
|---|---|
| **File** | `packages/max/workspace/AmoOperatorApproval.js` |
| **Function** | `runScoutForAmoMission` |
| **Line** | 506–508 (`Scout.discover({ mission, missionEngine: opts.missionEngine })`) |
| **Passed from** | `WorkspaceEngine.ask` → `maybeHandleAcquisitionMissionExecution` → `advanceDiscoveryAfterApproval` (`missionEngine: this._missionEngine`) |

TME then wraps the throw (`TransactionalExecution.js:275` `wrapAs(SPECIALIST, ...)`) without changing the message.

---

## 7. Memory vs Database (immediately before Discovery execution)

### AMO engine store

| Field | Value |
|---|---|
| **Mission exists?** | Yes |
| **Version?** | `1` (after plan lock) |
| **Stage?** | `discover` |
| **Status?** | `Discovering` |

### AMO database (`acquisition_missions`)

| Field | Value |
|---|---|
| **Mission exists?** | Yes (plan approval persisted via `persistStageCommit`) |
| **Version?** | `1` |
| **Stage?** | `discover` |
| **Status?** | `Discovering` |

### SPEC-022 store / `missions`

| Field | Value |
|---|---|
| **Mission exists?** | No |
| **Version / stage / status** | n/a |

**Are AMO memory and AMO persistence consistent?** Yes, for this ID. SPEC-169 verification already required that after plan-lock commit.

**Are AMO and SPEC-022 consistent?** No. SPEC-022 never received the identity. That inconsistency is not an AMO persist bug; Discovery consults a different identity space.

---

## 8. Runtime Identity

| Property | Value |
|---|---|
| **process id** | Single Node `server.js` process per replica. Not recorded on the mission. |
| **runtime id** | AMO `amo_rt_*` process singleton (SPEC-140). |
| **engine id** | AMO `amo_eng_*` for create / plan / TME. SPEC-022 `getMissionEngine()` singleton for Scout sync. |
| **worker id** | None. No Discovery worker pool. |
| **Railway instance** | One Node process per replica. AMO memory is per-process; hydrate from `acquisition_missions` recovers AMO identity across replicas. SPEC-022 persist is a separate `missions` table. |

**Did Discovery execute against the same runtime that created the mission?**

Same Node process and same AMO runtime for TME begin. Discovery specialist execution then addressed a **different engine** (SPEC-022 MissionEngine) that did not create the mission. A second Railway replica is not required for this failure.

---

## 9. Mission Identity Contract

| Stage | Mission ID | Store |
|---|---|---|
| Mission Created | `mission_<uuid>` | AMO memory |
| Mission Persisted | `mission_<uuid>` | `acquisition_missions` |
| Mission Loaded | `mission_<uuid>` | AMO hydrate / memory |
| Mission Approved (plan) | `mission_<uuid>` | AMO TME + `acquisition_missions` |
| Mission Reloaded | `mission_<uuid>` | AMO |
| Discovery Approved (command) | `mission_<uuid>` | AMO `executeMissionStage` |
| Discovery Lookup | `mission_<uuid>` | **SPEC-022 MissionStore / `missions`** |
| Discovery Execute | same string | throw `Unknown mission` |

The ID string is continuous. The first divergence is **Discovery Lookup**: same identifier, different identity space.

SPEC-022 IDs are minted as `msn_<timestamp36>_<rand>` (`MissionPlanner`). AMO IDs are `mission_<uuid>`. The operator error uses the AMO form.

---

## 10. Cache / Reload

Immediately before the throw, Discovery has already run `runDiscoveryPipeline` against the AMO mission object in memory. The failing read is not hydrate, session, or AMO snapshot.

| Source | Used by Discovery specialist sync? | Authoritative for AMO? |
|---|---|---|
| AMO engine memory | TME preconditions only | Runtime working set |
| Workspace hydration cache (`AmoWorkspaceHydration`) | No (already resolved) | No |
| Session context (`missionId` / `acquisitionMissionId`) | Binding only | No |
| Persisted AMO snapshot (`acquisition_missions`) | Not consulted for this throw | **Yes (ADR-075)** |
| AMO runtime singleton | Yes for TME | Working set |
| New AMO engine instance | No | — |
| SPEC-022 MissionEngine / `missions` | **Yes — `store.update`** | **No, for this mission** |

Authoritative store for the created mission: `persistStageCommit` → `acquisition_missions` (ADR-075).

Store that Discovery sync treats as authoritative: SPEC-022 `MissionEngine.store`.

---

## 11. First Divergence

Stop here.

| Field | Value |
|---|---|
| **File** | `packages/max/workspace/AmoOperatorApproval.js` → `packages/scout/Discovery.js` → `packages/mission-engine/MissionStore.js` (prod: `PostgresMissionStore.js`) |
| **Function** | `runScoutForAmoMission` binds AMO mission to SPEC-022 `missionEngine`; `syncMissionFromPipeline` looks up that ID; `store.get` / `store.update` is the first `null` / `Unknown mission` |
| **Line** | AmoOperatorApproval.js:506–508; Discovery.js:173 and 261–267; MissionStore.js:34–45 (PostgresMissionStore.js:117–130) |
| **Expected** | Discovery executes against the Acquisition Mission created and plan-approved in the AMO engine / `acquisition_missions` |
| **Actual** | After AMO TME lookup succeeds, Scout Discovery updates SPEC-022 `missions` by AMO id and throws `Unknown mission: mission_<uuid>` |
| **Reason** | Two mission identity spaces (SPEC-118 AMO vs SPEC-022 Mission Engine) share a string id at the Scout boundary. Creation never inserts into SPEC-022. Discovery’s first SPEC-022 get returns `null`. |

Earlier stages (create, persist, plan approval, TME preconditions) keep the same AMO id in the AMO store. Identity is not lost there.

---

## Root Cause

The first architectural divergence responsible for:

```text
Unknown mission:
mission_c1b6003f-8425-4939-b421-1350f5bd3790
```

is **Discovery specialist execution treating a SPEC-118 Acquisition Mission identifier as a SPEC-022 Mission Engine identifier**.

`runScoutForAmoMission` passes the AMO mission object and the workspace SPEC-022 `missionEngine` into `Scout.discover`. `syncMissionFromPipeline` then calls `missionEngine.store.update({ id: mission.id })`. That store’s `get` returns `null`. `update` throws `Unknown mission`.

No later stage is required to explain the operator result.

This audit does not propose a fix.
