# SPEC-170 — Mission Runtime Ownership Boundaries

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Max |
| **Created** | 2026-08-25 |
| **Depends on** | [SPEC-118](SPEC-118_Acquisition_Mission_Orchestration.md), [SPEC-131](SPEC-131_Transactional_Mission_Execution.md), [SPEC-123](SPEC-123_Unified_Scout_Discovery_Pipeline.md) |
| **ADR** | [ADR-089](../adr/ADR-089_Mission_Ownership_Shall_Not_Cross_Runtime_Boundaries.md) |

## Objective

Enforce that specialists execute only within the mission runtime that owns the mission. Prevent Acquisition Mission identifiers from being resolved or persisted through the SPEC-022 Mission Engine.

## Problem

`Scout.discover()` accepted an optional `missionEngine` and called `missionEngine.store.update()` using the AMO mission id. When AMO discovery ran inside TME, the Mission Engine store did not contain that id and threw `Unknown mission: mission_<uuid>`.

## Architecture

Introduce `MissionRuntimeOwnership`:

```text
resolveMissionRuntimeOwner(mission)
        ↓
resolveScoutDiscoveryRuntimePolicy({ mission, missionEngine, opts })
        ↓
AMO-owned? → syncToMissionEngine = false, attachViaLegacyFacade = false
Mission Engine-owned? → syncToMissionEngine = true (existing path)
```

## Runtime Owners

| Owner | Id prefix | Store | Discovery commit path |
|---|---|---|---|
| `amo` | `mission_` | AMO store + TME | `advanceDiscoveryAfterApproval` → `executeMissionStage` |
| `mission_engine` | `msn_` | Mission Engine store | `ScoutDiscoveryExecutor` → `Scout.discover` + sync |

## Scout.discover Contract

When `runtimeOwner === 'amo'` or the mission record is AMO-owned:

1. `missionEngine` is ignored for persistence (guard throws if supplied)
2. `attachScoutDiscovery` is disabled — TME owns the contribution commit
3. Discovery payload returns to the caller for TME commit

When Mission Engine owns the mission:

1. Existing `syncMissionFromPipeline` behavior is unchanged
2. Optional legacy `attachScoutDiscovery` bridge remains available

## Error Contract

Cross-runtime resolution fails closed:

```text
code: MISSION_RUNTIME_BOUNDARY_VIOLATION
message: Cannot sync Scout discovery into Mission Engine: Acquisition Mission mission_<uuid> must not be resolved through Mission Engine.
```

## Implementation

| Component | Change |
|---|---|
| `MissionRuntimeOwnership.js` | Owner detection, sync policy, boundary guard |
| `packages/scout/Discovery.js` | Apply runtime policy before Mission Engine sync |
| `AmoOperatorApproval.js` | `runScoutForAmoMission` never passes `missionEngine` |
| `AcquisitionOwnership.js` | Session/audit owner label `AMO` (not `MissionEngine`) |

## Verification

`packages/acquisition-mission/tests/spec170MissionRuntimeOwnership.test.js`:

- AMO id/record detection
- Scout.discover completes without Mission Engine for AMO missions
- Scout.discover rejects cross-runtime sync when Mission Engine is supplied
- `advanceDiscoveryAfterApproval` succeeds with legacy Mission Engine present but unused

## Acceptance Criteria

- [x] AMO discovery never calls `missionEngine.store.update` for AMO ids
- [x] Cross-runtime sync attempts throw `MISSION_RUNTIME_BOUNDARY_VIOLATION`
- [x] TME remains the sole AMO discovery commit path
- [x] Mission Engine discovery path unchanged for `msn_` missions
