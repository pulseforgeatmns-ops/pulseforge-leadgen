# SPEC-134 — Preserve AMO Presentation Contract

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-21 |
| **Depends on** | [SPEC-118](SPEC-118_Acquisition_Mission_Orchestration.md), [SPEC-129](SPEC-129_Mission_Runtime_Dispatch.md) |
| **Related** | AUDIT-015 AMO Presentation Regression |

## Objective

Once the AMO runtime owns a response, the response must remain in the AMO presentation contract. Ownership determines **who** owns the response. It does not determine **how** an AMO response is presented.

## Vision References

- [SPEC-118 Acquisition Mission Orchestration](SPEC-118_Acquisition_Mission_Orchestration.md)
- [ADR-055 Max Manages Missions](../adr/ADR-055_Max_Manages_Missions.md)

## Problem

After AMO mission selection, `buildOwnershipMissionResponse()` started from `buildAcquisitionMissionCommunication()` then overwrote AMO presentation with Mission Engine fields:

- Sources: `Mission Engine`, `Client Intelligence`
- Evidence Status: `✓ Blueprint attached`
- Operator Decision: `Continue in mission workspace?`

That discarded AMO sources (`acquisition_mission`, `scout`), Scout discovery artifacts, and AMO-stage operator decisions.

## Scope

- Preserve AMO presentation after create or resume in `buildOwnershipMissionResponse`.
- Keep sources, evidence status, operator decision, discovery artifact, and scout evidence on the AMO contract.
- Allow ownership metadata to be appended.
- Keep Blueprint as supporting evidence only.

## Out of Scope

- Changing workspace ownership routing (who owns the turn).
- Re-auditing AMO persistence, hydration, or Scout execution.
- SPEC-022 Mission Engine presentation for genuine legacy missions.

## Architecture

```text
AMO Mission
        ↓
buildAcquisitionMissionCommunication()
        ↓
applyAmoPresentationContract()   // AMO sources / evidence / decision / discovery
        ↓
AMO presentation + ownership metadata
        ↓
return
```

No Mission Engine presentation composer runs after AMO communication is built.

If `missionRuntime == AMO`, `buildOwnershipMissionResponse()` must preserve:

- sources
- evidenceStatus
- operatorDecision
- presentation
- discovery artifact
- scout evidence

## Implementation Plan

1. `applyAmoPresentationContract` in `packages/max/workspace/AcquisitionOwnership.js` fills AMO contract fields.
2. `buildOwnershipMissionResponse` returns that communication. Client Intelligence is `supportingEvidence` only.
3. Tests lock create, resume, and post-discovery resume against Mission Engine overwrite.

## Testing

- `packages/max/workspace/tests/spec134AmoPresentationContract.test.js`
- Existing SPEC-124 ownership tests continue to pass.

## Acceptance Criteria

After creating or resuming an AMO mission:

- [x] Sources are `acquisition_mission` and `scout`
- [x] Evidence Status reflects AMO execution (not Blueprint attached)
- [x] Discovery artifact is presented when Scout has contributed
- [x] Blueprint appears only as supporting evidence — not as the primary presentation
- [x] Operator Decision comes from the AMO stage
- [x] No Mission Engine presentation fields overwrite the AMO response
