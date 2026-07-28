# SPEC-051 — Artifact Resolution & State-Aware Planning

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Completed** | 2026-07-28 |
| **Depends on** | SPEC-041, SPEC-042, SPEC-043, SPEC-050, SPEC-023, SPEC-022; ADR-027, ADR-028, ADR-029, ADR-034 |
| **ADR** | [ADR-035 Plan Around State, Not Sequence](../adr/ADR-035_Plan_Around_State_Not_Sequence.md) |

## Objective

Mission Planning shall resolve required artifacts before selecting capabilities.

Capabilities are implementation strategies. Artifacts are execution requirements. The planner shall reason about **what is needed**, not immediately **how to obtain it**.

## Vision References

- [ADR-035 Plan Around State, Not Sequence](../adr/ADR-035_Plan_Around_State_Not_Sequence.md)
- [ADR-034 Intent Before Execution](../adr/ADR-034_Intent_Before_Execution.md)
- [ADR-027 Mission Planning Is Objective-Driven](../adr/ADR-027_Mission_Planning_Is_Objective_Driven.md)
- [ADR-028 Business State Flows Through Artifacts](../adr/ADR-028_Business_State_Flows_Through_Artifacts.md)
- [SPEC-041 Mission Planner](SPEC-041_Mission_Planner.md)
- [SPEC-042 Mission Artifact Bus](SPEC-042_Mission_Artifact_Bus.md)
- [SPEC-043 Operator Artifact Injection](SPEC-043_Operator_Artifact_Injection.md)
- [SPEC-050 Deterministic Mission Planning](SPEC-050_Deterministic_Mission_Planning.md)
- `docs/vision/Mission.md`

## Problem

Current planning assumes a fixed execution sequence:

```text
Campaign → Discovery → ProspectList → Campaign Builder
```

This causes unnecessary work. If a valid ProspectList already exists, Discovery should never execute. The planner should first determine whether the required artifact already exists.

## Design Principle

Mission execution is state-aware. Capabilities satisfy artifact requirements. They are not requirements themselves.

### Planning Model

**Current**

```text
Objective → Capabilities → Artifacts
```

**New**

```text
Objective → Required Artifacts → Artifact Resolution → Capability Selection → Execution
```

## Scope (v1)

- `packages/mission-engine/ArtifactResolver.js` — resolve required artifacts from catalog + Mission Plan parameters before capability selection
- Stages / capabilities declare `consumes`/`produces` (`requires`/`produces` aliases on capability descriptors)
- `ExecutionGraph.createExecutionGraph` runs Artifact Resolver and prunes acquisition stages whose outputs are already satisfied
- Discovery is an acquisition strategy for ProspectList — skipped when a compatible ProspectList is resolved
- Acquisition cost ordering: Current Mission → Operator Import → Previous Mission → Workspace → Capability
- Plan / Review Workspace surfaces resolved artifacts, acquisition decisions, and skip reasons
- Tests: `npm run test:mission` (`artifactResolution.test.js`)

## Out of Scope

- Full cross-mission durable artifact catalog (SPEC-032 Mission Memory)
- Interactive operator picker UI beyond display + continue/acquire messaging
- Replacing TYPE_SEED_STAGES entirely with a pure goal solver (seeds remain; resolver prunes)
- Live Company Intelligence / Campaign Builder (still stubs)
- Changing Artifact Bus consumption semantics (ADR-029 still holds)

## Architecture

```text
Mission Plan
      │
      ▼
Artifact Resolver
      │
      ▼
Execution Graph (required capabilities only)
```

### Resolution Strategy

For every required artifact:

```text
Artifact Required → Exists?
  YES → Use Existing Artifact
  NO  → Acquire Artifact → Continue
```

### Artifact Sources (priority)

1. Current Mission — already produced during execution / bus snapshot
2. Operator Import — CSV, spreadsheet, manual, uploaded, Mission Plan `prospectList` parameter
3. Previous Mission — previously generated, if compatible
4. Persistent Workspace — workspace memory / cached artifacts
5. Capability Acquisition — Discovery, enrichment, research — only if missing

### Conflict Resolution

When multiple artifacts satisfy a requirement, choose by:

1. Current Mission
2. Operator Explicit Selection
3. Highest Confidence
4. Most Recent
5. Compatible Schema

### Artifact Confidence Record

Every resolved artifact includes:

| Field | Example |
|---|---|
| type | ProspectList |
| source | Operator Import |
| confidence | High |
| freshness | Current Mission |
| compatible | true |

## Data Model

No new tables in v1. Resolution is recorded on the Mission plan:

```js
plan.artifactResolution = {
  required: ['ProspectList', ...],
  resolved: [{ type, source, confidence, freshness, compatible, pending? }],
  acquisitions: [{ artifactType, strategy, stageId?, reason }],
  skippedStages: { prospect_discovery: 'Compatible ProspectList already exists' },
}
```

Available candidates may be passed at plan time via `availableArtifacts` / Artifact Bus snapshot.

## Implementation Plan

1. ArtifactResolver + source priority + cost ranking
2. Wire into `createExecutionGraph` / `MissionPlanner.plan`
3. Capability `requires` / `produces` optional contracts (Stage Library remains planning authority)
4. Review Workspace panel for resolution decisions
5. Tests for ProspectList-present → Discovery skipped

## Migration Strategy

Backward compatible. Missions without `artifactResolution` plan as before (Discovery still seeded unless parameters / catalog satisfy ProspectList). No DB migration.

## Testing

- Unit: resolver source priority, conflict choice, acquisition cost
- Integration: campaign objective with `prospectList: current` → Discovery absent from execution graph
- Regression: campaign without existing ProspectList still includes Discovery
- `npm run test:mission`

## Acceptance Criteria

- [x] Mission Planning resolves artifact requirements before finalizing capability selection
- [x] Capabilities / stages declare required and produced artifacts
- [x] Existing compatible artifacts skip redundant capability execution (Discovery)
- [x] Discovery is an acquisition strategy rather than a mandatory stage
- [x] Operators can inspect artifact resolution decisions (plan + Review Workspace)
- [x] Execution graphs contain only required capabilities after resolution
- [x] Artifact source, confidence, freshness, and compatibility are recorded
- [x] Planner chooses the lowest-cost compatible acquisition strategy
- [x] Duplicate artifact generation is avoided when a compatible artifact exists

## Future Work

- Operator confirm/deny of resolved artifacts before run
- Multi-artifact inject (Ranking, Campaign) at plan time
- SPEC-032 durable catalog as first-class Previous Mission / Workspace source
- Pure goal → required-artifact solver without TYPE_SEED_STAGES
