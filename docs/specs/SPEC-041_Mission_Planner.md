# SPEC-041 — Mission Planner

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022 (Mission Engine), SPEC-023 (Capability Framework), SPEC-039 (Active Mission Resolver), SPEC-040 (Artifact Validation), ADR-010, ADR-011, ADR-025, ADR-026, ADR-027 |
| **Consumed by** | MissionExecutor, Active Mission Resolver, Max Mission Workspace, Command Deck Operations |

## Objective

Replace static mission-type capability chains with an **objective-driven planner** that constructs a complete execution pipeline.

The Mission Planner determines **how** to accomplish a business objective. It does **not** execute capabilities.

Success looks like: “Build Campaign 001… Generate intelligence… Review… Ready to Print…” → one Mission whose execution graph includes Discovery → Intelligence → Ranking → Campaign Builder → Campaign Review → Ready To Print — **not** a single-stage `campaign_review` Mission.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-027](../adr/ADR-027_Mission_Planning_Is_Objective_Driven.md) — mission planning is objective-driven
- [ADR-025](../adr/ADR-025_Active_Missions_Take_Precedence.md) — Active Mission Resolver before IntentRouter
- [ADR-026](../adr/ADR-026_Business_Success_Determines_Pipeline_Progress.md) — business artifact validation
- [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md) — Mission Engine thin slice
- [SPEC-039](SPEC-039_Active_Mission_Resolver.md) — resume / modify / diagnose
- [SPEC-040](SPEC-040_Mission_Artifact_Validation.md) — stage contracts / pipeline gate

## Problem

Current architecture:

```text
Operator Objective
        │
        ▼
IntentRouter
        │
        ▼
Mission Type
        │
        ▼
TYPE_CAPABILITY_CHAINS
```

This reduces an entire objective to a single mission type. Later-stage keywords (`review`, `mail package`) collapse complex objectives into single-capability Missions. The system was **classifying** objectives rather than **planning** them.

## Guiding Principle

```text
Operator Objective
        │
        ▼
IntentRouter          ← Is this a Mission?
        │
        ▼
Mission Created
        │
        ▼
Mission Planner       ← How do we accomplish it?
        │
        ▼
Execution Graph
        │
        ▼
Mission Executor
```

## Scope

### Responsibilities

Mission Planner shall:

- Parse business objectives
- Extract requested outcomes
- Select required pipeline stages
- Order stages via dependency resolution (never hardcoded order)
- Insert review gates
- Validate dependencies (cycles, missing, duplicates)
- Produce an execution graph + explanation
- Support incremental replanning after operator modifications

Mission Planner shall **never** execute capabilities.

### Stage Library

Planner selects from registered stages. Each stage advertises:

| Field | Purpose |
|---|---|
| Stage Name | Operator-facing label |
| Consumes | Input artifacts |
| Produces | Output artifacts |
| Dependencies | Upstream stage ids |
| Review Required | Planner-managed review gate |
| Priority | Tie-break among equal indegree peers |
| Outcome patterns | Objective keywords that **augment** selection |

### Stage Selection

- Seed stages from mission type (baseline pipeline)
- **Augment** with outcome keywords from the objective
- Keywords **compose** — they never replace the seed
- Review gates are inserted automatically when Ready To Print / Direct Mail require them

### Planner Outputs

Mission stores:

- Execution Plan (ordered executable steps)
- Execution Graph (nodes, edges, selected / skipped)
- Reasoning + `explainPlan` answers
- Review gates
- Planner version
- Replanning events (preserved / invalidated stages)

### API

| Function | Role |
|---|---|
| `createExecutionGraph(mission)` | Build graph from objective + type + extras |
| `replanGraph(mission)` / `planner.replan` | Incremental replan after modifications |
| `validateGraph(graph)` | Cycles, missing, duplicates, empty |
| `explainPlan(graph)` | Why included / skipped / review required |
| `insertStage` / `removeStage` / `replaceStage` | Graph surgery without recreating Mission |

### Mission Workspace

Surfaces: execution graph, current / completed / upcoming stages, review gates, dependencies, planner explanation.

## Out of Scope

- Executing capabilities (MissionExecutor)
- Changing Active Mission Resolver precedence (SPEC-039)
- Full Postgres schema for planner audit (v1: mission.plan + audit payloads)
- Auto-spawning follow-up Missions from outcomes (SPEC-029)

## Architecture

```text
IntentRouter.matchMissionType   → seed type only (build wins over stage keywords)
MissionPlanner.plan
  → createExecutionGraph
  → validateGraph (fail closed)
  → bind Discovery Profile / Playbook
  → materialize executable steps from registry
MissionExecutor.execute         → runs plan.steps only
ActiveMissionResolver._modify   → planner.replan (stale capabilities cascade)
```

### Integration points

| Component | Change |
|---|---|
| `StageLibrary.js` | Registered stages + seeds + composition edges |
| `ExecutionGraph.js` | Graph build / validate / explain / mutate |
| `MissionPlanner.js` | Replaces chain lookup with graph composition |
| `IntentRouter.js` | Build Campaign preferred over review/mail keywords |
| `MissionEngine.getWorkspace` | Exposes graph + explanation |
| `MissionResponse` | Surfaces planner pipeline in reasoning |

## Implementation Plan

1. ADR-027 Accepted; this spec Implemented thin slice; indexes updated.
2. Stage Library + Execution Graph APIs.
3. MissionPlanner.plan / replan wired; TYPE_CAPABILITY_CHAINS deprecated as seed mirror.
4. IntentRouter build-first ordering.
5. Workspace + Active Mission modify replanning.
6. Tests: `npm run test:mission` (missionPlanner.test.js).

## Migration Strategy

- Existing focused mission types (mail-only, review-only, proposal, inbox) keep single-stage seeds.
- Multi-outcome Build Campaign objectives gain augmented graphs without Mission recreation.
- `TYPE_CAPABILITY_CHAINS` retained as deprecated read-only mirror of seeds for compat tests.
- Rollback: not feature-flagged; revert planner commit restores chain lookup.

## Acceptance Criteria

- [x] Mission Planner replaces `TYPE_CAPABILITY_CHAINS` as the planning authority
- [x] Objectives generate execution graphs instead of mission-type-only chains
- [x] Stage keywords augment the graph instead of replacing it
- [x] Dependency validation prevents invalid graphs (mission creation fails closed)
- [x] Review gates are planner-managed
- [x] Graph supports incremental replanning after Mission modifications
- [x] Planner explanations are visible to operators (plan.explanation + workspace)

## Test Plan

```bash
npm run test:mission
```

- Multi-outcome Build Campaign → full composed pipeline
- Focused Review / Mail Package still single-stage
- explainPlan / validateGraph / insert·remove·replace
- replan invalidates stale capabilities
- IntentRouter: Build+Review → `campaign_creation`
