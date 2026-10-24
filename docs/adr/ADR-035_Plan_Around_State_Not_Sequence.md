# ADR-035 — Plan Around State, Not Sequence

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-051](../specs/SPEC-051_Artifact_Resolution_and_State_Aware_Planning.md) |
| **Related** | [ADR-027](ADR-027_Mission_Planning_Is_Objective_Driven.md), [ADR-028](ADR-028_Business_State_Flows_Through_Artifacts.md), [ADR-029](ADR-029_Artifact_Provenance_Must_Not_Affect_Consumption.md), [ADR-034](ADR-034_Intent_Before_Execution.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md) |

## Context

SPEC-041 made planning objective-driven and SPEC-050 separated intent from execution, but the planner still composed fixed capability chains (e.g. always seed Discovery for campaign creation). Operator-supplied or already-present ProspectLists only skipped Discovery at **execute** time (SPEC-043 inject). That re-ran acquisition work and forced operators through “Discovery Failed → Import” instead of “ProspectList Required → use existing.”

Traditional workflow engines execute fixed sequences. Pulseforge executes goal-oriented missions.

## Decision

1. **Mission Planning constructs execution graphs from artifact requirements rather than predefined capability sequences.**
2. **Capabilities exist to satisfy missing state.** They are not the state itself.
3. **An Artifact Resolver sits between Mission Plan and the final execution graph.** It asks “what information do I need?” before “how should I obtain it?”
4. **Acquisition strategies are ordered by cost:** Current Mission → Operator Import → Previous Mission → Workspace → Capability Acquisition.
5. **Discovery is one acquisition strategy for ProspectList**, not a mandatory stage.
6. **Resolved artifacts record source, confidence, freshness, and compatibility** for operator inspection.
7. Implementing contract: [SPEC-051 Artifact Resolution & State-Aware Planning](../specs/SPEC-051_Artifact_Resolution_and_State_Aware_Planning.md).

## Consequences

### Positive

- Skips redundant Discovery / enrichment when compatible artifacts exist
- Shortens execution time and improves operator experience
- Multiple acquisition paths can satisfy the same business requirement
- Execution graphs shrink to required capabilities only
- Aligns planner with Artifact Bus as business-state source of truth (ADR-028)

### Negative / tradeoffs

- Seeds remain as a starting set; v1 prunes after selection rather than pure goal solving
- Cross-mission reuse depends on callers supplying `availableArtifacts` until SPEC-032
- Operator “current ProspectList” without a catalog payload marks a pending resolution (inject still required before execute)

### Follow-ups

- [x] ArtifactResolver + ExecutionGraph prune (SPEC-051 v1)
- [x] Review Workspace resolution panel
- [ ] Interactive approve/deny of resolved artifacts
- [ ] SPEC-032 durable Previous Mission / Workspace catalog
- [ ] Pure required-artifact solver without TYPE_SEED_STAGES
