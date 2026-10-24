# ADR-027 — Mission Planning Is Objective-Driven

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-041](../specs/SPEC-041_Mission_Planner.md) |
| **Supersedes** | Static `TYPE_CAPABILITY_CHAINS` as planning authority (SPEC-022 thin slice) |
| **Related** | [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [ADR-025](ADR-025_Active_Missions_Take_Precedence.md), [ADR-026](ADR-026_Business_Success_Determines_Pipeline_Progress.md), [SPEC-039](../specs/SPEC-039_Active_Mission_Resolver.md), [SPEC-040](../specs/SPEC-040_Mission_Artifact_Validation.md) |

## Context

Static capability chains couple business objectives to fixed mission types. This causes later-stage keywords (such as “review” or “mail package”) to collapse complex objectives into single-capability Missions.

Example: “Build Campaign 001… Generate intelligence… Review… Ready to Print…” matched `campaign_review` and discarded the remainder of the objective.

IntentRouter was doing planning work. MissionPlanner was a table lookup.

## Decision

1. **Mission planning is separated from intent routing.**
2. **IntentRouter** determines whether an operator request creates or resumes a Mission (and seeds a mission type). Broad build intents outrank later-stage keywords.
3. **Mission Planner** determines the **execution graph** required to achieve the Mission’s objective — selecting stages from a Stage Library, ordering by dependencies, inserting review gates, and validating the graph.
4. **Mission Executor** executes the graph produced by the planner. The planner never executes capabilities.
5. **Stage keywords augment** the graph; they never replace the seed pipeline.
6. **Operator modifications** trigger targeted replanning; already-completed valid stages remain intact unless marked stale.
7. Implementing contract: [SPEC-041 Mission Planner](../specs/SPEC-041_Mission_Planner.md).

## Consequences

### Positive

- Complex objectives become complete workflows instead of single-capability Missions
- New capabilities can be introduced by registering stage metadata rather than modifying router logic
- Operator modifications trigger targeted replanning instead of Mission recreation
- Mission execution becomes extensible, explainable, and resilient to workflow growth

### Negative / tradeoffs

- Planner must keep Stage Library metadata aligned with Capability Registry ids
- Focused single-stage missions still use mission-type seeds (IntentRouter remains useful for cold-start typing)
- Graph validation fail-closed may reject malformed operator inserts (intentional)

### Follow-ups

- [x] Implement SPEC-041 thin slice (Stage Library, Execution Graph, planner wiring, IntentRouter build-first)
- [x] Surface explanations in Mission Workspace / Max MissionResponse
- [ ] Durable planner audit tables (SPEC-032 Mission Memory)
- Update CURRENT_STATE when Mission Planner ships — done
