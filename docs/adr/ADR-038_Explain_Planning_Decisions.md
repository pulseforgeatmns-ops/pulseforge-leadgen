# ADR-038 — Explain Planning Decisions

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-054](../specs/SPEC-054_Capability_Registry_and_Planner_Diagnostics.md) |
| **Related** | [ADR-011](ADR-011_Capability_Framework.md), [ADR-027](ADR-027_Mission_Planning_Is_Objective_Driven.md), [ADR-034](ADR-034_Intent_Before_Execution.md), [ADR-035](ADR-035_Plan_Around_State_Not_Sequence.md), [ADR-010](ADR-010_Mission_Engine.md) |

## Context

SPEC-050 and SPEC-051 made planning intent-driven and state-aware, but planning failures still surfaced thin messages (`Unknown capability`, `Acquire via unavailable` / empty Acquire-via labels). Operators could not tell whether a capability was missing, disabled, mis-aliased, or simply had no producer for a required artifact. The Capability Registry existed for execute-time resolution, while the planner still leaned on Stage Library string matching for discovery.

A planner should not merely fail. It should expose its reasoning.

## Decision

1. **The Mission Planner explains why capabilities were selected, rejected, or unavailable** using deterministic diagnostics derived from the Capability Registry.
2. **The Capability Registry is the single source of truth** for producer/consumer queries, mission aliases, enabled/version contract fields, and suggested matches.
3. **A Compatibility Resolver ranks registered producers** for required artifacts; when none are viable, it emits a structured diagnostic (artifact, status, expected producer, registered producers, possible causes, recommended action).
4. **Unknown mission text remains Notes** (ADR-034) but carries suggested registry matches — never invents runtime nodes and never uses bare "Unknown capability" as the only operator message.
5. **Review Workspace surfaces a Planning Diagnostics section** so an operator can diagnose a planning failure on one screen.
6. Implementing contract: [SPEC-054 Capability Registry & Planner Diagnostics](../specs/SPEC-054_Capability_Registry_and_Planner_Diagnostics.md).

## Consequences

### Positive

- Operators get actionable fixes instead of opaque failures
- Planner reasoning is inspectable (selected ✓ / missing ✗ / ranking losses)
- Registry queries answer "who produces/consumes X?" without code search
- Aligns planning discovery with the execute-time Capability Registry (ADR-011)

### Negative / tradeoffs

- Stage Library seeds and outcomePatterns remain for pipeline composition; full deprecation is a follow-up
- Diagnostics add payload size on mission plans (acceptable for operator UX)
- Execute-time runner still throws on missing ids (planner should have caught earlier); message is richer but still an Error

### Follow-ups

- [x] Registry contract fields + query APIs (SPEC-054 v1)
- [x] CompatibilityResolver + PlanningDiagnostics
- [x] Command Deck Planning Diagnostics panel
- [ ] Interactive capability registration from diagnostics
- [ ] Retire Stage Library as alias/producer authority once seeds are pure goal-solved
