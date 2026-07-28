# SPEC-054 — Capability Registry & Planner Diagnostics

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Completed** | 2026-07-28 |
| **Depends on** | SPEC-023, SPEC-041, SPEC-050, SPEC-051, SPEC-052; ADR-011, ADR-027, ADR-034, ADR-035 |
| **ADR** | [ADR-038 Explain Planning Decisions](../adr/ADR-038_Explain_Planning_Decisions.md) |

## Objective

The Mission Planner shall never fail with bare "Unknown capability" or "Acquire via unavailable" without explaining exactly why and how to resolve it.

The planner reasons over **registered capabilities**, not arbitrary strings. Every planning failure is diagnosable by an operator in one screen.

## Vision References

- [ADR-038 Explain Planning Decisions](../adr/ADR-038_Explain_Planning_Decisions.md)
- [ADR-011 Capability Framework](../adr/ADR-011_Capability_Framework.md)
- [ADR-027 Mission Planning Is Objective-Driven](../adr/ADR-027_Mission_Planning_Is_Objective_Driven.md)
- [ADR-034 Intent Before Execution](../adr/ADR-034_Intent_Before_Execution.md)
- [ADR-035 Plan Around State, Not Sequence](../adr/ADR-035_Plan_Around_State_Not_Sequence.md)
- [SPEC-023 Capability Framework](SPEC-023_Capability_Framework.md)
- [SPEC-041 Mission Planner](SPEC-041_Mission_Planner.md)
- [SPEC-050 Deterministic Mission Planning](SPEC-050_Deterministic_Mission_Planning.md)
- [SPEC-051 Artifact Resolution & State-Aware Planning](SPEC-051_Artifact_Resolution_and_State_Aware_Planning.md)

## Problem

Current failures expose implementation details instead of actionable diagnostics:

```text
Unknown capability.
Acquire via unavailable.
```

These messages do not answer:

- What artifact is missing?
- Which capability should produce it?
- Is the capability missing, disabled, mis-registered, or version-mismatched?
- Is there no compatible producer?

## Design Principle

Every planning failure should be diagnosable by an operator in one screen.

## Architecture

```text
Mission
  ↓
Intent Parser
  ↓
Planner
  ↓
Capability Registry
  ↓
Compatibility Resolver
  ↓
Execution Graph
```

The planner never searches code. It queries the registry.

## Capability Contract (planner-facing)

Every capability declares:

| Field | Purpose |
|---|---|
| `id` | Stable capability id (`business_intelligence`) |
| `name` | Operator-facing name |
| `consumes` / `requires` | Artifact inputs |
| `produces` | Artifact outputs |
| `version` | Contract version (default `1`) |
| `enabled` | Planner may select only when `true` |
| `missionAliases` | NL phrases resolved to this capability |

The Capability Registry is the single source of truth for discovery, producer/consumer queries, and alias resolution.

## Planner Resolution

```text
Need: Campaign
  ↓
Registry: Who produces Campaign?
  ↓
Campaign Builder (ranked by Compatibility Resolver)
```

If multiple producers exist → rank by compatibility / acquisition cost.  
If none exist → emit a deterministic diagnostic (never a bare unavailable string).

## Diagnostic Model

Missing producer example:

| Field | Example |
|---|---|
| Artifact | `ExecutionPackage` |
| Status | `Blocked` |
| Expected Producer | Direct Mail Execution |
| Registered Producers | None |
| Possible Causes | not registered · disabled · version mismatch · artifact contract mismatch |
| Recommended Action | Register a capability that produces `ExecutionPackage`. |

Unknown mission text becomes a **Mission Segment** diagnostic with input, intent confidence, suggested matches — still stored as Notes (ADR-034); never invents a runtime node.

## Registry Queries

The planner (and tests) can answer:

- Who produces X?
- Who consumes X?
- Why wasn't capability Y selected?
- Why did capability Z lose ranking?

## Review Workspace

**Planning Diagnostics** section:

- ✓ selected capabilities
- ✗ missing / blocked producers
- Reason + Suggested Fix per failure

## Scope (v1)

- Extend capability descriptors: `version`, `enabled`, `missionAliases`
- Registry APIs: `producersOf`, `consumersOf`, `resolveAlias`, `suggestMatches`, selection explanations
- `CompatibilityResolver` — rank registry producers for required artifacts
- `PlanningDiagnostics` — deterministic diagnostic objects + operator summaries
- Wire Intent Parser / Mission Plan / Artifact Resolver / Mission Planner
- Command Deck **Planning Diagnostics** panel
- Tests: `npm run test:mission` · `npm run test:capabilities`

## Out of Scope

- Replacing Stage Library seeds entirely (seeds remain; registry is authoritative for producers/aliases/diagnostics)
- Live interactive capability registration UI
- Changing execute-time CapabilityRunner contract beyond richer missing-id errors
- Full cross-mission Mission Memory (SPEC-032)

## Acceptance Criteria

- [x] Every capability is discoverable through the registry
- [x] Planner resolves producers via registry Compatibility Resolver (not Stage Library alone)
- [x] Missing producers generate deterministic diagnostics
- [x] Unknown objectives produce suggested matches (Notes, not nodes)
- [x] Every planning failure includes a recommended action
- [x] No generic bare "Unknown capability" or "Acquire via unavailable" in planner/operator diagnostics

## Implementation Notes

| Area | Location |
|---|---|
| Registry queries | `packages/capabilities/CapabilityRegistry.js` |
| Aliases / contracts | `packages/capabilities/artifactContracts.js` |
| Compatibility Resolver | `packages/mission-engine/CompatibilityResolver.js` |
| Diagnostics | `packages/mission-engine/PlanningDiagnostics.js` |
| Artifact Resolver wiring | `packages/mission-engine/ArtifactResolver.js` |
| Intent / Mission Plan | `IntentParser.js`, `MissionPlan.js` |
| Planner attach | `MissionPlanner.js` |
| UI | `public/command-deck/command-deck.js` |
| Tests | `packages/mission-engine/tests/plannerDiagnostics.test.js` |
