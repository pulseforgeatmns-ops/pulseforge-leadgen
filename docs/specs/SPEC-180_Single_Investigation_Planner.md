# SPEC-180 — Single Investigation Planner

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Epic** | [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |
| **Depends on** | [SPEC-179](SPEC-179_Canonical_Hypothesis_Objects.md), [SPEC-145](SPEC-145_Adaptive_Investigation_Planning.md), [SPEC-177](SPEC-177_Hypothesis_Driven_Discovery_Engine.md) |
| **Supersedes** | Dual planner paths (SPEC-145 vs SPEC-177) |

## Objective

One investigation planner converts canonical hypotheses into an executable investigation plan. Providers are assigned to evidence requirements — never to search keywords.

## Problem

Two planners produce different plan shapes:

| Planner | Version | Output Shape | Used By |
|---|---|---|---|
| `InvestigationPlanBuilder` | SPEC-145 | `providerSequence[]` ordered by cost | DiscoveryPipeline stage 3, InvestigationLoop |
| `HypothesisInvestigationPlanner` | SPEC-177 | `tasks[]` with evidence types + provider assignments | HypothesisDrivenDiscoveryEngine |

Both call `generateHypotheses()` independently. Plans are not interchangeable.

## Decision

`HypothesisInvestigationPlanner` (`packages/scout/coverage/HypothesisInvestigationPlanner.js`) is the **canonical planner**.

`InvestigationPlanBuilder` (SPEC-145) becomes a thin adapter that delegates to the canonical planner and projects output to the SPEC-145 shape for backward compatibility during migration.

## Canonical Plan Schema

```js
interface InvestigationPlan {
  version: 'SPEC-180'
  mission: object | null
  objective: string
  marketDefinition: MarketDefinition
  hypotheses: CanonicalHypothesis[]
  questions: InvestigativeQuestion[]
  evidenceRequirements: EvidenceRequirement[]
  assignedProviders: ProviderAssignment[]
  tasks: InvestigationTask[]
  satisfiedEvidence: string[]
  outstandingEvidence: string[]
  currentPhase: string
  sufficientlyInvestigated: boolean
  rationale: string
}
```

## Planner Invariants

1. Maximize uncertainty reduction per evidence task.
2. Avoid duplicate evidence collection.
3. Retry with alternative providers when evidence is insufficient.
4. Explain why each provider was selected (operator explainability).
5. Never emit provider-specific search strings as plan nodes.

## Migration

1. Update `HypothesisInvestigationPlanner` version tag to `SPEC-180`.
2. Wire `InvestigationPlanBuilder.buildInvestigationPlan()` to delegate internally.
3. DiscoveryPipeline stage 3 uses canonical planner exclusively.
4. Remove direct `InvestigationPlanBuilder` usage from `InvestigationLoop` after test migration.

## Acceptance Criteria

- [x] Single planner module produces all investigation plans in mission-path discovery
- [x] `InvestigationPlanBuilder` delegates to canonical planner (no independent hypothesis generation)
- [x] Plan version is `SPEC-180`
- [x] DiscoveryPipeline stage 3 and HypothesisDrivenDiscoveryEngine share the same plan builder
