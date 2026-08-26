# ADR-095 — Single Investigation Planner

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-26 |
| **Spec** | [SPEC-180](../specs/SPEC-180_Single_Investigation_Planner.md) |
| **Related** | [ADR-094](ADR-094_Canonical_Hypothesis_Objects.md), [ADR-064](../architecture/ADR-079_Understanding_Before_Recommendation.md), [SPEC-177](../specs/SPEC-177_Hypothesis_Driven_Discovery_Engine.md), [SPEC-145](../specs/SPEC-145_Adaptive_Investigation_Planning.md), [AUDIT-058](../architecture/AUDIT-058_Scout_Cognitive_Unification_Audit.md), [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |

## Context

AUDIT-058 identified two parallel investigation planners:

| Planner | Version | Output | Consumer |
|---|---|---|---|
| `InvestigationPlanBuilder` | SPEC-145 | `providerSequence[]` ordered by cost/gain | `DiscoveryPipeline` stage 3, `InvestigationLoop` |
| `HypothesisInvestigationPlanner` | SPEC-177/180 | `tasks[]` with evidence types + provider assignments | `HypothesisDrivenDiscoveryEngine` |

Both called hypothesis generation independently. `DiscoveryPipeline` built a SPEC-145 plan in stage 3, then `HypothesisDrivenDiscoveryEngine` created a separate SPEC-180 plan during execution. Two plans, two task graphs, two investigation states for the same mission.

## Decision

**One canonical investigation planner:** `HypothesisInvestigationPlanner` (`packages/scout/coverage/HypothesisInvestigationPlanner.js`).

### Canonical plan (SPEC-180)

```text
CanonicalHypotheses (SPEC-179)
  → InvestigativeQuestions
  → EvidenceRequirements
  → ProviderAssignments (evidence-native, not keyword-native)
  → InvestigationTasks (phased: identity → enrichment)
  → InvestigationPlan (version: SPEC-180)
```

### Compatibility adapter (SPEC-145)

`InvestigationPlanBuilder` delegates to the canonical planner and projects legacy fields (`providerSequence`, `evidenceRequired`, `stoppingConditions`) for consumers not yet migrated (e.g. `InvestigationLoop` tests). It does **not** generate hypotheses independently.

### Pipeline wiring

`DiscoveryPipeline` stage 3 creates the canonical plan once via `createHypothesisInvestigationPlan()`. The same plan object is passed to `runScoutAcquisitionIntelligence` → `constructCandidateUniverse` → `runHypothesisDrivenDiscovery`. No duplicate plan creation during execution.

## Invariants

1. Exactly one investigation plan per mission-path discovery run.
2. Exactly one task graph (`tasks[]` on the canonical plan).
3. Exactly one investigation state (owned by `HypothesisDrivenDiscoveryEngine`).
4. Providers are assigned to evidence requirements — never to search keywords.
5. Identity evidence completes before enrichment tasks.

## Consequences

### Positive

- Eliminates dual-plan divergence between pipeline planning and discovery execution.
- All mission-path investigations originate from canonical hypotheses (SPEC-179).
- Operator explainability (`buildOperatorExplanations`) applies uniformly.
- Clear migration path: legacy consumers read projected fields until fully migrated.

### Negative / deferred

- `InvestigationLoop` (test-only path) still consumes projected `providerSequence`; full migration to task-based execution is a follow-up.
- Provider learning from SPEC-143 influences legacy projection ordering only; canonical planner learning integration is deferred.

## Implementation

| File | Change |
|---|---|
| `packages/scout/coverage/HypothesisInvestigationPlanner.js` | Canonical planner; version `SPEC-180` |
| `packages/scout/investigation/InvestigationPlanBuilder.js` | Thin adapter delegating to canonical planner |
| `packages/scout/DiscoveryPipeline.js` | Stage 3 uses `createHypothesisInvestigationPlan`; passes plan to execution |
| `packages/max/scoutAcquisition/ScoutAdapter.js` | Forwards `investigationPlan` to `constructCandidateUniverse` |
| `packages/max/scoutAcquisition/CandidateUniverse.js` | Passes pre-built plan to `runHypothesisDrivenDiscovery` |
