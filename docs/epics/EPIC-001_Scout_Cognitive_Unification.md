# EPIC-001 — Scout Cognitive Unification

| Field | Value |
|---|---|
| **Status** | In Progress |
| **Priority** | Critical (Pilot 0 Blocker) |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Audit** | [AUDIT-058](../architecture/AUDIT-058_Scout_Cognitive_Unification_Audit.md) |

## Vision

Scout is no longer a search orchestrator. Scout is an **investigative intelligence system**. Every investigation shall follow one canonical cognitive pipeline.

## Motivation

Over several development cycles Scout evolved through multiple architectural generations:

```
Search → Evidence → Understanding
Hypothesis → Investigation → Evidence → Understanding → Judgment
```

[AUDIT-058](../architecture/AUDIT-058_Scout_Cognitive_Unification_Audit.md) demonstrated the architecture exists, but several legacy reasoning paths still survive. The remaining work is not adding intelligence — it is **eliminating duplicate cognition**.

## Objectives

When this Epic is complete:

| # | Invariant | Spec |
|---|---|---|
| 1 | One Market Definition | [SPEC-178](../specs/SPEC-178_Canonical_Market_Definition.md) |
| 2 | One Hypothesis Engine | [SPEC-179](../specs/SPEC-179_Canonical_Hypothesis_Objects.md) |
| 3 | One Investigation Planner | [SPEC-180](../specs/SPEC-180_Single_Investigation_Planner.md) |
| 4 | One Evidence Pipeline | [SPEC-181](../specs/SPEC-181_Evidence_Native_Execution.md) |
| 5 | One Provider Capability Planner | [SPEC-182](../specs/SPEC-182_Provider_Capability_Architecture.md) |
| 6 | One Execution Bridge | [SPEC-181](../specs/SPEC-181_Evidence_Native_Execution.md) |
| 7 | One Explainability Graph | [SPEC-183](../specs/SPEC-183_Cognitive_Explainability.md) |

Every recommendation shall trace:

```
Recommendation → Judgment → Understanding → Evidence → Hypothesis → Mission Objective
```

Never:

```
Recommendation → Google Places
```

## Deliverables

| Spec | Title | Status |
|---|---|---|
| SPEC-178 | Canonical Market Definition | Draft |
| SPEC-179 | Canonical Hypothesis Objects | Draft |
| SPEC-180 | Single Investigation Planner | Implemented |
| SPEC-181 | Evidence-Native Execution | Accepted |
| SPEC-182 | Provider Capability Architecture | Accepted |
| SPEC-183 | Cognitive Explainability | Accepted |

## Success Criteria

1. **Deleting a provider changes coverage. It never changes Scout's reasoning.**
2. Production cron Scout (`leadgen.js`) delegates search planning to the canonical pipeline.
3. No entry point bypasses `Scout.discover()` / `runDiscoveryPipeline()` for mission-scoped discovery.
4. Legacy orchestrators (`runIntelligencePipeline`, `investigate`, `executeCoveragePlan` as primary path) are retired or demoted to internal adapters.
5. Operator explainability answers trace the full cognitive chain.

## Implementation Phases

### Phase 0 — Governance (this PR)
- EPIC-001 charter, AUDIT-058, SPEC-178–183

### Phase 1 — Unify mission-path cognition
- Collapse `CandidateUniverse` discovery branches to `runHypothesisDrivenDiscovery`
- Introduce `CanonicalHypothesisEngine` merging business + terminology hypotheses
- Demote `InvestigationPlanBuilder` (SPEC-145) to delegate to `HypothesisInvestigationPlanner` (SPEC-177)

### Phase 2 — Explainability graph
- Consolidate `InvestigationGraph`, `InvestigationTree`, and `ScoutDiscoveryArtifact` into `ExplainabilityGraph`

### Phase 3 — Operational Scout migration
- Refactor `leadgen.js` to delegate to canonical pipeline (ADR-092 follow-up)
- Consolidate `scoutPublicSourcing.js` onto shared provider assignment

### Phase 4 — Legacy retirement
- Remove deprecated exports and parallel orchestrators
- Mark `ProspectDiscovery` capability deprecated

## Related Work

- SPEC-141 through SPEC-177 (cognitive building blocks — implemented)
- SPEC-172/173 (AMO evidence handoff boundary)
- ADR-092 (Identity Before Enrichment)
- AUDIT-006 (Scout Discovery Execution Audit)
