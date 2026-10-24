# ADR-098 — Cognitive Explainability

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-26 |
| **Spec** | [SPEC-183](../specs/SPEC-183_Cognitive_Explainability.md) |
| **Related** | [ADR-079](ADR-079_Understanding_Before_Recommendation.md), [ADR-097](ADR-097_Provider_Capability_Architecture.md), [ADR-095](ADR-095_Single_Investigation_Planner.md), [ADR-094](ADR-094_Canonical_Hypothesis_Engine.md), [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |
| **Depends on** | [SPEC-182](../specs/SPEC-182_Provider_Capability_Architecture.md) |

## Context

Scout recommendations were explainable only through fragmented structures:

| Structure | Scope |
|---|---|
| `InvestigationGraph` | Evidence nodes (SPEC-142) |
| `InvestigationTree` | Branch lineage (SPEC-158) |
| `MemoryGraph` | Market memory (SPEC-143) |
| `max/scoutAcquisition/Explainability.js` | Acquisition provenance (SPEC-100) |

Operators and Max could not reliably answer *why this recommendation*, *why this provider*, or *why not another company* from a single trace. Recommendations could appear to terminate at providers (Google Places, LinkedIn, Website) instead of mission objective.

SPEC-182 unified provider capabilities. SPEC-183 completes the cognitive layer: one business reasoning engine with one explainability graph.

## Decision

**One `ExplainabilityGraph` traces every recommendation through the full cognitive chain.**

```
Mission Objective
  ↓
Market Definition
  ↓
Hypotheses[]
  ↓
Investigation Plan (evidence requirements + provider assignments)
  ↓
Evidence[]
  ↓
Understanding (synthesized)
  ↓
Judgment (heuristics + sufficiency)
  ↓
Recommendation
```

### Graph node contract

Each node records:

- `source` — originating engine (mission, hypothesis engine, planner, synthesis, heuristics)
- `reasoning` / `rationale` — deterministic explanation text
- `confidence` — when applicable
- `supportingEvidence` / `contradictoryEvidence` — structured refs
- `parentIds` / `parentReasoningNode` — upward chain

### API surfaces

| Function | Audience |
|---|---|
| `buildExplainabilityGraph()` | Discovery pipeline (internal) |
| `serializeForOperator()` | Max / dashboard — full chain |
| `serializeForAmo()` | AMO boundary — SPEC-173 safe projection |
| `traceRecommendation()` | Inspection / operator Q&A |
| `answerOperatorQuestion()` | Natural-language graph traversal |

### Boundary rules

1. **Providers are implementation details.** Provider assignments appear under plan/evidence nodes — never as terminal parents of recommendations.
2. **No duplicate reasoning.** Graph composes from `InvestigationState`, hypothesis plan, synthesis, and judgment — not ad-hoc artifact walks.
3. **Two serializers.** Internal replay uses full graph; AMO contributions use `serializeForAmo()` without forbidden reasoning keys (SPEC-173).

## Invariants

1. Every recommendation node has traceable ancestors to `objective`.
2. Recommendation parents directly to `judgment` — not evidence or provider.
3. Graph builds from `DiscoveryPipeline` output before artifact handoff.
4. `ScoutDiscoveryArtifact` projects `cognitiveTrace` from the graph.
5. Explainability is deterministic graph traversal — no LLM invention (ADR-002, ADR-005).

## Consequences

### Positive

- Operators can answer why-questions across the full chain.
- AMO boundary gains a stable cognitive trace without leaking internal hypothesis runtime.
- Provider capability rationale (SPEC-182) composes into plan nodes.
- EPIC-001 end-state: one business reasoning engine, no provider-driven cognition.

### Negative / deferred

- `InvestigationGraph` and `InvestigationTree` remain for internal branch tracking until Phase 4 deprecation.
- Cron `leadgen.js` path still bypasses cognitive stack — out of SPEC-183 scope.
- Regex fallbacks in legacy `Explainability.js` remain for runs without graph data.

## References

- `packages/scout/explainability/ExplainabilityGraph.js`
- `packages/scout/DiscoveryPipeline.js`
- `packages/scout/adapters/ScoutDiscoveryArtifact.js`
- `test/spec183CognitiveExplainability.test.js`
