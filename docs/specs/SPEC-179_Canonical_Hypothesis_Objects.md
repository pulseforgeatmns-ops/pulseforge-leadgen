# SPEC-179 — Canonical Hypothesis Objects

| Field | Value |
|---|---|
| **Status** | Draft |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Epic** | [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |
| **Depends on** | [SPEC-178](SPEC-178_Canonical_Market_Definition.md), [SPEC-142](SPEC-142_Evidence_Driven_Investigation_Engine.md), [SPEC-158](SPEC-158_Market_Definition_Hypothesis_Engine.md) |
| **Supersedes** | Parallel hypothesis engines |

## Objective

One hypothesis engine produces typed hypothesis nodes from a canonical MarketDefinition. Business hypotheses, terminology hypotheses, and search strategies are facets of a single engine — not separate orchestrators.

## Problem

Three hypothesis systems coexist:

| Engine | File | Thinks In Terms Of |
|---|---|---|
| Business hypotheses | `investigation/HypothesisGeneration.js` | ICP evidence gaps |
| Terminology hypotheses | `investigation/SearchHypothesisEngine.js` | Market self-description |
| Search strategies | `hypothesis/MarketHypothesisRegistry.js` | Static vertical query templates |

`CandidateUniverse` selects between SPEC-177 (business) and SPEC-158 (terminology) via feature flags — duplicate cognition.

## Decision

Introduce `CanonicalHypothesisEngine` that emits a unified hypothesis set:

```js
interface CanonicalHypothesis {
  id: string
  kind: 'business' | 'terminology' | 'search_strategy'
  text: string
  status: 'open' | 'confirmed' | 'rejected' | 'inconclusive'
  requiredEvidence: string[]      // business kind
  searchTerms: string[]           // terminology / search_strategy kind
  gap: string | null              // business kind — links to evidence gap
  rationale: string
  parentId: string | null
  confidence: number | null
}
```

## Module

| File | Role |
|---|---|
| `packages/scout/hypothesis/CanonicalHypothesisEngine.js` | Unified hypothesis generation |
| `packages/scout/hypothesis/index.js` | Export canonical engine |

## API

```js
generateCanonicalHypotheses(marketDefinition, opts?) → {
  hypotheses: CanonicalHypothesis[]
  business: CanonicalHypothesis[]
  terminology: CanonicalHypothesis[]
  searchStrategies: CanonicalHypothesis[]
}
```

Implementation delegates to existing engines internally:

- `generateHypotheses()` for business kind
- `generateInitialSearchHypotheses()` for terminology kind
- `MarketHypothesisRegistry.getSearchQueries()` for search_strategy kind

## Invariants

1. Hypothesis generation never inspects provider availability.
2. All hypothesis kinds share the same `CanonicalHypothesis` shape.
3. Investigation planner consumes the unified set — no kind-specific orchestrators.
4. `executeHypothesisDrivenCoverage` (SPEC-158 orchestrator) is not invoked as a parallel branch; terminology hypotheses feed the canonical engine.

## Migration

1. Create `CanonicalHypothesisEngine.js` wrapping existing generators.
2. Update `HypothesisInvestigationPlanner` to call `generateCanonicalHypotheses`.
3. Remove `CandidateUniverse` branch selecting SPEC-158 orchestrator.
4. Preserve direct exports of legacy generators for unit tests until Phase 4 retirement.

## Acceptance Criteria

- [ ] `CanonicalHypothesisEngine` produces business + terminology + search_strategy hypotheses
- [ ] `CandidateUniverse` no longer branches on `useHypothesisEngine` / `useHypothesisDiscoveryEngine`
- [ ] `HypothesisInvestigationPlanner` consumes canonical hypotheses
- [ ] Existing SPEC-142 and SPEC-158 unit tests pass (legacy generators still callable)
