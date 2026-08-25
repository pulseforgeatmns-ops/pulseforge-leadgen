# SPEC-178 — Canonical Market Definition

| Field | Value |
|---|---|
| **Status** | Draft |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Epic** | [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |
| **Depends on** | [SPEC-158](SPEC-158_Market_Definition_Hypothesis_Engine.md), [SPEC-141](SPEC-141_Scout_Intelligence_Pipeline.md) |
| **Supersedes** | Parallel market models in SPEC-141 intelligence pipeline |

## Objective

One canonical `MarketDefinition` object is the sole semantic input to Scout cognition. No entry point may construct ad-hoc search terms, segment lists, or geography scopes outside this model.

## Problem

Market semantics currently exist in multiple shapes:

| Source | Location | Used By |
|---|---|---|
| `MarketDefinition.js` semantic models | `packages/scout/intelligence/MarketDefinition.js` | DiscoveryPipeline, SPEC-177 |
| `MarketUnderstanding.js` facade | `packages/scout/intelligence/MarketUnderstanding.js` | Intelligence pipeline (tests) |
| `SearchDefinition` in scoutAcquisition | `packages/max/scoutAcquisition/` | CandidateUniverse, leadgen |
| `MarketHypothesisRegistry` static templates | `packages/scout/hypothesis/MarketHypothesisRegistry.js` | leadgen.js only |

Operator intent is converted differently depending on entry point.

## Decision

`MarketDefinition` from `packages/scout/intelligence/MarketDefinition.js` is canonical. All other representations are **projections** of this object, never alternate sources of truth.

## Canonical Schema

```js
interface MarketDefinition {
  market: string
  geography: { label, cities?, radius?, state? }
  customerTypes: string[]
  decisionMakers: string[]
  businessModels: string[]
  terminology: string[]        // market self-description terms
  searchConcepts: string[]     // derived search concepts
  adjacentMarkets: string[]
  exclusions: string[]
  buyingSignals: string[]
  expectedEvidence: string[]
  segmentKey: string           // resolved vertical key
}
```

## Invariants

1. **Mission → Market Definition** happens exactly once, at DiscoveryPipeline stage 1.
2. `SearchDefinition` is a **projection** for provider adapters — it never drives hypothesis generation directly.
3. `MarketHypothesisRegistry` query templates are derived from `MarketDefinition.segmentKey`, not the reverse.
4. Market Definition revision (SPEC-158 `reviseMarketDefinition`) updates the canonical object in-place; projections are regenerated.

## Modules

| Module | Role | Change |
|---|---|---|
| `intelligence/MarketDefinition.js` | Canonical builder + reviser | No change — already canonical |
| `intelligence/MarketUnderstanding.js` | Facade | Delegate exclusively to MarketDefinition |
| `max/scoutAcquisition/SearchDefinition.js` | Adapter projection | Add `fromMarketDefinition(md)` factory |
| `hypothesis/MarketHypothesisRegistry.js` | Search strategy lookup | Read segmentKey from MarketDefinition only |

## Migration

1. Add `buildSearchDefinitionFromMarketDefinition(md, opts)` in scoutAcquisition.
2. DiscoveryPipeline stage 1 output becomes mandatory input to all downstream stages.
3. leadgen.js migration (Phase 3): build MarketDefinition from client config before any search.

## Acceptance Criteria

- [ ] Single `MarketDefinition` builder used by DiscoveryPipeline and CandidateUniverse
- [ ] No code path generates search segments without a MarketDefinition ancestor
- [ ] `MarketUnderstanding` delegates to `MarketDefinition` with no parallel logic
- [ ] Tests verify SearchDefinition is always a projection, never a source of hypotheses
