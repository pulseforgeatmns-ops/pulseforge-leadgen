# ADR-093 — Canonical Market Definition

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-178](../specs/SPEC-178_Canonical_Market_Definition.md) |
| **Related** | [ADR-087](ADR-087_Operator_Objective_Takes_Precedence.md), [ADR-092](ADR-092_Identity_Before_Enrichment.md), [AUDIT-058](../architecture/AUDIT-058_Scout_Cognitive_Unification_Audit.md), [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |

## Context

Multiple systems independently determined market semantics. Precedence was inconsistent:

- `mission.constraints.vertical` could override operator objective text
- `SearchDefinition` could inject default segments via `defaultSegmentsForNeed()`
- `ConceptLibrary.conceptsFromText()` duplicated regex inference
- `MarketDefinition.resolveSegmentKey()` checked supplied segments before objective text

AUDIT-058 demonstrated the failure mode:

```text
Acquire one recurring commercial cleaning client
from a short-term rental operator...
```

was classified as `property_management` instead of `short_term_rental` when a conflicting constraint was present.

## Decision

The operator's objective is **authoritative** for market segment resolution. Every downstream reasoning system derives from one canonical `MarketDefinition`. Mission constraints **refine** execution (e.g. `commercial_only`, geography) but **never redefine** the segment.

### Resolution order

```text
Operator Objective
        ↓
Mission Objective (locked plan segment)
        ↓
Evidence (dominant terminology / evidence-driven segment)
        ↓
Mission Constraints (vertical — only when higher layers are silent)
        ↓
Defaults (supplied segments, delegation hints)
```

### Projections

| Representation | Role |
|---|---|
| `MarketDefinition` (`packages/scout/intelligence/MarketDefinition.js`) | **Canonical** — sole semantic source |
| `SearchDefinition` | Adapter projection via `buildSearchDefinitionFromMarketDefinition()` |
| `MarketHypothesisRegistry` | Query templates keyed by `MarketDefinition.segmentKey` |
| `ConceptLibrary.expandConcepts()` | Terminology projection when `marketDefinition` is present |

## Invariants

1. The same mission input shall always resolve to the same `segmentKey`.
2. No downstream planner may override a resolved canonical segment.
3. `SearchDefinition.segments` must equal `MarketDefinition.segments` when projected.
4. `buildMarketDefinition()` builds semantic model first; search definition is derived, never the reverse.

## Removed patterns

- Constraint-first segment resolution
- Parallel segment inference across Scout entry points
- Multiple semantic models per mission (one builder, one object)

## Consequences

- STR vs property-management disambiguation follows operator wording, not stale constraints
- DiscoveryPipeline stage 1 output is mandatory input to all downstream stages
- `leadgen.js` migration (Phase 3) must build `MarketDefinition` before any search
- Mission Planner (SPEC-130) and Scout share `resolveCanonicalSegmentKey()` for consistent semantics

## Implementation

| Module | Change |
|---|---|
| `packages/scout/intelligence/MarketDefinition.js` | `resolveCanonicalSegmentKey()`, objective-first precedence |
| `packages/scout/intelligence/MarketUnderstanding.js` | Semantic model first; SearchDefinition as projection |
| `packages/max/scoutAcquisition/SearchDefinition.js` | `buildSearchDefinitionFromMarketDefinition()` |
| `packages/scout/Discovery.helpers.js` | Delegation segments from canonical resolver, not `constraints.vertical` alone |
| `test/scoutCanonicalMarketDefinition.test.js` | ADR-093 / SPEC-178 acceptance tests |
