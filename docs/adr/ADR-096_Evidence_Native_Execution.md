# ADR-096 — Evidence-Native Execution

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-26 |
| **Spec** | [SPEC-181](../specs/SPEC-181_Evidence_Native_Execution.md) |
| **Related** | [ADR-095](ADR-095_Single_Investigation_Planner.md), [ADR-092](ADR-092_Identity_Before_Enrichment.md), [SPEC-180](../specs/SPEC-180_Single_Investigation_Planner.md), [SPEC-177](../specs/SPEC-177_Hypothesis_Driven_Discovery_Engine.md), [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |

## Context

SPEC-180 unified investigation planning: plans are evidence-first. Tasks carry `evidenceType` and provider assignments — not search keywords.

Execution still converted evidence needs back into keyword search at the adapter boundary:

```text
Evidence Need → segment terminology → "Property Manager Bedford NH" → PlacesProvider.search()
```

This violated the planning invariant. Provider deletion (e.g. removing Google Places) would require planner changes because providers *were* the reasoning on the cron path.

## Decision

**Execution remains evidence-native.** Providers receive evidence requirements; providers determine search implementation.

### Provider request contract (SPEC-181)

```json
{
  "segment": "short_term_rental",
  "evidenceType": "identity",
  "geography": {
    "cities": ["Manchester", "Bedford", "Hooksett"],
    "state": "NH"
  }
}
```

### Provider responsibility

| Concern | Owner |
|---|---|
| Query generation | Provider (e.g. Google Places) |
| Pagination | Provider |
| Retries | Provider |
| Localization | Provider |
| Search string emission | **Never Scout** |

### Execution bridge

```text
InvestigationPlan.tasks[]
  → buildEvidenceRequest()           (EvidenceRequest.js)
  → adapter.discover({ evidenceRequest })
  → provider.collectEvidence(request)  (PlacesProvider, etc.)
  → ProviderEvidenceContract
  → EvidenceFusion + Identity Resolution
```

## Invariants

1. Scout never emits search strings on the mission path.
2. `scopedSearchForTask` produces `evidenceRequest`, not segment-derived query payloads.
3. Deleting Google Places requires zero planner changes — only provider registry / adapter wiring.
4. Provider failures trigger `revisePlanForUnavailableProviders`, not alternate reasoning paths.
5. `executeCoveragePlan` remains available only when no `MarketDefinition` exists (pre-cognitive legacy).

## Consequences

### Positive

- Planning and execution share the same evidence vocabulary.
- Providers are swappable without touching investigation logic.
- Google Places query templates live in the provider layer (via market hypothesis registry), not in the engine.
- Identity collection accepts rows without websites (ADR-092) on the mission path.

### Negative / deferred

- `leadgen.js` cron migration to the evidence bridge is Phase 3 (separate PR per SPEC-181).
- Legacy `PlacesProvider.search({ industry, location })` retained for backward compatibility until cron migration completes.

## Implementation

| File | Change |
|---|---|
| `packages/scout/coverage/EvidenceRequest.js` | Evidence-native provider request contract |
| `packages/scout/coverage/HypothesisDrivenDiscoveryEngine.js` | `scopeSearchDefinitionForTask` replaces segment keyword scoping |
| `packages/max/scoutAcquisition/DiscoveryAdapters.js` | Places adapter dispatches `evidenceRequest` to provider |
| `packages/capabilities/discovery/providers/PlacesProvider.js` | `collectEvidence()` owns query generation |
