# ADR-094 — Canonical Hypothesis Objects

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-26 |
| **Spec** | [SPEC-179](../specs/SPEC-179_Canonical_Hypothesis_Objects.md) |
| **Related** | [ADR-093](ADR-093_Canonical_Market_Definition.md), [ADR-092](ADR-092_Identity_Before_Enrichment.md), [AUDIT-058](../architecture/AUDIT-058_Scout_Cognitive_Unification_Audit.md), [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |

## Context

AUDIT-058 demonstrated that hypotheses lose semantic richness when flattened:

```text
gap → discarded
Every hypothesis becomes: business_exists, business_fit, buying_decisions
```

Three parallel hypothesis engines (business, terminology, search strategy) produced incompatible shapes. `CandidateUniverse` branched between SPEC-177 and SPEC-158 orchestrators. Business hypothesis templates carried `gap` metadata, but `generateHypotheses()` dropped it before investigation planning — causing every hypothesis to fall back to generic investigative questions.

## Decision

Hypotheses become **immutable reasoning objects** produced by one `CanonicalHypothesisEngine` from a canonical `MarketDefinition` (SPEC-178 / ADR-093).

### Canonical shape

Every hypothesis retains:

| Field | Purpose |
|---|---|
| `rationale` | Why this hypothesis exists |
| `confidence` | Current belief (null until evidence arrives) |
| `uncertainty` | Inverse of confidence; 1.0 when unknown |
| `gap` | Evidence gap this hypothesis resolves (business kind) |
| `requiredEvidence` | Evidence types needed to test the hypothesis |
| `supportingEvidence` | Observations that increase confidence |
| `contradictoryEvidence` | Observations that decrease confidence |
| `investigationStatus` | `pending` → `in_progress` → `complete` |
| `generatedQuestions` | Investigative questions derived from the gap |

Kinds: `business`, `terminology`, `search_strategy`. All share the same shape; kind-specific fields (`searchTerms` for terminology/search_strategy) are present but empty when not applicable.

### Investigation derivation

Questions derive from hypothesis gaps — not generic defaults:

```text
cleaning_responsibility → "Do they outsource cleaning?"
portfolio_size → "Does it manage STRs?"
```

Generic fallback (`business_exists`, `business_fit`, `buying_decisions`) applies **only when a hypothesis has no gap**.

### Engine delegation

`generateCanonicalHypotheses()` wraps existing generators without rewriting them:

- `generateHypotheses()` → business kind
- `generateInitialSearchHypotheses()` → terminology kind
- `MarketHypothesisRegistry.resolveMarketHypothesisBySegmentKey()` → search_strategy kind

Returned objects are frozen. Legacy generators remain exported for unit tests.

## Invariants

1. Hypothesis generation never inspects provider availability.
2. All hypothesis kinds share the same `CanonicalHypothesis` shape.
3. Investigation planner consumes the unified set — no kind-specific orchestrators.
4. `executeHypothesisDrivenCoverage` (SPEC-158 orchestrator) is not invoked as a parallel branch.
5. Every business hypothesis with a gap produces a unique investigation (no generic fallback).
6. `gap` propagates from segment templates through generation to question derivation.

## Consequences

- Segment-specific hypotheses (cleaning responsibility, portfolio size) produce targeted questions instead of generic ICP questions.
- `HypothesisDrivenDiscoveryEngine` and `HypothesisInvestigationPlanner` both consume canonical hypotheses.
- `MarketHypothesisRegistry` resolves by `segmentKey` (e.g. `short_term_rental` → `str_manager`).
- Legacy `InvestigationPlanBuilder` and `InvestigationLoop` still call `generateHypotheses()` directly — SPEC-180 migration retires them.

## Implementation

| Module | Change |
|---|---|
| `packages/scout/hypothesis/CanonicalHypothesisEngine.js` | Unified engine, immutable shape, generatedQuestions |
| `packages/scout/investigation/HypothesisGeneration.js` | Propagate `gap` and `rationale` |
| `packages/scout/investigation/types.js` | `buildHypothesis()` includes gap, uncertainty, evidence buckets |
| `packages/scout/hypothesis/MarketHypothesisRegistry.js` | `resolveMarketHypothesisBySegmentKey()` |
| `packages/scout/coverage/EvidenceRequirements.js` | `deriveQuestionsForHypothesis()` — gap-only derivation |
| `packages/scout/coverage/HypothesisDrivenDiscoveryEngine.js` | Uses canonical engine |
| `packages/scout/coverage/HypothesisInvestigationPlanner.js` | Already consumes canonical hypotheses |
| `test/scoutCanonicalHypothesisEngine.test.js` | ADR-094 / SPEC-179 acceptance tests |
