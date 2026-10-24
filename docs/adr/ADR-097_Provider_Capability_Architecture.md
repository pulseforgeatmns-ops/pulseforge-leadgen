# ADR-097 — Provider Capability Architecture

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-26 |
| **Spec** | [SPEC-182](../specs/SPEC-182_Provider_Capability_Architecture.md) |
| **Related** | [ADR-096](ADR-096_Evidence_Native_Execution.md), [ADR-095](ADR-095_Single_Investigation_Planner.md), [SPEC-175](../specs/SPEC-175_External_Discovery_Capability.md), [SPEC-141](../specs/SPEC-141_Scout_Intelligence_Pipeline.md), [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |

## Context

Providers were modeled twice:

| Registry | Concern |
|---|---|
| `intelligence/ProviderCapabilityRegistry.js` | Evidence types, cost tiers, confidence gain |
| `coverage/ExternalDiscoveryProviderRegistry.js` | Availability states (AVAILABLE/UNAVAILABLE/STUB) |

Assignment logic in `EvidenceProviderAssignment.js` used hardcoded `EVIDENCE_TO_PROVIDERS` maps. Capability evaluation in `evaluateDiscoveryCapability()` used a separate provider view. Adding or removing a provider required planner changes.

SPEC-181 established evidence-native execution. SPEC-182 completes the provider layer: providers become **capability engines** that advertise what evidence they can collect.

## Decision

**One unified provider capability registry** governs both evidence assignment and discovery availability.

### Provider capability schema

```js
{
  providerId: string,
  label: string,
  evidenceTypes: string[],      // investigative evidence (identity, reviews, …)
  capabilities: string[],         // low-level evidence capabilities (businesses, people, …)
  sourceType: string,             // adapter source type mapping
  availability: 'available' | 'unavailable' | 'stub',
  costTier: 'free' | 'cached' | 'local' | 'paid',
  confidenceGain: number,
  limitations: string[],
  unavailableReason: string | null
}
```

### Planner contract

The planner asks two questions at runtime — never "search Google Places":

1. **Who can answer this question?** → `registry.selectForEvidenceType(evidenceType)`
2. **Who should I search?** → ranked by availability, cost tier, confidence gain

New providers call `registry.register()` with advertised capabilities. Planners automatically consider them — **no planner modifications required**.

### Delegation (Phase 1)

| Module | Role |
|---|---|
| `coverage/ProviderCapabilityRegistry.js` | **Canonical** unified registry |
| `intelligence/ProviderCapabilityRegistry.js` | Backward-compatible facade |
| `coverage/ExternalDiscoveryProviderRegistry.js` | Backward-compatible facade |
| `coverage/EvidenceProviderAssignment.js` | Dynamic capability matching only |
| `coverage/DiscoveryCapabilityGate.js` | Same registry as assignment |

## Invariants

1. Provider assignment reads evidence types from the unified registry — never hardcoded provider lists in planners.
2. Availability changes affect which providers are assigned, not which hypotheses are generated.
3. Provider removal triggers `revisePlanForUnavailableProviders` — plan adapts, hypotheses unchanged.
4. `evaluateDiscoveryCapability()` and `assignProvidersForRequirements()` read the same registry.
5. `adapterForProvider()` resolves source types from registry entries, not inline maps.

## Consequences

### Positive

- Adding LinkedIn, a county records API, or a custom registry is a registry registration — not a planner edit.
- Evidence assignment and discovery gating share one source of truth.
- Operator explainability cites registry rationale (capability, reliability, cost tier).
- SPEC-181 evidence-native execution is fully provider-swappable.

### Negative / deferred

- Legacy `EVIDENCE_TO_PROVIDERS` export is derived at load time for test compatibility; consumers should migrate to `selectForEvidenceType()`.
- `google_places` / `google_maps` alias preserved for SPEC-175 compat.

## Implementation

| File | Change |
|---|---|
| `packages/scout/coverage/ProviderCapabilityRegistry.js` | New unified registry with `register()`, `selectForEvidenceType()` |
| `packages/scout/intelligence/ProviderCapabilityRegistry.js` | Delegates to unified registry |
| `packages/scout/coverage/ExternalDiscoveryProviderRegistry.js` | Delegates to unified registry |
| `packages/scout/coverage/EvidenceProviderAssignment.js` | Dynamic matching replaces hardcoded maps |
| `packages/scout/coverage/DiscoveryCapabilityGate.js` | Uses unified registry |
| `packages/scout/coverage/HypothesisDrivenDiscoveryEngine.js` | `adapterForProvider()` uses registry source types |
