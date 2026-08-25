# SPEC-182 — Provider Capability Architecture

| Field | Value |
|---|---|
| **Status** | Draft |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Epic** | [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |
| **Depends on** | [SPEC-175](SPEC-175_External_Discovery_Capability.md), [SPEC-141](SPEC-141_Scout_Intelligence_Pipeline.md), [SPEC-181](SPEC-181_Evidence_Native_Execution.md) |
| **Supersedes** | Dual provider registries |

## Objective

One provider capability model governs both evidence collection assignment and discovery availability. Deleting or disabling a provider changes coverage — never Scout's reasoning.

## Problem

Two registries model providers differently:

| Registry | File | Models |
|---|---|---|
| ProviderCapabilityRegistry | `intelligence/ProviderCapabilityRegistry.js` | Evidence types, cost tiers, confidence gain |
| ExternalDiscoveryProviderRegistry | `coverage/ExternalDiscoveryProviderRegistry.js` | Availability states (AVAILABLE/UNAVAILABLE/STUB) |

Assignment logic in `EvidenceProviderAssignment.js` and capability evaluation in `evaluateDiscoveryCapability()` use separate provider views.

## Decision

Unified provider capability schema:

```js
interface ProviderCapability {
  providerId: string
  label: string
  evidenceTypes: string[]           // what evidence this provider can collect
  sourceType: string                // adapter source type mapping
  availability: 'available' | 'unavailable' | 'stub'
  costTier: 'free' | 'cached' | 'local' | 'paid'
  confidenceGain: number
  limitations: string[]
  unavailableReason: string | null
}
```

## Module Changes

| Module | Change |
|---|---|
| `coverage/ProviderCapabilityRegistry.js` | **New** — unified registry merging both existing registries |
| `intelligence/ProviderCapabilityRegistry.js` | Delegate to unified registry |
| `coverage/ExternalDiscoveryProviderRegistry.js` | Delegate to unified registry |
| `coverage/EvidenceProviderAssignment.js` | Read from unified registry only |

## Invariants

1. Provider assignment reads evidence types from the unified registry — never hardcoded provider lists in planners.
2. Availability changes affect which providers are assigned, not which hypotheses are generated.
3. Provider removal triggers `revisePlanForUnavailableProviders` — plan adapts, hypotheses unchanged.
4. `evaluateDiscoveryCapability()` uses the same registry as evidence assignment.

## Migration

1. Create unified `ProviderCapabilityRegistry` in `coverage/`.
2. Adapter existing registries to delegate (no breaking changes in Phase 1).
3. Consolidate hardcoded `PROVIDER_TO_SOURCE_TYPE` maps into registry entries.
4. Update `HypothesisDrivenDiscoveryEngine.adapterForProvider()` to use unified lookup.

## Acceptance Criteria

- [ ] Single registry source for provider capabilities and availability
- [ ] Removing a provider from registry changes assignments but not hypothesis generation
- [ ] `evaluateDiscoveryCapability` and `assignProvidersForRequirements` read same registry
- [ ] Operator explainability cites registry rationale for provider selection
