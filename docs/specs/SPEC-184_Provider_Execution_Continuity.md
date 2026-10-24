# SPEC-184 — Provider Execution Continuity

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Priority** | Critical |
| **Depends on** | [SPEC-174](SPEC-174_Canonical_Evidence_Coverage.md), [SPEC-177](SPEC-177_Hypothesis_Driven_Discovery_Engine.md), PR #459 (AUDIT-059) |
| **ADR** | [ADR-099](../adr/ADR-099_Sensor_Observation_Continuity.md) |

## Problem

Provider execution telemetry is generated correctly but was not preserved across the Discovery contribution boundary (AUDIT-060).

```text
Scout Result          ✓ providerExecution
Scout Artifact        ✓ providerExecution
normalizeScoutDiscoveryPayload()  ✗ providerExecution removed
Validation / Rollback / Operator    ✗ sensor diagnostics lost
```

## Decision

`providerExecution` becomes part of the canonical Discovery contribution contract. It is never discarded.

## Canonical flow

```text
Provider → Provider Execution → Scout Result → Scout Artifact
  → Discovery Contribution → Mission Persistence → Presentation → Operator
```

No stage may remove it.

## Contract

```javascript
DiscoveryContribution {
  ...
  providerExecution: ProviderExecution[]  // observational only
}
```

## Implementation

| Module | Change |
|---|---|
| `packages/scout/coverage/ProviderExecution.js` | Normalize, extract, format provider execution records |
| `packages/scout/adapters/ScoutDiscoveryArtifact.js` | Pass through `providerExecution` from Scout payload |
| `packages/acquisition-mission/DiscoveryPayload.js` | Include `providerExecution` in contribution |
| `packages/acquisition-mission/DiscoveryPresentation.js` | Render Provider Execution section |
| `packages/acquisition-mission/TransactionalExecution.js` | Attach diagnostics to validation errors |
| `packages/max/workspace/AcquisitionMissionExecution.js` | Surface diagnostics on rollback response |

## Acceptance Criteria

- [x] `providerExecution` survives normalization
- [x] Persisted with Discovery contributions
- [x] Available after rollback (via error details)
- [x] Validation errors include provider diagnostics
- [x] Mission inspection exposes provider execution (via contribution payload)
- [x] Discovery presentation renders provider execution
- [x] No execution telemetry discarded before persistence

## Invariants

Provider telemetry is observational. It must never influence business reasoning. It exists only for explainability, debugging, operational visibility, and provider health.
