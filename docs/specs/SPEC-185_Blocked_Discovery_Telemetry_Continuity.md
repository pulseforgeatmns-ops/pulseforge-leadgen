# SPEC-185 — Blocked Discovery Telemetry Continuity

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Priority** | Critical |
| **Depends on** | [SPEC-184](SPEC-184_Provider_Execution_Continuity.md), [AUDIT-061](../architecture/AUDIT-061_Blocked_Discovery_Telemetry.md) |
| **ADR** | [ADR-100](../adr/ADR-100_Uniform_Discovery_Payloads.md) |

## Problem

Provider execution telemetry is preserved on successful discovery execution but was discarded on blocked provider-failure paths (AUDIT-061).

```text
PlacesProvider.lastExecution
  ↓
universe.providerReports
  ↓
runScoutAcquisitionIntelligence()
  ↓
SUCCESS    providerExecution ✓
BLOCKED    providerExecution ✗
```

Presentation, persistence, and normalization already support `providerExecution`. Only the blocked Scout return omitted it.

## Decision

Every Discovery exit path shall preserve provider execution telemetry.

- Successful execution
- Blocked execution
- Partial execution
- Provider failure
- Exception recovery

All produce the same canonical payload shape.

## Canonical invariant

Every Discovery payload shall contain:

```javascript
providerExecution: ProviderExecution[]
```

Never conditionally. Never only on success.

Blocked execution may omit opportunities, buying signals, and evidence. It shall never omit provider telemetry.

## Implementation

| Module | Change |
|---|---|
| `packages/max/scoutAcquisition/ScoutAdapter.js` | Include `providerExecution` on every blocked return; use `universe.providerReports \|\| []` when universe exists |

## Acceptance Criteria

- [x] All blocked returns include `providerExecution`
- [x] Success and blocked payloads share the same contract
- [x] DiscoveryPresentation renders telemetry for blocked executions
- [x] Rollback inspection exposes telemetry (via existing SPEC-184 normalization)
- [x] Operators see provider execution records instead of generic "Discovery provider failed." alone
