# ADR-100 — Uniform Discovery Payloads

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-26 |
| **Related** | [SPEC-185](../specs/SPEC-185_Blocked_Discovery_Telemetry_Continuity.md), [SPEC-184](../specs/SPEC-184_Provider_Execution_Continuity.md), [ADR-099](ADR-099_Sensor_Observation_Continuity.md), [AUDIT-061](../architecture/AUDIT-061_Blocked_Discovery_Telemetry.md) |

## Context

AUDIT-061 demonstrated that Scout blocked discovery returns dropped `providerExecution` while successful returns preserved it. Operators on blocked paths saw generic failure copy without sensor context, even when Places had executed and recorded HTTP status, Google status, latency, and errors.

## Decision

**Every Discovery outcome shall produce the same semantic payload contract for observational telemetry.**

Business outcomes may differ (opportunities present or empty, blocked vs completed). Telemetry never does.

Concretely:

1. Every Discovery payload includes `providerExecution: ProviderExecution[]`.
2. Early abort paths (invalid search definition, repository unavailable, fixture provider failure) use an empty array — never omit the field.
3. Paths after universe construction use `universe.providerReports || []`.
4. Catch paths returning a discovery payload must preserve `providerExecution` when available.

## Consequences

### Positive

- Operators can inspect blocked discovery runs with the same Provider Execution panel as successful runs.
- Downstream normalization, persistence, and presentation require no blocked-path special cases.

### Negative / constraints

- Blocked payloads carry a slightly larger observational envelope; this is intentional.

## Invariants

| Invariant | Enforcement |
|---|---|
| Blocked Scout returns include `providerExecution` | `ScoutAdapter.runScoutAcquisitionIntelligence()` |
| Success and blocked share payload contract | `ScoutAdapter` + `DiscoveryPayload.normalizeScoutDiscoveryPayload()` |
| Telemetry is observational only | `ADR-099` / `assertEvidenceAttached()` |
