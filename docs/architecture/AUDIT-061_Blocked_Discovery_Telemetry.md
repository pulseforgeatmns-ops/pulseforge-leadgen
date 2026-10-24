# AUDIT-061 — Blocked Discovery Telemetry Continuity

| Field | Value |
|---|---|
| **Status** | Remediated — SPEC-185 |
| **Date** | 2026-08-26 |
| **Related** | [SPEC-185](../specs/SPEC-185_Blocked_Discovery_Telemetry_Continuity.md), [SPEC-184](../specs/SPEC-184_Provider_Execution_Continuity.md), [ADR-100](../adr/ADR-100_Uniform_Discovery_Payloads.md), [AUDIT-060](AUDIT-060_Provider_Execution_Persistence.md) |

## Executive summary

**First divergence was the blocked Scout return.** `runScoutAcquisitionIntelligence()` included `providerExecution` on the success path but omitted it on six blocked return paths, even when `universe.providerReports` contained Places execution records.

**Remediation (SPEC-185):** Every blocked Discovery payload now includes `providerExecution: universe.providerReports || []` (or `[]` when universe was never constructed).

## Trace (post-remediation)

```text
PlacesProvider.lastExecution
  ↓  universe.providerReports
runScoutAcquisitionIntelligence()
  ↓  payload.providerExecution (success AND blocked)
buildScoutDiscoveryArtifact()
  ↓  artifact.providerExecution
normalizeScoutDiscoveryPayload()
  ↓  contribution.providerExecution
DiscoveryPresentation
  ↓  Provider Execution section (blocked runs included)
```

## Blocked paths remediated

| Path | `providerExecution` source |
|---|---|
| Fixture `provider_failure` mode | `[]` |
| Invalid search definition | `[]` |
| Repository load failure | `[]` |
| `constructCandidateUniverse` catch | `[]` |
| Capability blocked | `universe.providerReports \|\| []` |
| Provider error, zero companies | `universe.providerReports \|\| []` |
