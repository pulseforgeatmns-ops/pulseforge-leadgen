# SPEC-060 — Prospect Acquisition Framework

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-29 |
| **Supersedes** | Discovery-only prospect acquisition model |
| **Depends on** | SPEC-022, SPEC-023, SPEC-024, SPEC-040, SPEC-042, SPEC-043, SPEC-051, SPEC-052; ADR-026, ADR-028, ADR-029, ADR-035 |
| **ADR** | [ADR-044 Prospect Acquisition Independence](../adr/ADR-044_Prospect_Acquisition_Independence.md) |

## Objective

PulseForge campaigns operate on validated ProspectLists. Discovery is one method of producing candidates for a ProspectList — not a prerequisite. The Mission Engine executes successfully regardless of how prospects are acquired.

## Vision References

- `docs/vision/Mission.md`
- [ADR-044](../adr/ADR-044_Prospect_Acquisition_Independence.md)
- [ADR-035](../adr/ADR-035_Plan_Around_State_Not_Sequence.md) — Discovery is an acquisition strategy
- [ADR-029](../adr/ADR-029_Artifact_Provenance_Must_Not_Affect_Consumption.md)
- [SPEC-043](SPEC-043_Operator_Artifact_Injection.md)
- [SPEC-051](SPEC-051_Artifact_Resolution_and_State_Aware_Planning.md)
- [SPEC-024](SPEC-024_Prospect_Discovery_Capability.md)

## Problem

Campaign 001 demonstrated an architectural dependency that should not exist: Discovery failure could block campaign execution even when valid prospects already existed.

The business objective is to execute campaigns. Discovery is an accelerator, not a prerequisite.

PulseForge must support:

- Automated discovery
- Existing prospect lists
- Manual operator input
- CSV imports
- Future CRM integrations

All producing the same artifact contract downstream.

## Principles

1. Prospect acquisition is independent from campaign execution.
2. Every acquisition source produces the same contract (`CandidateSet`).
3. Everything downstream of `ProspectList` remains unchanged.
4. Campaign Builder never knows where prospects originated.

## Scope (v1)

- Prospect Acquisition domain (`packages/capabilities/acquisition/`)
- Provider contract: `available()`, `acquire()`, `metadata()`, `health()`
- Initial providers: Google Places (existing), Manual Prospect List, CSV Import, Existing Prospect Repository
- `CandidateSet` business artifact — providers publish candidates only
- Shared verification pipeline: `CandidateSet` → `ProspectList` + `RejectedProspects` + `VerificationReport`
- Provenance on every candidate (source, time, mission, provider, import method, operator, original data)
- Mission Planning selects acquisition strategy (`discovery` | `manual` | `csv` | `existing`)
- Mission Workspace Prospect Acquisition panel
- Tests: `packages/capabilities/tests/prospectAcquisition.test.js`

## Out of Scope

- Excel import (future)
- Live CRM / Apollo / ZoomInfo / Clay connectors (register as future providers)
- Rewriting Campaign Builder, Mail Package Builder, Campaign Review, or Execution
- Changing Artifact Bus consumption semantics (ADR-029 still holds)
- Forcing Discovery capability internals to stop verifying (Discovery remains a valid acquisition path; new providers must not publish ProspectLists)

## Architecture

```text
Mission
  ↓
Prospect Acquisition
  ├── Discovery Providers (Google Places, …)
  ├── Manual Prospect List
  ├── CSV Import
  ├── Existing Prospect Lists
  └── Future Integrations
  ↓
Prospect Verification
  ↓
ProspectList
  ↓
Campaign Builder → Mail Package → Review → Approval → Execution
```

### Provider Contract

Every provider implements:

| Method | Role |
|---|---|
| `available()` | Whether the provider can run in this environment |
| `acquire(request)` | Returns `{ candidates, evidence, warnings }` — never a ProspectList |
| `metadata()` | Static id, label, category, capabilities |
| `health()` | Liveness / config status for Workspace |

Providers publish **Candidates**. Verification owns **ProspectList** creation.

### Verification

Unchanged gates (SPEC-024) applied to all acquisition methods:

- Input: `CandidateSet`
- Output: `ProspectList`, `RejectedProspects`, `VerificationReport`

## Data Model

### Candidate provenance

```json
{
  "acquisitionSource": "csv_import",
  "acquisitionTime": "2026-07-29T10:00:00.000Z",
  "missionId": "msn_…",
  "provider": "csv_import",
  "importMethod": "csv",
  "operator": "jacob@gopulseforge.com",
  "originalData": { }
}
```

### CandidateSet payload

- `candidates[]` — normalized candidate records with provenance
- `candidateCount`
- `provider` / `acquisitionSource`
- `evidence[]`

## Implementation Plan

1. [x] Domain package + provider registry
2. [x] `CandidateSet` registry type + verification adapter
3. [x] Route operator import / existing-list refresh through CandidateSet → Verification
4. [x] Mission Plan `acquisitionStrategy` parameter + Intent Understanding cues
5. [x] Workspace Prospect Acquisition section
6. [x] Acceptance tests (`packages/capabilities/tests/prospectAcquisition.test.js`)
7. [x] Register `prospect_acquisition` capability; Campaign Builder remains ProspectList-only

## Migration Strategy

- Existing Discovery-produced ProspectLists remain valid.
- Operator injection API (`injectProspectList`) remains; internally acquires candidates then verifies.
- Campaign Builder continues to consume only `ProspectList` (ADR-029).
- No migration of historical missions required.

## Testing

- Unit: provider contract, CandidateSet build, CSV/manual/existing providers
- Integration: acquire → verify → ProspectList; Campaign Builder receives ProspectList only
- Regression: existing Discovery + operator injection tests still pass

## Acceptance Criteria

- [x] Campaigns execute without Discovery
- [x] Manual Prospect Lists produce valid ProspectLists
- [x] CSV imports produce valid CandidateSets
- [x] Existing Prospect Lists can be reused
- [x] Campaign Builder receives only ProspectLists
- [x] Discovery becomes optional
- [x] Acquisition source is preserved as evidence
- [x] All downstream pipeline behavior remains unchanged
- [x] Existing campaigns require no changes

## Future Work

- Excel import
- Yelp / Maps alternatives / Apollo / ZoomInfo / Clay / CRM connectors
- Provider health dashboard beyond Workspace panel
- Optional verification refresh policy for reused lists
