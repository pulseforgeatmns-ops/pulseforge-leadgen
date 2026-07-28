# SPEC-043 — Operator Artifact Injection

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-040, SPEC-041, SPEC-042, ADR-026, ADR-027, ADR-028, ADR-029 |
| **Consumed by** | Mission Workspace, Active Mission Resolver, Company Intelligence, downstream Mission stages |

## Objective

Allow an operator to inject validated business artifacts into an active Mission, bypassing upstream stages when appropriate.

This is a first-class capability that supports operator-assisted workflows while preserving Mission pipeline integrity. Discovery is one producer of a ProspectList — not the only producer.

## Vision References

- `docs/vision/Mission.md`
- [ADR-029](../adr/ADR-029_Artifact_Provenance_Must_Not_Affect_Consumption.md) — provenance must not affect consumption
- [ADR-028](../adr/ADR-028_Business_State_Flows_Through_Artifacts.md) — business state flows through artifacts
- [ADR-026](../adr/ADR-026_Business_Success_Determines_Pipeline_Progress.md) — business success determines pipeline progress
- [SPEC-042](SPEC-042_Mission_Artifact_Bus.md) — Mission Artifact Bus
- [SPEC-040](SPEC-040_Mission_Artifact_Validation.md) — Pipeline Gate
- [SPEC-041](SPEC-041_Mission_Planner.md) — Mission Planner

## Problem

Today:

```text
Discovery
    ↓
ProspectList
    ↓
Company Intelligence
```

If Discovery fails (e.g., Google Places unavailable), there is no way to continue even when the operator already has a qualified prospect list. Company Intelligence already consumes a validated ProspectList. The Artifact Bus resolves by type and validation status, not producer. The missing capability is operator publication of a validated artifact onto the Mission Artifact Bus.

## Guiding Principle

```text
Operator
    ↓
Import Prospect List
    ↓
Validation
    ↓
ProspectList Artifact
    ↓
Artifact Bus
    ↓
Mission Replan
    ↓
Resume at Company Intelligence
```

## Scope

### In scope (v1)

- Operator ingress path onto the Artifact Bus for `ProspectList`
- Manual entry, CSV import, and spreadsheet paste normalization
- Shared ProspectList schema + business validation (same path as Discovery-published lists)
- Provenance: `producer` / `source` / `createdBy` for audit only
- Mark Discovery **Satisfied (Operator Supplied)** after successful validation
- Mission resume at Company Intelligence
- Workspace recovery UX when Discovery blocks: Retry / Import / Cancel

### Out of scope

- Changes to Company Intelligence, Artifact resolution rules, or downstream capability logic
- Mission execution semantic changes beyond Discovery satisfaction + resume
- Live CRM connectors / API integrations (future — same normalize → validate → publish path)
- Durable artifact tables (SPEC-032)

## Supported Inputs

| Input | Status |
|---|---|
| Manual entry | v1 |
| CSV import | v1 |
| Spreadsheet paste | v1 |
| CRM export | Future |
| API integrations | Future |

All inputs normalize into a standard ProspectList artifact.

## Validation

**Required fields (per prospect):**

- Company Name

**Recommended:**

- Website
- Address

**Optional:**

- Phone
- Contact Name
- Notes

The imported artifact must pass the same registry + business validation as a Discovery-generated ProspectList. Missing recommended fields produce warnings, not blocks. Zero valid prospects → reject (do not publish consumable artifact).

## Provenance

Operator-created artifacts record origin without affecting consumption:

```json
{
  "producer": "operator_manual",
  "source": "csv_import",
  "validated": true,
  "createdBy": "operator"
}
```

Provenance remains visible in Workspace and replay. Per [ADR-029](../adr/ADR-029_Artifact_Provenance_Must_Not_Affect_Consumption.md), consumers resolve solely by type, validation status, and revision.

## Planner / Executor Behavior

When a validated ProspectList is injected:

1. Artifact Bus publishes a consumable ProspectList revision
2. Discovery is marked **Satisfied (Operator Supplied)** (`completed` with outcome `satisfied_operator_supplied`)
3. Planner invalidates downstream stages only when dependency changes require it
4. Mission resumes at Company Intelligence (skips Discovery)
5. No completed downstream work is discarded unless invalidated by dependency changes

## Workspace UX

If Discovery blocks, Workspace presents:

```text
Discovery failed.

Actions
• Retry Discovery
• Import Prospect List
• Cancel Mission
```

Selecting **Import Prospect List** opens the operator import flow. After validation succeeds, the Mission resumes automatically.

## Architecture

```text
POST /api/v1/missions/:id/artifacts/inject
       ↓
normalizeProspectRows (manual | csv | paste)
       ↓
validateOperatorProspectList (field + registry)
       ↓
ArtifactBus.publishArtifact (ProspectList)
       ↓
mark Discovery satisfied_operator_supplied
       ↓
MissionExecutor.execute (skip completed → Company Intelligence)
```

## Data Model

No new tables. Snapshot remains in `missions.deliverables.artifactBus` (SPEC-042).

Audit kinds:

- `artifact_injected` — operator ingress
- Existing Artifact Bus audit kinds for publish / validate / consume

## Implementation Plan

1. ProspectList field validation (required company name; recommended website/address warnings)
2. `OperatorArtifactInjection` service (normalize → validate → publish → satisfy → resume)
3. `MissionEngine.injectProspectList` + HTTP route
4. Workspace recovery panel + import UI
5. Tests for validation, provenance-agnostic consume, Discovery satisfaction, resume

## Testing

- Unit: CSV/paste normalize; required/recommended field rules; registry validation
- Integration: blocked Discovery → inject → Discovery satisfied → Company Intelligence consumes list
- Provenance: consumer cannot branch on producer; Workspace surfaces provenance
- Regression: Discovery fixture path still publishes ProspectList unchanged

## Acceptance Criteria

- [x] Operator can import a ProspectList into an active Mission
- [x] Imported ProspectLists pass the same validation pipeline as Discovery-generated ProspectLists
- [x] Discovery may be marked Satisfied (Operator Supplied) after successful validation
- [x] Mission resumes automatically at Company Intelligence
- [x] Company Intelligence cannot distinguish origin except through artifact provenance
- [x] Downstream stages behave identically regardless of artifact origin
- [x] Replay, audit history, and Workspace identify operator-created artifacts
- [x] Existing Discovery workflows continue without modification

## Future Work

- CRM / API ingress adapters
- Operator injection for additional artifact types (Campaign drafts, ranked lists)
- SPEC-032 durable store for operator upload files
