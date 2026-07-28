# SPEC-042 — Mission Artifact Bus

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-040, SPEC-041, ADR-010, ADR-011, ADR-026, ADR-027, ADR-028 |
| **Consumed by** | MissionExecutor, PipelineGate, Mission Workspace, Active Mission Resolver, future replay/caching |

## Objective

Introduce a first-class **Artifact Bus** that becomes the canonical data layer of the Mission Engine.

Capabilities no longer exchange arbitrary JSON as the source of truth. They publish **immutable, versioned business artifacts** that downstream stages consume through the Mission Engine.

Success looks like: Discovery publishes `ProspectList` v1 → Ranking consumes the newest validated revision → Campaign Builder publishes `Campaign` v1 → operator inspects provenance and history in Mission Workspace → replay from `Campaign` v2 skips Discovery and Ranking.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-028](../adr/ADR-028_Business_State_Flows_Through_Artifacts.md) — business state flows through artifacts
- [ADR-026](../adr/ADR-026_Business_Success_Determines_Pipeline_Progress.md) — business success determines pipeline progress
- [ADR-027](../adr/ADR-027_Mission_Planning_Is_Objective_Driven.md) — objective-driven planning
- [SPEC-040](SPEC-040_Mission_Artifact_Validation.md) — artifact validation / Pipeline Gate
- [SPEC-041](SPEC-041_Mission_Planner.md) — Stage Library + execution graphs

## Problem

Current execution resembles:

```text
Discovery
    ↓
return { prospects }
    ↓
Ranking
```

Outputs are implementation-specific objects. Problems: no standard schema, weak provenance, difficult replay/debugging/caching/comparison, and stage coupling. Capabilities exchange implementation details instead of business artifacts.

## Guiding Principle

```text
Discovery
        │
        ▼
Prospect List Artifact
        │
        ▼
Company Intelligence Artifact
        │
        ▼
Opportunity Ranking Artifact
        │
        ▼
Campaign Artifact
```

The Mission owns the Artifact Bus. Pipeline Gate validates before publication. Downstream stages never consume unpublished or quarantined outputs.

## Scope

### Artifact principles

Artifacts are **immutable**, **versioned**, **typed**, **self-validating**, **traceable**, and **replayable**. Capabilities never mutate an artifact — they publish a new revision.

### Artifact model

```text
Artifact {
  id, missionId, stageId, revision, artifactType, schemaVersion,
  producer, createdAt, validationStatus, dependencies, metadata, payload
}
```

### Artifact Registry (initial)

| Type | Alias (Stage Library) | Typical producer | Typical consumers |
|---|---|---|---|
| DiscoveryProfile | `discovery_profile` | Planner / Discovery | Discovery |
| ProspectList | `prospect_list` | Discovery | Enrichment, Ranking |
| CompanyIntelligence | `company_intelligence` / `enriched_list` | Enrichment | Ranking |
| OpportunityRanking | `ranked_prospects` | Ranking | Campaign Builder |
| Campaign | `campaign` | Campaign Builder | Mail / Review |
| MailPackage | `mail_package` | Mail Package Generator | Review / Execution |
| ReviewDecision | `review_decision` | Campaign Review | Execution |
| ExecutionPackage | `execution_package` | Direct Mail Execution | Delivery |
| DeliveryResults | `delivery_results` | Execution | Outcome Intelligence |
| OutcomeSummary | `outcome_summary` | Outcome Intelligence | Operator Inbox |

### Artifact Bus API

- `publishArtifact()`
- `getArtifact()`
- `getLatestArtifact()` — newest **validated** revision (quarantined invisible)
- `getArtifactHistory()`
- `validateArtifact()`
- `compareArtifacts()`
- `replayFromArtifact()`
- `consumeArtifact()` — resolve + emit `ArtifactConsumed`
- `getArtifactGraph()` / `listMissionArtifacts()` — workspace

### Artifact events (Mission audit stream)

`ArtifactPublished` · `ArtifactValidated` · `ArtifactQuarantined` · `ArtifactSuperseded` · `ArtifactConsumed`

### Integration

```text
Capability.execute → CapabilityResult
       ↓
PipelineGate (SPEC-040)
       ↓
ArtifactBus.publish  ← only when gate.publishOutputs (or quarantine stamp)
       ↓
Next stage inputs ← ArtifactBus.consume / getLatest
```

Mission Planner plans stages. Mission Executor runs stages. Artifact Bus carries business state. Pipeline Gate validates before advancing.

### Workspace

Mission Workspace exposes an **Artifacts** section: type, revision, validation status, summary, provenance, dependencies, and payload inspection. Supports compare of two revisions.

### Persistence (v1)

Artifact Bus snapshots persist in `missions.deliverables.artifactBus` (JSONB). No dedicated table in v1 (aligns with SPEC-040 posture; durable store deferred to SPEC-032).

### Compatibility

- Feature flag `MISSION_ARTIFACT_BUS=0` restores flat `priorOutputs` merge only.
- Capability `outputs` remain; Mission Engine adapts them into typed artifacts on publish.
- Stage Library `consumes` / `produces` snake_case names resolve via Artifact Registry aliases.

## Out of Scope

- Dedicated Postgres artifact tables / cross-mission artifact catalog
- Capabilities calling `publishArtifact` directly (adapter path is v1)
- Parallel stage branching / multi-writer conflict resolution
- Automatic caching reuse across Missions
- Full UI diff highlighter for every payload field (v1 returns structured compare result)

## Dependencies

- SPEC-040 PipelineGate + validation statuses
- SPEC-041 Stage Library produces/consumes
- ADR-028 decision record
- Existing `mission_audit_events` for artifact event kinds

## Architecture

```text
Operator objective
  → Mission Planner (graph)
  → Mission Executor
       for each stage:
         consume validated artifacts (bus)
         CapabilityRunner
         PipelineGate
         publish / quarantine (bus)
  → Mission Workspace (Artifacts + Audit)
```

## Data Model

In-memory + `deliverables.artifactBus` snapshot:

```json
{
  "version": 1,
  "artifacts": [ { "id": "art_…", "artifactType": "ProspectList", "revision": 1, "…": "…" } ],
  "events": [ { "type": "ArtifactPublished", "artifactId": "art_…", "at": "…" } ]
}
```

## Implementation Plan

1. Artifact Registry + type aliases + minimum validators
2. ArtifactBus (publish / get / history / validate / compare / replay / consume / graph)
3. MissionExecutor publish after gate; consume before next stage; persist snapshot
4. MissionEngine.getWorkspace artifacts payload
5. Command Deck Mission Workspace Artifacts section
6. Tests + docs indexes + CHANGELOG / CURRENT_STATE / DECISIONS

## Migration Strategy

Legacy:

```js
return { prospects };
```

becomes (engine-managed in v1):

```js
publishArtifact({ type: 'ProspectList', payload: { prospects } });
```

No DB migration. Flag-off restores prior merge. Rollback = set `MISSION_ARTIFACT_BUS=0`.

## Testing

- Unit: registry aliases, validate, publish revisions, quarantine invisible to getLatest, compare, replay plan
- Integration: full campaign mission publishes ProspectList → OpportunityRanking → Campaign; workspace lists artifacts
- Flag off: existing SPEC-040 tests still pass with priorOutputs path

## Acceptance Criteria

- [x] Every gated stage publishes typed artifacts to the Artifact Bus
- [x] Stages resolve inputs through the Artifact Bus (validated latest revision)
- [x] Artifacts are immutable and versioned
- [x] Validation status set before / at publication (PipelineGate + bus)
- [x] Quarantined artifacts are never returned by `getLatestArtifact` / `consumeArtifact`
- [x] `replayFromArtifact` returns a skip plan from a revision
- [x] Workspace exposes artifact history and provenance
- [x] Artifact graph replaces implicit-only data flow for inspection
- [x] ADR-028 accepted and indexed

## Future Work

- Capability-native `publishArtifact` (retire output adapter)
- Durable artifact store (SPEC-032)
- Deterministic cache reuse across identical dependency graphs
- Rich Mission Workspace compare UI
- Branching / parallel artifact lineages
