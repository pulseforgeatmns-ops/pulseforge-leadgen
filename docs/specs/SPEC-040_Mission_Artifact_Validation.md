# SPEC-040 — Mission Artifact Validation & Discovery Resolution

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022 (Mission Engine), SPEC-023 (Capability Framework), SPEC-024 (Prospect Discovery), SPEC-032 (Mission Memory), SPEC-039 (Active Mission Resolver), ADR-002, ADR-010, ADR-011, ADR-017, ADR-021, ADR-026 |
| **Consumed by** | MissionExecutor, MissionPlanner, Prospect Discovery, Max Mission Workspace, Command Deck Operations |

## Objective

Ensure every Mission stage produces **valid business artifacts** before downstream stages execute.

A stage is not successful merely because its code executed. It is successful only when it produces the expected artifacts or explicitly records why it could not.

This specification also establishes **deterministic Discovery Profile resolution** so campaigns execute against the correct geography and playbook.

Success looks like: Discovery with 0 verified companies → **Blocked** with “Discovery returned zero verified companies” → pipeline pauses → operator sees review. Discovery with 17 of 20 → **Completed With Warnings** → pipeline may advance. Profile selection always records reason, geography, and confidence.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- `docs/vision/Product_Experience.md`
- [ADR-026](../adr/ADR-026_Business_Success_Determines_Pipeline_Progress.md) — business success determines pipeline progress
- [ADR-002](../adr/ADR-002_Explainable_AI.md) — explainable outcomes
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — Capability Framework
- [ADR-017](../adr/ADR-017_Intelligence_Before_Execution.md) — intelligence before execution
- [ADR-021](../adr/ADR-021_Human_Approval_Before_Execution.md) — human approval before execution
- [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md) — Mission Planner / Executor
- [SPEC-024](SPEC-024_Prospect_Discovery_Capability.md) — Discovery Profiles
- [SPEC-032](SPEC-032_Mission_Memory.md) — Mission Memory / evidence
- [SPEC-039](SPEC-039_Active_Mission_Resolver.md) — Active Mission Resolver

## Problem

Current behavior allows stages to report **Completed** while producing empty results:

```text
Discovery — Completed
Discovered: 0
Verified: 0
Rejected: 0
```

The pipeline then continues with empty inputs. This creates misleading Mission progress and invalid downstream artifacts.

Discovery Profile selection can also soft-score across geographies, silently leaving a client’s pinned market.

## Guiding Principle

```text
Current Stage
      ↓
Artifact Validation
      ↓
Pass? ──yes──→ Publish → Advance
      │ no
Validation Failed → Pause Mission → Operator Review
```

Technical `CapabilityResult.status === completed` is necessary but not sufficient.

## Scope

### Discovery Profile Resolution

Strict precedence (first match wins; no silent geography hop):

```text
Mission Constraints
        ↓
Explicit Operator Override
        ↓
Pinned Client Discovery Profile
        ↓
Client Default Geography
        ↓
Mission Type Default
```

Rules:

- When one or more **client** profiles exist, selection occurs **only among those profiles** (never global profiles for another geography).
- Resolution always emits a **Resolution Report**: selected profile, selection reason, geography, confidence, overrides applied.
- Ambiguous multi-profile client without override → lower confidence + alternatives (or blocked when no safe default).

### Stage Artifact Contracts

Each stage declares required inputs, expected outputs, and validation rules.

| Stage | Inputs | Outputs | Validation (v1) |
|---|---|---|---|
| Discovery | Discovery Profile | Prospect List | Profile exists; geography valid; prospect list published; **count > 0** for Completed |
| Company Enrichment / Intelligence | Prospect List | Enriched / Intelligence packages | One package per prospect when inputs non-empty |
| Ranking | Intelligence / Prospects | Ranked Opportunities | Ranking score assigned when inputs non-empty |
| Campaign Builder | Ranked Opportunities | Campaign | Prospect count > 0 |

### Stage Outcomes

| Outcome | Meaning |
|---|---|
| **Completed** | All validation passed |
| **Completed With Warnings** | Execution succeeded; artifacts produced; non-blocking issues (e.g. 17 of 20) |
| **Blocked** | Required business inputs unavailable or empty required outputs — pipeline pauses |
| **Failed** | Unexpected system failure — pipeline stops |

### Artifact Validation & Pipeline Gate

- Each published artifact validates schema, completeness, and business rules.
- Failing artifacts are **quarantined** — never consumed downstream.
- Before advancing: validate → publish only if pass → advance; else pause for operator review.
- Evidence records input/output artifacts, validation results, warning and failure reasons.

### Mission Review (operator surface)

Review displays stage status, published artifact counts/quality/validation, and human-readable blocking issues.

### Metrics (instrumentation hooks)

Track discovery success rate, average prospect yield, validation failures, warning frequency, profile selection accuracy, pipeline stop locations (v1: audit payloads; dashboards deferred).

## Out of Scope

- Full Company Intelligence live packages (SPEC-030) — enrichment stub contract still gated
- Durable metrics warehouse / Command Deck charts
- Changing ADR-021 approval semantics for outbound
- Auto-widening geography without operator override
- Postgres schema for resolution reports (v1: mission fields + audit)

## Dependencies

- SPEC-022 MissionExecutor / MissionPlanner
- SPEC-024 Discovery Profile store + selector
- SPEC-039 Active Mission Resolver (session continuity when operator fixes blocks)
- ADR-026 Business Success Determines Pipeline Progress
- Feature flag: `MISSION_ARTIFACT_VALIDATION=0` restores prior “technical completed advances” behavior

## Architecture

```text
MissionPlanner
  → DiscoveryProfileResolver.resolve() → Resolution Report on mission
MissionExecutor (per step)
  → CapabilityRunner.run()
  → PipelineGate.evaluate(contract, result, context)
       → Completed | CompletedWithWarnings | Blocked | Failed
  → Publish validated artifacts into priorOutputs
  → OR pause mission (waiting) with blockingIssues + quarantine
```

### Integration points

| Component | Change |
|---|---|
| `DiscoveryProfileResolver` / `ProfileSelector` | Deterministic precedence + report |
| `StageArtifactContracts` | Per-capability business contracts |
| `ArtifactValidator` + `PipelineGate` | Validate / quarantine / outcome |
| `MissionExecutor` | Gate after each step; pause on Blocked/Failed |
| `MissionResponse` / Operations | Surface stage outcomes + blocking reasons |
| Prospect Discovery | Empty yield remains technical complete; gate reclassifies to Blocked |

## Data Model

### Resolution report (on mission / constraints)

| Field | Purpose |
|---|---|
| `profile` | Snapshot used |
| `reason` / `selection` | Why chosen (pinned_client, explicit_override, …) |
| `geography` | Label / cities / state |
| `confidence` | 0–1 |
| `overridesApplied` | Operator overrides |

### Stage outcome record (audit + plan step)

| Field | Purpose |
|---|---|
| `outcome` | completed \| completed_with_warnings \| blocked \| failed |
| `publishedArtifacts` | Validated artifacts |
| `quarantinedArtifacts` | Failed validation |
| `blockingIssues` | Human-readable |
| `warnings` | Non-blocking |
| `validation` | Per-artifact results |

## Implementation Plan

1. ADR-026 Accepted; this spec Proposed → Implemented thin slice; indexes updated.
2. DiscoveryProfileResolver with precedence + report; ProfileSelector delegates.
3. Stage contracts + ArtifactValidator + PipelineGate.
4. MissionExecutor gate wiring; audit kinds; deliverables include stage outcomes.
5. Mission Workspace / Operations copy for Blocked / Warnings.
6. Tests + flag off path.

## Migration Strategy

- Additive: no DB migration required for v1 (audit + mission JSON fields).
- Flag `MISSION_ARTIFACT_VALIDATION=0` → previous advance-on-technical-complete behavior.
- Existing in-flight Missions: next step run applies gate; already-completed empty steps are not retroactively rewritten.

## Testing

- Unit: profile precedence; never hop geography when client profile exists; resolution report fields
- Unit: contracts — empty discovery → Blocked; shortfall → Warnings; campaign with 0 prospects → Blocked
- Integration: MissionExecutor pauses and does not run enrichment after empty discovery
- Flag-off: empty discovery still advances (legacy)
- Regression: fixture discovery with yields still reaches review_required

## Acceptance Criteria

- [x] Discovery Profile always resolves deterministically with a Resolution Report
- [x] Empty artifacts never count as successful business outcomes (Blocked)
- [x] Downstream stages consume validated artifacts only
- [x] Pipelines pause on missing required business inputs
- [x] Operator sees explicit blocking reasons (progress + audit + Mission response)
- [x] Validation results become part of Mission evidence (audit)
- [x] Published artifacts include provenance and validation status

## Future Work

- Postgres-backed resolution / validation event tables
- Command Deck metrics strip for yield and stop locations
- Per-client minimum yield thresholds (hard block vs warn)
- SPEC-030 intelligence package completeness rules
- UI glyphs: ✓ / ⚠ / ⛔ per stage in Mission Workspace
