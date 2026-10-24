# SPEC-052 — Typed Artifact Validation

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Completed** | 2026-07-28 |
| **Depends on** | SPEC-040, SPEC-042, SPEC-043, SPEC-050, SPEC-051; ADR-026, ADR-028, ADR-034, ADR-035 |
| **ADR** | [ADR-036 Trust Through Contracts](../adr/ADR-036_Trust_Through_Contracts.md) |

## Objective

All artifacts entering Pulseforge shall be validated against their declared schema before entering the Artifact Bus.

Natural language is not an artifact. Artifacts are typed, validated contracts.

Success looks like: mission prose never becomes ProspectList rows; only schema-valid, semantically sound payloads publish as consumable bus revisions; validation failures appear in Review Workspace as non-executable.

## Vision References

- [ADR-036 Trust Through Contracts](../adr/ADR-036_Trust_Through_Contracts.md)
- [ADR-028 Business State Flows Through Artifacts](../adr/ADR-028_Business_State_Flows_Through_Artifacts.md)
- [ADR-034 Intent Before Execution](../adr/ADR-034_Intent_Before_Execution.md)
- [ADR-026 Business Success Determines Pipeline Progress](../adr/ADR-026_Business_Success_Determines_Pipeline_Progress.md)
- [SPEC-040 Mission Artifact Validation](SPEC-040_Mission_Artifact_Validation.md)
- [SPEC-042 Mission Artifact Bus](SPEC-042_Mission_Artifact_Bus.md)
- [SPEC-043 Operator Artifact Injection](SPEC-043_Operator_Artifact_Injection.md)
- [SPEC-050 Deterministic Mission Planning](SPEC-050_Deterministic_Mission_Planning.md)
- [SPEC-051 Artifact Resolution & State-Aware Planning](SPEC-051_Artifact_Resolution_and_State_Aware_Planning.md)
- `docs/vision/Mission.md`

## Problem

Mission Planning correctly separates operator intent. Artifact Resolution correctly resolves dependencies. However, the system permitted arbitrary text to enter execution as structured artifacts.

Example:

```text
Build Campaign 001...

Reuse existing ProspectList...
```

being interpreted as:

```text
Prospect 1
Prospect 2
Prospect 3
```

This violates the platform contract.

## Design Principle

Every artifact must prove what it is before the platform trusts it.

```text
Outside World
  Operator / CSV / Email / Website / PDF / API
        ↓
Artifact Candidate
        ↓
Artifact Validator
   PASS → Artifact Bus → Mission Execution
   FAIL → Remain Plain Text → Operator Review
```

## Artifact Contract

Every artifact defines:

| Field | Example |
|---|---|
| type | ProspectList |
| version | 1 |
| schema | ProspectListSchema |
| validator | ProspectListValidator |

## Scope (v1)

- `packages/mission-engine/ArtifactValidator.js` — identify type → schema → semantic → compatibility
- ProspectList rejects natural language / mission objectives / notes as company rows
- Operator chat detection (`detectOperatorProspectListInMessage`) fail-closed on NL
- Artifact Bus `validateArtifact` / `publishArtifact` use the typed validator
- Registry semantic hooks for Campaign, Sales Intelligence, Mail Package minimums
- Review Workspace surfaces `artifactValidationFailures`
- Tests: `npm run test:mission` (`typedArtifactValidation.test.js`)

## Out of Scope

- LLM-based type inference
- Auto-repair of corrupt JSON into artifacts
- Changing Pipeline Gate stage-outcome semantics (SPEC-040 still owns advance/block)
- Full JSON Schema documents per type (v1 uses registry validators + semantic rules)
- Interactive quarantine → edit → revalidate UI beyond display

## Validation Responsibilities

Validators verify:

- Schema
- Required fields
- Types
- Minimum viable content
- Compatibility
- Version

Examples:

| Type | Semantic rules (v1) |
|---|---|
| ProspectList | ≥1 valid company; company name present; not free-form prose |
| Campaign | Valid Campaign object with prospectCount > 0 |
| SalesIntelligenceProfile | Required company + reasoning / strategy fields |
| MailPackage | Required renderable package / letter outputs |

## Validation Pipeline

```text
Artifact Candidate
      ↓
Identify Type
      ↓
Schema Validation
      ↓
Semantic Validation
      ↓
Compatibility Check
      ↓
PASS → Artifact Bus
FAIL → Remain Plain Text / Quarantine
```

## Failure Modes (reject)

- Natural language
- Mission objectives
- Notes / logs / LLM explanations
- Corrupt JSON
- Wrong schema version
- Unknown artifact type

## Artifact Bus Rule

Only validated artifacts may enter the Artifact Bus as consumable revisions.

Everything downstream may safely assume: “I am receiving the artifact I requested.”

## Review Workspace

Validation failures appear as:

```text
Artifact Validation
ProspectList
FAILED
Reason
  Input is natural language.
  No valid prospect rows detected.
```

## Architecture

| Component | Role |
|---|---|
| `ArtifactValidator` | Boundary validation authority (SPEC-052) |
| `ArtifactRegistry` | Type definitions + schema validators |
| `ArtifactBus` | Publishes only after validator PASS (or explicit quarantine stamp) |
| `OperatorArtifactInjection` | Ingress; NL never auto-injects |
| `PipelineGate` | Stage business outcomes (SPEC-040) — complementary, not replaced |
| Command Deck Review | Surfaces failures; non-executable |

## Implementation Plan

1. ADR-036 Accepted; this spec Implemented thin slice; indexes updated.
2. ArtifactValidator pipeline + NL / semantic helpers.
3. Wire registry + bus + operator detection.
4. Mission deliverables `artifactValidationFailures` + Review Workspace panel.
5. Tests + CHANGELOG / CURRENT_STATE.

## Migration Strategy

- Additive; no DB migration.
- Existing consumable bus revisions unchanged.
- Stricter ingress may stop auto-inject of ambiguous prose pastes (operators use CSV / Import).

## Testing

- Unit: NL mission prose is not ProspectList; CSV with headers still injects
- Unit: unknown type / wrong schema version rejected
- Unit: Campaign / SI / MailPackage minimum semantic failures
- Integration: ArtifactBus does not publish consumable revision for NL payload
- Regression: SPEC-043 CSV paste auto-inject still works

## Acceptance Criteria

- [x] Every artifact declares its schema (registry type + schemaVersion)
- [x] Every artifact passes validation before consumable publication
- [x] Natural language never becomes structured artifacts
- [x] Downstream systems assume validated contracts (capabilities do not re-validate)
- [x] Validation failures remain reviewable but non-executable
- [x] Artifact Bus accepts validated artifacts only (quarantine for gate failures)

## Future Work

- Formal JSON Schema documents exported per type
- Quarantine edit → revalidate operator flow
- Cross-mission compatibility matrix beyond major schema version
