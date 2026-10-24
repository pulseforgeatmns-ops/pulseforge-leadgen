# SPEC-056 — Evidence-Driven Capability Planning

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Completed** | 2026-07-28 |
| **Depends on** | SPEC-041, SPEC-050, SPEC-051, SPEC-054, SPEC-055; ADR-034, ADR-038, ADR-039 |
| **ADR** | [ADR-040 Separate Evidence Acquisition from Capability Selection](../adr/ADR-040_Separate_Evidence_Acquisition_from_Capability_Selection.md) |

## Objective

Enable Mission Planning to determine what evidence is required to satisfy the operator's intent before selecting capabilities.

Operators ask questions. The planner determines what evidence must exist to answer them. Capabilities are selected to acquire missing evidence.

## Vision References

- [ADR-040 Separate Evidence Acquisition from Capability Selection](../adr/ADR-040_Separate_Evidence_Acquisition_from_Capability_Selection.md)
- [ADR-039 Separate Understanding from Execution](../adr/ADR-039_Separate_Understanding_from_Execution.md)
- [ADR-038 Explain Planning Decisions](../adr/ADR-038_Explain_Planning_Decisions.md)
- [ADR-034 Intent Before Execution](../adr/ADR-034_Intent_Before_Execution.md)
- [SPEC-055 Intent Understanding](SPEC-055_Intent_Understanding.md)
- [SPEC-054 Capability Registry & Planner Diagnostics](SPEC-054_Capability_Registry_and_Planner_Diagnostics.md)
- [SPEC-051 Artifact Resolution and State-Aware Planning](SPEC-051_Artifact_Resolution_and_State_Aware_Planning.md)

## Problem

SPEC-055 correctly understands operator intent.

Example:

> Operator: "Why did Campaign 001 fail?"
>
> MissionIntent: Campaign Diagnostics

The planner previously jumped straight to Campaign Review + Outcome Intelligence. Those capabilities consume existing evidence. They cannot explain failures whose evidence has never been produced.

Result: Discovery returned zero verified companies. No explanation exists because no diagnostic evidence exists.

## Design Principle

Execution plans should be driven by evidence requirements, not merely intent classification.

| Stage | Answers |
|---|---|
| Intent Understanding | What does the operator want? |
| Evidence Planning | What information is required to answer that question? |
| Capability Planning | How do we acquire missing evidence and complete the goal? |

## Three-Stage Planning

```text
Operator
  ↓
Intent Understanding
  ↓
MissionIntent
  ↓
Evidence Planning
  ↓
EvidencePlan
  ↓
Capability Planning
  ↓
MissionPlan
  ↓
Execution
```

## Evidence Requirements

MissionIntent declares required evidence (descriptive — does not choose capabilities).

Example:

```text
MissionIntent
  goal: Campaign Diagnostics
  requiresEvidence:
    - DiscoveryExecution
    - DiscoveryTrace
    - DiscoveryDiagnostics
    - MissionState
```

## Evidence Planner

| Input | Role |
|---|---|
| MissionIntent | Required evidence list |
| Artifact Catalog | What is already available |
| Capability Registry | Who can produce missing evidence |

| Output | Role |
|---|---|
| EvidencePlan | `available`, `missing`, `acquisitions`, `blocked`, counts |

## Capability Selection

Capabilities are selected only after evidence planning.

Instead of jumping to Campaign Review, the planner prepends diagnostic producers for missing evidence:

```text
Discovery Diagnostics → Campaign Review → Outcome Intelligence
```

## Diagnostic Artifacts

Read-only diagnostic artifact types (never mutate business state):

- DiscoveryExecution
- DiscoveryTrace
- DiscoveryDiagnostics
- CapabilityExecution
- CapabilityFailure
- MissionDiagnostics
- MissionState
- ProviderSelection
- CandidateCounts
- VerificationResults
- Exceptions

## Diagnostic Capabilities

Capabilities may expose diagnostics independently of execution.

| Capability | Produces |
|---|---|
| Discovery | ProspectList |
| Discovery Diagnostics | DiscoveryTrace, DiscoveryDiagnostics, DiscoveryExecution, … |

Discovery Diagnostics may inspect logs, replay execution metadata, or explain failures without rebuilding the campaign.

## Planner Rules

1. If evidence required by MissionIntent is missing → planner MUST attempt to acquire it.
2. Planner MUST NOT answer diagnostics using incomplete evidence.
3. If evidence cannot be acquired → report `Unable to answer` with missing evidence + reason (e.g. no registered producer) — never invent a false “Discovery failed” narrative.

## Review Workspace

New section: **Evidence Requirements**

Shows each required type as Available / Acquired / Blocked (with reason).

## Architectural Boundary

| Layer | Owns |
|---|---|
| Intent Understanding | Language |
| Evidence Planning | Questions / information needs |
| Capability Planning | Execution selection |
| Capabilities | Work |

No capability performs planning.

## Scope (v1)

- `MissionIntent.requiresEvidence` declared per intent category
- `EvidencePlan` + `EvidencePlanner` between Understanding and Capability Planning
- Diagnostic artifact types in Artifact Registry (read-only flag)
- `Discovery Diagnostics` capability (read-only producer)
- Capability Planner merges diagnostic acquisitions before review/outcome stages
- Incomplete-evidence / no-producer → blocked “Unable to answer” draft
- Review Workspace Evidence Requirements panel
- Existing deterministic execution paths unchanged for non-diagnostic intents

## Out of Scope

- Full historical log replay across Railway deploys
- Interactive evidence picker UI
- Mutating “fix Discovery” from diagnostic mode
- LLM-authored evidence requirement inference

## Acceptance Criteria

- [x] MissionIntent declares required evidence
- [x] Evidence Planning executes before Capability Planning
- [x] Missing evidence automatically schedules diagnostic capabilities
- [x] Diagnostic capabilities produce typed diagnostic artifacts
- [x] Planner never answers diagnostics using incomplete evidence
- [x] Review Workspace displays evidence acquisition
- [x] Existing deterministic execution remains unchanged
- [x] Diagnostic capabilities are read-only and cannot mutate business state

## Tests

`npm run test:mission` — `evidencePlanning.test.js` (+ existing intent / deterministic suites)
