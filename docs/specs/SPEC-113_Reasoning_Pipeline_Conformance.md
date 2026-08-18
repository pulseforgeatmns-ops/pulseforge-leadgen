# SPEC-113 — Reasoning Pipeline Conformance

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical — Anchor Pilot 0 |
| **Owner** | Max Core Reasoning |
| **Created** | 2026-08-18 |
| **Depends on** | [SPEC-111](SPEC-111_Operator_Intent_Taxonomy.md), [SPEC-110](SPEC-110_Business_Intelligence_Synthesis.md), [SPEC-109](SPEC-109_Intent_Bound_Response_Selection.md), [SPEC-108](SPEC-108_Claim_Grounding_Competency_Graduation.md), [SPEC-107](SPEC-107_Evidence_Grounded_Recommendation_Orchestration.md), [SPEC-105](SPEC-105_Max_Operating_Evidence_Retrieval.md) |
| **ADR** | [ADR-050 Single Governed Reasoning Pipeline](../adr/ADR-050_Single_Governed_Reasoning_Pipeline.md) |

Repository SPEC-112 / ADR-049 remain Acquisition Intelligence Model. This pipeline work is numbered **SPEC-113**.

## Objective

Ensure every operator interaction enters one governed reasoning pipeline.

No legacy reasoning path may bypass Intent Classification, Analysis Mode, Response Contract, Retrieval, Claim Grounding, Business Intelligence, or Response Composition.

## Problem

Max demonstrated two operator-facing reasoning behaviors:

1. **Governed pipeline** — Business Intelligence, Unknown Analysis, Risk Assessment, Recommendation, Evidence.
2. **Legacy Blueprint Advisory** — essays such as "I'd start by proving a repeatable commercial acquisition motion..."

The legacy path originated in Client Intelligence / Blueprint advisory synthesis. It produced inconsistent operator experiences and treated Blueprints as reasoning engines.

## Design Principle

There shall be exactly one operator reasoning pipeline.

Blueprints become evidence. Specialists produce intelligence. Max is the sole orchestrator. Only `ResponseContract` composes operator-facing reasoning.

Unknown intent fails toward Retrieval or Summary, never Recommendation or Blueprint Advisory.

## Canonical Pipeline

```text
Operator Request
        ↓
Intent Classification
        ↓
Analysis Mode Selection
        ↓
Response Contract
        ↓
Evidence Retrieval
        ↓
Claim Grounding
        ↓
Business Intelligence Synthesis
        ↓
Reasoning
        ↓
Response Composition
```

## Isolation Rules

1. **Entry points** — every operator-facing reasoning path routes into `ReasoningPipeline.bindGovernedReasoning` then retrieval/composition, or is removed.
2. **Blueprint isolation** — approved Blueprints inform goals, desired state, and business understanding. They do not independently generate recommendations.
3. **Specialist isolation** — Scout, Paige, Rex, Emmett, and Cal produce intelligence. None compose final operator reasoning responses.
4. **Single composer** — `ResponseContract` (`COMPOSER_ID`) assembles operator responses. Specialists never determine response structure.
5. **Analysis mode enforcement** — every governed request selects an Analysis Mode before retrieval. No default Blueprint fallback.

## Logging

Every governed response exposes internally:

- Intent
- Analysis Mode
- Response Contract
- Evidence Count
- Grounded Claims
- Business Intelligence Objects
- Reasoning Components
- Composer

Stored on `structured.metadata.pipelineLog`.

## Acceptance

| Prompt | Analysis Mode |
|---|---|
| How is Anchor doing? | Summary |
| What should I do next? | Recommendation |
| What's preventing growth? | Diagnosis |
| What don't we know? | Unknown Analysis |
| What's risky? | Risk Assessment |
| What outreach has been sent? | Retrieval |
| Should Scout investigate? | Investigation |

No prompt may invoke Blueprint Advisory directly.

## Scope

1. `ReasoningPipeline.js` — single bind + pipeline log
2. WorkspaceEngine uses the bind before retrieval
3. CIE advisory synthesis is a provider, not an operator-facing responder
4. Retrieval no longer returns null for classified recommendations so CIE cannot intercept
5. Plan continuity (decompose, step select/advance) uses ActiveClientReasoning as evidence inside the same pipeline
6. Retrieval yields Paige / operator-objective content asks so specialists remain providers, not competing composers
7. Unknown-intent Retrieval fallback is limited to client-business reasoning (not desk, mission, or planning chatter)
8. Competency `reasoning_pipeline_conformance`

## Out of Scope

- Workspace UI rendering of pipeline log
- New recommendation engine
- Autonomous execution
- Durable assimilation of operator corrections

## Testing

- `packages/max/workspace/tests/reasoningPipelineConformance.test.js`
- `test/reasoningPipelineConformance.test.js`

## Acceptance Criteria

- [x] One reasoning pipeline
- [x] Blueprint Advisory is no longer operator-facing
- [x] All response contracts are honored for the audit prompt table
- [x] All analysis modes route correctly
- [x] No legacy shortcut produces an operator reasoning response
