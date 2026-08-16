# SPEC-096 — Max Specialist Direction & Operator Rationale

**Status:** Implemented (v1 thin slice)  
**Initial specialist:** Paige  
**Primary interface:** Max  
**Depends on:** [SPEC-092](SPEC-092_Content_Outcome_Intelligence.md), [SPEC-093](SPEC-093_Paige_Outcome_Learning_Loop.md), [SPEC-094](SPEC-094_Max_to_Paige_Campaign_Content_Delegation.md), [SPEC-095](SPEC-095_Max_Durable_Operator_Objectives.md)

> **Numbering note:** The product brief titled this work "SPEC-095 Specialist Direction". Repository SPEC-095 is already [Max Durable Operator Objectives](SPEC-095_Max_Durable_Operator_Objectives.md), so this work is numbered **SPEC-096**.

## Purpose

When the operator disagrees with a Paige recommendation, Max captures what should change, why, how broadly it applies, and what Paige should learn — without requiring the operator to manage Paige directly or fill out specialist feedback forms.

Flow:

`Paige recommendation → Max → Operator judgment → Max interpretation → Paige refinement → Max → Operator`

## v1 Thin Slice

- **Paige only** — extensible `specialist` field for future agents
- **Max-only surface** — Accept / Discuss with Max on recommendations
- **Durable persistence** — `content_recommendations` + `specialist_directions` tables
- **Operator learnings** — stored in `content_learnings` with `scope.learningSource = operator_direction`
- **Fail closed** — refinement/persistence failures preserve original recommendation state

## Key Files

| File | Role |
|---|---|
| `services/specialistDirection.js` | Interpretation, persistence, refinement orchestration |
| `services/contentLearning.js` | `refineContentRecommendation()`, `createOperatorDirectionLearning()` |
| `services/maxPaigeCampaignDelegation.js` | Persists recommendations; Accept / Discuss actions |
| `packages/max/workspace/SpecialistDirectionContext.js` | Workspace pre-routing adapter |
| `packages/max/workspace/WorkspaceEngine.js` | Wires direction turn before Paige delegation |
| `public/command-deck/command-deck.js` | Accept / Discuss with Max UI actions |
| `migrations/2026-08-16-specialist-direction.sql` | Schema |

## Operator Interaction

Recommendation UI exposes:

- **Accept**
- **Discuss with Max**

Max interprets natural-language feedback into structured direction:

- `disposition`: accept | refine | reject
- `acceptedElements[]` / `changedElements[]`
- `updatedDirection` (what to aim at)
- `rationale` (why — preserved separately)
- `scope`: recommendation_only | experiment_campaign | durable_preference | business_constraint

Scope ambiguity triggers clarification (e.g. "That's too technical.").

## Evidence Hierarchy

Operator direction is authoritative for **intent**. SPEC-092/093 observed evidence remains unchanged. Operator-sourced learnings are scoped and must not become universal rules (e.g. campaign SMB focus ≠ "technical content performs poorly").

## Acceptance Fixture

Original: `AI systems should understand uncertainty before acting.`  
Operator feedback: move toward SMB operators while preserving operator-first framing.  
Expected revision example: `Small business owners shouldn't have to become AI experts to benefit from AI.`

## Deferred

- Multi-specialist orchestration (Penny, Scout, Emmett)
- Autonomous publishing or execution
- Generic operator-memory architecture
- Specialist dashboards

## Tests

- `test/specialistDirection.test.js` — service-layer acceptance criteria
- `packages/max/workspace/tests/specialistDirection.test.js` — workspace adapter routing
