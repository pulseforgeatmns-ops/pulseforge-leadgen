# SPEC-109 — Intent-Bound Response Selection

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical — Anchor Pilot 0 |
| **Owner** | Max Core Reasoning |
| **Created** | 2026-08-18 |
| **Depends on** | [SPEC-102](SPEC-102_Max_Retrieval_Before_Delegation.md), [SPEC-102F](SPEC-102F_Max_Development_Framework.md), [SPEC-103](SPEC-103_Durable_Business_Understanding_Retrieval.md), [SPEC-105](SPEC-105_Max_Operating_Evidence_Retrieval.md), [SPEC-107](SPEC-107_Evidence_Grounded_Recommendation_Orchestration.md), [SPEC-107A](SPEC-107A_Recommendation_Claim_Grounding.md), [SPEC-108](SPEC-108_Claim_Grounding_Competency_Graduation.md) |
| **ADR** | [ADR-046 Intent Determines Response Structure](../adr/ADR-046_Intent_Determines_Response_Structure.md) |

## Objective

Ensure Max's response structure is determined by the operator's intent **before** advisory reasoning begins.

Retrieval requests retrieve. Summaries summarize. Challenges challenge. Recommendations recommend. Advice is not a universal response type.

## Problem

Current flow after SPEC-107 resembles:

```text
Retrieve evidence
        ↓
Blueprint advisory
        ↓
Return recommendation
```

That works for recommendation requests. It fails for retrieval requests because the recommendation replaces the requested answer.

Example:

> Operator: What have we completed recently?
>
> Current: I'd recommend proving a repeatable acquisition motion...
>
> Expected: Recently completed: Campaign 001... (optional recommendation last)

## Design Principle

Operator intent determines **response structure**.
Evidence determines **content**.
Reasoning determines **recommendations**.

Those are separate stages.

## Pipeline

```text
Operator
     ↓
Intent Classification
     ↓
Response Contract Selection
     ↓
Retrieve evidence
     ↓
Ground claims
     ↓
Reason
     ↓
Delegate to specialists (if needed)
     ↓
Compose response according to contract
```

The response contract is selected **before any specialist delegation**.

## Response Contract Registry

Rather than scattered if-statements, Max selects one of:

| Contract | Required | Optional | Forbidden |
|---|---|---|---|
| **Retrieval** | verified state, unknowns | evidence | unsolicited strategy, acquisition recommendations |
| **Summary** | observed operating state, goals, unknowns | recommendations (last) | — |
| **Recommendation** | current state, reasoning, recommendation, confidence, evidence | — | — |
| **Challenge** | claim identified, evidence reviewed, revision, updated recommendation | — | — |
| **Investigation** | known, need specialist?, expected outputs | — | answering from unsupported memory |

Recommendations appear only when the operator asked **or** the contract explicitly permits an optional recommendation. Even then: **answer first, advise second**.

Desk work (preparation-only canary, fillable table, packet review) is not a retrieval contract. Those turns keep their existing desk handlers. Status questions such as `When was Campaign 001 mailed?` still use focused operating-evidence answers under the Retrieval contract.

## Scope

1. Response contract registry (`RetrievalContract`, `SummaryContract`, `RecommendationContract`, `ChallengeContract`, `InvestigationContract`)
2. Contract selection immediately after cognitive-mode classification
3. Composition according to the selected contract
4. CIE / Scout must not swallow retrieval, summary, challenge, or investigation as Blueprint advisory

## Out of Scope

- New recommendation engine
- Durable assimilation of operator corrections (still after SPEC-108)
- Changing SPEC-105 inventory semantics for existing inventory prompts
- Autonomous execution

## Architecture

Exact integration extends:

- `ResponseContract` — registry, `selectResponseContract()`, `composeAccordingToContract()`
- `CognitiveMode` — summary and completed-retrieval classification
- `RetrievalBeforeDelegationContext` — select contract before retrieve / compose
- `OperatingEvidenceRetrieval` — fill contract sections from retrieved evidence
- `WorkspaceEngine` — contract selected before CIE or Scout
- `ScoutAcquisitionContext` — investigation contract wraps specialist work
- Competency Registry — `intent_bound_response_selection`

## Testing

- `packages/max/workspace/tests/intentBoundResponseSelection.test.js`
- `test/intentBoundResponseSelection.test.js`

## Acceptance Criteria

- [x] Retrieval (`What outreach has already been sent?`) returns operating state and does not immediately recommend strategy
- [x] Summary (`How is Anchor Cleaning doing?`) separates observed state, goals, and unknowns; recommendation optional and last
- [x] Recommendation (`What should we do next?`) is primary
- [x] Challenge (`That's incorrect.`) revises reasoning
- [x] Investigation (`Investigate commercial prospects.`) creates investigation and does not answer from unsupported memory
- [x] Response contract is selected before specialist delegation

## Future Work

- Presentation-layer formatting of contract sections in the Workspace UI
- Additional contracts (planning, execution confirmation) if those intents need the same split
