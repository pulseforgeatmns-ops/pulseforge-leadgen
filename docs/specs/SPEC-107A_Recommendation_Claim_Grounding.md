# SPEC-107A — Recommendation Claim Grounding & Challenge

| Field | Value |
|---|---|
| **Status** | Completed |
| **Target Version** | v0.1 |
| **Priority** | Critical — Anchor Pilot 0 |
| **Owner** | Max Core Reasoning |
| **Created** | 2026-08-18 |
| **Completed** | 2026-08-18 |
| **Succeeded by** | [SPEC-108](SPEC-108_Claim_Grounding_Competency_Graduation.md) |
| **Depends on** | [SPEC-105](SPEC-105_Max_Operating_Evidence_Retrieval.md), [SPEC-106](SPEC-106_Operator_Reported_Operating_Evidence.md), [SPEC-107](SPEC-107_Evidence_Grounded_Recommendation_Orchestration.md) |

## Objective

Close the epistemic-grounding failure exposed immediately after SPEC-107.

Every material operating-state premise used in a recommendation must be grounded in retrieved evidence or explicitly labeled as inference. When the operator challenges a specific claim, Max must retrieve evidence relevant to that claim, then confirm, qualify, or retract — and revise the recommendation if the retracted premise was material.

No new reasoning engine. No new memory subsystem. No autonomous execution.

## Problem

SPEC-107 established `retrieve → reason → recommend`. Max then introduced an unsupported operating-state premise:

> An outbound email motion is already active.

Available evidence showed prospect inventory, Scout companies, Campaign 001 AO activity, historical touchpoints, Emmett disabled, and autosend disabled. None of those facts prove a currently active outbound email motion.

When challenged — "What evidence in PulseForge tells you that?" — the turn was treated as inventory/CIE retrieval and restated the operating inventory instead of retracting the claim.

## Core Rule

```text
existence != enabled
enabled != ready
planned != active
historical activity != current activity
mission != execution
prospect inventory != outreach
expected != completed
```

Max must preserve those distinctions. Inference is allowed. Presenting inference as operating fact is not.

## Architecture

Exact integration extends:

- `OperatingStateRecommendation` — `assessEmailMotion()` treats current execution only from an explicit current-execution flag or current-status rows. Historical touchpoints are historical. Email missions are planned/intent. Emmett absent from `enabled_agents` is disabled.
- `RecommendationClaimChallenge` — classifies targeted claim challenges and operator corrections; evaluates the challenged claim; composes confirm / qualify / retract; keeps a session working model only.
- `CognitiveMode` — claim challenges classify as `explanation` via `claim_challenge` before operating-inventory retrieval.
- `RetrievalBeforeDelegationContext` — challenge path loads claim-relevant evidence and does not return the SPEC-105 inventory dump.
- Session `context.lastRecommendation` / `retractedPremises` / `operatorDeniedEmailActive` — working-model correction only. Not SPEC-106 durable operating memory. Max-generated statements are never persisted as operating fact.

CIE must not swallow a claim challenge. SPEC-106 must not persist "No, email outbound isn't running" as a campaign event.

## Out of Scope

- SPEC-106 operator-memory semantics
- Rebuilding SPEC-105
- A truth engine or ML
- Enabling Emmett, autosend, or campaigns
- Autonomous corrections
- Hard-coded Anchor / Emmett conclusions

## Testing

`packages/max/workspace/tests/recommendationClaimGrounding.test.js` and `test/recommendationClaimGrounding.test.js`.

## Acceptance Criteria

- [x] Supported mailed-August-6 claim traces to SPEC-106 operator-attested evidence
- [x] Unsupported active-email claim is retracted without an inventory dump
- [x] Historical email sends are not called current
- [x] Email missions are labeled planned/intent, not execution
- [x] Emmett excluded from enabled agents is described as disabled
- [x] Retracting an unsupported active-email premise revises the recommendation
- [x] Operator correction is accepted as working-model correction, not a SPEC-106 write
- [x] No autonomous action
