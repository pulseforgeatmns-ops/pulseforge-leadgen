# SPEC-108 — Claim Grounding Competency Graduation

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical — Anchor Pilot 0 |
| **Owner** | Max |
| **Created** | 2026-08-18 |
| **Depends on** | [SPEC-102](SPEC-102_Max_Retrieval_Before_Delegation.md), [SPEC-102F](SPEC-102F_Max_Development_Framework.md), [SPEC-103](SPEC-103_Durable_Business_Understanding_Retrieval.md), [SPEC-104](SPEC-104_Persistent_Operator_Context.md), [SPEC-105](SPEC-105_Max_Operating_Evidence_Retrieval.md), [SPEC-106](SPEC-106_Operator_Reported_Operating_Evidence.md), [SPEC-107](SPEC-107_Evidence_Grounded_Recommendation_Orchestration.md), [SPEC-107A](SPEC-107A_Recommendation_Claim_Grounding.md) |

## Purpose

Graduate claim grounding from an implementation detail into a permanent reasoning competency.

SPEC-107A taught Max to retract an unsupported operating-state claim when challenged during an email recommendation. SPEC-108 generalizes that behavior into a transferable cognitive capability that applies across every recommendation domain.

This specification does **not** introduce persistent memory. Its purpose is to ensure Max reasons correctly before new knowledge systems are added.

## Problem

Pilot 0 exposed an architectural failure mode: Max may correctly retrieve information while incorrectly reasoning about operating state.

Examples:

- treating scheduled work as completed
- treating inventory as execution
- treating objectives as reality
- treating assumptions as observations

A reasoning system must distinguish observed evidence, inferred state, planned future state, and operator assumptions before producing recommendations.

## Design Principle

**Recommendations may only depend on supported operating-state claims.**

Whenever a recommendation requires an assertion about current business state, Max must first determine whether that assertion is supported by available evidence.

## Operating State Evaluation

Every operating-state claim is evaluated before recommendation generation.

| Classification | Meaning | Result |
|---|---|---|
| **Supported** | Evidence directly supports the claim | Recommendation proceeds |
| **Partially supported** | Evidence supports only part of the claim | Recommendation is qualified |
| **Unsupported** | No evidence supports the claim | Claim is excluded; recommendation is rebuilt from supported information |

Example (partially supported):

> I know a follow-up has been scheduled. I do not currently have evidence that it has occurred.

## Challenge Handling

When an operator challenges a recommendation (`That isn't true.`), Max identifies which operating-state claim produced the recommendation and evaluates that claim.

| Outcome | When | Behavior |
|---|---|---|
| **Confirm** | Evidence supports the claim | Keep the claim. Present supporting evidence. Do not retract. |
| **Qualify** | Evidence partially supports the claim | Admit overstatement. Separate planned/inventory/objective from observed state. |
| **Retract** | Evidence does not support the claim | Drop the claim. State the distinction that was violated. |
| **Revise** | The retracted premise was material | Rebuild the recommendation from supported claims only. Never merely apologize. |

## Architecture

Exact integration extends:

- `ClaimGrounding` — domain-general support classification (`supported` / `partially_supported` / `unsupported`) and challenge framing. Topic evaluators cover email motion, follow-up, outreach, inventory, objectives, and campaign completion. Unknown topics fall through a generic evaluator; email is not the default.
- `OperatingStateRecommendation` — evaluates operating-state claims before `reasonOverOperatingState`. Unsupported current-execution claims cannot drive the recommendation. Retracted `outreach_begun` revises to first outreach.
- `RecommendationClaimChallenge` — identifies the challenged claim from the operator turn or `lastClaim`, then confirm / qualify / retract / revise. Session working model only.
- Competency Registry — `claim_grounding` graduated 2026-08-18.

Distinctions encoded:

```text
planned != completed
inventory != execution
goals != operating state
historical != current
mission != execution
```

## Transfer Scenarios

| Scenario | Evidence | Incorrect inference | Expected |
|---|---|---|---|
| 1. Email activity | SPEC-107A fixture | Outbound email is already active | Retract. Revise. |
| 2. Planned ≠ completed | Follow-up scheduled tomorrow | Follow-up occurred | Retract. Revise. |
| 3. Inventory ≠ execution | 67 prospects discovered | Outreach has begun | Retract. Explain distinction. Recommend first outreach. |
| 4. Goals ≠ operating state | Blueprint: acquire twenty commercial clients | You are expanding your commercial business | Qualify. Clarify objective versus observed state. |
| 5. Supported remains supported | Campaign complete. Delivery logs. | Operator: "That's incorrect." | Confirm. Present evidence. Do not retract. |

## Competency Graduation

Registry id: `claim_grounding`

> Before recommendations are produced, operating-state claims are evaluated against available evidence and classified as supported, partially supported, or unsupported. Operator challenges trigger confirmation, qualification, revision, or retraction without fabrication of evidence.

## Testing

- `packages/max/workspace/tests/claimGrounding.test.js`
- `test/claimGrounding.test.js`
- Existing SPEC-107A regressions remain required:
  - `packages/max/workspace/tests/recommendationClaimGrounding.test.js`
  - `test/recommendationClaimGrounding.test.js`

## Explicit Non-Goals

This specification does **not**:

- persist operator corrections
- modify SPEC-106 knowledge events
- create new memory systems
- alter Operator Context persistence
- introduce graph learning

Those belong to future knowledge-layer specifications.

## Acceptance Criteria

- [x] Email challenge regression retained
- [x] Planned versus completed retracts and revises
- [x] Inventory versus outreach retracts, explains, and recommends first outreach
- [x] Goal versus reality qualifies objective against observed state
- [x] Supported campaign-complete claim is confirmed with evidence
- [x] `claim_grounding` registered and graduated in the Competency Registry
- [x] Workspace and shared recommendation tests pass
- [x] SPEC-107A marked Completed
- [x] Recommendation path is not email-default
