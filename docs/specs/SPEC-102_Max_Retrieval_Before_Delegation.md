# SPEC-102 — Max Retrieval Before Delegation

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Max Core Reasoning |
| **Created** | 2026-08-17 |
| **Depends on** | [SPEC-098](SPEC-098_Max_Specialist_Delegation_Contract.md), [SPEC-099](SPEC-099_Client_Experience_Convergence.md), [SPEC-099A](SPEC-099A_Scout_Investigation_Provenance.md), [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md), [SPEC-100A](SPEC-100A_Scout_Acquisition_Discovery_Foundation.md) |

> **Numbering note:** The product brief called this SPEC-101. Repository SPEC-101 is [Max Specialist Result Interrogation](SPEC-101_Max_Specialist_Result_Interrogation.md). This retrieval-before-delegation layer is **SPEC-102**.

> **Training framework:** This spec graduates the **Retrieve Before Delegation** competency under [SPEC-102F Max Development Framework](SPEC-102F_Max_Development_Framework.md).

## Objective

Teach Max a fundamental management behavior: **do not delegate work you can already answer.**

Delegation is expensive. Thinking is cheaper. Memory retrieval is cheaper still.

```text
Memory → Reasoning → Specialist → Execution
```

not

```text
Question → Scout
```

## Problem

Max routed questions toward specialists too aggressively. The production failure:

> Operator: What do you currently understand about our service area?
> Max delegated to Scout.

Scout discovers markets. He does not remember what Max already knows.

Session stickiness after a Scout turn (`acquisitionLoop` / `lastScoutEvaluation`) made *any* later question look like acquisition work. Topic words such as "cleaning" were enough to re-invoke Scout.

## Scope

1. Cognitive-mode classification before any specialist routing
2. Retrieval-before-delegation gate for every specialist
3. Durable-knowledge inspection (Blueprint → Playbook → KG → mission → prior investigations → briefing → conversation)
4. Investigation threshold before creating a new delegation
5. Conversation continuity: after specialist work, default toward retrieval / explanation / reflection
6. Unknown is acceptable — never invent specialist work to fill a gap
7. Tests for the acceptance questions

## Out of Scope

- Changing Scout geography resolution
- Wiring new specialists
- A new memory store or knowledge-graph product
- Command Deck visual redesign
- Replacing SPEC-101 interrogation answers

## Cognitive modes

| Mode | Examples | Delegation |
|---|---|---|
| Retrieval | What do you know about Anchor? What is our service area? Who is Aji? | Never |
| Explanation | Why did you recommend that? Why didn't you elevate Acquisition? | Never |
| Reflection | Do you trust Scout? What are you uncertain about? | Never |
| Investigation | Find opportunities. Investigate property managers. Research competitors. | Specialist if necessary |
| Recommendation | What should we do next? Should we target property managers? | Retrieve first; investigate only if evidence is insufficient |
| Planning | Help me build Campaign 3. Create a rollout plan. | Internal reasoning / existing mission path |
| Execution | Send this email. Approve this recommendation. Launch campaign. | May delegate |

Questions that must never automatically invoke Scout: *What do we know… / What is… / Why… / When… / Who… / Explain… / Summarize… / Compare… / Reflect…*

## Investigation threshold

Only invoke a specialist when one of these is true:

- New external information is required
- Current knowledge is stale
- Coverage is insufficient
- Operator explicitly requested investigation
- A specialist possesses unique capability unavailable to Max

Otherwise stay inside Max.

## Architecture

```text
Operator message
      ↓
Classify cognitive mode
      ↓
SPEC-101 interrogation (inspect existing specialist work)
      ↓
SPEC-102 retrieval (answer from durable knowledge, or "I don't currently know.")
      ↓
Need a specialist?
   /            \
 yes             no
  ↓               ↓
Scout / Paige    CIE / reasoning / mission
```

## Implementation

| File | Role |
|---|---|
| `packages/max/specialistDelegation/CognitiveMode.js` | Intent classification |
| `packages/max/specialistDelegation/RetrievalGate.js` | Delegation threshold |
| `packages/max/workspace/RetrievalBeforeDelegationContext.js` | Pre-specialist workspace hook |
| `packages/max/workspace/WorkspaceEngine.js` | Retrieval before Scout |
| `packages/max/workspace/ScoutAcquisitionContext.js` | Scout entry respects the gate |
| `packages/max/scoutAcquisition/NeedAssessment.js` | Session stickiness no longer auto-delegates |
| `services/maxPaigeCampaignDelegation.js` | Same gate for Paige |

## Testing

- `test/retrievalBeforeDelegation.test.js`
- `packages/max/workspace/tests/retrievalBeforeDelegation.test.js`

## Acceptance Criteria

- [x] "What do you understand about our service area?" never invokes a specialist
- [x] "What do you know about Anchor?" never invokes a specialist
- [x] "Why didn't you elevate Acquisition?" never invokes a specialist
- [x] "What did Scout investigate?" never invokes a specialist
- [x] "What are you uncertain about?" never invokes a specialist
- [x] "Find commercial cleaning opportunities." may invoke Scout
- [x] "Investigate property managers." may invoke Scout
- [x] "Research competitors." may invoke Scout
- [x] "Look for expansion signals." may invoke Scout
- [x] After specialist work, subsequent questions default toward retrieval rather than repeating delegation
- [x] If retrieval fails, Max says he doesn't currently know — he does not call Scout
- [x] Gate applies to every specialist, not only Scout

## Future Work

- Richer knowledge-graph inspection when that store exists
- Recommendation evidence scoring beyond explicit investigation verbs
- Operator-facing copy refinements after live Anchor diagnosis
