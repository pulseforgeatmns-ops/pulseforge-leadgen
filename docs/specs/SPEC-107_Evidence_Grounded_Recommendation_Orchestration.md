# SPEC-107 — Evidence-Grounded Recommendation Orchestration

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical — Anchor Pilot 0 |
| **Owner** | Max Core Reasoning |
| **Created** | 2026-08-17 |
| **Depends on** | [SPEC-103](SPEC-103_Durable_Business_Understanding_Retrieval.md), [SPEC-105](SPEC-105_Max_Operating_Evidence_Retrieval.md), [SPEC-106](SPEC-106_Operator_Reported_Operating_Evidence.md) |

## Objective

When an operator asks for a recommendation grounded in existing operating evidence, Max must retrieve that evidence and then reason over it. Retrieval is a prerequisite to recommendation, not a replacement for it.

No new recommendation engine. No autonomous execution. No hard-coded Emmett or Anchor conclusion.

## Problem

Anchor Pilot 0 asked:

> Given that update and what PulseForge already knows about Campaign 001, what should I focus on next to build the repeatable commercial pipeline?

Max retrieved Campaign 001, AO leads, prospects, Scout inventory, operator-attested mail, and planned follow-up — then returned the inventory and said "Ask for a recommendation only after reviewing this inventory."

Reasoning reported: `Classified operator intent as retrieval.`

The operator had already asked for a recommendation.

## Audit (production path)

For the production prompt, before this spec:

| Step | Result |
|---|---|
| Cognitive mode | `retrieval` via `operating_evidence` — `looksLikeExistingEvidenceRetrieval` ran before recommendation classification |
| Hard retrieval | true (`shouldRetrieveOperatingEvidence`) |
| SPEC-105 | `isOperatingEvidenceQuestion` true (`what PulseForge already knows`); `isOperatingGroundedRecommendation` false (phrase not in the grounded-rec regex; `what should I focus` was not a recommendation verb) |
| Recommendation classification | never reached |
| CIE | does not claim (`shouldRetrieveOperatingEvidence`) |
| Handler | `maybeHandleOperatingEvidenceTurn` |
| Early return | `recommend=false` → inventory compose + `nextInvestigations: Ask for a recommendation only after reviewing this inventory.` |
| Existing machinery | `composeRecommendationFromEvidence` could consume the bundle, but was not invoked |
| Capability/policy | not loaded into the recommendation path |

## Core Rule

```text
RETRIEVE → REASON → RECOMMEND
```

A turn may have `primaryIntent = recommendation` with `requiresOperatingRetrieval = true`. Those concepts are not mutually exclusive.

Pure retrieval ("What's the current state of Campaign 001?") still returns inventory.

## Architecture

Exact integration extends:

- `CognitiveMode` — compound recommendation+retrieval keeps recommendation as primary intent
- `OperatingEvidenceRetrieval` — SPEC-105 retrieval reused as reasoning context
- `OperatingStateRecommendation` — reasons over the retrieved bundle + capability/policy
- `RetrievalBeforeDelegationContext` — inventory-only early return only when recommendation intent is absent

CIE remains authoritative for durable business understanding and must not swallow an evidence-grounded recommendation after operating evidence is retrieved.

## Capability / policy

Existing `clients.enabled_agents` and `clients.autosend_enabled` (plus optional readiness) are read, not mutated. Statuses: available / disabled / blocked / not_ready / unknown.

Max may recommend *evaluating* activation. It must not claim it can send immediately, and it must not execute.

## Out of Scope

- New recommendation engine or memory system
- CIE onboarding changes
- SPEC-106 persistence semantics
- Enabling Emmett, autosend, campaigns, or Scout
- Hard-coded Anchor / Emmett / Campaign 001 conclusions

## Testing

`packages/max/workspace/tests/evidenceGroundedRecommendation.test.js` and `test/evidenceGroundedRecommendation.test.js`.

## Acceptance Criteria

- [x] Pure retrieval still returns inventory
- [x] Compound recommendation retrieves, then recommends
- [x] Highest-leverage reasoning uses retrieved state (not a hard-coded Emmett answer)
- [x] Thin prospect supply does not mechanically recommend email activation
- [x] Active email motion seeks another constraint
- [x] Planned follow-up is not reported as completed
- [x] Disabled outbound policy is stated; no immediate-send claim
- [x] No autonomous action
