# SPEC-105 — Max Operating Evidence Retrieval

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Max Core Reasoning |
| **Created** | 2026-08-17 |
| **Depends on** | [SPEC-102](SPEC-102_Max_Retrieval_Before_Delegation.md), [SPEC-103](SPEC-103_Durable_Business_Understanding_Retrieval.md), [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md), [SPEC-098](SPEC-098_Max_Specialist_Delegation_Contract.md) |

## Objective

Enable Command Deck Max to retrieve existing business operating evidence before reasoning or recommending when an operator asks about past or current business activity.

This closes the primary gap exposed by Anchor Pilot 0. It does **not** create a new operating-intelligence subsystem. It connects the existing Max workspace retrieval architecture to operating evidence PulseForge already stores and can already read.

## Problem

Anchor Cleaning passed durable business-understanding retrieval (SPEC-103). When asked for an evidence-based inventory of campaigns, prospects, outreach, leads, walkthroughs, and outcomes, Workspace Max answered from the approved CIE Blueprint (`KNOWN` / `INFERENCE` / `UNKNOWN` / `EVIDENCE NEEDED`) and recommended a new acquisition experiment.

Three interacting causes:

1. Workspace Max lacked the legacy AO / Campaign 001 retrieval path.
2. Operating questions bypassed `isHardRetrievalQuestion()`.
3. CIE claimed the turn because it saw business/acquisition concepts.

## Core Rule

When an operator asks a question whose answer depends on existing business activity: **retrieve before recommend.**

Unknown is acceptable. Missing evidence is acceptable. Repeating Blueprint-derived advice instead of inspecting available operating evidence is not.

## Architecture

```text
Operator prompt
    → classify intent
    → operating-evidence? (SPEC-105)
    → retrieve AO / prospects / Scout state / missions / objectives / activity / outcomes
    → compose verified / inferred / not recorded / unavailable
    → investigate through specialist only if genuinely required
    → reason
    → recommend only if requested
```

CIE advisory reasoning must not intercept operating retrieval. Exact integration extends:

- `RetrievalBeforeDelegationContext`
- `OperatingEvidenceRetrieval` (thin composition over existing stores)
- `aoBriefingService`
- Scout `ExistingIntelligence` / acquisition state (read, do not launch)
- mission / objective / activity readers

## Epistemic States

| State | Meaning |
|---|---|
| Verified | Durable evidence directly supports the claim |
| Inferred | Reasonable interpretation, not direct proof |
| Not recorded | Operator may know it; PF has no durable record |
| Unavailable | Relevant data may exist in a source Max cannot currently access |

Campaign layers stay distinct: intent, execution, observation, outcome, learning. Target lists, AO seed notes, and mission artifacts do **not** prove Campaign 001 was mailed.

## Out of Scope

- New persistence for physical mail, Yelp, walkthrough events, campaign learning, or operator-reported events
- CIE semantic contamination fixes
- A parallel Operating Intelligence Engine

## Testing

`packages/max/workspace/tests/operatingEvidenceRetrieval.test.js` and `test/operatingEvidenceRetrieval.test.js`.

## Acceptance Criteria

- [x] Operating-state questions classify as retrieval
- [x] CIE does not claim explicit operating-evidence inventory turns
- [x] Existing AO Campaign 001 evidence reaches Workspace Max
- [x] Existing prospects retrieve without launching Scout
- [x] Missions/objectives are not treated as executed outcomes
- [x] Activity/touchpoints are tenant-scoped
- [x] Campaign intent does not imply mail execution
- [x] Recommendation questions retrieve operating evidence first
- [x] Anchor retrieval cannot leak client 1 or 11
- [x] Missing tenant context fails closed
- [x] SPEC-103 business-understanding questions still use CIE/Blueprint retrieval
