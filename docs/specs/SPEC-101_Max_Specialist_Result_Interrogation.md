# SPEC-101 — Max Specialist Result Interrogation & Cognitive Trace

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Created** | 2026-08-17 |
| **Depends on** | [SPEC-098](SPEC-098_Max_Specialist_Delegation_Contract.md), [SPEC-099A](SPEC-099A_Scout_Investigation_Provenance.md), [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md), [SPEC-100A](SPEC-100A_Scout_Acquisition_Discovery_Foundation.md) |

> **Numbering note:** The product brief called this SPEC-100A. Repository SPEC-100A is [Scout Acquisition Discovery Foundation](SPEC-100A_Scout_Acquisition_Discovery_Foundation.md). This interrogation and cognitive-trace layer is **SPEC-101**.

## Objective

Make completed specialist work conversationally inspectable by Max.

A specialist result is not a terminal response. It becomes part of Max's inspectable cognitive history. Delegation does not transfer accountability for understanding the work.

## Problem

SPEC-100's first production test exposed a missing capability. After Scout reported `Geography could not be resolved`, the operator asked why and what geography Max had given Scout. Max repeated the previous result instead of answering the diagnostic question.

Max possessed a specialist result but could not reliably reason about the work that produced it.

## Scope

1. Cognitive-trace projection over existing SPEC-098/099A/100 records
2. Persist available vs supplied vs consumed context
3. Follow-up intent recognition (interrogation vs new investigation)
4. Recent specialist referent resolution
5. Trace retrieval before domain routing
6. Deterministic failure-boundary classification where the trace permits
7. Inspectable Max evaluation, distinct from the specialist result
8. Evidence-layer semantics (business / investigation / system / Max judgment)
9. No unnecessary specialist rerun
10. Generic contract for current and future specialists
11. Tenant-scoped durable retrieval
12. Light inspection surface on existing INVESTIGATION · PROVENANCE

## Out of Scope

- Fixing Scout geography resolution
- Hardcoding Manchester
- Changing candidate discovery
- Adding Faye, Link, Ivy, Penny, or specialist #2
- Aji onboarding changes
- A universal debugging dashboard
- Another orchestration subsystem
- Inventing missing traces
- Command Deck spatial redesign

## Core principle

Scout can investigate. Paige can analyze content. Max remains responsible for understanding enough about the delegated work to discuss it with the operator.

Max may say "I don't know" when the trace does not establish cause. That is a successful epistemic response.

## Architecture

```text
Operator message
      ↓
Is this interrogating recent work?
      ↓
   YES       NO
    ↓         ↓
Trace       Normal
retrieve    routing
    ↓
Max reasoning from SpecialistCognitiveTrace
    ↓
Answer (never verbatim result replay)
```

`SpecialistCognitiveTrace` is a projection:

- delegation + available / supplied context (SPEC-098)
- execution + consumed context + investigation (SPEC-099A / SPEC-100A)
- result + Max evaluation (SPEC-098 / SPEC-100)

No new monolithic table.

## Context layers

| Layer | Meaning |
|---|---|
| Available to Max | What Max knew or could retrieve at delegation time |
| Supplied to specialist | What Max actually included in the delegation |
| Consumed by specialist | What the specialist successfully interpreted |

These diagnose different failures. If the available layer was never recorded, Max must not infer it.

## Failure boundaries

- Context retrieval failure
- Delegation failure
- Specialist interpretation failure
- Capability failure
- External dependency failure
- Evidence insufficiency
- Unknown (cause remains unknown)

## Implementation

| File | Role |
|---|---|
| `packages/max/specialistDelegation/CognitiveTrace.js` | Trace projection |
| `packages/max/specialistDelegation/ContextLayers.js` | Available / supplied / consumed |
| `packages/max/specialistDelegation/InterrogationIntent.js` | Intent + referent resolution |
| `packages/max/specialistDelegation/InterrogationAnswer.js` | Operator-facing answers |
| `packages/max/workspace/SpecialistInterrogationContext.js` | Pre-routing workspace hook |
| `migrations/2026-08-17-specialist-result-payload.sql` | Persist result payload |

## Testing

- `test/specialistCognitiveTrace.test.js`
- `packages/max/workspace/tests/specialistInterrogation.test.js`
- Existing SPEC-098 / SPEC-099A / SPEC-100 / SPEC-100A regressions

## Acceptance Criteria

- [x] Interrogation recognized vs new investigation
- [x] Recent specialist referent resolution
- [x] Supplied context retrievable
- [x] Available vs supplied vs consumed distinguishable
- [x] Failure boundary classification
- [x] Unknown cause remains unknown
- [x] No hallucinated explanation
- [x] No unnecessary specialist rerun
- [x] Max evaluation inspectable
- [x] Investigation provenance inspectable
- [x] Business evidence remains distinct
- [x] Multiple investigation disambiguation
- [x] Tenant isolation
- [x] Refresh/session durability via the delegation store
- [x] No verbatim result replay for a materially different question
- [x] Scout satisfies the generic trace contract
- [x] Future specialists can use the same contract
- [x] Immediate geography acceptance test: PASS A, B, or C — never result replay

## Future Work

- Persist richer available-context snapshots from CIE without sending them to specialists
- Wire Paige / Penny / Cal adapters onto the same consumed-context contract
- Operator-facing inspection copy refinements after live Anchor diagnosis
