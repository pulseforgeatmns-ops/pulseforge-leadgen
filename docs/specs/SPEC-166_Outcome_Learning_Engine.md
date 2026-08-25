# SPEC-166 — Outcome Learning Engine

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Priority** | Critical |
| **Owner** | Max + Scout |
| **Created** | 2026-08-25 |
| **Depends on** | [SPEC-165](SPEC-165_Strategic_Decision_Engine.md), [SPEC-118](SPEC-118_Acquisition_Mission_Orchestration.md) |
| **ADR** | [ADR-086](../architecture/ADR-086_Every_Decision_Must_Teach.md) |

## Problem

Intelligence without feedback eventually becomes opinion. SPEC-159 through SPEC-165 build forward-looking understanding, heuristics, opportunity ranking, and strategic allocation — but nothing closes the loop from **prediction → execution → observed business outcome → improved future decisions**.

Operators need to know: **Were we right?** Not just "did the email send?" but "did the walkthrough book?" and "what should change next time?"

## Objective

Introduce an Outcome Learning Engine that:

1. Captures every recommendation as a **prediction** with expected outcome and confidence
2. Observes **actual business outcomes** after execution
3. Compares prediction vs reality with **accuracy scoring**
4. Records **root cause analysis** when predictions fail
5. Produces **learning objects** for Scout heuristics, Max strategy, Paige messaging, and organizational knowledge
6. Surfaces **Outcome Review** in the Mission Intelligence Report
7. Answers operator questions like "What have we learned this month?"

## Pipeline

```
Scout → Understanding → Judgment → Opportunity → Decision → Execution → Outcome → Learning → Future Decisions
```

| Stage | Question |
|---|---|
| Strategic Decision (SPEC-165) | What should we do today? |
| Execution | Did we do it? |
| Outcome | Did the business result happen? |
| Learning (SPEC-166) | What should change next time? |

## Core Module

`packages/acquisition-mission/OutcomeLearning.js`

Engine integration: `packages/acquisition-mission/Engine.js`

Persistence: `acquisition_mission_predictions`, `acquisition_mission_outcome_evaluations`, `acquisition_mission_outcome_learnings`

## Outcome Model

Every resolved prediction produces:

| Field | Purpose |
|---|---|
| `missionId` | Mission scope |
| `opportunityId` | Which opportunity was predicted |
| `recommendation` | What was recommended |
| `operatorAction` | What the operator did |
| `expectedOutcome` | What was predicted |
| `actualOutcome` | What happened |
| `outcomeDelta` | Prediction vs reality gap |
| `lessons` | Durable learning statements |
| `confidenceAdjustment` | Calibration signal (evaluate-only, never mutates live MIR) |

## Learning Pipeline

Every completed outcome updates four systems (as learning records — **never auto-applied**):

| System | Learning kind |
|---|---|
| Scout | `business_heuristic` — strengthened/weakened |
| Max | `strategy`, `opportunity_rule` |
| Paige | `messaging` |
| Organization | `organizational` — institutional knowledge |

## API

| Route | Purpose |
|---|---|
| `GET /api/v1/amo/missions/:id/outcome-learning` | Mission outcome review |
| `POST /api/v1/amo/missions/:id/outcome-learning/evaluate` | Manually evaluate pending prediction |
| `GET /api/v1/amo/outcome-learning` | Tenant organizational learning rollup |
| `GET /api/v1/amo/learning` | Extended with `outcomeLearning` section |
| `POST /api/v1/amo/ask` | "What have we learned this month?" |

## Mission Intelligence Report

New section: `outcomeReview`

- Predictions (total, pending, resolved)
- Accuracy rate
- Lessons
- Heuristic updates
- Strategy updates
- Recent evaluations (prediction, expected, actual, accuracy, lesson)

## Invariant

**Every recommendation made by PulseForge must eventually resolve into an observed business outcome that can improve future recommendations.**

No recommendation disappears. Every recommendation teaches. Learnings are never auto-applied (ADR-055, ADR-082, ADR-008).

## Acceptance Criteria

| # | Scenario | Status |
|---|---|---|
| 1 | Max recommends outreach → outcome tracked → prediction compared with reality | ✓ |
| 2 | Prediction succeeds → relevant heuristics strengthen | ✓ |
| 3 | Prediction fails → root cause recorded → strategy updated | ✓ |
| 4 | Operator asks "What have we learned this month?" → organizational summary | ✓ |
| 5 | MIR includes prediction, actual outcome, accuracy, lessons, heuristic/strategy updates | ✓ |

## Tests

`packages/acquisition-mission/tests/spec166.test.js`

Run: `npm run test:amo`
