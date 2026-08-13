# SPEC-093 — Paige Outcome Learning Loop

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Created** | 2026-08-13 |

## Objective

Close the next loop after SPEC-092:

**Intent → Content → Outcome → Learning → Recommendation → Operator Decision → Next Experiment**

Paige retrieves SPEC-092 outcomes, evaluates them relative to objectives, persists evidence-supported learnings with deterministic confidence and explicit status, and generates explainable next-experiment recommendations. Operator authority remains absolute. No autonomous publishing or strategy mutation.

## Vision References

- [SPEC-092 Content Outcome Intelligence](SPEC-092_Content_Outcome_Intelligence.md)
- [SPEC-013 Outcome Intelligence](SPEC-013_Outcome_Intelligence.md) / ADR-008
- [SPEC-021 Learning & Belief Evolution](SPEC-021_Learning_and_Belief_Evolution_Engine.md)
- [Intelligence Architecture](../vision/Intelligence_Architecture.md)
- ADR-045 Evidence Before Reasoning

## Problem

SPEC-092 records what happened after content was published. Pulseforge still lacks a thin, explainable path from those observations to durable learnings and next-experiment recommendations that respect sample-size caution, attribution uncertainty, and objective-relative evaluation.

## Scope

- Durable `content_learnings` records (status, confidence, supporting/contradicting publications)
- Objective-relative publication evaluation (no universal content score)
- Deterministic confidence (observation vs generalization)
- Single-post safeguard (`signal` max for n=1 generalized claims)
- Attribution-aware business-outcome reasoning
- Qualitative signal participation
- Audience composition visibility
- Relevant learning retrieval for planning
- Structured `ContentRecommendation` + experiment payload
- Manual evaluate / recompute / recommend triggers
- Minimal UI on `/content-outcomes`
- Tenant isolation

## Out of Scope

- LinkedIn API / automated ingestion
- Predictive virality / ML scoring / embeddings optimization
- Autonomous publishing or campaign strategy mutation
- Multi-touch marketing attribution
- Recommendation analytics subsystem
- New Paige agent, event bus, or analytics warehouse

## Dependencies

- SPEC-092 content outcome service and tables
- Existing auth / client scoping
- Optional knowledge dual-write (non-blocking)

## Architecture

```text
SPEC-092 Outcomes
        ↓
services/contentLearning.js  (deterministic reasoning)
        ↓
content_learnings
        ↓
Paige recommendation (structured) → operator UI / API
```

**LLM as translator, not source of truth.** Confidence, status, evidence selection, and contradiction handling are deterministic.

## Data Model

Table: `content_learnings` (`migrations/2026-08-13-paige-outcome-learning.sql`)

Fields include: `learning_type`, `statement`, scope dimensions, `confidence` / `observation_confidence` / `generalization_confidence`, `sample_size`, supporting/contradicting publication IDs, evidence/uncertainty summaries, `status` (`signal|emerging|supported|contradicted|stale`).

## Implementation Plan

1. Spec + migration + service (memory + Postgres)
2. Routes + UI panels on `/content-outcomes`
3. Tests for safeguards, attribution, contradiction, recommendations
4. Docs (README, CURRENT_STATE, CHANGELOG)

## Migration Strategy

Forward + rollback SQL. Additive. Apply on Railway before production evaluation of the breakout-post record.

## Testing

- `test/contentLearning.test.js`
- `test/contentLearningRoutes.test.js`

## Acceptance Criteria

- [x] Retrieve SPEC-092 outcomes and evaluate relative to objectives
- [x] Quantitative metrics and business outcomes remain distinct
- [x] Qualitative signals and audience composition participate
- [x] Attribution strength affects conclusions
- [x] Learnings persist with status + deterministic confidence
- [x] Supporting/contradicting publications are traceable
- [x] Single publications cannot produce `supported` generalized learnings
- [x] Learnings re-evaluate as evidence changes
- [x] Recommendations expose evidence path, uncertainty, and next experiment
- [x] No cloning of successful posts; no autonomous publish/strategy mutation
- [x] Tenant isolation enforced; tests pass

## Future Work

- Optional operator accept/modify/reject recording for recommendations
- Campaign sequence intelligence beyond `campaign_id` context
- Automated post-publication evaluation (after manual trigger proves value)

**SPEC-092 records reality. SPEC-093 learns from it — cautiously.**
