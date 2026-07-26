# SPEC-012 — Operator Intelligence

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v1.0.0 |
| **Priority** | Critical |
| **Owner** | TBD |
| **Created** | 2026-07-26 |
| **Completed** | 2026-07-26 |
| **Version** | v1.0.0 |

## Objective

Teach Pulseforge to understand operator behavior and continuously improve what it surfaces — without changing deterministic business logic. Operator Intelligence personalizes ordering, suggestions, and presentation. It never alters evidence, confidence, reasoning, or policy.

## Vision References

- `docs/vision/Product_Constitution.md` (§11 Cognitive load)
- `docs/vision/Product_Experience.md`
- `docs/vision/Intelligence_Architecture.md`
- [SPEC-004](SPEC-004_Max_Briefing_Engine.md) — Morning Brief assembly
- [SPEC-007](SPEC-007_Command_Deck_Composition_Engine.md) — immutable compose
- [SPEC-008](SPEC-008_Command_Deck_UI.md) — render-only UI
- [SPEC-009](SPEC-009_Max_Intelligence_Workspace.md) — Max suggestions
- [SPEC-011](SPEC-011_Live_Intelligence_Loop.md) — Live Intelligence (adjacent layer)
- [ADR-007](../adr/ADR-007_Operator_Intelligence.md)

## Problem

Pulseforge understands the market. It does not yet understand the operator. The same Morning Brief layout and Max suggestion chips are shown regardless of what gets acted on, ignored, investigated, or approved. There is no feedback loop from real decisions back into presentation — only into (unchanged) deterministic intelligence.

## Philosophy

The intelligence stack reasons about businesses.
Operator Intelligence reasons about interaction.
These are intentionally separate.

```text
Knowledge
        │
Reasoning
        │
Memory
        │
Briefing
        │
Policy
        │
Live Intelligence
──────────────────────────
Operator Intelligence
──────────────────────────
Command Deck
        │
Max
```

Operator Intelligence never changes facts. It changes presentation.

## Scope

- Common `InteractionEvent` model for meaningful operator actions
- Per-recommendation `RecommendationLearning` aggregates
- Explicit recommendation outcome lifecycle
- Adaptive Morning Brief / section visual priority (never hide)
- Max suggestion personalization from tenant conversational preferences
- Internal trust signal on recommendations (does not replace confidence)
- Internal Intelligence Quality Dashboard (not customer-facing)
- HTTP APIs to record events / outcomes and read learning + quality

## Out of Scope

- Altering evidence, confidence, reasoning, or policy outcomes
- Autonomous re-ranking of business opportunity scores
- Customer-facing analytics product
- Durable Postgres event log (process-scoped in v1, same as LiveLoop / Workspace)
- Cross-tenant learning
- A/B experimentation framework

## Dependencies

- ✅ SPEC-007 / SPEC-008 Command Deck compose + UI
- ✅ SPEC-009 Max Workspace suggestion chips
- ✅ SPEC-010 Investigation surfaces (event sources)
- ✅ SPEC-011 Live Intelligence (adjacent; does not subsume)

## Architecture

```text
Command Deck / Investigation / Max Workspace
        │
        ▼
POST /api/v1/operator/events
        │
        ▼
OperatorEngine.track()
        ├── InteractionStore (append-only)
        ├── LearningStore (RecommendationLearning)
        ├── OutcomeTracker (lifecycle)
        ├── TrustScorer (usefulness signal)
        └── PreferenceLearner (Max chips)
                │
                ▼
compose() → AdaptivePresentation.decorate(deck)
openWorkspace() → personalized suggestions
GET /api/v1/operator/quality → internal dashboard
```

### Hard rules

Operator Intelligence **may**:

- personalize ordering
- personalize suggestions
- improve presentation

It **may never**:

- alter evidence
- modify confidence
- rewrite reasoning
- override policy
- invent intelligence

The deterministic stack remains authoritative.

## Data Model

No new Postgres tables in v1. Process-scoped stores (same durability as LiveLoop EventStore / Workspace SessionStore).

### InteractionEvent

```text
InteractionEvent {
  id, type, tenantId, operatorId,
  recommendationId?, companyId?, section?,
  depth?, timestamp, payload?
}
```

Types: `ViewedRecommendation` · `OpenedEvidence` · `AskedMax` · `ExpandedReasoning` · `ComparedCompanies` · `DismissedCard` · `SnoozedRecommendation` · `ApprovedRecommendation` · `IgnoredRecommendation` · `OpenedTimeline` · `ReturnedToDeck` · `OpenedSection`

### RecommendationLearning

```text
RecommendationLearning {
  recommendationId, tenantId,
  viewed, ignored, approved, dismissed, openedInMax,
  investigatedDepth, timeToDecisionMs,
  firstViewedAt, decidedAt, lastEventAt, outcome, trust
}
```

### Outcome lifecycle

```text
Recommended → Reviewed → Approved → Executed → Successful
                         ↘ Dismissed | Expired | Contradicted
```

### Adaptive presentation envelope

Attached to `CommandDeckModel.presentation` (additive):

```text
{
  sectionOrder: string[],
  sectionDominance: { [sectionId]: 'high'|'normal'|'quiet' },
  preferences: { topIntents: string[] }
}
```

Sections are never removed — only reordered / visually quieted.

## Implementation Plan

1. Spec + ADR-007 + heartbeat docs
2. `packages/max/operator/` — types, stores, learning, outcomes, trust, adaptive, preferences, quality, engine
3. Wire into `createMaxReasoningRuntime` (compose decorate + Max suggestion personalization)
4. HTTP: events, outcomes, learning, quality
5. Command Deck UI: emit interaction events; honor presentation envelope
6. Tests

## Migration Strategy

- Additive only; decks without `presentation` render as today
- No schema migration
- Rollback: stop emitting events / ignore `presentation` envelope

## Testing

- Unit: event build, learning updates, outcome transitions, trust score, adaptive order, preference ranking, quality metrics
- Unit: OperatorEngine track → decorate → suggestions
- Manual: act on cards → section dominance shifts; Ask Max chips reflect compare/evidence habits

## Acceptance Criteria

- [x] Interaction event model implemented
- [x] Recommendation outcome lifecycle tracked
- [x] Operator behavior captured
- [x] Adaptive presentation supported
- [x] Max suggestion personalization implemented
- [x] Internal quality dashboard available
- [x] Deterministic reasoning remains unchanged

## Future Work

- Durable operator event log when knowledge dual-write ships
- Per-operator (not only tenant) preference profiles
- Outcome → Successful linkage from CRM close signals
- Optional quality dashboard HTML surface for admins
