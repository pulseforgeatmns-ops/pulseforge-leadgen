# SPEC-003 — Temporal Intelligence & Memory

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v0.8.1 |
| **Priority** | High |
| **Owner** | TBD |
| **Created** | 2026-07-26 |
| **Completed** | 2026-07-26 |

## Objective

Teach Max to understand change over time.

The reasoning engine (SPEC-002) evaluates a **snapshot**. The Memory Engine evaluates **motion**.

Instead of only:

> Company score = 82

Max understands:

> Company score 71 → 82 (+11) because
> - New Operations Manager
> - Overflow confidence increased
> - Website updated

## Design Principle

Max shouldn't remember facts. Max remembers **transitions**.

- The graph already stores state.
- Memory stores how state changed.

## Vision References

- `docs/vision/Intelligence_Architecture.md`
- [SPEC-002](SPEC-002_Max_Reasoning_Engine.md)
- [ADR-002](../adr/ADR-002_Explainable_AI.md)
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md)

## Problem

Without temporal memory, operators and future briefings cannot answer “what’s different?” without recomputing insight in the UI. The command deck should **receive** ready-made transition answers, not compute them.

## Scope

- `packages/max/memory/` — snapshots, diff, change_detection, timeline, history, watchers, tests
- Snapshot Engine (append-only, structured only)
- Diff Engine (deterministic)
- Change Detector
- Timeline Builder
- Memory queries: `whatChanged`, `whyChanged`, `history`, `trend`, `scoreHistory`, `confidenceHistory`
- Watch registration (detection only — no notifications)
- Recommendation evolution (history → trend → reason → forecast)
- Temporal explainability: Why → Evidence → History → Change → Reason

## Out of Scope

- LLM summaries / morning briefing copy
- Push notifications / email alerts
- Dashboard UI / Jarvis command deck
- Runtime agent wiring
- Autonomous prioritization execution

## Dependencies

- ✅ SPEC-002 Max Reasoning Engine (v0.8.0)

## Architecture

```text
Knowledge (what we know)
  ↓
Reasoning (what it means)     ← SPEC-002
  ↓
Memory (how it changed)       ← SPEC-003
  ↓
Briefing (how to communicate) ← future
  ↓
Automation (how to act)       ← future
```

```text
ReasoningEngine.evaluate()
        ↓
MemoryEngine.remember()
        ↓
SnapshotStore (append-only)
        ↓
Diff → ChangeDetector → Timeline / Watches / Evolution
```

## Data Model

### ReasoningSnapshot

`tenantId`, `companyId`, `timestamp`, `recommendation`, `score`, `confidence`, `strategyResults`, `claims`, `evidence` — no LLM output.

### ReasoningDiff

`scoreDelta`, `confidenceDelta`, `newClaims`, `removedClaims`, `newEvidence`, `removedEvidence`, `strategyChanges`.

### ChangeEvent

Structured types: score/confidence up/down, new/removed claims & evidence, strategy motion, new decision maker, hiring, contradiction, priority/type/action changes.

## Implementation Plan

1. SnapshotRepository + InMemory + Serializing (parity)
2. SnapshotEngine / DiffEngine / ChangeDetector
3. TimelineBuilder / WatchRegistry / RecommendationEvolution
4. TemporalExplanationEngine
5. MemoryEngine query surface
6. Tests + docs / release v0.8.1

## Migration Strategy

- Additive library only under `packages/max/memory`
- Existing SPEC-002 tests remain green
- Agents/server unwired

## Testing

```bash
npm run test:max
```

Covers: snapshot generation, replay, diff correctness, trend detection, change detection, history ordering, repository parity, determinism, integration with ReasoningEngine.

## Acceptance Criteria

- [x] Snapshot engine implemented
- [x] Diff engine implemented
- [x] Change detector implemented
- [x] Timeline history implemented
- [x] Memory queries operational
- [x] Watch registration supported
- [x] Runtime still unwired
- [x] Existing tests remain green

## Future Work

- ~~Morning briefings consuming `whatChanged()` aggregates~~ → SPEC-004 / v0.9.0
- Operator alerts from triggered watches (push/email)
- Persistent Postgres snapshot store
- Autonomous prioritization (still approval-gated)

## Definition of Done

By completion of v0.8.1, Max understands not only what the world looks like, but how it has evolved. Every recommendation can be compared against its previous state, every change can be explained, and every trend can be tracked — without LLMs or changing the deterministic reasoning core.
