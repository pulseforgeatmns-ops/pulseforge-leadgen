# SPEC-004 — Max Briefing Engine

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v0.9.0 |
| **Priority** | Critical |
| **Owner** | TBD |
| **Created** | 2026-07-26 |
| **Completed** | 2026-07-26 |

## Objective

Transform Max's structured reasoning into deterministic operator briefings.

The Briefing Engine does **not** perform reasoning. It orchestrates existing intelligence into a coherent operational picture.

## Philosophy

The Briefing Engine never computes. It assembles.

Everything it presents must already exist in:

- Knowledge
- Reasoning
- Memory

This separation keeps the system explainable.

## Vision References

- `docs/vision/Intelligence_Architecture.md`
- [SPEC-002](SPEC-002_Max_Reasoning_Engine.md)
- [SPEC-003](SPEC-003_Temporal_Intelligence_Memory.md)
- [ADR-001](../adr/ADR-001_Conversation_First.md)
- [ADR-002](../adr/ADR-002_Explainable_AI.md)

## Problem

Operators need a complete operational picture — priorities, changes, risks, watch alerts, and recommendations — without recomputing insight in the UI or inventing narrative. Knowledge, Reasoning, and Memory already produce the facts; nothing assembled them into a briefing contract.

## Scope

- Package `packages/max/briefing/` — builders, sections, digest, priorities, templates, presentation, tests
- Single entry: `max.brief({ tenantId, asOf, period })`
- Executive summary, priority queue, changes, watch alerts, risks, recommendations, metrics
- Daily / weekly / monthly digests
- Deterministic prioritization (score, confidence, trend, urgency, contradiction severity)
- Presentation Adapter extension point (structured default; optional markdown)
- Structured domain objects only (no UI ownership)

## Out of Scope

- LLM / natural-language briefing copy
- Push notifications / email delivery
- Dashboard UI rendering
- Runtime agent wiring (`maxAgent.js` unchanged)
- New reasoning or mutation of Knowledge / Memory

## Dependencies

- ✅ SPEC-002 Max Reasoning Engine (v0.8.0)
- ✅ SPEC-003 Temporal Intelligence & Memory (v0.8.1)

## Architecture

```text
Knowledge Graph
        │
Reasoning Engine
        │
Memory Engine
        │
───────────────
 Briefing Engine
───────────────
        │
Presentation Adapter  ← extension point (v0.9.0)
        │
Operator surface (CLI / dashboard / assistant — future)
```

```js
const briefing = await max.brief({
  tenantId,
  asOf,
  period: 'daily', // | 'weekly' | 'monthly'
});
```

Returns:

```text
Briefing {
  summary
  priorities
  changes
  watchAlerts
  risks
  recommendations
  metrics
}
```

No UI. No formatting by default. Only structured objects.

### Presentation Adapter

```text
Briefing Engine → Presentation Adapter → Operator surface
```

- Default: structured identity (`format: 'structured'`)
- Optional: `present: true, format: 'markdown'` for CLI/review rendering of existing fields only
- Adapters never assemble or invent briefing content

## Data Model

### Briefing

Domain object with fixed section order: Summary → Priority Queue → Changes → Watch Alerts → Risks → Recommendations → Metrics (+ meta).

### Priority item

`score`, `confidence`, `why`, `whyNot`, `trend`, `urgency`, `contradictionSeverity`, `rankScore`, `rank` — sourced from Memory recommendation + period transition.

### Metrics

`buildTimeMs`, `queryCount`, `recommendationCount`, `memoryLookups`, `strategyCount`.

## Implementation Plan

1. BriefingTypes + PeriodWindow / DigestBuilder
2. Prioritizer (deterministic)
3. CompanyContextCollector (Memory-only assembly)
4. Section builders + BriefingBuilder / BriefingEngine
5. Presentation Adapter
6. Tests + docs / release v0.9.0

## Migration Strategy

- Additive library under `packages/max/briefing`
- `createMaxReasoningRuntime()` gains `briefing` + `brief()`
- Existing reasoning/memory APIs unchanged
- Agents/server remain unwired

## Testing

```bash
npm run test:max
```

Covers: daily/weekly/monthly digests, empty tenant, large tenant, stable ordering, watch alert inclusion, recommendation ordering, change summaries, performance, no `evaluate()` during brief.

## Acceptance Criteria

- [x] Briefing builder implemented
- [x] Executive summary implemented
- [x] Priority queue implemented
- [x] Change section implemented
- [x] Watch alerts integrated
- [x] Risks implemented
- [x] Recommendation ordering deterministic
- [x] Daily/weekly/monthly digests supported
- [x] Presentation Adapter extension point
- [x] Runtime remains unwired
- [x] Existing reasoning unchanged

## Future Work

- Wire Max agent (shadow) to `brief()` for morning digests
- Wire Max / outbox to `decide()` before side effects (SPEC-005)
- Additional presentation adapters (JSON:API, Slack blocks)
- Persistent briefing snapshots / audit log
- Operator conversation surface consuming Briefing domain objects

## Definition of Done

By completion of v0.9.0, Max can produce a complete operational briefing from the current knowledge graph, reasoning engine, and temporal memory. The output is fully deterministic, evidence-backed, and ready to drive any interface—from a CLI to a web dashboard to a future conversational assistant.
