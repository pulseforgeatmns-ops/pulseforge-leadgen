# ADR-006 — Live Intelligence Evolution

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-26 |
| **Spec** | [SPEC-011](../specs/SPEC-011_Live_Intelligence_Loop.md) |
| **Supersedes** | — |
| **Related** | [ADR-002](ADR-002_Explainable_AI.md), [ADR-005](ADR-005_LLM_Presentation_Engine.md) |

## Context

Command Deck and Max Workspace ship as snapshot surfaces: compose once, render once. Operators lose trust when they must hard-refresh to know whether a recommendation is still true. Full page replacement also disorients investigation and conversation. Temporal memory (SPEC-003) already detects transitions, but there is no product-level event model, lifecycle, or UX contract for continuous evolution.

## Decision

**Intelligence evolves in place. Refresh is a fallback, not the product language.**

1. Every meaningful update is an append-only `IntelligenceEvent` with entity, severity, summary, evidence refs, and optional lifecycle transition.
2. Command Deck soft-polls a since-cursor and applies gentle patches (fade-in, one-shot movement indicators). Wholesale re-render is reserved for first load and recovery.
3. Morning Brief accumulates evolution entries; it is not regenerated as a blank slate for the operator.
4. Max Workspace surfaces awareness lines derived from events since session open — presentation only; it never invents change (ADR-005).
5. Investigation focus stays stable; material updates offer “New intelligence available / Review.”
6. Notifications fire only for material events (HLA replaced, watch promoted, confidence threshold, policy block, opportunity expired).

v1 transport is soft poll. SSE/WebSocket may replace transport later without changing the event model.

## Consequences

### Positive

- Operators stay oriented; cognitive load stays low (Constitution §11)
- Explainable transitions reuse SPEC-003 change detection
- Max feels observant without becoming a second brain

### Negative / tradeoffs

- Process-scoped event store is non-durable across restarts (same as Workspace sessions)
- Soft poll has latency vs push
- Empty graph still yields quiet decks until dual-write is live

### Follow-ups

- SPEC-011 LiveLoopEngine + Command Deck evolve UX
- Durable event log when knowledge dual-write ships
- Optional SSE transport behind the same `IntelligenceEvent` contract
