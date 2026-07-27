# SPEC-011 — Live Intelligence Loop

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

Transform Pulseforge from a static morning briefing into a living intelligence system. The operator never wonders “Is this still true?” — the system continuously answers that question through gentle evolution, not full refreshes.

## Vision References

- `docs/vision/Product_Constitution.md` (§11 Cognitive load)
- `docs/vision/Product_Experience.md`
- `docs/vision/Intelligence_Architecture.md`
- [SPEC-003](SPEC-003_Temporal_Intelligence_Memory.md) — transitions / change detection
- [SPEC-006](SPEC-006_Command_Deck.md) — Command Deck product surface
- [SPEC-007](SPEC-007_Command_Deck_Composition_Engine.md) — immutable compose
- [SPEC-008](SPEC-008_Command_Deck_UI.md) — render-only UI
- [SPEC-009](SPEC-009_Max_Intelligence_Workspace.md) — Max awareness during conversation
- [SPEC-010](SPEC-010_Intelligence_Navigation.md) — investigation continuity
- [ADR-006](../adr/ADR-006_Live_Intelligence_Evolution.md)

## Problem

Today the operator opens Pulseforge, reads a briefing, investigates, and acts. Between those moments the deck is frozen. Reopening or hard-refreshing replaces the whole picture. Max does not know what changed since the conversation opened. Investigation pages have no continuity when evidence shifts. There is no common event model for “intelligence matured.”

## Philosophy

Nothing should “refresh.” Everything should evolve. The interface should feel like watching intelligence mature — not polling an API.

## Scope

- Common `IntelligenceEvent` model
- Lifecycle for every intelligence object: Detected → Verified → Strengthened → Contradicted → Resolved → Archived
- Live evolution across Command Deck (gentle card updates; movement indicators animate once; new intelligence fades in)
- Incremental Morning Brief evolution (append entries — never wholesale replacement as the primary UX)
- Max awareness of material changes during active conversations
- Investigation continuity: stable focus + “New intelligence available / Review” instead of interruption
- Per-entity live timeline of significant transitions
- Notifications limited to material events only

## Out of Scope

- WebSocket / SSE transport (soft poll with cursor is sufficient for v1)
- Push email / SMS / OS notifications
- Live CRM dual-write (events still fail closed when graph empty)
- Replacing `/dashboard` as default landing
- Autonomous execution of recommendations

## Dependencies

- ✅ SPEC-003 Temporal Memory (ChangeDetector, TimelineBuilder)
- ✅ SPEC-007 / SPEC-008 Command Deck compose + UI
- ✅ SPEC-009 Max Workspace session memory
- ✅ SPEC-010 Investigation trail + detail composers

## Architecture

```text
CommandDeckComposer.compose() / Memory remember()
        │
        ▼
LiveLoopEngine.observeDeck / observeChanges
        │
        ├── IntelligenceEvent store (append-only, process-scoped)
        ├── LifecycleTracker (entity state machine)
        ├── MaterialFilter (notify subset)
        └── BriefingEvolution entries
                │
                ▼
GET /api/v1/intelligence/live?since=
        │
        ├── Command Deck soft evolve (UI)
        ├── Investigation continuity banner
        └── Max awareness on open / ask
```

### Live Update Model

Every intelligence object has a lifecycle. The UI reflects transitions naturally.

### Intelligence Events

Every meaningful update becomes an event:

```text
IntelligenceEvent {
  id, type, entity, severity, timestamp, summary, relatedEvidence, material, lifecycle
}
```

Examples: new hiring signal · confidence increased · recommendation changed · evidence contradicted · policy blocked execution · opportunity expired.

### Notification Philosophy

Never spam. Notify only when intelligence materially changes:

- Highest Leverage Action replaced
- Watch Alert promoted
- Confidence crosses threshold
- Recommendation blocked
- Opportunity expired

## Data Model

No new Postgres tables in v1. Process-scoped append-only event store (same durability posture as Workspace SessionStore). View models gain:

- `live.cursor` — opaque since-token for soft poll
- `live.evolution[]` — incremental briefing / deck entries
- `live.notifications[]` — material-only
- `timeline[]` on recommendation / company — lifecycle history from LiveLoop (+ existing memory/knowledge timeline)

## Implementation Plan

1. Spec + ADR-006 + heartbeat docs
2. `packages/max/live/` — types, store, lifecycle, deck diff, material filter, awareness, engine
3. Wire into `createMaxReasoningRuntime` + compose observe
4. HTTP: `GET /api/v1/intelligence/live`, timeline, notifications
5. Command Deck soft poll + gentle evolve; investigation banner; Max awareness
6. Tests

## Migration Strategy

- Additive only; `/command-deck` gains soft poll — full replace remains fallback
- No schema migration
- Rollback: disable soft poll; deck returns to load-once behavior

## Testing

- Unit: event build, lifecycle transitions, deck diff, material filter, awareness copy
- Unit: LiveLoopEngine observe → eventsSince → notifications
- Manual: open deck → inject / wait for evolve → cards fade; open investigation → banner; open Max → awareness line

## Acceptance Criteria

- [x] Common IntelligenceEvent model introduced
- [x] Live evolution supported across Command Deck
- [x] Briefing updates incrementally
- [x] Max detects changes during active conversations
- [x] Investigation pages remain stable during updates
- [x] Timeline records every significant intelligence transition
- [x] Notifications limited to material events

## Future Work

- SSE / WebSocket push
- Durable cross-process event log
- Operator preference for notification severity floor
- Email digest of material events only
