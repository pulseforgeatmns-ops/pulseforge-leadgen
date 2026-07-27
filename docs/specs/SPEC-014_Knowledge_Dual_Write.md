# SPEC-014 — Knowledge Dual-Write & Operational Readiness

| Field | Value |
|---|---|
| **Status** | In Progress |
| **Target Version** | v1.0.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-26 |
| **Depends on** | SPEC-001A/B/C, SPEC-001 Persistent Store, SPEC-002–013 |

## Objective

Ensure every real-world business event becomes durable intelligence so the Command Deck, Max, Operator Intelligence, and Outcome Intelligence operate on live data. Pulseforge is operated, not merely built — the Command Deck must never report "Market is quiet" unless the market is actually quiet.

## Vision References

- `docs/architecture/Knowledge_Graph_Architecture.md`
- `docs/architecture/Memory_Architecture.md`
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md)
- [SPEC-001B](SPEC-001B_Graph_Synchronization_Engine.md)
- [SPEC-006](SPEC-006_Command_Deck.md) · [SPEC-013](SPEC-013_Outcome_Intelligence.md)

## Problem

The intelligence library (Knowledge → Reasoning → Memory → Briefing → Policy → Command Deck → Operator → Outcome) is implemented, but production CRM/Scout/webhook events never dual-write into Knowledge. `maxRuntime` boots an empty in-memory graph. Morning Brief stays quiet while Anchor Scout is busy. Failed writes have no outbox. There is no admin confidence panel or end-to-end flight trace.

## Scope

- Knowledge dual-write from CRM producers (`dbClient`, Scout, touchpoints) via `GraphSyncEngine`
- Operational event contract + taxonomy (discovery, communications, calls, meetings, signals, recommendations, outcomes)
- Durable outbox + automatic retry when Knowledge apply fails
- Persistent sync ledger (Postgres)
- Boot Max / Command Deck on persistent Knowledge + sync + ingestor
- Admin Validation Dashboard (events today, queue depth, failures, evidence, recs, outcomes, last write)
- Admin Flight Recorder (hidden journey UI for a single entity/event chain)
- Automated end-to-end validation harness
- Cron worker for outbox drain

## Out of Scope

- Replacing Max orchestration `prospect_signal_events` (fan-out, do not merge stores)
- Durable Operator / Outcome / Live stores (still process-scoped; dual-write feeds Knowledge only)
- Making `/command-deck` the default landing
- Customer-facing health UI
- Rewriting agents to touch `GraphRepository` directly

## Dependencies

- Persistent graph tables (`knowledge_*`)
- `GraphSyncEngine` + mappers
- `createMaxReasoningRuntime` / `utils/maxRuntime.js`
- Postgres pool (`db.js`)

## Architecture

```text
Operational Event
        ↓
  Knowledge Dual-Write (outbox first)
        ↓
  GraphSyncEngine.apply  (idempotent ledger)
        ↓
  KnowledgeEventBus → KnowledgeIngestor
        ↓
  Knowledge Store (Postgres)
        ↓
  Reasoning → Memory → Briefing → Policy
        ↓
  CommandDeckComposer → Operator → Outcome → UI
```

Principle: every event happens once; every consumer observes; no downstream reconstruction.

## Data Model

### Event contract

```text
KnowledgeEvent {
  id, tenantId, entityId, entityType, eventType,
  timestamp, source, payload, evidence
}
```

### Tables

- `knowledge_outbox` — durable queue; never silently discard
- `knowledge_sync_ledger` — persistent idempotency keys
- `knowledge_flight_stages` — Flight Recorder stage log

## Implementation Plan

1. Migration + PostgresSyncLedger + outbox writer/retry
2. Dual-write helpers + operational mappers
3. Hook `dbClient` / Scout fan-out; wire `maxRuntime` to persistent Knowledge
4. Admin Validation Dashboard + Flight Recorder
5. E2E validation + cron `/cron/knowledge-outbox`
6. Docs / CURRENT_STATE / CHANGELOG

## Migration Strategy

Additive SQL migration. Feature flag `KNOWLEDGE_DUAL_WRITE` (default on when unset / not `0`). Rollback drops new tables only; CRM and existing `knowledge_*` graph tables remain.

## Testing

```bash
npm run test:knowledge
npm run test:dual-write
node scripts/knowledgeE2EValidation.js --dry-run
```

## Acceptance Criteria

- [x] All wired operational events dual-write to Knowledge
- [x] Writes are idempotent (ledger + stable IDs)
- [x] Failed writes are queued and retried
- [x] Command Deck reflects live operational data when events exist
- [x] Max references newly created evidence without delay (same process)
- [x] Operator Intelligence receives interaction events (existing path)
- [x] Outcome Intelligence receives lifecycle events (existing path)
- [x] Admin validation dashboard operational
- [x] Flight Recorder shows event journey stages
- [x] End-to-end validation passes

## Exit Criteria

Placeholder empty states disappear whenever live data exists. A newly discovered prospect can be traced from ingestion through recommendation, investigation, execution, and outcome. Every visible recommendation has an auditable evidence chain. Morning Brief reflects real market activity. Anchor can be operated day-to-day through Pulseforge.

## Future Work

- Durable Operator / Outcome / Live event logs
- SSE Flight Recorder streaming
- Full Brevo/call/meeting producer coverage beyond touchpoint + Scout hooks
- One business week of Anchor-only operation (success metric; not a code gate)
