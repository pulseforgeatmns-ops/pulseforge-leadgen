# SPEC-MAX-RELIABILITY-003 — Durable Attention & Operational Continuity

Max maintains durable attention items so unresolved operational obligations survive restarts, deployments, and conversation boundaries.

Lifecycle: `EVIDENCE → STATE → DECISION → ACTION → ATTENTION → RE-EVALUATION → RESOLUTION`

## Entry points

- `POST /api/v1/max/attention/cycle` — run one attention scheduler cycle for a client
- `GET /api/v1/max/attention/operator-queue` — operator-visible attention budget slice
- `GET /api/v1/max/attention/health` — scheduler heartbeat / observability
- `POST|GET /cron/max-attention-cycle?secret={CRON_SECRET}` — production loop (all active clients, or `client_id`)
- Library: `packages/max/attention`

## Persistence

Migration: `migrations/2026-10-05-max-reliability-attention.sql`

Tables: `max_attention_items`, `max_attention_scheduler_runs`, `max_attention_heartbeats`.

Schema helper: `utils/maxAttentionSchema.js`.

## Integration

- SPEC-002 `evaluateOperationalDecision` accepts optional `attentionStore` and upserts attention after each decision.
- Post-ingest re-evaluation wakes matching attention items on evidence before calling 002.
- Scheduler claims due items (`FOR UPDATE SKIP LOCKED`), invokes 002, records evaluation, updates attention; resolution requires decision evidence (not scheduler presence alone).

## Tests

`test/maxAttentionReliability.test.js` (included in `npm run test:max`).

Depends on: `docs/SPEC-MAX-RELIABILITY-001.md`, `docs/SPEC-MAX-RELIABILITY-002.md`.
