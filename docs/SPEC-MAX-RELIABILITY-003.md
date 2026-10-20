# SPEC-MAX-RELIABILITY-003 — Durable Attention & Operational Continuity

Max maintains durable attention items so unresolved operational obligations survive restarts, deployments, and conversation boundaries.

Lifecycle: `EVIDENCE → STATE → DECISION → ACTION → ATTENTION → RE-EVALUATION → RESOLUTION`

## Entry points

- `POST /api/v1/max/attention/cycle` — run one attention scheduler cycle for a client
- `GET /api/v1/max/attention/operator-queue` — operator-visible attention budget slice
- `GET /api/v1/max/attention/health` — scheduler heartbeat / observability
- `POST|GET /cron/max-attention-cycle?secret={CRON_SECRET}` — explicit single-client cycle (`client_id` or `clientId` is required in query/body; conflicting or malformed selectors return HTTP 400 before initialization). No implicit default or all-client dispatch.
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

## Operational deployment contract

This module exposes a cycle endpoint; it does not start an in-process timer. A separately approved scheduler must invoke it with an explicit authorized tenant. `max-decay-daily` runs a different scoring/decay job and is not attention-scheduler evidence. Never enable attention scheduling merely to make a heartbeat green: cycles may create durable decisions and AO tasks.

Compatibility: callers that omitted the tenant or relied on invalid selectors falling back to tenant 1 must add an explicit positive integer. Existing explicit query selectors remain supported; body selectors and consistent aliases are accepted. No deployed caller configuration is changed by this source patch.

Verify operational health using direct read-only queries, not the health endpoint (its store initialization can run DDL). For each approved tenant, require a recent terminal completed scheduler run, no stuck running/failed cycles, and a matching heartbeat. An empty cycle proves execution only, not attention ingestion or evaluation; test the latter separately with approved bounded fixtures. The global heartbeat can be advanced by another tenant, so tenant run evidence is mandatory.

## API tenant authorization (separate compatibility change)

Cycle and operator-queue requests reload authenticated and effective principals from `users` before any store initialization. Both must currently be active admin/manager users; impersonation requires a current authenticated admin. Managers use their current DB tenant binding; admins use their selected session tenant (or explicit DB binding if none selected). All supplied body/query aliases must be valid and match that authorized tenant. The selected client must remain active. Legacy password-only sessions, unbound managers, stale roles/tenant bindings, and arbitrary query-selected cross-tenant access are denied. Admins must first select the desired tenant via the established tenant-selection flow. An impersonated AO cannot inherit admin attention-write rights. No new roles or grants are created.

This authorization change is distinct from the cron selector repair and needs explicit release review. The global health route is unchanged and remains unsuitable for read-only operational probing because it initializes schema.

## Scheduler initialization and cycle bounds

Scheduler calls now perform read-only required-table/column projections and require the existing global heartbeat row before recording a run. They never run store initialization, DDL, client seeding or user backfills. An absent prerequisite fails with `MAX_ATTENTION_SCHEMA_NOT_READY`; reviewed migrations and seed repair must be performed separately. These projections verify required objects/columns, not every constraint, type or migration version. Concurrent schema changes can still fail a cycle and must be avoided during operation. Other ingestion/queue/health initialization paths are unchanged.

Cron accepts optional `limit` in query or JSON body, an integer 1–20. Omission retains 20; malformed or conflicting values return HTTP 400 before readiness/dispatch. This caps claimed attention items, not every durable write: an evaluation can also record a decision, action intent and AO follow-up. It does not activate a timer, broaden tenant scope or authorize production execution. A zero-item cycle proves invocation only.
