# SPEC-MAX-RELIABILITY-002 — Evidence-Bound Decision & Action Execution

Max converts canonical PulseForge state (SPEC-001) into justified decisions and verified actions:

`OBSERVE → UNDERSTAND → PRIORITIZE → DECIDE → AUTHORIZE → EXECUTE → VERIFY → LEARN/UPDATE`

## Entry points

- `POST /api/v1/max/decisions/evaluate` — evaluate one trigger (`services/maxDecisionExecutionService.js`)
- `POST /api/v1/max/decisions/scan-expectations` — scan open/overdue expectations and decide
- `GET /api/v1/max/decisions/:id/receipt` — inspect a material decision receipt
- Library: `packages/max/decisionExecution`
- Post-ingest hook: successful `POST /api/v1/max/ingest` runs `reevaluateOnIngestion` when Postgres is available

## Persistence

Migration: `migrations/2026-10-05-max-reliability-decision-execution.sql`

Tables: `max_operational_decisions`, `max_operational_action_intents`, `max_ao_follow_up_tasks`.

Schema helper: `utils/maxDecisionExecutionSchema.js` (lazy on first Postgres decision write).

## Invariants

- No consequential action without evidence, rationale, authority class, and verification (where applicable).
- `NO_ACTION` and `INSUFFICIENT_EVIDENCE` are explicit outcomes.
- Idempotency keys prevent duplicate AO tasks on scheduler replay.
- Prior decisions can be `SUPERSEDED` when new canonical evidence arrives.

## Tests

Acceptance scenarios: `test/maxDecisionExecution.test.js` (`npm run test:max` includes this file).

Depends on: `docs/SPEC-MAX-RELIABILITY-001.md` (`packages/max/stateIngestion`).
