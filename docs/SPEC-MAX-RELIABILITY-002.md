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

## Authenticated API boundary

Evaluate, expectation scan and receipt reads use the same current-principal and
tenant-selection policy as Max attention through
`utils/maxOperationalAuthorization.js`. A body/query selector cannot override
the authorized workspace; omitted selectors no longer silently choose tenant 1.
Every supplied alias must be a valid matching positive integer. Revoked users,
stale role/tenant bindings, legacy unverified actors and inactive tenants are
rejected before business-service dispatch or schema initialization. Impersonated
AOs do not inherit the authenticating admin's decision-write authority. Existing
decision policy, idempotency, approvals and verification are unchanged.

Actual-route regressions are in `test/maxOperationalAuthorization.test.js` and
included in the full and strict Max suites. Passing these synthetic-service
tests does not establish live execution, production routing accuracy or recovery.
