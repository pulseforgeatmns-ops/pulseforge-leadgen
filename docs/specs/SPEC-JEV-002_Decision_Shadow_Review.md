# SPEC-JEV-002 — Decision Shadow Review + Mismatch Audit

Status: implemented; deployment and production migration are separate steps.

## Objective and scope

Make [SPEC-JEV-001](SPEC-JEV-001_Mission_Routing_Shadow_Evaluator.md) events
durable and reviewable by an operator/developer. Existing routing audits use
stdout; no durable log ingestion/query system is configured in this repository.
Use the existing Postgres deployment for storage, with an isolated writer pool.

The observer still cannot route, change a response, modify a mission/session,
consume approval, or trigger execution. Review recommendations are evidence for
human investigation. There is no live-routing flag or automatic correction.

## Architecture and data model

`DecisionService._write()` independently dispatches the existing stdout event
and `ShadowEventSink.write()`. Persistence runs on `setImmediate`, outside the
response path, including when provider capacity is exhausted. Routing never
awaits storage. The default sink is shared across service instances in one
process. It creates its dedicated `pg.Pool` only when writing; it never imports
or closes the application's shared `db.js` pool.

`decision_shadow_events` has a UUID primary key on `decision_id`. A repeated ID
uses `ON CONFLICT DO NOTHING`, preserving the original evidence. Nested
`current_route`, `errors`, and optional `raw_redacted_response` use JSONB.
Indexes support recent events, tenant history, mismatches, and error rows.
There are no foreign keys or writes to business tables. Missing session,
tenant, or mission correlation stays null; rows survive business-record deletion.

All original fields are retained without reinterpretation:

```text
event, spec, schema_version, decision_id, mode, source, session_id, tenant_id,
message_index, message_chars, message_truncated, provider, requested_provider,
requested_model, model, mission_id, current_route, routing_latency_ms, status,
intent, confidence, mission_bound_probability, approval_probability,
inspection_probability, requires_human_clarification, risk_if_misrouted,
recommended_route, route_matches, comparison, latency_ms, fallback_provider,
fallback_reason, errors, timestamp, raw_redacted_response
```

Stored `spec` remains `SPEC-JEV-001` and `schema_version` remains `1`, matching
the emitted event. `SPEC-JEV-002` identifies the review report/diagnostics.
Postgres stores the timestamp as TIMESTAMPTZ; JSON output uses an ISO timestamp.
No new operator-message, transcript, response-body, prompt, or credential fields
are collected. Optional raw data remains the existing validated projection,
controlled by `DECISION_SHADOW_LOG_RAW`; default is null.

## Failure isolation and configuration

| Variable | Default | Effect |
| --- | --- | --- |
| `DECISION_SHADOW_ENABLED` | `false` | Existing master gate. Disabled mode creates no evaluations, logs, pool, or writes. |
| `DECISION_SHADOW_PERSIST_ENABLED` | `true` | Exact `false` restores stdout-only behavior while keeping shadow evaluation. |
| `DECISION_SHADOW_DB_MAX_PENDING` | `64` | Deferred/in-flight write cap, valid range 1–1000, per default process sink. |
| `DECISION_SHADOW_DB_TIMEOUT_MS` | `1000` | Connection-acquisition and server statement timeout; valid range 50–10000 ms. Client query timeout adds 250 ms. |
| `DATABASE_URL` | existing app setting | Destination Postgres database; required for storage and review. |
| `DATABASE_SSL` | TLS on | Existing convention: exact case-insensitive `false` allows local/disposable Postgres. |

The separate pool allows at most two connections, expires idle connections after
one second, and does not keep the process alive while idle. At capacity, storage
drops the new write and keeps its stdout event. Slots remain occupied until the
actual write settles, so a stuck driver cannot cause unbounded pending work.
No automatic retry or unbounded backlog is created.

Connection failures, missing tables, statement/query timeouts, and rejected
writes are swallowed. Fixed-code `[DECISION_SHADOW_PERSISTENCE]` warnings and
process-local counters expose missing configuration, capacity drops, or write
failures. Warnings are limited to one per 30 seconds per sink; they never include
SQL, exceptions, connection strings, credentials, or provider response bodies.
`stats()` reports successful insert attempts (including duplicate no-ops), failed
writes, dropped writes, and pending writes. Counters reset on process restart.
The original stdout and persistence sinks fail independently.

Successfully inserted rows are durable. This is best-effort telemetry, not a
durable job queue: process death, exhausted capacity, or DB failure can lose
observations. Existing stdout rows are not automatically backfilled. Provider
failures are stored with their original `status=error`, safe error codes/HTTP
status, noop fallback, and unavailable comparison; they never fail requests.
`DecisionService.drain()` also waits for persistence for tests/shutdown callers;
normal routing must never call it. Forced shutdown can lose pending writes.
Configuration is read on service/sink creation; restart after flag changes.

## Migration and rollout

Apply the idempotent migration to the intended database using its existing
administrative connection, then deploy this code:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/2026-09-21-decision-shadow-review.sql
```

The migration creates only the shadow table and indexes. The application DB
role needs INSERT and SELECT on that table (including SELECT for conflict
handling); if a separate migration owner is used, grant those permissions using
the deployment's real role names. No schema creation occurs during a request.

Keep existing Jev/shadow flags enabled to collect events. Persistence defaults
on under the existing master gate; no additional enable flag is needed. If the
app is deployed before migration, stdout continues and failed writes produce
diagnostics; lost writes are not replayed. Verify the table receives new events
with the command below.

Rollback: set `DECISION_SHADOW_PERSIST_ENABLED=false` and restart for stdout-only
observation, or `DECISION_SHADOW_ENABLED=false` and restart to stop all new shadow
work. Pending work can finish in the old process. Existing rows remain available
for read-only review in either mode. Do not drop the table as an operational
rollback; retain audit evidence. No automatic retention/deletion job is added.

## Operator/developer review

From the repository with the intended `DATABASE_URL` available:

```sh
npm run decision:review
npm run decision:review -- --tenant 10 --limit 50
npm run decision:review -- --tenant 10 --mismatches --json
npm run decision:review -- --tenant 10 --warnings --json
npm run decision:review -- --tenant 10 --errors --json
npm run decision:review:warnings -- --limit 50
node scripts/reviewDecisionShadowWarnings.js --tenant 10 --json
```

Warning candidates are derived at query time by `classifyDecisionMismatch()`; see
[SPEC-JEV-003](SPEC-JEV-003_Operator_Visible_Routing_Mismatch_Warnings.md).

In an existing Railway service shell, the same commands use its environment.
The default is the latest 50 stored evaluations across tenants, ordered by event
timestamp and decision ID. Limits are 1–500. `--tenant` restricts every returned
row and count. `--mismatches`/`--errors` filter **before** the limit so newer
successful turns cannot hide older mismatches/errors. `--warnings` returns the
latest rows matching the SPEC-JEV-003 likely mission-inspection warning
predicate. Missing migrations or DB
access fail the report with a nonzero exit, rather than returning a false empty
report. `--help` needs no database connection.

Text output contains a summary and per-row review table. `--json` includes all
fields in `evaluations`, plus `mismatches`, `likely_mission_inspections`,
`operator_warnings`, and `errors`. Summary counts cover only the returned sample, not all history. The
mismatch rate denominator includes only comparable match/mismatch rows; fallback,
skipped, and error/unavailable rows are not treated as matches. Empty samples
have a null mismatch rate. Status and error-code breakdowns keep failures visible.

The command performs SELECT only and works with a read-only DB role. It is an
admin/developer path protected by DB credentials, not a new web endpoint. The
optional tenant filter is report scoping, not an authorization boundary; people
with a cross-tenant DB account can review all tenants. Do not expose this script
through a client-facing route without separate tenant authorization.

## Mismatch heuristic

Flag **likely mission inspection** when all of these apply:

- The row is an evaluated Jev mismatch and the observed production route did not fail.
- `current_route.route='conversation'` or `current_route.raw_route='intelligence'`.
- `intent='status_check'` or `recommended_route='inspection'`.
- `confidence >= 0.90` and `inspection_probability >= 0.85` (see
  [SPEC-JEV-003](SPEC-JEV-003_Operator_Visible_Routing_Mismatch_Warnings.md)).

This is a review-only threshold, not a production routing threshold. It catches
the production Anchor STR observation: `status_check`, confidence `0.99`,

SPEC-JEV-005 (mission inspection state consistency) builds on this routing fix:
once Jev routes to `inspection`, the answer layer resolves canonical missions and
validates snapshot consistency before generating status/confidence prose.
inspection probability `0.93`, recommendation `inspection`, and current route
`conversation / intelligence / ClientIntelligence`. Lower-confidence mismatches
still appear in the general mismatch list. Null/unknown comparisons and provider
errors are not promoted as likely inspection cases. A mismatch does not prove
Jev is correct.

## Validation

```sh
npm run test:decision
npm run test:decision:postgres
```

The first command runs the existing SPEC-JEV-001 routing/schema/provider suite
and new failure-isolation, bounded-capacity, CLI, and mismatch tests. The second
requires local `initdb`/`pg_ctl` and always starts a disposable database, ignoring
any production DATABASE_URL. It applies migration twice, round-trips every field,
verifies duplicate protection and tenant/filter ordering, persists provider
failures, exercises an actual table-lock timeout and missing-table recovery,
and runs both CLI formats under a SELECT-only role. No test calls live Jev.
