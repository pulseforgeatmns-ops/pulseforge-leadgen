# MAX-P0-004 — Durable Execution Ownership

Status: implemented on a development branch; not deployed. No worker, cron, provider, tenant policy, production migration, or frontend path is enabled by this change.

## Design

`max_durable_execution_tasks` is the authoritative record for delegated work. It retains tenant and objective identity, optional mission/parent relation, specialist and scope, immutable input/dependency snapshots, deadlines, retry budget, checkpoints, result/evidence references, and every recovery/cancellation/escalation reason. It is deliberately separate from the P0-003 action ledger: task ownership answers **who may proceed**; the ledger answers **whether an external action has already been claimed or may be unknown**.

An accepted handoff is inserted before an acknowledgement is returned. The `(tenant_id, logical_key)` unique key and input fingerprint make repeated delegation replay the first task and reject a changed request. A worker is not started by this package; an explicitly authorised worker may claim a queued task later.

State sequence: `requested → queued → claimed → running → awaiting_verification → completed`. `blocked_dependency`, `recovery_required`, `failed`, `cancelled`, and `escalated` are explicit alternatives. Specialist completion only moves to `awaiting_verification`; evidence is required for `completed`.

## Ownership and recovery

Claims atomically allocate a token, increment a monotonic fence epoch, and set a database-clock lease. Every start, heartbeat, checkpoint, completion, and external-action pre-dispatch check validates tenant, token, epoch, active state, and unexpired lease. Cancellation clears the owner and lease, preventing later work by an already-running process. A stale or superseded owner cannot write a checkpoint/result or enter the provider path.

Recovery never replays blindly. An expired active lease becomes `recovery_required`, preserving the checkpoint. Revalidation compares persisted dependency references with supplied current references: drift becomes `blocked_dependency`; an exact match returns it to `queued`. Deadlines and retry budget exhaustion escalate instead of looping forever. Unknown provider outcomes remain in the P0-003 ledger and are reconciliation-only.

`OutboundExecutionAdapter` accepts an optional `durableTaskOwnership` binding. Where supplied, it fails closed without the P0-003 ledger and invokes `requireOwner` immediately before the ledger dispatch marker. Thus an ownership transfer cannot produce a second logical send, and P0-004 does not create or bypass another action claim.

## Persistence and migration

Apply `migrations/2026-11-25-max-p0-004-durable-execution-ownership.sql` only after P0-003 and only with separate production approval. It is additive: two new tables and indexes; no existing row/table/policy changes. Old code ignores them, so application rollback is a previous deploy while retaining the records. Do not drop them while unfinished tasks or action claims could require recovery.

## Acceptance evidence

`test/maxDurableExecutionOwnershipPostgres.test.js` runs on disposable real PostgreSQL and is included in `npm run test:max:reliability-core` (the hosted Max orchestration lane). It covers durable/idempotent acceptance, two-pool concurrent claiming, stale-owner fencing after takeover, expired-lease recovery, checkpoint preservation, budget escalation, dependency drift, cancellation, specialist-vs-verified completion, tenant scope, and a stale-owner P0-003 guarded-dispatch injection with zero provider calls.

The suite intentionally requires `initdb` and `pg_ctl`; it does not skip when PostgreSQL is unavailable. Process restart and backend termination are already covered by P0-003's real PostgreSQL suite; P0-004's persisted records are reloaded by a fresh ownership store in the durable-acceptance test. Hosted execution is required before merge.

## Known limits

- No production worker is supplied or enabled. A later worker integration must use `claim`, revalidate dependencies, recover only bounded eligible tasks, and maintain heartbeats.
- The current dependency comparison is exact JSON reference equality. A domain adapter must persist canonical dependency versions; it must not silently reinterpret a dependency.
- Cancellation preserves P0-003 unresolved external-action claims. Operators must reconcile those effects separately.
- There is no new UI. Existing structured Max interfaces can expose the task row and event stream.

## Controlled release

1. Keep workers, Levi, schedules, and outbound execution disabled.
2. Run the Render Shell report below; require `read_only: "on"`, P0-003 installed, and no unexpected catalog/migration discrepancies.
3. Obtain separate approval, apply P0-003 if absent, then P0-004 migration once. Re-run the report and require `durableExecutionOwnershipInstalled: true`.
4. Deploy manually with `PF_DISABLE_BACKGROUND_WORKERS` still set. Run read-only health and task-state inspection only.
5. Roll back by redeploying the previous app revision; retain both reliability tables. Never erase unresolved task or action-ledger records as part of rollback.

## Render Shell production schema report (read-only)

In the Render Shell for `pulseforge-api-migration`, at the checked-out application root:

```sh
node scripts/max-reliability/schemaReport.cjs > /tmp/max-reliability-schema-report.json && cat /tmp/max-reliability-schema-report.json
```

Expected result: one JSON document with `kind: "max_reliability_schema_report"`, `server.read_only: "on"`, catalog counts, `maxCore`, `reliabilityCoreInstalled`, `durableExecutionOwnershipInstalled`, tenant/mission aggregates, and no row content. Before either approved migration the corresponding installed flag is `false`; after it is applied the flag must be `true`. Any connection, privilege, timeout, or unexpected missing-migration result is a stop condition.

Required environment: `DATABASE_URL` must be present and point to the private Render PostgreSQL endpoint. Preserve `PF_DISABLE_BACKGROUND_WORKERS` as set, `PF_MIGRATION_RECEIVER=0`, no `MISSION_STORE_ALLOW_EPHEMERAL`, and keep `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED`, `MAX_INTERPRETATION_SHADOW`, and any Levi/outbound activation flag disabled. The report itself needs no provider credentials and performs no write.
