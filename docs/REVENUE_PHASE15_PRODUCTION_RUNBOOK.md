# Revenue Phase 1.5 Production Runbook

**Production status:** unauthorized until a named operator approves a bounded window and the controlled Anchor canary plan. This runbook does not authorize execution.

## Scope and safety contract

Phase 1.5 hardens the existing closed-loop revenue foundation. It does not enable retention automation, send customer communications, mutate historical ledger events, or authorize autonomous follow-up. Refunds and corrections are new compensating events; existing `revenue_events` rows are never edited or deleted.

The required CI check is `Revenue PostgreSQL Integration / revenue-postgresql-required`. Repository rules must mark that check required before merging revenue changes. The workflow creates a fresh PostgreSQL cluster, applies the complete revenue migration chain, runs lifecycle/replay/rollback/isolation/reconciliation tests, and destroys the cluster.

## Backup verification

Before the change window:

1. Create an encrypted `pg_dump --format=custom` backup under the production backup policy.
2. Record the object identifier, encrypted and plaintext SHA-256 hashes, database identity, start/end timestamps, and operator.
3. Independently download, hash, decrypt, and restore it into disposable PostgreSQL.
4. Run `npm run revenue:rebuild -- --all-tenants --compare-only` against the restored copy.
5. Stop if the report is not `passed`, any unexplained event exists, or any ledger/projection/source metric differs.

## Migration

Approved command shape:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/2026-07-18-anchor-closed-loop-revenue-phase15.sql
```

The migration is transactional. `ALTER TABLE revenue_outcomes` takes an `ACCESS EXCLUSIVE` lock while adding the count columns. `REVOKE` and the new tables take catalog/table locks. Run in a quiet bounded window with a pre-approved lock timeout. Stop rather than retry automatically if the lock cannot be acquired.

All five database flags default false. All five matching environment flags also default false. A capability is active only when both layers are true.

## Verification SQL

```sql
SELECT client_id, revenue_schema_enabled, revenue_operator_reads_enabled,
       revenue_operator_writes_enabled, revenue_max_reads_enabled,
       revenue_followup_recommendations_enabled
FROM revenue_feature_flags ORDER BY client_id;

SELECT has_table_privilege('revenue_application','revenue_events','UPDATE') AS can_update,
       has_table_privilege('revenue_application','revenue_events','DELETE') AS can_delete;

SELECT tgname FROM pg_trigger
WHERE tgrelid='revenue_events'::regclass AND NOT tgisinternal;

SELECT client_id,status,mismatches,reconciled_at
FROM revenue_reconciliation_runs ORDER BY reconciled_at DESC LIMIT 20;
```

Expected: all flags false after migration, both application-role privileges false, append-only trigger present, and no failed reconciliation.

## Feature-flag sequence

Each step requires the preceding verification to pass. Set the environment and tenant row for only the approved tenant.

1. `REVENUE_SCHEMA_ENABLED=true` and `revenue_schema_enabled=true`.
2. `REVENUE_OPERATOR_READS_ENABLED=true` and `revenue_operator_reads_enabled=true`.
3. Run compare-only reconciliation; require exact equality.
4. `REVENUE_OPERATOR_WRITES_ENABLED=true` and `revenue_operator_writes_enabled=true` for the bounded Anchor canary only.
5. Record one controlled Anchor lifecycle with an operator-supplied idempotency key and correlation ID.
6. Run compare-only reconciliation; require zero mismatches and zero unexplained events.
7. Enable `revenue_max_reads_enabled` only after separate read approval.
8. Keep `revenue_followup_recommendations_enabled=false` until Phase 2 approval. No retention automation is authorized here.

## Rebuild and reconciliation

```sh
npm run revenue:rebuild -- --client-id=10 --dry-run
npm run revenue:rebuild -- --client-id=10 --compare-only --from=2026-07-01T00:00:00Z --to=2026-08-01T00:00:00Z
npm run revenue:rebuild -- --client-id=10 --apply
npm run revenue:rebuild -- --all-tenants --compare-only
```

`--dry-run` and `--compare-only` roll their diagnostic records back and make no durable changes. `--apply` requires the schema and operator-write flags at both layers. Output includes ledger events, projected outcomes, mismatches, before/after totals, unexplained events, per-tenant three-way totals, and an execution correlation ID. A mismatch exits nonzero.

## Operational rollback

First disable all five environment flags. Then run:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/2026-07-18-anchor-closed-loop-revenue-phase15.rollback.sql
```

This fail-closes every tenant and preserves ledger, projection, reconciliation, and operator-audit evidence. Do not drop revenue tables after production evidence exists. Correct financial facts through a separately authorized compensating ledger event linked with `supersedes_event_id` and `is_compensating_event=true`.

## Stop conditions

Stop before writes or roll back the active transaction if any condition occurs:

- backup identity, hash, restore, or deployment revision differs;
- required CI check is absent or failing;
- migration lock exceeds the approved timeout;
- any feature flag is unexpectedly enabled;
- append-only privilege or trigger check fails;
- tenant mismatch, duplicate ambiguity, unexplained event, or reconciliation mismatch is nonzero;
- projection rebuild differs between replays;
- provider, lifecycle, customer communication, or follow-up activity appears;
- Anchor is not the explicitly approved canary tenant;
- the bounded window expires or the named operator withdraws approval.

## Required named approval

The production artifact must identify the operator, role, identity, approved commit/deployment, backup identifiers and hashes, migration hashes, change-window start/end, Anchor client ID `10`, exact canary actions, rollback owner, and accepted stop conditions. Blank placeholders do not authorize execution.
