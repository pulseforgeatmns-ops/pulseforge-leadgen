# JEV shadow reconciliation

The runtime INSERT in `ShadowEventRepository.FIELDS` always includes the ten
JEV-006 observability columns. The September 21 base migration creates none of
those columns; September 30 JEV-006 adds them and creates
`decision_shadow_evidence`. A database with only the base migration rejects the
whole INSERT with PostgreSQL 42703. Evidence writes also require JEV-006.

Railway production logs, inspected November 9, report `schema_column_missing`,
`persisted:0`. They do not identify the missing column or prove which migrations
were applied. OAuth withholds database credentials. The exact deployed missing
column set and migration history therefore remain unverified. Do not treat the
architecture document's unapplied-migration hypothesis as a verified diagnosis.

## Operator procedure (no automatic DDL)

1. In the existing Railway service environment, run
   `node scripts/checkDecisionShadowSchema.js`. It performs only a catalog SELECT,
   prints missing column names, and returns 2 if incomplete. No credentials are
   printed. Save the result with the deployment SHA and database identity.
2. Inspect the deployment's migration records and compare the base migration
   `migrations/2026-09-21-decision-shadow-review.sql` and
   `migrations/2026-09-30-jev-006-shadow-observability.sql` with the actual schema,
   including column types, constraints and indexes. If base columns or types
   differ, stop and prepare a targeted reconciliation from that evidence.
3. If the only missing fields are exactly the additive JEV-006 fields/table,
   obtain production-change approval and run the existing JEV-006 SQL through
   the governed operator migration workflow. Its transaction and IF NOT EXISTS
   clauses make replay safe for the known expected schema. Do not create a
   second speculative migration or edit historical data.
4. Repeat the SELECT-only preflight. Check `decision_shadow_events` and
   `decision_shadow_evidence` with a normal read-only inspection turn. Confirm
   new decision IDs, tenant scope, timestamps, full projections and review CLI
   retrieval; confirm logs report successful persistence. Preserve an audit of
   the migration and checks. No mission state or outbound gate is changed.

Rollback: revert the telemetry classification change if necessary. Retain
additive audit columns and collected rows; dropping them loses evidence.

Correct operational inspection previously compared as intelligence because
`observedRoute` omitted the `operational_inspection` owner. It now maps that
owner to inspection using handler metadata, without reclassifying message text.
Operational analysis remains intelligence. Production routing is unchanged.
