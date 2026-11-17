# Verified Railway source inventory: Pulseforge vs Signal

Status: READ-ONLY AUDIT COMPLETE FOR TABLE SIZES AND DECLARED FOREIGN KEYS. No rows exported, no destination restored, no production changes.

## Source and destination
- Railway source database: `railway`, PostgreSQL **18.6**, 9,218,078,399 bytes (**8.585 GiB**). Measured through Railway's SQL Query tab (2026-11-16).
- Render Pulseforge staging database: PostgreSQL **16**, 10 GB provisioned; independently connected by a read-only `SELECT 1` staging probe. Do not restore a PostgreSQL 18 dump into 16 without a verified compatibility plan. Prefer upgrading the *empty staging* database to 18; requires separate UI operation and subsequent connectivity test.
- The source database is under a Railway past-due subscription warning. Prioritize an offsite, portable logical dump; Railway volume backups alone cannot be restored directly to Render.

## Largest measured relations
| Current source table | Total bytes | Source estimated rows | Proposed owner |
|---|---:|---:|---|
| signal_opportunity_decisions | 7,656,611,840 | 1,569,564 | Signal |
| signal_dashboard_records | 500,391,936 | 314,600 | Signal |
| places_api_requests | 670,425,088 | 1,191,455 | Pulseforge |
| acquisition_outbound_events | 101,548,032 | 30,294 | Pulseforge |
| agent_log | 47,620,096 | 90,110 | Pulseforge |
| signal_distribution_request_attempts | 2,785,280 | 25,343 | Signal |
| signal_market_observations | 1,556,480 | 1,585 | Signal |

The two largest Signal tables total **8,157,003,776 bytes = 7.597 GiB = 88.49%** of the database. The residual after excluding just these two is **1,061,074,623 bytes = 0.988 GiB**, including smaller Signal tables and all Pulseforge data. These are relation sizes, NOT dump sizes; indexes/TOAST affect physical storage. Estimated rows derive from pg_class.reltuples, not exact COUNT(*). Values of -1 mean no estimate, NOT zero rows.

### Important unexpected storage
`signal_opportunity_decisions`: table total 7.657 GB, reported main heap 396.75 MB, main indexes 700.36 MB; approximately **6.559 GB** is storage outside those two measurements (primarily TOAST and its indexes). This warrants schema/JSON payload analysis; it does not prove bloat. Do not VACUUM FULL, REINDEX, truncate, or delete on Railway to 'fix' this.

## Ownership and declared FK graph findings
- The supplied complete FK listing shows **no declared foreign keys** from either `signal_opportunity_decisions` or `signal_dashboard_records` to Pulseforge tables.
- Signal research / distribution FK clusters appear internally connected: `signal_sources`, `signal_source_clusters`, `signal_tokens`, `signal_research_observations`, `signal_distribution_monitors`, and descendants. Preserve them together, including empty/low-volume tables.
- **Do not classify by name alone**: `prospect_signal_events` joins `prospects`, `companies`, `clients`; `warm_signal_state` and `warm_signal_events` join `prospects`. These are Pulseforge acquisition signals, not Signal product data.
- `market_observations` references `market_emails` and `market_companies`; treat as Pulseforge market intelligence pending code audit, not automatic Signal ownership.
- Absence of cross-product declared foreign keys is not proof of independence. Check triggers, views, functions, implicit JSON relationships, code-level joins and process-level connection pools before moving any data.

## Preliminary approach
1. **Protect data first**: inspect Railway Postgres Backups tab; create/check backup if account/volume allows, and produce a *portable offsite PostgreSQL 18 logical dump* to a secure encrypted location. Do not put data or credentials in GitHub/chat. Retain source untouched.
2. **Resolve versions**: align the empty Render Pulseforge destination with PG18 or validate an alternative schema/data conversion; PostgreSQL explicitly does not guarantee dumping to older major versions.
3. **Freeze ownership manifest**: assign all source tables to Signal/Pulseforge; check non-FK dependencies. Preserve every relation in one of the destinations, or explicitly document a shared integration system.
4. **Implement separate Signal pool** via `SIGNAL_DATABASE_URL` and inject into all Signal components. The current `routes/signalV1.js` imports `../db`; do not flip a variable alone and assume isolation.
5. **Snapshot-and-copy rehearsal**: use schema + table-filtered exports and restore into isolated databases; validate exact counts, foreign keys, integrity, business-critical smoke tests, job suppression and ownership.
6. **Follow-up data sync and cutover** require a delta strategy, cutover authorization and tested rollback. Railway remains production until then.

## Go/no-go and budget
- 10 GB is **plausibly adequate for Pulseforge-only data**: less than 1.1 GB remains after excluding only the two largest Signal relations, but check restore overhead and final ownership.
- Signal needs an independent storage/cost calculation. Its largest two tables already use ~8.16 GB; don't provision a 10 GB DB until capacity headroom and a cost agreement are verified.
- Approved base budget is $68/month total, excluding separately approved usage. No additional paid Signal instance until cost review.
- Source is PostgreSQL 18.6; destination PostgreSQL 16. **Direct backward restore not guaranteed** by PostgreSQL docs.

## Evidence
- Source: user-run Railway SQL query result containing `tables`, `foreign_keys`, `database_bytes`, and `postgres_version`.
- Catalog SQL: `scripts/migration/railwaySourceInventoryOneQuery.sql`.
- PostgreSQL 18 pg_dump compatibility: https://www.postgresql.org/docs/18/app-pgdump.html
- Render PG upgrade procedure: https://render.com/docs/postgresql-upgrading
- Railway backup types and caveats: https://railway.com/guides/postgres-backups-restores

## 2026-11-17 code staging status (no live cutover)
- On `migration/render-isolated-staging`, `signalDb.js` offers `SIGNAL_DATABASE_URL`. When `SIGNAL_DB_SEPARATION_REQUIRED=1`, the shared `DATABASE_URL` fallback is blocked; matching connection strings are rejected.
- Signal V1 route, dashboard runtime and worker, Signal operator, research replay/ingest, standalone dashboard server, and Signal dashboard migration entry point have been routed to the new helper. Legacy behavior remains available until cutover.
- `test/signalDatabaseRouting.test.js` checks missing/matching/invalid Signal configuration. These changes remain unmerged and unactivated.
- The standalone dashboard currently resolves owner identity from its database `users` table. A physically separated Signal DB without migrated auth/identity will fail. `SIGNAL_STANDALONE_AUTH_MIGRATED=1` is deliberately required in standalone split mode until schema, identity and session migration are validated. Do not set this flag merely to bypass the gate.
- Render API staging branch is isolated by `PF_MIGRATION_STAGING=1`; no application routes, outbound or worker start. Render has 10 GB destination PostgreSQL 16; Railway source PG18.6. Do not transfer before a portable backup and PG major-version compatibility plan.
- The failed tablet `pg_dump` attempts did not complete a backup. Never treat the failed Termux session as a recovery copy.
- **Next blocking items**: portable verified source backup, separate Signal storage/budget approval, and comprehensive dependency/identity migration rehearsal.
