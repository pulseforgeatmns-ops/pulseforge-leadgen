# Railway → Render: isolated restore rehearsal

**Status: PREPARED, NOT EXECUTED.** Never run this checklist against production Railway or activate the Signal split before explicit cutover authorization.

## Evidence already captured

- Source Railway PostgreSQL 18.6 at last inventory: 9,218,078,399 bytes; source remains live and changing.
- Private backup in Railway `MigrationBackup` S3 bucket: `migration-backups/railway-20261117-full-pg18.dump`. Completed 2026-11-17, 727,898,203 bytes, SHA-256 `4d44ad1f73ffcb5a3ee48ee8b30c78c2430f803da05915b40b84b6101a736628`.
- Independent temporary worker verified object checksum and PostgreSQL 18 `pg_restore --list`: 2,137 TOC items, 322 TABLE definitions, 319 TABLE DATA items, 350 INDEX items. This confirms a readable archive directory, **not restored rows or schema correctness**.
- User has downloaded `railway-20261117-full-pg18.dump` to tablet Downloads, with Android reporting 728 MB. **Do not mark tablet SHA verified solely from this screenshot**; independently compare checksum on the downloaded file.
- Render destination `dpg-dbtgtloae00c73dd8040-a`: PostgreSQL **18**, 10 GB, available; isolated staging API only. It is intended for Pulseforge, not the full combined 9.2 GB source.
- Draft PR #1129 is isolated/unmerged. `PF_MIGRATION_STAGING=1` must remain set. No outbound, workers, hooks, production scheduler or Stripe startup in the rehearsal.

## Dependency and capacity gates (do first)

1. Preserve the full archive in both Railway private bucket and a verified **non-Railway** private copy. Keep the Railway source online. Treat backup as sensitive.
2. Produce a reviewed ownership manifest for **all** 322 source tables and dependencies before any table filter. Names alone are not sufficient. Explicitly keep `prospect_signal_events`, `warm_signal_state`, and `warm_signal_events` with Pulseforge, not the separate Signal product.
3. Review PostgreSQL extensions, views, functions, triggers, sequences, FK edges, ownership, user identity/auth/session dependencies and application SQL joins. Record shared/duplicated tables and compatibility strategy.
4. Reserve adequate target space and budget for both products. Source `signal_opportunity_decisions` ~7.657 GB and `signal_dashboard_records` ~0.500 GB; Pulseforge remaining data is ~1.061 GB **including** smaller Signal tables. Don't restore both largest Signal tables into the 10 GB Pulseforge database.
5. Have a **secure transfer path** into the isolated Render environment. Render external Postgres IP allowlist is empty; do not turn on public access merely for convenience.
6. Rehearsal must write only into an isolated empty/disposable destination, never the running production DB and never a shared staging DB holding irreplaceable records.

## Archive and restore validation

1. Check exact backup byte length, SHA-256, custom `PGDMP` header and `pg_restore --list`. The TOC listing is safe; it does not write to a database.
2. Review the TOC and ownership manifest to produce a fully covered partition of selected **data** entries. Don't use `--exclude-table-data` or name-prefix-only filters as a substitute for reviewing dependencies.
3. Restore reviewed schema, roles/privileges (using controlled owner/ACL strategy), sequences, TABLE DATA, indexes/constraints and post-data into the isolated database. Stop on errors; capture errors, warnings, and object counts. A successful `pg_restore --list` alone does not satisfy this.
4. Compare every expected source table with its restored destination: existence, exact row counts captured from a consistent snapshot, sequence values, key constraints, indexes, FK checks, required extensions and app business queries. Treat estimations (`reltuples`) as **not** exact counts.
5. Validate Render connectivity without enabling migrations, workers, outbound, Signal dashboards/collectors, or other side effects. Test client auth/session boundary explicitly.
6. Confirm no customer secrets, PII, data payloads, URLs with credentials, or SQL dumps were logged or committed.

## Production cutover is separate and explicitly gated

- This 2026-11-17 dump is a point-in-time snapshot, **not a live synchronized replica**. New Railway writes since the dump must be reconciled using a planned delta or controlled write freeze.
- Before cutover: test idempotent delta replay, owner/client mappings, count drift, rollback, DNS/secrets, background-job authority, and end-to-end P0 autonomous outbound + Max routing.
- After authorized switch: track errors and a defined observation period. Keep Railway data and both backups until reconciliation and rollback window have passed.
- Do **not** merge PR #1129, set `SIGNAL_DB_SEPARATION_REQUIRED=1`, provision extra paid Signal DB storage, or change Railway production on the strength of a TOC validation.

## Current concrete blockers

- Actual source-to-Render **restore** has not happened.
- Need source ownership dependency review and a separately priced Signal destination with capacity headroom.
- Need source snapshot vs current live write delta strategy.
- Tablet copy download visible; local SHA-256 comparison wasn't returned to chat.

## 2026-11-17 follow-on audit and data partition validation (completed)

- One-shot read-only Railway metadata worker captured `docs/migration/railway-source-catalog-20261117.json` (full metadata, no customer rows); worker was deleted. At audit time: 319 physical tables, 361 declared FKs, 3 SQL views with 7 view dependencies, 80 application triggers, extensions `pgcrypto`, `plpgsql`, `uuid-ossp`.
- Proposed physical owners: 274 Pulseforge and 45 Signal tables, based on table names and known acquisition-signal exceptions. **Zero declared FKs and zero view dependencies** cross this *provisional* boundary. This does **not** prove code-level isolation.
- Candidate ownership map: `docs/migration/source-ownership-candidates-20261117.json`. Its classifications are not an approved export or restore list.
- Backup one-shot validator `scripts/migration/partitionBackupDataOnce.py` re-downloaded the 727,898,203-byte private PG18 archive, rechecked SHA-256, and confirmed **319 / 319** archived `TABLE DATA` entries exactly correspond to the live catalog's 319 physical tables. Pulseforge subset **274 / 274** and Signal subset **45 / 45** were each successfully decompressed independently with `pg_restore --data-only --use-list ... --file /dev/null`. Status `PARTITION_CHECK_COMPLETE`, deployment `866577cd-59f3-49b3-94f5-ff93b1f021df`. No database writes, restorations, or customer data logged. The temporary worker was deleted.
- **Critical code dependency**: `standalone/signal-dashboard/app.js` reads `public.users` for owner identity, password and session authorization, while Signal owns `signal_dashboard_session`. An isolated Signal database will not authenticate until identity is explicitly migrated/replicated or auth service is separated with tested revocation/authorization semantics.
- Physical capacities from catalog: Signal candidate tables **8,163,663,872 bytes** (~8.16 GB) and Pulseforge candidate tables **1,032,675,328 bytes** (~1.03 GB), excluding ~22 MB database-level overhead. These are source physical relation sizes, not dump or restored sizes.
- Backup schema has 322 `TABLE` definitions vs the 319 current physical `r,p` tables; the separate 3 SQL views account for the observed difference in archive object classifications. This needs a schema restoration confirmation, not just inferred compatibility.
- **Still NOT performed:** actual SQL restore to Render PostgreSQL, exact restored row counts, sequence and trigger verification, runtime identity/auth migration, or cutover.

## 2026-11-17 Render staging gated test deployment passed
- Render deploy `dep-dbu8g7l9fdbs73e0o05g`, commit `97e75623f894d0686c49870908a652a7d606a4db`, finished **live** at `2026-11-17T17:01:24Z`. Dockerfile's isolated-staging branch now runs both database-routing and catalog-manifest unit tests **before** starting `migrationStagingServer.js`.
- Render application logs explicitly report **12 tests, 12 pass, 0 fail, 0 skipped**, then `[migration-probe] isolated read-only health service listening`. This is a bounded migration unit suite, not full repository integration/CI or restored-data validation.
- Provisional owner mapping is in `docs/migration/source-ownership-candidates-20261117.json` (274 Pulseforge, 45 Signal). The catalog `docs/migration/railway-source-catalog-20261117.json` contains 378 relation/sequence/view objects, 319 physical tables, 361 FK edges, 80 triggers, and 7 view dependencies.
- The one-time archive partition worker was removed. Final Railway environment check shows only original `Postgres` and `OpenClaw Main` services and retained `MigrationBackup` bucket; no staged changes.
- **Blocked, not claimed complete**: physical restore into Render, Signal-only destination provisioning/budget, exact data reconciliation and standalone dashboard identity/auth separation. No schema or customer rows have been written to Render.

## 2026-11-17 CRITICAL: restore preflight caught wrong Render staging DATABASE_URL
- A one-time authenticated transfer ticket was accepted (`2026-11-17T17:20:38Z`), but the Render restore worker **failed closed at its first read-only preflight**, logging `MIGRATION_RESTORE_FAILED {"reason":"target_is_not_empty_postgres18"}`. It **never downloaded or restored the archive**, **never executed SQL writes**, and did not affect Railway production.
- Subsequent **read-only** in-service catalog inspection showed 378 non-system objects, exactly **9,218,275,007 bytes** database size (matching the Railway live source catalog at 16:50Z) and existing rows: `users=17`, `prospects=3944`, `signal_dashboard_records=317881`. This is NOT an empty destination.
- Render destination identity guard is now required by `scripts/migration/validateMigrationDatabaseUrl.js`: it only permits the intended database name `pulseforge_production_migration`, blocks Railway hostnames, and confirms the server-returned `current_database()` before returning healthy. The staging service startup log explicitly showed `migration_database_name_mismatch`. **This confirms the service's configured DATABASE_URL is not the intended Render database**; do not attempt further restore until corrected.
- Restore endpoint was disabled by changing `MIGRATION_RESTORE_ONE_TIME_APPROVED` and `MIGRATION_BRIDGE_TOKEN` to inert values; temporary Railway sender `migration-send-render-ticket-once` was deleted. Staging Dockerfile was reverted to the read-only Node health service and redeployed successfully.
- Render deployment at `2026-11-17T17:29:58Z` reports **15/15 migration tests passing**, and logs `[migration-probe] unsafe destination URL: migration_database_name_mismatch`. Failure is expected until the service DATABASE_URL is replaced.
- **Operator action**: in the Render Dashboard, open PostgreSQL `pulseforge-production-migration`, copy the **internal database URL** for the PostgreSQL 18 instance; open Web Service `pulseforge-api-migration` → Environment → replace **DATABASE_URL** with that value. Do **not** paste the credential in chats or GitHub. Revalidate healthy read-only staging before any restore ticket.
- All previous assumptions that a 200 staging `SELECT 1` was evidence of connecting to the Render destination are **invalid**. It tested *a* database, not its identity. Never drop/overwrite the observed 378-object database on this basis.

## 2026-11-17 ISOLATED PULSEFORGE RESTORE SUCCESS (snapshot only)
- Restored source backup `migration-backups/railway-20261117-full-pg18.dump` into the **empty** Render PostgreSQL 18 database `pulseforge_production_migration` using temporary authenticated one-shot Render receiver `scripts/migration/restoreGatewayRender.py`. Receiver enforced database host `dpg-dbtgtloae00c73dd8040-a`, expected name, empty catalog, major version 18, checksum, and repeat empty preflight before any write.
- Railway sender authenticated with short-lived presigned private bucket GET URL; received HTTP 202 and was deleted. Archive was re-downloaded by Render and reverified: **727,898,203 bytes**; SHA-256 `4d44ad1f73ffcb5a3ee48ee8b30c78c2430f803da05915b40b84b6101a736628`.
- `2026-11-17T17:42:06Z`: `MIGRATION_RESTORE_ARCHIVE_VERIFIED`, `MIGRATION_RESTORE_SELECTION_VERIFIED`: 319 total source TABLE DATA entries; 274 Pulseforge selected; 45 Signal excluded; one Signal SEQUENCE SET excluded. This rehearsal restores common schema including **empty Signal tables**, and skips their data.
- `2026-11-17T17:42:10Z`: `MIGRATION_RESTORE_SOURCE_ROWS_COUNTED`: **1,627,867 source snapshot rows** across **274** Pulseforge tables, using decompressed archive COPY streams. `MIGRATION_RESTORE_TRANSACTION_STARTED`.
- `2026-11-17T17:42:50Z`: `MIGRATION_RESTORE_TRANSACTION_COMMITTED` (single transaction, exit-on-error).
- `2026-11-17T17:43:02Z`: `MIGRATION_RESTORE_RECONCILIATION_PASSED`: all **274 Pulseforge table exact row counts equal archive** (total **1,627,867**); all **45 Signal schema tables have zero rows**. `MIGRATION_RESTORE_COMPLETE` status `RESTORED_AND_ROW_COUNTS_MATCH`. Render PostgreSQL server reports 18.6.
- Temporary gateway token and activation variables were set to inert values. Dockerfile returned to the read-only Node staging probe, with staging guards and 15 migration unit tests. **Verify new deployment is live before declaring gateway retired.**
- Source Railway production database was not modified. Railway bucket and downloaded tablet backup retained.
- **Not a cutover**: point-in-time snapshot only; Railway has continued writing since export. Signal data remain on Railway. Separate Signal database budget, auth/identity coupling, app compatibility tests, full business-function reconciliation, write freeze/delta strategy, rollback, and production migration authorization remain unresolved.

## 2026-11-17 post-restore service lockdown verified
- One-time restore permission and bearer token reset to inert values in Render environment.
- Restored branch's normal Dockerfile (Node read-only staging health probe with `PF_MIGRATION_STAGING=1` and **15** guard tests). Render deploy `dep-dbu94jgn74is73d8u4cg`, Git commit `a4ca2af53d1429f1d56e37c4168b2a49b1ddf835`, finished **LIVE** at `2026-11-17T17:44:19Z`.
- Read-only startup logs: **15 tests passed, 0 failed**; approved database name `pulseforge_production_migration`, PostgreSQL version 18.6 (`180006`), **378 non-system objects**, restored database size **991,557,311 bytes** (~0.99 GB). No active restore endpoint on this service.
- Render PostgreSQL instance remains **available** (10 GB provisioned). Railway inventory shows only original `Postgres` and `OpenClaw Main` services and retained private `MigrationBackup` bucket, no pending changes.
- Draft PR #1129 remains **OPEN, DRAFT, UNMERGED**. No Railway production cutover or data deletion.

## 2026-11-21 Pulseforge-selective restore contract

Pinned archive: `migration-backups/railway-20261121-full-pg18.dump` (728,012,480 bytes, SHA-256 `4cf4e95bf56706e09ba411349ed68cfcf8725fef2ee4964ed6af73fce29fd734`). Target is empty `pulseforge_production` on host prefix `dpg-dbtgtloae00c73dd8040-a`. The November 17 snapshot database `pulseforge_production_migration` must remain untouched.

Authorization: `SELECTIVE_PULSEFORGE_OPERATIONAL_RESTORE` (`authorizedSignalDataExclusion=true`). Restore **274 Pulseforge operational TABLE DATA** entries. Keep the **45 Signal tables as empty schema** for application compatibility. Do **not** restore Signal table data into Pulseforge PostgreSQL. Do **not** increase Render's 10 GB disk (`increaseRenderDisk=false`). This is not a silent discard: Signal history remains in the full November 21 archive for a separate Signal database workstream (`signalHistoryRetainedInArchive=true`, `silentlyDiscardSignalData=false`). `prospect_signal_events`, `warm_signal_state`, and `warm_signal_events` stay with Pulseforge.

Storage check uses the Pulseforge physical floor **1,032,675,328 bytes** plus the preserved snapshot and 12% WAL headroom. Against the live sibling databases that is about **2.17 GB**, which fits the existing 10 GB disk. The 9.218 GB combined source floor is retained in the contract as archive identity only; it is not the restore capacity requirement.

PR #1146 stays unmerged. Production writers stay off. `PF_MIGRATION_STAGING=1`. Railway remains authoritative until cutover approval. Signal migration must not block the Pulseforge restore.

## 2026-11-22 receiver armed (awaiting Railway ticket)

Render service `pulseforge-api-migration` deploy `dep-dc1f87vf3r2c73ciaufg` is **live** on commit `1a4688b2` with `Dockerfile.migration-render-restore`. `DATABASE_URL` is the internal URL for empty `pulseforge_production`. `PF_MIGRATION_STAGING=1`. One-time restore approval is armed. Disk remains 10 GB.

HTTP `/healthz` → `200 isolated restore service available`. Direct Tigris download is deployed at `73afcba` (`dep-dc1fd6ff3r2c73ciq0s0`). HTTP `/internal/migration/preflight` currently returns `503 missing_tigris_credentials`: `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` are not present on `pulseforge-api-migration` (API listing or runtime). No `pg_restore` has run. Snapshot `pulseforge_production_migration` is unchanged. The receiver remains armed on empty `pulseforge_production`.

