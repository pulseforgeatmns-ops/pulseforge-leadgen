# Railway → Render cutover: execution record (2026-11-21)

Executed in Claude Code session `railway-render-cutover-5q7z85`. This file records what was done and what was verified. It is not a plan.

## Done

1. **Merged #1145** (structured `evidenceDate` / `sourceType` on review-only imports) into the cutover branch. It is not on `main`.
2. **Final-migration tooling** (`scripts/migration/`):
   - `exportFinalSnapshot.py`: a one-shot Railway worker.
     - Opens one REPEATABLE READ READ ONLY transaction and calls `pg_export_snapshot()`.
     - Runs `pg_dump --snapshot` on the full database, including Signal, and streams it to the private bucket.
     - In the **same snapshot**, fingerprints every non-Signal table, plus sequences and catalog counts.
     - Uploads `<key>` and `<key>.manifest.json`, and refuses to overwrite either.
   - `restoreFinalRender.py`: a one-shot Render receiver.
     - Checks the archive against an operator-pinned SHA-256 and the manifest.
     - Rehearsal mode restores into a NEW `pulseforge_rehearsal_<stamp>`; final mode restores into `pulseforge_production`, refusing if it exists.
     - Restores in a single transaction with exit-on-error. Signal tables are created empty.
     - Re-fingerprints the result and compares row counts, content digests, sequences (destination ≥ source) and catalog counts. Any difference fails it.
     - Never drops or renames the 2026-11-17 database.
   - `sendFinalRestoreTicket.py`: a one-shot Railway sender of presigned URLs.
   - `fingerprintSql.py`: the shared fingerprint SQL.
     - The digest is a sort-free numeric sum of the md5 words, so it writes no temp files.
     - `temp_file_limit` is 8 MB, and parallel workers are off.
3. **Local proof** on a disposable PostgreSQL:
   - a write committed after the snapshot is excluded from both the dump and the fingerprints;
   - a tampered manifest fails reconciliation;
   - a final restore refuses an existing `pulseforge_production`;
   - the Nov 17 database is untouched.
4. **Incident, contained.** The first export (2026-11-20 12:32Z) used a sorted digest that spilled temp files on the 96%-full Railway volume (`places_api_requests`).
   - PostgreSQL rejected that one read-only query (`No space left on device` on `pgsql_tmp`). The worker failed closed and was stopped.
   - Disk returned to its 9.61 GB baseline; there was no data loss.
   - Fixed by item 2's sort-free digest.
5. **Fresh pre-freeze full backup of Railway (2026-11-21):** see §Backup below.
6. **Hostname:** `sketchAgent.js` and `utils/telegram.js` now read `APP_URL`, and the setter dashboard's login links are relative.

## Backup

| Item | Value |
|---|---|
| Object | `MigrationBackup` bucket, `migration-backups/railway-20261120-prefreeze-pg18.dump` (+ `.manifest.json`) |
| Snapshot | 2026-11-21T12:44:44Z, a single exported snapshot (consistent while writes continued) |
| Size / SHA-256 | 728,011,922 bytes / `1fc6604330e81b85edb14022a75f5b7ac70e56b4934c2a17302ba206693bd85a` |
| Manifest SHA-256 | `73e6562f4837daa65c415989efd4414fb1505ff6212cc3b1575f1c31f40a6647` |
| Verification | Re-downloaded from the bucket after upload: SHA-256 and size match, and the `PGDMP` header is present |
| Content | Full database: 319 tables (274 Pulseforge + 45 Signal), 3 views, 80 triggers, 779 indexes, 3,629 constraints, 56 sequences |
| Pulseforge fingerprints | 274 tables, **1,630,255 rows** (Nov 17: 1,627,867), plus a content digest per table and every sequence value |
| Source impact | Disk stayed at its 9.61 GB baseline throughout; the job was read-only |

Status: **checksum-verified, not yet restore-tested on Render.** The Nov 17 archive and its Render restore are untouched.

## Not done, and why

- **A second agent is driving the same cutover.** A Cursor agent (co-authored by Jacob) repointed the Render service `pulseforge-api-migration` to `migration/render-current-main` at 12:36Z. It committed the `pulseforge_production` destination identity and prepared its own Railway export (`migration-offsite-export-20261121`).
  - Two agents writing to one Render database and one Railway source is unsafe.
  - So no Render-side restore, deploy or traffic change was made from this session.
  - The receiver variables this session set on that service were reset to inert values (`MIGRATION_BRIDGE_TOKEN=disabled`, approvals `OFF`). Setting and resetting them caused two redeploys of the other agent's commit, which is the same read-only staging probe.
- **Production secrets.** Railway returns variable *names only* to this session (about 150 on `pulseforge-leadgen`).
  - The Render production service cannot get its secrets from here without passing them through a chat transcript.
  - The owner pastes Railway's Raw Editor into Render → Environment → "Add from .env", excluding `DATABASE_URL` and `RAILWAY_*`.
- **Write freeze, traffic switch, webhook re-pointing, and Railway retirement** depend on the two items above. Retirement also needs the 7-day acceptance window and an off-Railway copy of the full archive (Signal data has no other copy).
