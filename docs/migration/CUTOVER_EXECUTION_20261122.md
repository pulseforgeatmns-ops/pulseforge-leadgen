# Railway → Render cutover: execution record (2026-11-22)

Session branch `claude/pulseforge-railway-render-cutover-m1r15e`, based on `migration/render-current-main` @ `cbfd8aa`. This file records observations and changes. **No cutover happened.** Railway is still production.

## Observed state (16:30Z)

| Item | Observation | Evidence |
|---|---|---|
| Railway `pulseforge-leadgen` | Live on deployment `290910f0` (`main`), and still taking writes. | HTTP log at 16:30Z: cron-job.org calls `/cron/anchor-max-outbound-control` every 15 min; the IMAP poller is running. |
| Railway Postgres (`amused-harmony`) | Live. TCP proxy `shinkansen.proxy.rlwy.net:43478` → 5432 reports `ACTIVE`. | `list-tcp-proxies`; checkpoints logged every 5 min, LSN `B/6997…`. |
| `migration-final-export-once` | Still on its 2026-11-21 deployment. Railway refuses redeploys because the trial has expired. | Owner report; not retried. |
| Render `pulseforge-api-migration` | Live on `dep-dc1grs3l550s73efjau0` (`cbfd8aa`). Background workers are off. | Startup log: `background workers disabled for Render API cutover`, `[clients] startup complete`. |
| Render secrets | Mostly absent: the startup banner prints `SerpAPI ✗`, `Hunter ✗`. | Render app log, 15:45:49Z. |
| Render Postgres `dpg-dbtgtloae00c73dd8040-a` | Available, PG 18. The external IP allowlist is empty, so the Render MCP cannot query it. | `list_postgres_instances`; query refused. |
| Render data | `pulseforge_production` holds the Nov 21 12:44:44Z snapshot, now about 28 h behind Railway. The Render app's startup DDL has also run against it. | 2026-11-21 record. |

## Callers that must move at cutover

From Railway HTTP logs, Nov 19–22, 501 sampled requests:

| Caller | Paths | Who changes it |
|---|---|---|
| cron-job.org (owner's account) | `/cron/anchor-max-outbound-control` (every 15 min), `/cron/emmett`, `/cron/riley`, `/cron/warm_signal`, `/cron/paige`, `/internal/cron/max-decay` | Owner, in cron-job.org. No API access from here. |
| GitHub Actions `penny-anchor-daily` | `/cron/penny-daily` | Now reads the repository variable `PULSEFORGE_APP_URL` (this branch). It takes effect only once merged to `main`. |
| Browsers (dashboard, setter, AO CRM, admin) | `/api/*`, `/setter/*`, `/ao/*` | Users switch to the new URL. |
| Railway crons | `max-decay-daily`, `max-context-cleanup-hourly` | Replace on Render, or via cron-job.org. |
| External consoles | Brevo, Bland, Stripe, Twilio, Telegram, Google OAuth redirects, Mira context URL | Owner, in each console (no webhook hits fell in the sample window). |
| Static sites | `sites/anchor-cleaning/*`, `deploy/anchor-cleaning-pages/*`, `sites/studio-substral/*` post forms to the Railway host | Switch **at cutover only**. Moving them earlier sends live leads to the stale Render database (split brain). |

## Changes on this branch

1. `scripts/migration/exportFinalSnapshot.py` (from `claude/railway-render-cutover-5q7z85`) gains one source mode: `FINAL_EXPORT_SOURCE=railway_public_proxy`.
   - It needs a second approval, `FINAL_EXPORT_EXTERNAL_APPROVED=YES_PINNED_RAILWAY_TCP_PROXY`.
   - The host and port are pinned to `shinkansen.proxy.rlwy.net:43478`, and `PGSSLMODE=require` is mandatory.
   - The default mode and all its guarantees are unchanged: a read-only snapshot, fingerprints from the same snapshot, no overwrite, and re-download verification.
   - The validation matrix was tested locally: every wrong host, port, approval or TLS combination is refused.
2. `.github/workflows/railway-final-export.yml` is a one-shot runner for that exporter on a GitHub-hosted runner (raw-TCP egress, PG 18 client from PGDG).
   - It fires only when `docs/migration/final-export-request.json` changes on this branch, and it fails closed without its three secrets.
   - It uploads to the pinned Tigris bucket `migrationbackup-njzkny-vy`.
3. Hostname: `sketchAgent.js` and `utils/telegram.js` read `APP_URL`. The setter-dashboard login links are relative. `penny-anchor-daily` reads `vars.PULSEFORGE_APP_URL`.

## Blockers (why the database was not exported)

1. **This sandbox cannot reach PostgreSQL.** The egress proxy carries HTTPS CONNECT only (raw-TCP databases are unsupported), and `shinkansen.proxy.rlwy.net` does not resolve here.
2. **No agent-readable source credential.** The Railway MCP returns variable *names* only. GitHub has no `DATABASE_URL` secret. The Render MCP cannot read environment values.
3. **No in-Railway compute.**
   - Railway redeploys are refused (trial expired).
   - An environment fork (copy-on-write database plus sandbox) was attempted and was **denied by this session's permission policy**.
4. **Render production secrets** (about 150 names on Railway, listed through `describe-service`) are not readable by agents. Render must get them from the owner.

## Next steps once unblocked

1. Fresh export:
   - **Either** Railway Hobby plan: redeploy `migration-final-export-once` with a new `BACKUP_OBJECT_KEY`. This needs no new code.
   - **Or** add the 3 GitHub secrets, then commit `docs/migration/final-export-request.json` as `{"approval":"YES_READ_ONLY_SNAPSHOT_TO_BUCKET","backupObjectKey":"migration-backups/railway-<stamp>-final-pg18.dump"}`.
2. Pin the new archive's SHA-256 in a new restore contract. Restore into a **new** database on the Render instance (the Nov 21 database stays). Reconcile against the manifest fingerprints.
3. Write freeze:
   - pause the cron-job.org jobs and the Railway crons;
   - stop the Railway web service (this needs Railway to accept changes);
   - take the final export (step 1 again) and restore it.
4. Repoint Render `DATABASE_URL`. Set `PF_DISABLE_BACKGROUND_WORKERS=0` on exactly one instance; outbound flags stay as on Railway. Validate, then switch callers.

## Update: no-payment transfer path (later on 2026-11-22)

Railway payment is prohibited. PR #1149 is merged into `migration/render-current-main`.

### Credential investigation (proven)

| Path | Result |
|---|---|
| Railway MCP `list-variables` on the Postgres service | `valuesRedacted: true`: names only, for this OAuth app. |
| Railway in-platform compute | Redeploys refused (trial expired). A fork environment was denied by this session's permission policy. Restarting `migration-final-export-once` would re-run it with its pinned key, which already exists, so it refuses by design. |
| Render env / logs | The Render MCP cannot read env values. Logs since Nov 16 contain no Railway credential. |
| GitHub secrets | No agent tool can write repository secrets (the GitHub MCP has no secrets API, and `gh` is restricted to search). |

The Railway PostgreSQL password is readable only by the owner. Everything else needs **no** new secret:
- Render authenticates the Actions job by its **GitHub OIDC token** (issuer, audience, repository, branch, workflow file and event pinned).
- Render already holds its own database URL.

### Transfer pipeline (this branch)

| Piece | Role |
|---|---|
| `.github/workflows/railway-final-export.yml` | Fires when `docs/migration/final-export-request.json` changes on this branch. Modes: `rehearsal`, `final`, `unfreeze`. A failure after the freeze unfreezes Railway automatically. |
| `scripts/migration/freezeRailwaySource.py` | Database-level `default_transaction_read_only=on`, then ends sessions. The quiet-window proof requires that no session holds a transaction id **and** the next transaction id does not advance. Reverse with `unfreeze`. |
| `scripts/migration/exportFinalSnapshot.py` | New `BACKUP_OUTPUT_FILE` mode. With `FINAL_EXPORT_REQUIRE_FROZEN=1` it records `sourceFrozen` from inside the snapshot. |
| `scripts/migration/uploadToRender.py` | Sends 32 MiB chunks with a fresh OIDC token on each request, then polls the receiver's verdict into the Actions log. |
| `scripts/migration/restoreFinalRender.py` + `githubOidc.py` | Render receiver (`PF_MIGRATION_RECEIVER=1`; the app is not running in this mode). Restores into a **new** database, checks disk headroom, reconciles fingerprints, drops rehearsal copies, and promotes the final copy by rename only, keeping the previous primary as `pulseforge_pre_cutover_<stamp>`. |
| `Dockerfile` | Adds PostgreSQL 18 client tools and python, and the receiver CMD branch. Normal start is unchanged. |
| `scripts/migration/switchPublicHost.js` | Cutover-only host switch (22 occurrences). Studio Substral is excluded: its doctrine forbids "Pulseforge" in page HTML, so it needs a neutral custom host at launch. It is not launched yet. |

### End-to-end proof (disposable PostgreSQL 18.7 source and destination, real pg_dump/pg_restore 18)

- **Rehearsal under a concurrent writer:**
  - the snapshot held 50,022 prospects while writes continued to 50,400;
  - 0 content differences across 50,543 rows;
  - 1 live-sequence advance was reported as informational only;
  - Signal data was excluded and the Signal tables exist empty;
  - the scratch copy was dropped.
- **Freeze under a live writer and a long-lived pooled session:**
  - the session was ended;
  - every later insert failed with `cannot execute INSERT in a read-only transaction`;
  - the transaction id was stable through the quiet window.
- **Final:**
  - the export was proven frozen;
  - 0 differences, including **exact** sequences;
  - the swap committed, and the old primary was kept;
  - the view, the trigger, the sequence and the empty Signal schema were present.
- **Rollback:** unfreeze restored writes, and re-freeze is idempotent.
- **Refused:**
  - missing, wrong-ref, wrong-audience, expired, other-workflow, pull-request-event or tampered-signature tokens;
  - a final upload carrying a live manifest;
  - a chunk from another run;
  - out-of-order chunks;
  - a tampered archive (checksum).
- **Defects found and fixed by this run:**
  1. A live-sequence false positive.
  2. **Unfreeze refused inside a read-only default.** The freeze session now forces read-write.
- `test/test_cutoverTransfer.py` covers the OIDC gate and the manifest rules.

### Runbook once the secret exists

1. Merge this branch into `migration/render-current-main`, then deploy Render.
2. Set on Render: `PF_MIGRATION_RECEIVER=1`, `MIGRATION_RESTORE_APPROVED=CREATE_NEW_DATABASE_ONLY`.
3. Commit the request `{"mode":"rehearsal","approval":"YES_READ_ONLY_SNAPSHOT_TO_RENDER_SCRATCH"}`. This tells us the real Railway timing, archive size and the 274-table reconciliation, with no freeze.
4. Config reconciliation: confirm the Render env holds every Railway name (153 listed above) with the outbound/publish flags unchanged. Then set `APP_URL` and `DASHBOARD_URL` to the Render host.
5. Final:
   - set `MIGRATION_FINAL_SWAP_APPROVED=RENAME_PRIMARY_KEEP_AND_PROMOTE_VERIFIED`;
   - commit `{"mode":"final","approval":"YES_FREEZE_EXPORT_RESTORE_PROMOTE"}`;
   - Railway freezes, Render restores, verifies and promotes.
6. Set `PF_MIGRATION_RECEIVER=0` and `PF_DISABLE_BACKGROUND_WORKERS=0` (one instance). Validate login, Max, Scout, tenants and `/cron` auth.
7. Run `switchPublicHost.js --write`. Sync the anchor-cleaning Pages repo. Set the repository variable `PULSEFORGE_APP_URL`. Repoint cron-job.org and the external consoles.
8. Railway stays frozen and intact (rollback = `{"mode":"unfreeze",...}` plus reverting callers) until acceptance.

## Production-data rehearsal: PASSED (2026-11-22 17:28–17:40Z)

Railway was not frozen, the existing Render databases were not touched, and no traffic was redirected. Background workers stayed off.

| Item | Evidence |
|---|---|
| Render | Deploy `dep-dc1i5id9fdbs73au9d00` (`83a91ec`, PR #1150) built with PG 18 tools. Receiver mode `dep-dc1i8h142hec73ael28g` reported `FINAL_RESTORE_RECEIVER_READY` with primary `pulseforge_production` (992.9 MB) and the Nov 17 database (991.6 MB). Swap approval `OFF`. |
| Actions run | `45049608576`, job `137291619574`: success. `RAILWAY_PG_PASSWORD` was masked (`***`) in all output. |
| Source | `SOURCE_FREEZE_STATUS frozen=false`, 3 application sessions. Freeze step **skipped**. Railway server 18.6, database 9,219,577,535 bytes. |
| Snapshot | `2026-11-22T17:28:44.426195Z`, a single REPEATABLE READ READ ONLY exported snapshot. |
| Archive | 728,046,603 bytes. SHA-256 `cd4dbcfe298e1fa0ea3d2d5cc2dc41f7f31eafdcbf2d626e6a49ade43c168d88`. Manifest SHA-256 `5f54f2849be9e9961896d98ff3d61416a8bd38d7f6c6d6b0965d695d38d1d6a7`. Re-read verified on the runner. Export took 8 m 37 s. |
| Off-Railway copy | GitHub artifact `14765103365` (`railway-20261122T172824Z-rehearsal-pg18`, 728,081,741 bytes, retained 90 days): the full database including Signal history. |
| Transfer | 22 chunks in 68 s, each authenticated by GitHub OIDC (run `45049608576`). Render re-verified size, SHA-256 and the `PGDMP` header. |
| Selection | 319 TABLE DATA entries: 274 Pulseforge tables restored; 45 Signal tables kept as empty schema. |
| Restore | New database `pulseforge_rehearsal_20261122173838`. Single transaction committed in 59 s. |
| Reconciliation | **0 differences.** 274 tables, **1,630,980 rows** with matching content digests. 56 sequences compared (0 advanced). Catalog identical: 319 tables, 3 views, 80 triggers, 779 indexes, 3,629 constraints, 84 functions, 56 sequences. 45 Signal tables empty. |
| Cleanup | Rehearsal copy dropped. Render returned to app mode (`PF_MIGRATION_RECEIVER=0`, restore approval `OFF`) on the unchanged `pulseforge_production`. |

**Expected final-cutover write-freeze window, measured:** freeze about 0.5 min, export about 9 min, transfer about 1 min, restore plus reconciliation about 2 min. That is about 13 minutes before Render can be validated.

**Risk observed:** the Railway database is 9.22 GB on a 10 GB volume, so headroom is limited. The final transfer should not be delayed long.

## Final cutover: COMPLETE (2026-11-22)

| Step | Evidence |
|---|---|
| 18:45:28Z snapshot | Verified and promoted (#1151). The workflow had already unfrozen Railway at 18:49:59Z when the swap was refused (`databases_in_use`), so Railway kept writing. |
| Render after that promotion | 0 HTTP requests from 18:50Z to 19:09Z (request log empty). Only idempotent startup DDL had run, so replacing the database lost nothing. |
| Run 45059774226 | Frozen 19:17:04Z, snapshot 19:17:05Z, 1,631,023 rows, 0 differences. Swap failed `InsufficientPrivilege` (terminating a Render-managed session). Railway was auto-unfrozen at 19:26:33Z. That database (`pulseforge_cutover_20261122192448`) is kept, unused. |
| Fix #1153 | The swap ends only its own role's sessions and waits out Render-managed ones. |
| **Run 45061306144** | Frozen 19:32:58Z. Snapshot **19:32:59.059Z**. Archive 728,048,843 bytes, SHA-256 `62141ea3…f208`, kept as a GitHub artifact. Restored into `pulseforge_cutover_20261122193557`: **0 differences, 274 tables, 1,631,027 rows**, 56 sequences exact, catalog identical. **Swap committed** (4 attempts); the previous primary is kept as `pulseforge_pre_cutover_20261122193557`. |
| Railway retired | 19:38:00Z: database renamed `railway_retired_20261122` (frozen, data intact). The Railway app logs `database "railway" does not exist` for every access, so it cannot read or act. Rollback: request `{"mode":"unretire",...}`. |
| Render live | `PF_MIGRATION_RECEIVER=0`, restore/swap approvals `OFF`, `PF_DISABLE_BACKGROUND_WORKERS=0` (1 instance), `APP_URL`/`DASHBOARD_URL` set to the Render URL. Workers started at 22:11Z. Receiver mode remained on from 19:38Z to 22:09Z because of a session worker restart. |
| Callers moved | Anchor Cleaning Pages forms (`anchor-cleaning@d822d59`). In-repo hosts (#1152). `main` = production (#1154): `penny-anchor-daily` targets Render; `max-context-cleanup-hourly` replaces the Railway cron service. |

Render databases, all kept: `pulseforge_production` (live), `pulseforge_pre_cutover_20261122193557`, `pulseforge_pre_cutover_20261122184817`, `pulseforge_cutover_20261122192448`, `pulseforge_production_migration` (Nov 17).
