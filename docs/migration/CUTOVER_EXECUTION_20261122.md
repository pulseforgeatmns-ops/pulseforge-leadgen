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
