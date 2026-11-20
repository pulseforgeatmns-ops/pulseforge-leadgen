# Railway exit: reconciliation and cutover plan

**Status:** prepared, not executed (2026-11-19). Everything below was gathered read-only. The plan carries PR #1146, which stays in draft until every gate in §7 passes.

The 2026-11-17 Render restore is a **recovery baseline, not the final production copy**. Railway has kept taking writes and deployments since then.

## 1. Code and deployments

| Where | Commit | Notes |
|---|---|---|
| GitHub `main` | `f2b5bef` (#1144) | |
| Railway `pulseforge-leadgen` (production) | `f2b5bef`, deployment `290910f0` | Autodeploys from `main` |
| Railway `max-decay-daily` (cron `0 8 * * *`) | `f2b5bef`, deployment `a997b354` | Autodeploys from `main` |
| Railway `max-context-cleanup-hourly` (cron `0 * * * *`) | `f2b5bef`, deployment `197afe75` | Autodeploys from `main` |
| Render `pulseforge-api-migration` | `1fe69d6` on `migration/render-isolated-staging` | Read-only staging probe |
| `migration/render-current-main` (#1146) | contains **all** of `main` | Nothing on `main` is missing from it |

**Merged since the 2026-11-17 backup (12 commits, all deployed to Railway, none on Render):**
- #1137, #1138, #1131, #1132, #1133, #1134, #1135, #1139, #1140, #1142: Max reasoning and safety work.
- #1143 and #1144: Levi Scout.

**Schema changes since the backup:** none in `migrations/`; only a rollback script was added. The code changes are flag-gated or read-only. Startup DDL and operator migrations run against production since the backup are captured by a fresh dump either way.

**Other Railway code sources** (decide per service, §6):
- `signal-dashboard`: crashed.
- `telegramCallerFeed`: failed; branch `codex/signal-opportunities-v1`, with a volume.
- `signal-historical-backfill`: has a volume.
- The `max-continuity-test` environment: `max-continuity-api`, `max-gemini-latency-eval`, and its own PostgreSQL 18 on a 50 GB volume.

## 2. Database identity and change since the snapshot

- **Source of truth:** Railway project `amused-harmony`, service `Postgres`.
  - Image `postgres-ssl:18`, server 18.6 at the Nov 17 inventory.
  - Private endpoint `postgres`; TCP proxy active.
  - Volume **10,000 MB**.
- **Disk: 9.61 GB used (about 96%).** It grew from a 7-day low of 8.97 GB to a 9.63 GB peak. ⚠ **Urgent, independent of the migration:** if the volume fills, PostgreSQL stops accepting writes. Most of the size is Signal (`signal_opportunity_decisions` was about 7.66 GB on Nov 17).
- **Image auto-updates are on** (vulnerability updates, Sat/Sun window), and each update restarts the database. Pause them for the cutover window.
- **Backup bucket:** `MigrationBackup` (Railway, region `iad`). It holds `railway-20261117-full-pg18.dump`: 727,898,203 bytes, SHA-256 `4d44ad1f…736628`.
- **Render destination:**
  - `pulseforge-production-migration` (PostgreSQL 18.6, 10 GB) holds the reconciled Nov 17 Pulseforge snapshot: 274 tables, 1,627,867 rows.
  - External access is blocked (empty IP allowlist), so reaching it from outside Render needs the in-Render path used on Nov 17.
- **Changed since the snapshot:** rows **cannot be measured from this session**, because no SQL access to either database is available here. Known writers since Nov 17:
  - application traffic (Max sessions and ledger, agent_log, touchpoints, sessions);
  - the in-process schedulers (Scout enrichment ticks are logged hourly, though blocked by `anchor_research_provider_paused`; Signal shadow; warm routing; Mira digest; Anchor governed scheduler);
  - the two Railway crons;
  - the Signal writers.

  Expect inserts, **updates** (prospect state, sessions, mission state) and **deletes** (session expiry, context cleanup) across operational tables. The fresh full backup (§4) removes the need to enumerate them. `scripts/migration/tableFingerprints.js` then proves equivalence, including updates and deletes.

## 3. Operational state outside PostgreSQL

| Item | Railway today | On Render |
|---|---|---|
| Environment variables | about 150 names on `pulseforge-leadgen` (list via the Railway API; values never copied by agents) | Owner enters the values in Render. New: `DATABASE_URL` (internal URL). **Decide:** keep `SESSION_SECRET` (sessions survive) or rotate (everyone re-logs in). Rotate `CRON_SECRET` once the callers are updated. |
| Crons | `max-decay-daily`, `max-context-cleanup-hourly` (Railway cron services); GitHub Actions `penny-anchor-daily` calls `…railway.app/cron/penny-daily` | Render Cron Jobs (paid; approval) or re-point the GitHub Actions; update the URLs |
| In-process schedulers | Inside `server.js`: Signal shadow, Mira digest, warm routing, Anchor unenriched enrichment, Anchor governed scheduler, governed-outbound stage supervisor | Move with the web service. Render must run **one** instance until the locks are confirmed. |
| Redis / queues / caches | None found (queues are in PostgreSQL) | — |
| Persistent volumes | `telegramCallerFeed` (`signal-caller-data`), `signal-historical-backfill` (`signal-historical-data`), OpenClaw Main volume, milo-workspace volumes | Archive or retire per product (§6) |
| Object storage | `MigrationBackup` bucket; `atlas-fec-evidence` bucket (election-finance-atlas project) | Copy off Railway before shutdown |
| Public hostname | `pulseforge-leadgen-production.up.railway.app`, **no custom domain** | No DNS switch is possible for this hostname. Every caller must be re-pointed. **Recommend a custom domain on Render** so the next move needs only a DNS change. |
| Hard-coded Railway hostname (code) | **30 files:** the Anchor Cleaning and Studio Substral public sites, dashboards, Vera/Cal/Sketch/warm routing/Telegram utils, `scoutExpansion`, `aoFieldService`, the anchor-portal demo, 4 GitHub workflows, verification scripts | One code change to read `APP_URL` / a single constant, deployed **before** Railway shutdown |
| Webhooks and callbacks (external consoles) | Brevo → `/webhooks/brevo` and `/api/webhooks/brevo`; Bland → `/webhooks/bland`; Stripe webhook; Google OAuth redirect URIs (Gmail, Calendar, GBP, Clarity, Studio Substral); Twilio; Telegram bot; the Mira context endpoint used by Claude | Re-point at cutover (§5 step 8). Webhook signing secrets stay the same. |
| Tenant flags and authorization state | Mostly in PostgreSQL (clients, grants, missions, authorization constraints), plus env flags (`*_ENABLED`, `GOVERNED_OUTBOUND_TENANT_IDS`, …) | Data moves with the dump. Env flags are copied **verbatim**, with no flag turned on that is off today. |

## 4. Backup and restore approach (recommended)

**Use a fresh full backup and restore. Do not build a custom delta migration.** Two reasons:
- The Pulseforge subset is about 1 GB, and the Nov 17 restore committed 1.63 M rows in about 40 s.
- The proven Nov 17 tooling (`exportRailwayToBucket.py` → `restoreGatewayRender.py`, with checksum, empty-target, version and identity guards) applies unchanged.

**Steps:**
1. Take a full custom-format `pg_dump` (PostgreSQL 18 client) **after** the write freeze. It includes the Signal tables, so nothing is lost even before the Signal decision.
2. Verify the copy:
   - SHA-256;
   - the `PGDMP` header;
   - `pg_restore --list` TOC counts;
   - a second, **off-Railway** copy whose checksum is compared independently.
3. Restore the 274 Pulseforge tables into a **new empty database on the existing Render PostgreSQL instance** (for example `pulseforge_production`):
   - no new paid infrastructure;
   - **the Nov 17 snapshot database is never overwritten**;
   - `--no-owner --no-acl`, single transaction, exit on error;
   - the Signal schema created empty, as on Nov 17.
4. Reconcile:
   - row counts against the archive (as on Nov 17);
   - **plus** `tableFingerprints.js` on Railway (frozen) and on Render, then `--compare`. Every table must match on content and every sequence must be ≥ the source.
   - Then the business checks in §5 step 7.

**PostgreSQL 18 compatibility:**
- Source 18.6 → destination 18.6, same major version.
- Use the v18 `pg_dump`/`pg_restore`.
- Extensions `pgcrypto`, `plpgsql` and `uuid-ossp` exist on Render.
- Three views and 80 triggers are restored with the schema; verify the counts.
- Row-text fingerprints are valid only between the same major versions, which this is.
- The Nov 17 dump already restored cleanly into Render 18.6.

**Code change needed for step 3:** `scripts/migration/validateMigrationDatabaseUrl.js` pins the database name to `pulseforge_production_migration`. The production database name must be added deliberately, as a reviewed change.

## 5. Cutover procedure

1. **Prerequisites** (gates in §7):
   - Railway autodeploy off;
   - the disk risk addressed;
   - the Signal decision made;
   - the Render web service image built from the approved commit and validated in staging mode;
   - the hostname code change merged;
   - the Render env vars entered by the owner;
   - a rehearsal of steps 2–6 completed.
2. **Write freeze** (start of downtime):
   - set the Railway web service to maintenance, or scale it to 0;
   - stop both Railway cron services;
   - pause the GitHub Actions crons;
   - stop the Signal writers;
   - pause Postgres image auto-updates.

   Confirm no writes with `pg_stat_activity` and with a stable `pg_stat_database` `xact_commit` reading.
3. **Final backup:** full dump, plus the source fingerprints taken under the same freeze.
4. **Off-platform verification:** a second copy outside Railway, with matching checksums.
5. **Restore** into the new Render database (§4.3).
6. **Reconcile:** row counts, fingerprints `--compare` with zero differences, sequences ≥ source, view and trigger counts, tenant list, and tenant 21 / Anchor / MSHI spot reads.
7. **Deploy Render production** from the approved commit (`PF_MIGRATION_STAGING` unset, one instance). Then validate:
   - `/login`, the session store, and every role;
   - Max chat on a known tenant;
   - `/api/status`, `/dashboard`, `/setter`, `/closer`;
   - a dry-run `/cron` call with the secret;
   - the in-process schedulers start once;
   - **no outbound sends** during validation (Anchor/Babrun outbound and Paige publishing remain under their existing flags).
8. **Switch traffic:**
   - re-point the webhook and OAuth callback URLs, the GitHub Actions `APP_URL`, the public-site intake endpoints and the Mira context URL;
   - enable the Render crons;
   - keep Railway stopped but intact.
9. **Observe for 7 days:**
   - error rate, cron runs, webhook deliveries, and the Max / Scout / Levi audit;
   - a daily fingerprint spot check of critical tables is optional.
10. **After acceptance:**
    - a final Railway archive (full dump plus bucket copies) stored off Railway;
    - then delete the Railway services and cancel billing (§6).

**Rollback** (any time before step 10):
- Railway is never modified beyond the freeze. Restart the Railway services, revert the callback URLs, and leave Render running read-only for diagnosis.
- After writes have started on Render, rolling back requires dumping Render and restoring it to Railway. Decide go / no-go within the first hours.

**Estimated write downtime: 60–90 minutes.**

| Step | Time |
|---|---|
| Freeze | about 5 min |
| Dump | about 15–20 min (full database) |
| Off-platform verify | about 5–10 min |
| Restore | about 2 min |
| Fingerprints, both sides | about 10 min |
| Render validation | about 15 min |
| Callback switch | about 15–30 min |

A rehearsal will tighten these numbers.

## 6. Exiting Railway completely

Billing ends only when **every** Railway project is migrated, archived or retired.

**`charming-trust`:**
- production: `pulseforge-leadgen`, the 2 crons, the Signal services;
- `max-continuity-test` environment.

**`amused-harmony`:** Postgres, OpenClaw Main (never deployed, has a volume), `MigrationBackup`.

**Other projects:**
- `election-finance-atlas-ingestion`: about 38 Bun functions active Nov 17–18, plus the `atlas-fec-evidence` bucket. A separate product.
- `milo-workspace`: LibreChat (failed), MongoDB, an agent-demo function, two unmounted volumes.
- `anchor-exeter-demo`: Postgres plus a web service, never deployed.

**Signal data:** about 8.2 GB, with no destination yet. Choose one:
- (a) a separate Signal database on Render (paid; at least 20 GB);
- (b) archive off-platform and retire;
- (c) keep temporarily (this blocks billing cancellation).

The standalone Signal dashboard authenticates against Pulseforge `users`, so it cannot run against a separate database without an auth change.

## 7. Approval gates (each needs explicit owner approval)

1. Disable Railway autodeploy (approved; the setting must be changed in the Railway dashboard, see the checkpoint).
2. Address the Railway disk at 96%: resize the volume, or reduce Signal growth. **Urgent.**
3. The Signal data decision (§6).
4. The production database name, and adding it to the destination guard.
5. Merge the hostname-centralisation code change, and choose a custom domain.
6. Render production resources: Cron Jobs, plan sizing, an optional custom domain.
7. A rehearsal restore into a scratch database on Render.
8. The write freeze, and its scheduled window.
9. The Render production deploy and the traffic switch.
10. Railway shutdown and billing cancellation, after the observation period and the final off-platform archive.
