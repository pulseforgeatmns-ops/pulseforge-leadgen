# MAX-P0-003 — Focused Reliability Review and Release Preparation

Companion to `docs/MAX-P0-003-RELIABILITY-CORE.md`. Covers the §2 review, the advisory-lock audit, the load-sensitive failure register, and the §3 operator checklist.

Status at writing:
- `main` = `3bc3360`. It contains the reconciliation (#1146) and the MAX-P0-003 core (#1162).
- The review fixes below are on branch `ccr-f987a12e-t5jhov`, in a new PR.
- Render production is still `69e7216`. It deploys `migration/render-current-main` with auto-deploy off. Nothing has been deployed.

## 1. Branch reconciliation (§1)

| Item | Evidence |
|---|---|
| Original do-not-merge rationale | Reviewed. The Docker `CMD`/Railway auto-deploy concern no longer applies: the `Dockerfile` is identical, Railway is retired, and Render does not deploy `main`. The apt CI blocker is resolved. |
| Commits preserved | `main..deploy` was 26 commits and `deploy..main` was 0. The merge was a fast-forward of content, and the production SHA `69e7216` is an ancestor of `main`. |
| CI on the head | 26/26 checks passed. |
| Scheduled-workflow hazard | Merging would have activated `levi-scout-weekly`. It was neutralised before the merge: `b5a544b` made it manual-dispatch only, and `0ad281b` added a CI test that prevents a recurring Levi trigger. |
| Result | #1146 merged (`60df4c3`). `main` is authoritative and contains everything production runs. |

**Remaining divergence:** Render still builds `migration/render-current-main` (`69e7216`), which is now *behind* `main`. Before the next deploy, either point the service at `main` or fast-forward the deploy branch to the reviewed `main` commit.

**Still active from `main` (not changed here — owner decision):**
- `max-context-cleanup-hourly`: hourly `POST /cron/max-conversation-context-cleanup`; succeeding since 22:12Z on Nov 24.
- `penny-anchor-daily`: `POST /cron/penny-daily` for tenant 10 every day at 12:00Z.

Both contradict "keep all external schedules disabled". To stop them: Actions → workflow → *Disable workflow*, or the same one-line change as `b5a544b`.

## 2. Focused review (§2)

### 2.1 Defects found and repaired

| ID | Severity | Finding | Fix | Evidence |
|---|---|---|---|---|
| **R1** | High (introduced by MAX-P0-003) | Anchor recovery approval (`governedOutboundAdapters.js`) held the **old ADR-075 two-int key** (750825) on its dedicated connection to exclude other mission writers between review and commit. Mission leases use a single-bigint key in a different lock space, so the exclusion silently stopped covering lease holders. CAS still prevented lost updates, but concurrent conflicting execution became possible. | The recovery path now takes `MISSION_SESSION_LOCK_SQL` (the lease's key, session-level, on the same dedicated connection). Session and transaction advisory locks on one key conflict. | `dedicated-client session holders … exclude each other` (both directions). `anchorDailyOutboundRecoveryApprovalPostgres` 32/32, with its race hook moved to the new key; assertion unchanged. |
| **R2** | High (pre-existing) | Tenant-mailbox SMTP (`sendTenantEmail`, used by `tenantOutreachScheduler`) **bypassed the claim ledger**. Its duplicate check matched only `SENT`, so a crash after SMTP acceptance (row left `PENDING`) caused a **re-send**. A failure after the boundary (timeout or dropped connection after data) was recorded as `FAILED, providerAccepted:false`, which is false certainty. | PostgreSQL stores now claim before the `PENDING` row and commit the dispatch marker inside the innermost `dispatch()` immediately before `sendMail`, so an authorization refusal never marks a dispatch. `smtpFailureIsDefinitive()`: an SMTP reply ≥400, or a pre-transfer DNS/TCP/TLS/auth/envelope failure, is definitive. Mid-transfer drops, timeouts and lost acknowledgements are unknown and never retried (row: `failure_code=external_action_outcome_unknown`, `providerAccepted=null`). A lost row is restored from the acknowledged claim without re-sending. | `test/maxReliabilityTenantMailboxPostgres.test.js` 5/5: 6 concurrent sends → 1 SMTP call; mid-transfer failure blocks retry; connect failure retries once; crash restore; fails closed without the migration. 13 existing mailbox/scheduler suites unchanged (224/225 both sides). |
| **R3** | Medium | Idempotency was **optional**. The Mission Workspace approval button sent no key, so a double click or retry created two independent requests. Downstream barriers (decision consumption, ledger) caught most cases; receipts did not. | `POST /api/v1/amo/missions/:id/execute` now returns **428 `idempotency_key_required`** for every intent whose action class is external (`provider_research`, `outreach_authorization`, `outreach`, `paid_media`; e.g. `APPROVE_DISCOVERY`, `APPROVE_EXECUTION`, `EXECUTE_OUTBOUND`). Planning intents are unchanged. The UI sends one key per click and ignores a second click while one is in flight. | `p0ReviewOnlyAmendmentPostgres` asserts 428 plus a correlation id. Its helper sends a key per call, like the UI; 403/422/tenant assertions unchanged. |
| **R4** | Medium | Receipt and claim **non-expiry was only a convention**. A future cleanup job or ad-hoc `DELETE` could re-enable an irreversible action. | Migration triggers: `DELETE`/`TRUNCATE` of claims and audit are forbidden. A receipt may be deleted only while `in_progress` (the pre-execution release after lock contention). Completed, failed and interrupted receipts are permanent. | `durable barriers cannot be expired …` |
| **R5** | Medium | Recovery `--apply` recorded **no operator identity**, and nothing guarded against applying to the wrong tenant by typo. | `--apply` requires `--operator <id>` and `--confirm-tenant <id>` equal to `--tenant`. The actor is recorded on every changed row's audit and on one `recovery_applied` summary row. Plan mode writes nothing. | `recovery apply requires an operator identity and audits the run`; CLI guards exercised. |
| **R6** | Low–Medium (same defect class as the original) | Four workers took a session lock via `pool.query` and released it via another `pool.query`: `miraTranscriptionAgent`, `miraClassifierAgent`, `miraRouterAgent`, `warmRoutingAgent`. | `utils/workerAdvisoryLock.js`: lock, work and unlock on one dedicated connection. If the unlock fails, the connection is destroyed, not pooled. | `worker advisory lock … never leaks onto the pool`; agent unit tests 9/9. |

### 2.2 Answers to the review questions

**Is idempotency mandatory or optional?** It depends on the path:

| Path | Request receipt | External-action claim |
|---|---|---|
| REST `/execute`, external intents | **Mandatory** (428 without a key) | Mandatory |
| REST `/execute`, planning intents | Optional | n/a |
| Mission Workspace buttons | Always sent | Mandatory for sends |
| Max chat (`WorkspaceEngine` → router) | **None** | Mandatory for sends |
| Governed outbound executor, `executeAnchorOneOutbound.js` | n/a (router) | Mandatory (adapter) |
| Tenant mailbox scheduler | n/a | Mandatory (R2) |

Chat has no request identity: a duplicated "Approved." can approve the next gate. Gate-bound approval (`MAX_GATE_BOUND_APPROVAL`, default off) narrows this but doesn't close it. This is proposed for MAX-P0-004 (client message ids → task records).

**Does any execution path bypass durable claims?**

| Provider path | Status |
|---|---|
| AMO outbound (Brevo) | Claimed |
| Governed outbound | Claimed |
| Tenant mailbox SMTP | **Claimed (R2)** |
| Paige social publishing (`contentPublication` adapters: Buffer, Facebook, GBP) | **Not claimed.** Human-approved through its own publish pipeline; outside Max execution. Recommended for MAX-P0-004. |
| Notification-only Brevo sends (Rex, Vera, Max briefing, setter, registration, walkthrough/Substral notifications, Signal) | Not claimed. Operator/system notifications, not prospect outreach. |
| Penny Meta Ads adapter | Read-only, no writes. |
| Twilio / Bland (Sam, Cal Batch) | Legacy agents outside Max execution; workers disabled. Recommended for MAX-P0-004. |

**Could receipt expiration permit duplicates?** No:
- No TTL exists in code.
- The DB triggers (R4) forbid removing any non-`in_progress` receipt, any claim, or any audit row.
- The only deletion path removes an `in_progress` receipt after a pre-execution lock refusal, and it is audited.
- Even if a receipt were lost, outbound sends are still deduplicated by the claim, keyed on execution identity rather than the request.

**Crash behaviour at every boundary:**

| Boundary (crash point) | Durable state left | Next attempt / recovery | Tested |
|---|---|---|---|
| Before claim | nothing | normal | — |
| After claim, before dispatch marker | `claimed` | blocked `in_progress` → recovery `abandoned_not_dispatched` → re-claim (attempt 2) | ✔ process crash |
| After marker, before provider call | `dispatch_attempted` | blocked → recovery `outcome_unknown` → evidence reconcile. **Conservative:** the message was never sent but is treated as unknown | state identical to next row |
| After provider call, before acknowledgement persisted | `dispatch_attempted` | same as above, no blind retry | ✔ process crash (exit 137); 3 retries refused |
| After acknowledgement, before the execution record or message row is written | `provider_acknowledged` | replay restores the record from `provider_ref`; no re-send | ✔ adapter replay; ✔ mailbox restore |
| After the execution record, before the TME commit | claim acknowledged; mission rolled back | re-run replays the send; the mission stage re-commits | ✔ adapter replay semantics |
| Lease backend killed mid-stage | transaction aborted; lock released | `mission_lease_lost`; stage rolled back | ✔ |
| After TME commit, before receipt complete | receipt `in_progress` | recovery → `interrupted` → 409 `request_outcome_unknown`; never re-run | ✔ |
| During recovery | per-row CAS updates | rerun is idempotent | ✔ |

**Tenant isolation and competing mission versions:**
- Leases refuse a mission owned by another tenant.
- Cross-tenant upserts are refused (404, no disclosure).
- Claims, receipts, audit and recovery are keyed and filtered by tenant.
- Competing versions: 8 processes → 1 commit. A stale engine is refused, and the hydrate baseline matches the CAS (regression test).

Open item **R7 (owner decision):** an execution identity includes the prepared revision. For tenants **outside** the governed program (10/13), a new Paige revision of the *same* mission/prospect after a send is a new identity, so the prospect could receive a second, revised message. The governed store's `already_attempted` check prevents this only for tenants 10/13. Whether a re-approved revision may ever re-send is a policy decision. Recommended default: refuse while any `sent`/`attempted`/unknown record exists for the same (mission, prospect).

**Recovery script authorization and auditability:** see R5.
- Authorization is shell access to `DATABASE_URL` plus explicit `--operator` and `--confirm-tenant`.
- It never dispatches, retries or reconciles. It only moves stale claims and receipts.
- Output is redacted, with keys hashed.

### 2.3 Session-level advisory lock audit

The original count of "six" was low. Every `pg_try_advisory_lock`/`pg_advisory_lock` site in the repository:

| Site | Connection handling | Status | Relevance |
|---|---|---|---|
| `governedOutboundAdapters.js` (Anchor recovery approval) | dedicated client | **R1 fixed** (key) | **High**: Max/AMO + Anchor outbound |
| `tenantOutreachScheduler.js` ×2, `tenantOutreachExecutorLock.js` | dedicated client | safe | **High**: outbound executor single-flight (worker disabled) |
| `governedOutboundStore.js:95`, `governedOutboundReplies.js`, `governedOutboundControlDispatch.js` | dedicated client | safe | **High**: governed outbound / Max control loop |
| `utils/emmettAutosend.js` | dedicated client | safe | High: Emmett autosend (disabled) |
| `tenantMailboxPollExecutor.js` | dedicated client | safe | Medium: inbound replies |
| `maxDecayAgent.js` | dedicated client | safe | Medium: Max attention decay |
| `packages/max/clarity/store.js` | dedicated client | safe | Low: Clarity review |
| `warmRoutingAgent.js` | **pool.query** | **R6 fixed** | Medium: warm-lead alerts |
| `miraTranscriptionAgent.js`, `miraClassifierAgent.js`, `miraRouterAgent.js` | **pool.query** | **R6 fixed** | Low: Mira capture pipeline |
| `miraDigestAgent.js` | dedicated client | safe | none |
| `services/signalDashboard/store.js` | dedicated client | safe | none (Signal) |
| `services/revenuePhase16bRunner.js` | single-connection client | safe | none (revenue runner) |
| `scripts/validateBabrunCanonicalMission.js` | dedicated client | safe | script |

Residual note for the "safe" sites: most release the client normally even if `pg_advisory_unlock` throws. If unlock fails on a healthy socket, the lock could stay on a pooled connection. `withWorkerAdvisoryLock` handles this by destroying the connection. Migrating the High-relevance sites to it is listed in MAX-P0-004 scope.

## 3. Load-sensitive failure register (tracked separately)

| ID | Test | Failure | Reproduction | Cause | Production relevance | Action |
|---|---|---|---|---|---|---|
| L1 | `anchorDailyOutboundSupervisedWorker`: "two hangs … admits healthy tenant" and "hung quarantine … healthy tenant" | `worker_outcome_uncertain` on the **healthy** run (lines 19/23) | Reproduced **on the unmodified candidate**: 5 failures in 4 runs with 8 CPU hogs on 4 cores; 3 in 4 on this branch. 0 failures without load. | The test gives the healthy worker a **200 ms** deadline to spawn a Python launcher and a Node worker and finish. Under CPU starvation it misses it. | Integrity: none. The supervisor fails in the safe direction (quarantine as uncertain, never "completed"). Availability: production deadlines are 120 s / 300 s / 1200 s (poll/tick/control), so the margin is about 600×. On a saturated single-CPU Render instance, very slow worker startup would cause quarantines (stalls), not wrong state. | Keep tracked. Recommend raising the test's healthy-run deadline to ≥2 s. Pre-existing and not caused by MAX-P0-003, so not changed here. |
| L2 | `anchorScopedResumeLifecyclePostgres`, `anchorDailyOutboundRecoveryQuarantinePostgres` | uncaught `57P01` "asynchronous activity after the test ended" | Seen once with 4 suites in parallel; not reproduced in 14 later loaded runs. | Test harness: raw `pool.end()` then `pg_ctl stop`. `pg-pool` can resolve `end()` before sockets close (documented in `test/helpers/closePostgresPool.js`). | None (teardown only). | **Fixed**: both files now use `createTrackedPostgresPool` + `closePostgresFixture` (same pattern as `c9467ad`). 31/31. |
| L3 | `anchorDailyOutboundSupervisedWorker` under concurrency | as L1 | — | as L1 | as L1 | Merged into L1. |

Passing in isolation was **not** treated as proof. L1 was reproduced deliberately on both trees and its production behaviour was reasoned from the supervisor code. L2 was fixed at the root.

## 4. Operator checklist (§3) — read-only, no mutation authorised

All steps are read-only. **Do not** run the migration, change any variable, or open the database to the internet.

### 4.1 Read-only schema report in Render Shell

Render Dashboard → service **`pulseforge-api-migration`** → **Shell**. The shell opens in the container working directory **`/app`** (`WORKDIR /app`), as user `pptruser`. `DATABASE_URL` is already set, and the image includes `psql` 18.

The deployed image (`69e7216`) predates the Node report script, so use the psql form. Paste this as **one block**:

```bash
cd /app && PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20s' \
  psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -f - <<'SQL'
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
\pset footer off
\echo '== server'
SELECT current_database() AS database, current_user AS role, split_part(version(), ' ', 2) AS pg_version, pg_size_pretty(pg_database_size(current_database())) AS size, current_setting('transaction_read_only') AS read_only, now() AS observed_at;
\echo '== catalog (rehearsal reference: 319 tables, 3 views, 80 triggers, 779 indexes, 3629 constraints, 84 functions, 56 sequences)'
SELECT (SELECT count(*) FROM pg_tables WHERE schemaname='public') AS tables, (SELECT count(*) FROM pg_views WHERE schemaname='public') AS views, (SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal) AS triggers, (SELECT count(*) FROM pg_indexes WHERE schemaname='public') AS indexes, (SELECT count(*) FROM information_schema.table_constraints WHERE table_schema='public') AS constraints, (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public') AS functions, (SELECT count(*) FROM pg_sequences WHERE schemaname='public') AS sequences;
\echo '== Max / AMO / MAX-P0-003 core tables'
SELECT t AS table_name, to_regclass(t) IS NOT NULL AS present FROM unnest(ARRAY['clients','users','session','prospects','companies','agent_log','agent_actions','acquisition_missions','acquisition_mission_events','acquisition_mission_contributions','acquisition_mission_observations','acquisition_mission_outcomes','acquisition_mission_execution_audit','acquisition_outbound_executions','max_conversation_context','tenant_outreach_messages','max_external_action_claims','max_request_receipts','max_reliability_audit']) AS t;
\echo '== tenants (counts only)'
SELECT c.id, c.name, c.active, (SELECT count(*) FROM prospects p WHERE p.client_id=c.id) AS prospects FROM clients c ORDER BY c.id;
\echo '== missions by tenant'
SELECT tenant_id, count(*) AS missions, max(updated_at) AS last_updated FROM acquisition_missions GROUP BY tenant_id ORDER BY tenant_id;
\echo '== freshness'
SELECT 'agent_log' AS source, max(created_at)::text AS latest FROM agent_log UNION ALL SELECT 'acquisition_mission_events', max(at)::text FROM acquisition_mission_events UNION ALL SELECT 'session (expire)', max(expire)::text FROM session;
\echo '== databases on this server'
SELECT datname FROM pg_database WHERE datname LIKE 'pulseforge%' ORDER BY 1;
\echo '== advisory locks held now'
SELECT locktype, classid, objid, objsubid, granted, pid FROM pg_locks WHERE locktype='advisory';
ROLLBACK;
SQL
```

The same SQL is in the repository as `scripts/max-reliability/schema-report-readonly.sql`. After an image containing this branch is deployed, the equivalent is `cd /app && node scripts/max-reliability/schemaReport.cjs > /tmp/schema-report.json`.

**Expected output (what is normal):**
- `== server`: `database = pulseforge_production` (the promoted copy), `pg_version 18.x`, size about 1 GB, **`read_only = on`**. If `read_only` is not `on`, stop.
- `== catalog`: close to the rehearsal reference: 319 tables (274 Pulseforge + 45 empty Signal), 3 views, 80 triggers, 779 indexes, 3,629 constraints, 84 functions, 56 sequences. Small increases are expected from startup `ensure*` DDL since 2026-11-22; decreases are not.
- `== core tables`: every row `t`, **except** `max_external_action_claims`, `max_request_receipts` and `max_reliability_audit`, which must be `f` (migration not yet applied).
- `== tenants`: includes ids 1, 2, 5, 10, 13 (Babrun), 21 (Levi), with non-zero prospect counts for active tenants.
- `== missions by tenant`: rows for tenants 10, 13, 21 with recent `last_updated`.
- `== freshness`: recent timestamps (after the 2026-11-22 cutover), consistent with workers being off. No burst of new `agent_log` writes beyond the hourly cleanup.
- `== databases`: `pulseforge_production`, plus any retained `pulseforge_pre_cutover_<stamp>`.
- `== advisory locks`: **empty**, or only rows whose `pid` belongs to an active request. A long-lived row means a leaked session lock; report it.
- Any `ERROR: cannot execute … in a read-only transaction` means something attempted a write. That would be a bug in the report; stop and report it.

Send the full output back for comparison. It contains no row content beyond tenant names and counts.

### 4.2 Required environment — names and expected values

Values are read from the code (`=== '1'`, `=== 'true'`, etc.). Secrets: confirm **presence only**; never paste values.

| Variable | Expected | Why / parse rule |
|---|---|---|
| `DATABASE_URL` | set (secret) | Render internal connection string. |
| `SESSION_SECRET` | set (secret) | |
| `CRON_SECRET` | set (secret), and identical in GitHub Actions secrets | Cron routes fail closed when unset (MAX-P0-002). |
| `APP_URL`, `DASHBOARD_URL` | `https://pulseforge-api-migration.onrender.com` (or the custom domain) | |
| `ANTHROPIC_API_KEY` | set (secret) | Daily Max briefing, `/api/max/ask`, Paige. |
| `GEMINI_API_KEY` | optional (secret) | Workspace phrasing only; deterministic fallback without it. |
| `MAX_PRESENTATION_MODEL` | unset (default `gemini-3.1-flash-lite`) | |
| `MAX_CHAT_MODEL` | unset (default `claude-sonnet-4-6`) | |
| **`PF_DISABLE_BACKGROUND_WORKERS`** | **`1`** | Exactly `'1'` disables workers (`server.js:109`). Any other value starts them. |
| `PF_MIGRATION_RECEIVER` | unset or `0` | `1` boots the restore receiver instead of the app. |
| `PF_MIGRATION_STAGING` | unset | `1` boots the staging server. |
| `MISSION_STORE_ALLOW_EPHEMERAL` | **unset** | `'1'` allows an in-memory mission store (fail-open). |
| **`ANCHOR_GOVERNED_OUTBOUND_ENABLED`** | **`false`** | **Default is ON**: enabled unless exactly `'false'`. |
| `BABRUN_GOVERNED_OUTBOUND_ENABLED` | `false` | |
| `PULSEFORGE_GOVERNED_OUTBOUND_ENABLED` | `false` | |
| `SUBSTRAL_GOVERNED_OUTBOUND_ENABLED` | `false` | |
| `GOVERNED_OUTBOUND_TENANT_17_ENABLED` | `false` | |
| `GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED` | `false` | |
| `ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED` | unset or `false` | Only `'true'` enables. |
| `ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED` | unset or `false` | Only `'true'` enables. |
| **`ANCHOR_RESEARCH_PROVIDERS_PAUSED`** | **`true`** | Paused only when `'true'`. |
| `EMMETT_AUTOSEND_ENABLED` | unset or `false` | `1/true/yes/on` enables. |
| `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED` | unset or `false` | Only `'true'` enables. |
| `MAX_FRAME_REASONING` | unset (off) | Only `'true'` enables. |
| `MAX_INTERPRETATION_SHADOW` | unset (off) | Only `'true'` enables. `MAX_INTERPRETATION_PROVIDER` / `MAX_INTERPRETATION_MODEL` unset. |
| `MAX_CONVERSATION_CONTEXT`, `MAX_CONVERSATION_LEDGER` | as currently set; `true` only if already approved | Only `'true'` enables. |
| `MAX_GATE_BOUND_APPROVAL` | unset (off) unless approved | Default-off narrowing. |
| `MAX_OPERATIONAL_READ_PRECEDENCE`, `MAX_OPERATIONAL_READ_FOCUSED_EVIDENCE`, `MAX_PROSPECT_NARRATIVE_CLAIMS` | unset (on) | These are rollback switches. Only `false/0/off/no/disabled` turns them off. |
| `SIGNAL_DB_SEPARATION_REQUIRED` | unset | `'1'` requires a separate Signal database. |

### 4.3 Consolidated checklist

- [ ] Render Shell report run (4.1); output returned; `read_only = on`; the three MAX-P0-003 tables absent; catalog consistent with the rehearsal.
- [ ] Environment checked against 4.2. In particular `PF_DISABLE_BACKGROUND_WORKERS=1`, `ANCHOR_GOVERNED_OUTBOUND_ENABLED=false`, `ANCHOR_RESEARCH_PROVIDERS_PAUSED=true`, `MISSION_STORE_ALLOW_EPHEMERAL` unset.
- [ ] Decision on the two scheduled workflows still active from `main` (`max-context-cleanup-hourly`, `penny-anchor-daily`).
- [ ] Decision on Render's deploy branch (point at `main`, or fast-forward `migration/render-current-main` to the reviewed commit) **before** any future deploy. No deploy is authorised now.
- [ ] Decision on R7 (re-send after a revision, non-governed tenants).
- [ ] Review-fix PR CI green.
- [ ] Migration `2026-11-25-max-p0-003-reliability-core.sql`: **not authorised**; awaiting separate approval.

## 5. Acceptance register delta

| Item | Status |
|---|---|
| Branch reconciliation | **Met.** `main` is authoritative and contains production. |
| Idempotency mandatory for external effects (REST/UI) | **Met** (R3). Chat path: open → MAX-P0-004. |
| No bypass of durable claims for prospect outreach | **Met** for Brevo and tenant SMTP (R2). Social publishing and Twilio/Bland are outside the ledger → MAX-P0-004. |
| Receipts cannot expire | **Met**, enforced in the DB (R4). |
| Crash boundaries explicit and recoverable | **Met** (table 2.2). One conservative case: marker committed but never sent → treated as unknown. |
| Tenant isolation / competing versions | **Met**; R7 open (policy). |
| Recovery authorization and audit | **Met** (R5). |
| Session-lock audit | **Met**: 4 defective sites fixed (R6); 1 key regression fixed (R1). |
| Load-sensitive failures | **Tracked**: L1 open (pre-existing, safe direction); L2 fixed. |
| Production schema verification | **Open**: awaiting the operator's Render Shell output. |
| **pgAdmin public exposure** | **Residual, accepted by the operator (deferred). Not a deployment blocker.** The Render service `pgAdmin-dpg-dbtgtloae00c73dd8040-a` is reachable from `0.0.0.0/0` and attached to the production database, so its protection rests entirely on pgAdmin authentication. No evidence found that it participates in execution integrity. Re-escalate only on new evidence (e.g. credential exposure or unexpected pgAdmin sessions in `pg_stat_activity`). |

## 6. Retention protections: lifecycle and data-management verification

The R4 triggers forbid `DELETE`/`TRUNCATE` on `max_external_action_claims` and `max_reliability_audit`, and `DELETE` on any `max_request_receipts` row that is not `in_progress`. Checked against every lifecycle path:

| Concern | Finding | Status |
|---|---|---|
| Application lifecycle transitions | Claim → dispatch → result, re-claim, reconcile, recovery and receipt finish are all `UPDATE`. The only `DELETE` is the receipt release after pre-execution lock contention (`in_progress`, still allowed and audited). | ✔ no conflict |
| Tenant AMO reset (`deleteAllAmoData`) | Deletes AMO mission tables only. Claims, receipts and audit are retained. Intended: they record what may have been sent, and mission ids are random, so a reset mission cannot collide with old claims. | ✔ intended |
| Tenant/workspace deletion | No such code path exists. If one is added, it must not delete claims for actions that were or may have been dispatched. Use the scrub procedure below for personal data. | ✔ (constraint recorded) |
| Restore / migration tooling | `restoreFinalRender.py` uses `pg_restore` into a **new** database. Triggers are created in the post-data section after rows load. No `TRUNCATE`/`DELETE` in `scripts/migration`. | ✔ |
| Test fixtures | No fixture applies all migrations in a loop (the three `readdirSync(migrations)` users filter for their own files). Suites that apply this migration do not truncate. | ✔ |
| Personal data | Claims store `subject_ref` (prospect id), mission id, provider message id and hashes; **no recipient or content**. Audit details pass through `redact()`: content keys removed, secrets removed, e-mails replaced by a 16-hex SHA-256 prefix (pseudonymous). **Receipts were the gap:** they stored the full execute response, including operator free text. Fixed: `/execute` now stores an allowlisted summary only (`executeReceipt`). | ✔ after fix |
| Erasure requests | `UPDATE` is not blocked. Erasure scrubs fields (e.g. `UPDATE max_reliability_audit SET detail = '{}'::jsonb WHERE …`; `UPDATE max_external_action_claims SET subject_ref = NULL WHERE …`). The claim row itself must stay, or the duplicate-send barrier disappears. | ✔ procedure |
| Growth | Roughly one claim and 3–5 audit rows per external action, and one receipt per keyed request. Negligible at current volumes (outbound paused). Indexes are partial or tenant-led. | ✔; review at scale |
| Archival | Not possible with ordinary statements, by design. A future archival must be a separately approved migration: export, then `ALTER TABLE … DISABLE TRIGGER` within one transaction, deleting only rows for actions in terminal states older than an agreed horizon, then re-enable. Claims for `dispatch_attempted`/`outcome_unknown` are never archived. | documented |
| Limit of the guard | The table owner (the application role on Render) can `DISABLE TRIGGER` or `DROP TABLE`. The triggers stop accidental and automated deletion (cleanup jobs, retention scripts, mistaken statements), not a deliberate owner action. | accepted |
| `session_replication_role = replica` | Bypasses ordinary triggers; requires superuser. Render managed databases do not grant superuser to the application role. Confirm with `SELECT rolsuper FROM pg_roles WHERE rolname = current_user;` (expected `f`). | accepted |

## 7. Final consolidation (owner directive "MAX-P0-003 — Final Review Consolidation")

### 7.1 R7 decision implemented
Policy: legitimate follow-up or revised outreach happens only through a distinct, authorized logical outreach intent. Changing content must not bypass an existing claim, unresolved outcome, suppression or duplicate restriction. No outreach capability is added.

Implementation (`OutboundExecutionAdapter.js`, `externalActionLedger.findBlockingForSubject`): within one mission (the logical outreach intent), a prospect receives at most one outbound action. A revised artifact is refused with `outreach_intent_already_dispatched` while the same tenant/mission/prospect has, under another key:
- a claim in `claimed`, `dispatch_attempted`, `provider_acknowledged`, `verified` or `outcome_unknown`; or
- a `SENT`/`ATTEMPTED` execution record.

Only `failed_definitive`/`abandoned_not_dispatched` (nothing delivered) let a revision proceed. The tenant-mailbox path was already covered: same key with different content → `idempotency_key_reused`. Tests: four R7 cases in `maxReliabilityOutboundAdapterPostgres`.

### 7.2 Scheduler decisions
| Workflow | Decision | Evidence |
|---|---|---|
| `penny-anchor-daily` | **Paused** (schedule removed; `workflow_dispatch` kept; not invoked). CI guard in `test/ciPlanPr.test.js` fails if a schedule returns. Takes effect when #1163 merges. "Disable workflow" in Actions pauses it immediately. | negative control: the guard fails on the old file |
| `max-context-cleanup-hourly` | **Stays active**: strictly internal maintenance. `routes/cron.js:870` is one bounded `DELETE … LIMIT 500 … FOR UPDATE SKIP LOCKED` of expired `max_conversation_context` rows, behind timing-safe `CRON_SECRET` auth, registered before `/cron/:agent`. No triggers on the table, no specialist, no external call. | code inspection |
| `levi-scout-weekly` | Manual-dispatch only (`b5a544b`), guarded by `0ad281b`. | unchanged |

### 7.3 Independent review of R1–R6
A separate reviewer with no implementation context reviewed `3bc3360..4a6052b` read-only. Verdicts and dispositions:

| Item | Reviewer verdict | Disposition |
|---|---|---|
| **R2.1** `smtpFailureIsDefinitive` treated `command:'CONN'` / `ECONNECTION+CONN` as definitive. nodemailer 8.0.10 tags socket errors, unexpected closes and timeouts with `'CONN'` at **any** stage (`smtp-connection/index.js:225, 959–961, 986`), including after DATA. A lost 250 would have become `failed_definitive` → re-claimable → **re-send**. The original test used an unrealistic `command:'DATA'`. | **DEFECT, high**: verified in the installed source | **Fixed.** Definitive only for SMTP reply ≥400, `EAUTH`/`EENVELOPE`/`EDNS`, socket-layer `syscall` connect/getaddrinfo, or a "connect/getaddrinfo E…" message. Tests use the real shapes (close, reset, timeout = unknown; refused/DNS/550/auth = definitive). |
| **R2.2** A failure after `sendMail` returned (recording success, saving the row, a lost authorization COMMIT) could be classified definitive if it looked like a connect error. | DEFECT, medium | **Fixed**: a `providerReturned` flag makes any later failure non-definitive. Test injects a connect-shaped ledger failure after acceptance → no re-send. |
| **R2.3** Every PostgreSQL-backed `sendTenantEmail` (scheduler sends, governed replies, mailbox verification) fails closed until the migration is applied — broader than "AMO sends". | deploy-order note | **Documented** as a deploy prerequisite (§7.5). Intended fail-closed behaviour. |
| R2.4 The fingerprint omitted `referencesHeader`; the key-reuse message was misleading. R2.5 `smtp_undefined` code. | low | **Fixed**: `referencesHeader` added; accurate message; `responseCode ?? code`. |
| **R3.1** With no body intent, `executeCanonical` uses the pending decision (possibly discovery or execution approval), so the 428 check saw a planning intent. | **DEFECT, high** | **Fixed**: a key is required when the intent is absent. |
| R3.2 Untrimmed intent bypassed the classification. | DEFECT, medium | **Fixed**: the intent is trimmed before classification, and the trimmed value executes. |
| R3.3 Continuation intents. Also found while verifying: `APPROVE_PLAN` with any `executionMode` can chain into discovery (`ExecutionRouter.maybeChainAutonomousDiscovery`). | uncertain → confirmed | **Fixed**: key required for `AUTONOMOUS_PROGRESSION`, `MISSION_CONTINUATION`, `CONTINUE_INVESTIGATION`, and any request with `executionMode`. |
| R3.4 A new key per click meant a retry after a network error was a new request. | low | **Fixed**: the UI reuses the key for the same mission+intent until any HTTP response arrives. |
| R1 | CORRECT WITH CAVEATS | R1.2 **fixed**: unlock failure is logged, never masks a committed approval; the dedicated client is destroyed anyway. R1.1 (uncertain): a `persistMissionState` without an explicit pool inside the critical section would be refused (safe direction, not a deadlock). The approval path makes no such call; the full real-PostgreSQL approval suite passes 32/32. Recorded as a constraint in the handoff. |
| R4 | CORRECT WITH CAVEATS | R4.2 header **fixed**. R4.1: `UPDATE` is unguarded (an owner could rewrite state then delete). Accepted as defence-in-depth (see §6). |
| R5 | CORRECT | R5.2 `opts.now` **fixed**. R5.1: e-mail-shaped operator ids are hashed by redaction; use non-e-mail operator ids. |
| R6 | CORRECT WITH CAVEATS | R6.2 **fixed**: a failed lock query destroys the connection. R6.1: the lock holds one pooled connection for the run's duration; an idle-killing proxy would drop the lock mid-run (the work continues unlocked). Recorded for MAX-P0-004. |
| Test changes | no assertion removed or loosened | Coverage note: `p0ReviewOnlyAmendmentPostgres` now always sends a key. The keyless cases are pinned in `test/maxReliabilityPolicy.test.js`. |

### 7.4 Regression (before/after, same container, sequential, failure sets compared by name)
Baseline `origin/main` @ `3bc3360` vs PR #1163 @ `4a6052b`. Later commits re-ran the targeted suites: reliability-core 47/47; review-only + recovery approval + CI plan 71/71.

| Suite | `main` | #1163 | New failures |
|---|---|---|---|
| `test:amo` ratchet | no failures outside baseline | no failures outside baseline | 0 |
| `test:global-smoke` | 10/0 | 10/0 | 0 |
| `test:anchor-outbound` | 877/0 | 877/0 | 0 |
| 24 mission-persistence/pilot PostgreSQL suites | 212 pass / 3 fail | 212 / 2 | 0 (the extra `main` failure is an L2 teardown race in `p0ReviewOnlyAmendmentPostgres`, not a fix) |
| 13 mailbox/scheduler suites | 224/0 (1 skip) | 224/0 | 0 |
| `test:max` | 2946/0 | 2946/0 | 0 |
| `test:max:reliability-core` | n/a | 47/47 | — |

### 7.5 Deploy prerequisites added by this PR
1. Apply `2026-11-25-max-p0-003-reliability-core.sql` **before** deploying code that contains #1162/#1163 (separate approval). Until then, all AMO outbound sends **and every PostgreSQL-backed tenant-mailbox send (scheduler sends, governed replies, mailbox verification)** are refused with `reliability_schema_missing` (fail closed), and keyed `/execute` returns 503.
2. API clients of `/execute` must send `Idempotency-Key` for external-effect, intent-less, continuation or `executionMode` requests (428 otherwise). The Mission Workspace UI does.
