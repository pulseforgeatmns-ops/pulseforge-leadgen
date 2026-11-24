# MAX-P0-002 — Operator security and configuration runbook

**Incident:** unauthenticated access to production cron routes on Render (`pulseforge-api-migration`), confirmed 2026-11-24.
**Root cause:** `CRON_SECRET` is unset on Render. Thirteen legacy handlers in `routes/cron.js`, plus `/cron/knowledge-outbox`, skip authentication when it is unset.
**Containment owner:** Jacob (owner). Every step marked **[APPROVAL]** is a production change that needs an explicit go.

Rules for this runbook:
- No secret value ever appears in a ticket, chat, commit, PR, log, screenshot or shell history.
- Generate secrets on the operator's own machine. Paste them only into the target console's secret field.
- Do not enable background workers (`PF_DISABLE_BACKGROUND_WORKERS` stays `1`).
- Do not change any outbound, autosend, publish, research or Signal flag.
- Do not run Scout, Emmett, Sam, Cal or Paige publishing manually.

**Order of operations:**

| Step | Purpose | Closes the exposure? |
|---|---|---|
| §1 | Set `CRON_SECRET` | **Yes, immediately.** The legacy guard enforces the secret once it is set, with no deploy needed. |
| §2 | Deploy PR #1161 | Makes the routes fail closed if the secret is ever lost again |
| §3 | Verify | — |
| §4 | pgAdmin | Second exposure |
| §5 | Release gate | Prevents recurrence |
| §6 | Read-only DB access | Prerequisite for §7 |
| §7 | Investigate activity during the open window | — |
| §8 | Environment reconciliation | — |

**Do §1 now. Do §4 in the same sitting.**

---

## 1. `CRON_SECRET` across Render, GitHub and cron-job.org

### 1.1 Generate (operator machine only)

```sh
# Prints a 64-hex-character secret. Do not paste it anywhere but the secret fields below.
openssl rand -hex 32
```

Use a **new** value; do not reuse Railway's. The old value was sent in URL query strings (`?secret=`) to Railway and cron-job.org, where it may be logged. Keep the value in a password manager entry named `pulseforge CRON_SECRET (2026-11-24)` until §1.5 is complete, then keep it there as the record.

### 1.2 GitHub (do this first; it changes nothing in production)

1. Repository `pulseforgeatmns-ops/pulseforge-leadgen` → **Settings → Secrets and variables → Actions → Repository secrets**.
2. Update `CRON_SECRET` (or create it if missing) with the new value.
3. Under **Variables**, confirm `PULSEFORGE_APP_URL` = `https://pulseforge-api-migration.onrender.com`, or that it is unset, in which case the workflows default to that host.

GitHub callers and their transport (no change needed; for reference):

| Workflow | Route | Transport |
|---|---|---|
| `max-context-cleanup-hourly.yml` | `POST /cron/max-conversation-context-cleanup` | `Authorization: Bearer` |
| `penny-anchor-daily.yml` | `POST /cron/penny-daily` | JSON body `{secret, client_id: 10}` |
| `levi-scout-weekly.yml` | `POST /cron/scout_levi` | `Authorization: Bearer` (only runs once the file is on `main`) |

### 1.3 cron-job.org (do this second; it is harmless while Render's secret is still unset)

Log in to the owner's cron-job.org account. For **every** job targeting `pulseforge-api-migration.onrender.com` (and any still targeting the old Railway host), set the new secret in the transport that route accepts:

| Job (route) | Accepted transport | Recommended setting |
|---|---|---|
| `/cron/paige`, `/cron/riley`, `/cron/emmett`, `/cron/warm_signal`, any other `/cron/<agent>` | `POST` JSON body `secret`, or `GET` query `secret` | Switch to **POST**, header `Content-Type: application/json`, body `{"secret":"<value>"}`. This keeps the secret out of URLs. |
| `/cron/anchor-max-outbound-control` | `Authorization: Bearer <value>` (POST) | Request header `Authorization: Bearer <value>` |
| `/cron/anchor-daily-outbound`, `/cron/governed-outbound-tick`, `/cron/anchor-outbound-replies` | `Authorization: Bearer` (POST) | Same. **These are outbound routes:** confirm with the owner whether each job should remain enabled; if unsure, disable the job, which is reversible. |
| `/internal/cron/max-decay` | `Authorization: Bearer <MAX_DECAY_CRON_SECRET>`, a **different** secret | See §8 (`MAX_DECAY_CRON_SECRET`) |

Record, for each job: URL path, method, schedule, enabled or disabled, and the last 10 execution timestamps and status codes from cron-job.org's history. **§7 needs this.**

### 1.4 Render **[APPROVAL]**

1. Render dashboard → service **pulseforge-api-migration** → **Environment**.
2. Add `CRON_SECRET` with the new value (mark it as secret if offered).
3. Choose **Save and deploy**. This restarts the currently deployed commit (`357e747`) with the new variable; no code change. Background workers remain disabled: `PF_DISABLE_BACKGROUND_WORKERS` is unchanged.
4. Wait for the deploy event to report live (Render → Events).

**Effect:**
- Legacy routes now reject requests without the secret.
- Fail-closed routes (context cleanup, Penny, Levi, Anchor governed, Clarity, max-attention) start accepting the correctly configured callers.

### 1.5 Immediate check (no side effects)

```sh
B=https://pulseforge-api-migration.onrender.com
# Must be 401 (was 400 before §1.4). Unknown agent → nothing runs even if auth passed.
curl -s -o /dev/null -w '%{http_code}\n' "$B/cron/p0-audit-nonexistent-agent?secret=wrong"
```

If this returns **400**, the secret is not active on the running instance. Stop and re-check §1.4.

---

## 2. Secure deployment of PR #1161 **[APPROVAL]**

Preconditions:
- §1 complete.
- PR #1161 CI green on its head: `PR CI gate`, `Full platform regression (off PR path)` (including `Acquisition mission suite (test:amo ratchet)`), `Levi Scout`.
- One reviewer has read the diff of `routes/cron.js`, `routes/knowledgeAdmin.js` and `utils/cronAuth.js`.

1. Record the rollback target: current live deploy `dep-dc2p9obm8hqs7386utd0` (commit `357e747`).
2. Merge PR #1161 into `migration/render-current-main` using **Create a merge commit**, not squash, so audit commit `5f26c0d` stays addressable. Record the merge SHA.
3. Wait for the push-triggered `Full platform regression` run on that merge SHA to succeed. This is the deployment candidate's own check; PR checks ran on a different SHA.
4. Deploy exactly the merge SHA:
   - **Preferred** (once §5 is done): Actions → **Render production release gate** → Run workflow → `sha` = merge SHA.
   - **Until §5 is done:** Render → pulseforge-api-migration → **Manual Deploy → Deploy latest commit**. The branch tip must equal the merge SHA; check before clicking.
5. Confirm in Render → Events that the deployed commit equals the merge SHA, and that the startup log shows `[startup] background workers disabled for Render API cutover`.

**Rollback:** Render → Events → previous deploy → **Rollback** (or `git revert -m 1 <merge SHA>` on the branch and redeploy). Do **not** remove `CRON_SECRET` during rollback; §1 alone keeps the exposure closed.

---

## 3. Post-deployment authentication verification (non-side-effecting)

Run from the operator machine. Never put the real secret on a command line: read it into a variable without echo.

```sh
B=https://pulseforge-api-migration.onrender.com
read -rs CRON_SECRET; export CRON_SECRET   # paste the secret; nothing is shown or stored in history

code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

# A. Legacy guard rejects wrong/absent secrets (expect 401 401 401)
code "$B/cron/p0-audit-nonexistent-agent?secret=wrong"; echo
code "$B/cron/p0-audit-nonexistent-agent"; echo
code -X POST -H 'content-type: application/json' --data '{"secret":"wrong"}' "$B/cron/p0-audit-nonexistent-agent"; echo

# B. Correct secret passes auth, but an unknown agent is rejected before any I/O (expect 400)
code -X POST -H 'content-type: application/json' \
  --data "$(jq -n --arg s "$CRON_SECRET" '{secret:$s}')" "$B/cron/p0-audit-nonexistent-agent"; echo

# C. Previously fail-open, high-risk routes now refuse without a secret (expect 401 each).
#    These calls carry no secret, so no handler runs.
for p in execute-anchor-one-outbound tenant-outreach-executor recover-anchor-canonical-outbound \
         seed-direct-mail-ao scoutExpansion pulse-health scout-places-diagnostic tenant-mailbox-poll \
         signal-shadow continue-anchor-str-scout-investigation inspect-anchor-canonical-outbound; do
  printf '%-40s %s\n' "$p" "$(code "$B/cron/$p")"
done
printf '%-40s %s\n' knowledge-outbox "$(code -X POST "$B/cron/knowledge-outbox")"

# D. Bearer routes reject a wrong token (expect 401)
code -X POST -H 'Authorization: Bearer wrong' "$B/cron/max-conversation-context-cleanup"; echo
code -X POST -H 'Authorization: Bearer wrong' "$B/internal/cron/max-decay"; echo

# E. App health and session auth unchanged (expect 200, 401, 401)
code "$B/healthz"; echo; code "$B/api/me"; echo; code -X POST "$B/api/v1/max/workspace/ask"; echo

unset CRON_SECRET
```

Do **not** call a real agent route with the correct secret to "test" it: that runs the agent.

Instead, confirm that legitimate callers recovered:
- **GitHub → Actions → "Max conversation-context cleanup (hourly)"**: next run green. This runs the approved bounded DELETE of expired rows.
- **"Penny Anchor daily review"**: next 12:10Z run green.
- **cron-job.org history** for each job: 2xx after §1.4. A 401 means that job's transport or secret is wrong.

Record every result in the incident log.

---

## 4. pgAdmin suspension or restriction **[APPROVAL]**

Service `pgAdmin-dpg-dbtgtloae00c73dd8040-a`:
- created 2026-11-24 00:00Z;
- public at `https://pgadmin-dpg-dbtgtloae00c73dd8040-a.onrender.com` (login page confirmed reachable);
- inbound allowlist `0.0.0.0/0`;
- auto-deploys from `github.com/render-examples/pgadmin` `main`, a third-party repository.

**Recommended: suspend now (reversible).**
1. Render → service **pgAdmin-dpg-dbtgtloae00c73dd8040-a** → **Settings** → **Suspend Service**.
2. Verify: `curl -s -o /dev/null -w '%{http_code}\n' https://pgadmin-dpg-dbtgtloae00c73dd8040-a.onrender.com/login` no longer returns 200.

**If pgAdmin must stay available**, do all of these:
1. **Settings → Build & Deploy → Auto-Deploy: Off**, so upstream commits cannot change what runs next to your database.
2. **Settings → Inbound IP rules/allowlist**: replace `0.0.0.0/0` with the operator's static IP `/32` only.
3. Confirm the pgAdmin admin password is long and unique, and that no saved server entry stores the database owner password. Prefer the read-only role from §6.

**Follow-up (read-only first):**
- Identify who created the service. Render → Events, and the team audit log.
- Review pgAdmin's own login log on its disk (`/var/lib/pgadmin`) via Render Shell before suspending, if that is feasible quickly. Otherwise suspend first.
- If any unknown login cannot be ruled out, **rotate the database credentials** (Render Postgres → Connections/Credentials) and update the app's `DATABASE_URL` in the same change window. That restarts the app; plan it.
- Decide **delete vs retain**. Deleting also deletes its disk; export the login log first if needed.

---

## 5. Deployment hook and branch protection

### 5.1 Render deploy hook → GitHub secret **[APPROVAL]**

1. Render → **pulseforge-api-migration** → **Settings → Deploy Hook** → copy the URL. It is a credential: anyone holding it can trigger deploys.
2. GitHub → Settings → Secrets and variables → Actions → **New repository secret** `RENDER_DEPLOY_HOOK_URL` = that URL.
3. GitHub → Settings → **Environments** → `production`. It is created on first use of the gate; create it now. Add **Required reviewers** (Jacob plus one more if available) and restrict deployment branches to `migration/render-current-main`.
4. First use: run **Render production release gate** with the current tip SHA. In Render → Events, confirm the deployed commit equals the input SHA.
   - The workflow appends `&ref=<sha>`. If Render ignores `ref`, it deploys the branch tip, and the gate already refuses any SHA that is not the tip.
   - If the deployed commit ever differs from the input SHA, stop using the gate and report it.
5. Keep **Auto-Deploy: Off** on the service. Stop deploying from the dashboard except for documented emergencies, and record any such deploy in the incident log.
6. Render → Team settings: limit who can deploy or change environment variables on this service.

### 5.2 Branch protection (GitHub → Settings → Rules → Rulesets → New branch ruleset) **[APPROVAL]**

Target `migration/render-current-main` (and `main` when it becomes the deploy source):
- Require a pull request before merging; 1 approval; dismiss stale approvals on new commits.
- Require status checks to pass: `PR CI gate`, `Acquisition mission suite (test:amo ratchet)`, the `Full platform regression` matrix jobs, `Levi Scout (tenant 21) contract and audit` when it runs. Check names appear in the picker after they have run once on a PR to this branch; PR #1161 provides that.
- Require branches to be up to date before merging.
- Block force pushes. Restrict deletions.
- Bypass list: empty. Or owner only, with every bypass logged in the incident log.

### 5.3 Default branch / scheduled jobs (decision)

GitHub runs scheduled workflows only from the default branch.
- **Option A:** merge draft PR #1146 into `main` after #1161, so `main` contains the deploy branch, and keep both protected.
- **Option B:** make `migration/render-current-main` the default branch.

Until one of these is done, `levi-scout-weekly.yml` never fires.

---

## 6. Read-only database access without exposing PostgreSQL

Constraint: the Render Postgres inbound allowlist is empty, so no public access. Keep it that way.

**Recommended path: Render Shell on the app service.** It uses the private network; no allowlist change. The image includes the PostgreSQL 18 client.

### 6.1 One-time: create a read-only audit role **[APPROVAL]**

This is a DDL change. Run it from Render → pulseforge-api-migration → **Shell**. The shell already holds the owner `DATABASE_URL` in its environment; do not print it.

```sh
read -rs AUDIT_PW   # generate offline (openssl rand -hex 24); paste; not echoed
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -v pw="$AUDIT_PW" <<'SQL'
CREATE ROLE pf_audit_ro LOGIN PASSWORD :'pw' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT CONNECTION LIMIT 2;
ALTER ROLE pf_audit_ro SET default_transaction_read_only = on;
ALTER ROLE pf_audit_ro SET statement_timeout = '30s';
GRANT CONNECT ON DATABASE pulseforge_production TO pf_audit_ro;
GRANT USAGE ON SCHEMA public TO pf_audit_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pf_audit_ro;
SQL
unset AUDIT_PW
```

`ALTER DEFAULT PRIVILEGES` is not granted, so tables created later are not visible to this role. Re-run the `GRANT SELECT` when needed.

### 6.2 Use

```sh
# In Render Shell. Builds a URL for the read-only role from the owner URL without printing either.
read -rs AUDIT_PW
export AUDIT_DATABASE_URL="$(node -e 'const u=new URL(process.env.DATABASE_URL);u.username="pf_audit_ro";u.password=process.env.AUDIT_PW;u.pathname="/pulseforge_production";console.log(u.toString())')"
unset AUDIT_PW
psql "$AUDIT_DATABASE_URL" -X -c "select current_user, current_database(), current_setting('default_transaction_read_only')"
# expect: pf_audit_ro | pulseforge_production | on
```

Prohibited:
- adding an IP to the Postgres allowlist;
- running pgAdmin against production;
- copying either URL out of the shell;
- using the owner URL for audits.

Remove access when the investigation closes: `DROP ROLE pf_audit_ro;` **[APPROVAL]**.

**Alternative**, only if Shell is unavailable: a temporary allowlist entry for a single operator `/32`, with the read-only role, removed the same day and recorded in the incident log.

---

## 7. Investigating potential unauthorized cron activity

**Window:**
- **Start:** 2026-11-22 19:30Z. Production promotion to Render; `CRON_SECRET` absent from then on. If Render served production data earlier, use the first live deploy of that service.
- **End:** the moment §1.4 went live.

**Known legitimate callers in the window (allowlist for reconciliation):**
- cron-job.org jobs: from §1.3 history.
- GitHub Actions runs: cleanup (all 401), Penny (401), Levi weekly (never ran).
- The in-process Levi weekly clock (tenant 21; free SODA only).

Observed Render log times:
- Paige ≈ 12:00Z daily;
- Riley ≈ 13:30Z daily;
- Emmett assessments 14:30Z and 16:15Z on Nov 24.

Run all queries as `pf_audit_ro` (§6). Each is read-only. Replace `:start` and `:end`. Save outputs to the incident folder, not to chat.

```sql
\set start '2026-11-22 19:30:00+00'
\set end   'YYYY-MM-DD HH:MM:SS+00'   -- §1.4 live time

-- 7.1 Every agent run in the window, by agent and hour (compare with the caller allowlist)
SELECT agent_name, action, status, client_id, date_trunc('hour', ran_at) AS hour, count(*)
  FROM agent_log WHERE ran_at BETWEEN :'start' AND :'end'
 GROUP BY 1,2,3,4,5 ORDER BY hour, agent_name;

-- 7.2 High-risk agents specifically (sends, calls, SMS, paid research, recovery scripts)
SELECT agent_name, action, status, client_id, ran_at, left(coalesce(error_msg,''),120) AS err
  FROM agent_log
 WHERE ran_at BETWEEN :'start' AND :'end'
   AND agent_name ~* '(emmett|sam|cal|bland|scout|enrich|tenant_outreach|anchor|penny|pulse|knowledge|signal|seed|recover|execute)'
 ORDER BY ran_at;

-- 7.3 Dispatches refused by policy (an anonymous caller hitting a disabled agent leaves these)
SELECT agent_name, action, client_id, ran_at, payload
  FROM agent_log WHERE ran_at BETWEEN :'start' AND :'end' AND action IN ('dispatch_blocked','cron_dispatch')
 ORDER BY ran_at;

-- 7.4 Paid provider usage (Places, Anthropic, Gemini, enrichment) in the window
SELECT provider, tenant_id, date_trunc('hour', created_at) AS hour, count(*) AS calls,
       sum(coalesce(estimated_usd, reserved_usd, 0)) AS usd
  FROM api_usage_events WHERE created_at BETWEEN :'start' AND :'end'
 GROUP BY 1,2,3 ORDER BY hour;

-- 7.5 Outbound effects: touchpoints by channel/outcome (email, SMS, call)
SELECT channel, action_type, outcome, client_id, date_trunc('hour', created_at) AS hour, count(*)
  FROM touchpoints WHERE created_at BETWEEN :'start' AND :'end'
 GROUP BY 1,2,3,4,5 ORDER BY hour;

-- 7.6 Mission execution audit and execution approvals (execute-anchor-one-outbound mints approvals)
SELECT tenant_id, mission_id, specialist, stage, commit_status, at
  FROM acquisition_mission_execution_audit WHERE at BETWEEN :'start' AND :'end' ORDER BY at;
SELECT tenant_id, mission_id, specialist, kind, at
  FROM acquisition_mission_contributions
 WHERE at BETWEEN :'start' AND :'end' AND kind ~* 'approv|execut|outbound'
 ORDER BY at;

-- 7.7 Governed outbound items created/attempted in the window
SELECT tenant_id, status, date_trunc('hour', attempted_at) AS hour, count(*),
       count(provider_message_id) AS provider_accepted
  FROM acquisition_outbound_items WHERE attempted_at BETWEEN :'start' AND :'end'
 GROUP BY 1,2,3 ORDER BY hour;

-- 7.8 Knowledge outbox drains and Scout expansion artefacts (if tables exist)
SELECT to_regclass('knowledge_outbox'), to_regclass('scout_expansion_runs');
```

Column names are taken from the repository schema. If a query errors with `42703` or `42P01`, record the error, adjust only the column or table name, and keep it read-only.

**Provider-side evidence (owner consoles, same window):**
- **Brevo:** transactional logs (sender domains for tenants 1, 10, 13).
- **Twilio:** messaging logs.
- **Bland:** call logs.
- **Google Cloud:** Places API metrics by hour.
- **Anthropic and Gemini:** usage by hour.
- **Render:** logs, searching for `[cron]` error lines and agent banners (`Emmett infrastructure assessment`, `Paige complete`, `[Riley]`) at times outside the caller allowlist.

**Interpretation:**
- Every run or effect must map to a known caller time (±5 min). Anything that doesn't is **unexplained**.
- **Unexplained with no external effect** (e.g. a refused dispatch, read-only inspection): record it and close.
- **Unexplained with an external effect** (email, SMS, call, paid spend, approval contribution): escalate as a security incident. Preserve the rows (do not edit or delete). Rotate the provider keys involved. Notify affected recipients if required. Open a dedicated incident record.
- Report the result as: *window, callers reconciled, unexplained count by class, evidence file list*.

---

## 8. Environment-variable reconciliation

Source of truth for names:
- Railway production `pulseforge-leadgen`: 152 names, read 2026-11-24 via the Railway API, values redacted.
- Render-only names: from the cutover records.

Render's API here cannot list names. The owner compares them in the Render **Environment** tab. Values are never copied by agents.

**Code reference** is the result of searching the deployed tree:
- **indirect**: referenced by name from configuration (database integration rows, `PAIGE_SOCIAL_ACCOUNTS` JSON);
- **none**: no reference found.

### 8.1 Execution-sensitive flags: decide each value explicitly, never by bulk copy

Some of these are **protective only when set to `true`**. If one is missing on Render, that protection is **off**. Verify these first.

| Name | Semantics in code | Required containment value |
|---|---|---|
| `ANCHOR_RESEARCH_PROVIDERS_PAUSED` | Pause is active only when `=== 'true'` | **`true`**. If it is absent on Render, tenant-10 paid research is *not* paused. **Verify immediately.** |
| `ANCHOR_EMAIL_RECOVERY_MAINTENANCE` | Closed recovery state only when `=== 'true'` | **`true`** (keep the closed state) |
| `ANCHOR_GOVERNED_OUTBOUND_ENABLED` | Closed state requires `=== 'false'` | **`false`** |
| `ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED`, `ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED`, `ANCHOR_GOVERNED_PREPARATION_ENABLED` | Enable Anchor outbound machinery | `false` / unset during P0 |
| `BABRUN_GOVERNED_OUTBOUND_ENABLED`, `GOVERNED_OUTBOUND_TENANT_13_ENABLED`, `GOVERNED_OUTBOUND_TENANT_IDS` | Enable tenant-13 / mailbox-tenant outbound clock | `false` / unset / empty during P0 |
| `EMMETT_AUTOSEND_ENABLED` | Global autosend | `false` / unset |
| `PAIGE_SOCIAL_PUBLISH_ENABLED` | Live social publishing (`=== 'true'`) | unset |
| `WARM_ROUTING_ENABLED` | Warm routing (`=== 'true'`) | unset until §6 R2 of the MAX-P0-002 report |
| `MAX_ORCHESTRATION_ENABLED`, `MAX_SHADOW_MODE`, `MAX_*_ACTIONS_ENABLED`, `MAX_*_TASKS_ENABLED`, `MAX_HOT_ESCALATIONS_ENABLED`, `MAX_STATE_TRANSITIONS_ENABLED`, `MAX_WARM_SEQUENCE_ENABLED`, `MAX_SCORING_ENABLED`, `MAX_ENRICHMENT_RETRY_ENABLED`, `MAX_RECYCLE_ACTIONS_ENABLED`, `MAX_SEQUENCE_ACTIONS_ENABLED`, `MAX_CALL_TASKS_ENABLED` | Max orchestration writes and tasks | Keep Railway's last values. If unknown, unset (off). Never enable during P0. |
| `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED`, `SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED`, `SCOUT_ANCHOR_FACILITY_RECOVERY_ENABLED`, `SCOUT_LEVI_RESEARCH_ONLY_ENABLED` (Render), `SCOUT_LEVI_PAID_PROVIDERS_APPROVED` | Scout research gates | Keep current. Levi paid providers stays unset. |
| `PROSPEO_ENABLED`, `PROSPEO_DAILY_CAP`, `BOUNCER_ENABLED` | Paid enrichment and verification | Keep Railway values; caps must be present if enabled |
| `REVENUE_OPERATOR_WRITES_ENABLED`, `REVENUE_SCHEMA_ENABLED` | Revenue writes | Keep Railway values |
| `SCORECARD_BREVO_SYNC_ENABLED` | Brevo list sync (no code reference found) | unset |
| `SIGNAL_OPERATOR_ENABLED`, `SIGNAL_OPERATOR_RELAY_ENABLED`, `SIGNAL_OPERATOR_RELAY_CONSENT`, `SIGNAL_OPERATION_MODE`, `SIGNAL_SHADOW_MODE` | Signal product | Signal owner decides. Workers are off regardless. |
| `DECISION_SHADOW_ENABLED`, `DECISION_PROVIDER`, `JEV_*` | Shadow evaluation (no authority) | Optional. JEV schema unverified (S-6). |
| `PF_DISABLE_BACKGROUND_WORKERS` (Render) | Disables in-process workers | **`1`** (unchanged) |
| `PF_MIGRATION_RECEIVER`, `MIGRATION_RESTORE_APPROVED`, `MIGRATION_FINAL_SWAP_APPROVED` (Render) | Cutover receiver | `0` / unset / unset. **Remove the approvals.** |

### 8.2 Required for current production function

| Name(s) | Purpose | Observed on Render |
|---|---|---|
| `DATABASE_URL` | Postgres (Render internal URL) | Present (app works) |
| `SESSION_SECRET` | Session signing | Presumed present (login works). Must not fall back to the code default. Verify it is set. |
| `CRON_SECRET` | Cron auth | **Absent → §1** |
| `APP_URL`, `DASHBOARD_URL` | Links in notifications | Verify = Render host |
| `ANTHROPIC_API_KEY` | Paige, Max briefing, legacy Max chat, Pulse health | **Absent** (Paige SDK error) |
| `GEMINI_API_KEY` | Max workspace presentation writer | Unverified |
| `MAX_DECAY_CRON_SECRET` | `/internal/cron/max-decay` Bearer | Unverified; no caller |
| `GMAIL_CREDENTIALS`, `GMAIL_TOKEN` or `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`; `RILEY_ACCESS_TOKEN`, `RILEY_REFRESH_TOKEN`, `RILEY_TOKEN_EXPIRY` | Riley inbox | **Absent** (Riley error) |
| `GOOGLE_REFRESH_TOKEN`, `GOOGLE_CALENDAR_REFRESH_TOKEN`, `GOOGLE_SERVICE_ACCOUNT_JSON`, `GOOGLE_SHEETS_REFRESH_TOKEN` | GBP, Calendar, Sheets | Likely absent (same family as Riley) |
| `BREVO_API_KEY`, `BREVO_WEBHOOK_SECRET` | Email send and webhook verification | Unverified. The webhook secret must exist before Brevo is repointed. |
| `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET` | Billing and webhooks | Unverified |
| `CLARITY_GOOGLE_CLIENT_ID`, `CLARITY_GOOGLE_CLIENT_SECRET`, `CLARITY_TOKEN_ENCRYPTION_KEY`, `CLARITY_TOKEN_KEY_ID` | Clarity | Railway holds the last one under a malformed name (backticks included in the name), so code never read it. **Create it correctly as `CLARITY_TOKEN_KEY_ID`.** |
| `MIRA_CONTEXT_SECRET` | Mira context endpoint | Unverified |
| `ADMIN_EMAIL`, `ADMIN_PASSWORD`, `DASHBOARD_PASSWORD` | Bootstrap / fallback auth | Verify. **Rotate** if Railway values were ever shared; the fallback must not be weak. |

### 8.3 Optional / provider credentials (set only when the dependent feature is intentionally enabled)

`APOLLO_API_KEY`, `HUNTER_API_KEY`, `PROSPEO_API_KEY`, `SERPAPI_KEY`, `GOOGLE_PLACES_KEY`, `BOUNCER_API_KEY`, `OPENAI_API_KEY`, `JEV_API_KEY`, `BUFFER_ACCESS_TOKEN`, `BUFFER_CHANNEL_ID`, `BUFFER_LINKEDIN_PERSONAL_ID`, `ANCHOR_BUFFER_TOKEN` (indirect via `PAIGE_SOCIAL_ACCOUNTS`), `PAIGE_SOCIAL_ACCOUNTS`, `PAIGE_WRITER_MODEL`, `PAIGE_EVALUATOR_MODEL`, `FACEBOOK_PAGE_ACCESS_TOKEN`, `FACEBOOK_PAGE_ID`, `FACEBOOK_SESSION`, `LINKEDIN_SESSION`, `GOOGLE_ADS_CLIENT_ID`, `GOOGLE_ADS_CLIENT_SECRET`, `GOOGLE_ADS_DEVELOPER_TOKEN`, `ANCHOR_GOOGLE_ADS_CUSTOMER_ID`, `ANCHOR_GOOGLE_ADS_REFRESH_TOKEN`, `ANCHOR_GOOGLE_REFRESH_TOKEN`, `STUDIO_SUBSTRAL_CLIENT_ID`, `STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN`, `GITHUB_TOKEN`, `GITHUB_REPO`, `TODOIST_API_TOKEN`, `MAX_SPREADSHEET_APPROVER_USER_ID`.

Present on Render per the startup banner: `SERPAPI_KEY` ✓, `HUNTER_API_KEY` ✓.

**Execution-capable credentials.** Their presence enables external effects once the relevant flag or caller exists:
- `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_PHONE_NUMBER` (SMS);
- `BLAND_PHONE_NUMBER` (+ `BLAND_API_KEY`, absent on Railway too) (calls);
- `ANCHOR_MAILBOX_PASSWORD` (indirect via mailbox integration rows), `BABRUN_MAILBOX_IMAP_PASSWORD`, `BABRUN_MAILBOX_SMTP_PASSWORD` (mailboxes);
- `TELEGRAM_*`, `MIRA_TELEGRAM_BOT_TOKEN`, `SIGNAL_TELEGRAM_*` (notifications);
- `JACOB_PHONE`, `JACOB_TELEGRAM_CHAT_ID`, `NOTIFY_PHONE`, `OPS_ALERT_EMAIL`, `OPS_ALERT_PHONE` (alert destinations, needed for M9 alerting).

Set these only with the owning feature's activation plan (MAX-P0-002 report §6 R3).

### 8.4 Obsolete on Render (do not copy)

| Name(s) | Reason |
|---|---|
| `RAILWAY_*` (11 names) | Railway platform injection |
| `PUPPETEER_SKIP_DOWNLOAD` | Build-time only (the Dockerfile handles it) |
| `PORTAL_RUNTIME_PASSWORD`, `SCORECARD_BREVO_LIST_ID`, `SCORECARD_BREVO_SYNC_ENABLED`, `STRIPE_PUBLISHABLE_KEY`, `BABRUN_DORMANCY_CHANGE_ID`, `GOOGLE_ADS_LOGIN_CUSTOMER_ID`, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS` | No reference in the deployed code (search covered `.js`/`.cjs`/`.json`/Dockerfiles). Confirm with the owner before discarding, since a value may be needed by an external tool. |
| `` `CLARITY_TOKEN_KEY_ID` `` (with backticks) | Malformed duplicate; replace with the correct name (§8.2) |
| `S3_*` / migration bucket keys (Render, if present) | Cutover only. Remove after the migration archive retention decision. |

### 8.5 Procedure

1. Export the Render name list: Render → Environment, names only, into the incident folder.
2. Diff it against §8.1–§8.4.
3. Set the §8.1 values first, then §8.2, in **one** Render change window (one redeploy). Record each name changed; no values.
4. Re-run §3 sections A–E.
5. Check the startup log for `[startup] background workers disabled…`, no `SerpAPI ✗`/`Hunter ✗` regressions, and no `missing … credentials` lines at the next Paige (12:00Z) and Riley (13:30Z) runs.
