# MAX-P0-002 — Production Integrity Recovery

**Date:** 2026-11-24 (UTC). **Baseline:** MAX-P0-001 (`docs/audits/MAX-P0-001-PRODUCTION-READINESS-AUDIT.md`). **P0 status:** open. Feature freeze active.

**State legend:**
- **Implemented**: code on the session branch.
- **Tested**: automated tests pass locally.
- **Deployed**: live on Render.
- **Prod-verified**: observed working in production.

Nothing in this package is deployed. No merge, deploy, restart, configuration change, credential creation, migration, Scout run or outbound action was performed. The only infrastructure change was to this agent's own sandbox network allowlist (two Render hostnames), so that the read-only probes below could run.

---

## 1. Confirmed production configuration defects

| # | Defect | Evidence (2026-11-24) | Impact |
|---|---|---|---|
| C1 | **`CRON_SECRET` is unset on Render production** | Probe `GET /cron/p0-audit-nonexistent-agent?secret=invalid-probe` → **400 `Unknown agent`** (the wrong secret passed the guard). Without a secret: also 400. `POST /cron/max-conversation-context-cleanup` without auth → 401. Probe design: an unknown agent is rejected before any DB or provider access (`routes/cron.js` `runCronAgent`, first line). | **Security, P0.** 13 legacy cron handlers plus `/cron/knowledge-outbox` accept **any anonymous caller** (see §4). Fail-closed routes (context cleanup, Penny daily, Anchor governed, Clarity, Levi) refuse **everyone**, legitimate callers included. |
| C2 | Anthropic credential absent (`ANTHROPIC_API_KEY` / `ANTHROPIC_AUTH_TOKEN`) | Paige at 12:00Z on Nov 23 and Nov 24: SDK error `Could not resolve authentication method…`, 5/5 channels failed. The SDK reads those two env names by default (`utils/meteredAnthropic.js:37`). | Paige, the daily Max briefing (`maxAgent.js`), legacy `/api/max/ask` (`routes/maxChat.js`), Pulse health and the cron summaries are non-functional. **Correction to MAX-P0-001:** the Max *workspace* presenter uses Gemini (`GEMINI_API_KEY`), not Anthropic. Gemini presence is UNVERIFIED. |
| C3 | Riley Gmail credentials absent: both `GMAIL_CREDENTIALS` and the `GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET` fallback | `[Riley] Gmail processing failed: Missing Gmail OAuth credentials` at 13:30Z on Nov 23 and Nov 24. `rileyAgent.js:205-227` tries both sources. | No inbound reply triage. Missing Google client credentials probably also break Vera, Cal and GBP publishing (inferred from shared variable names). |
| C4 | Background workers disabled (`PF_DISABLE_BACKGROUND_WORKERS=1`) | Startup log at 13:46:11Z. `server.js:109-139`. | §5 inventory. |
| C5 | Max context cleanup: 401 on every run | Actions `max-context-cleanup-hourly.yml`, latest runs all `failure`, `curl: (22) 401`. Root cause: C1, because the route fails closed with an empty secret. | Expired `max_conversation_context` rows accumulate (only if the continuity flag is on; UNVERIFIED). |
| C6 | Penny Anchor daily review: failing since the cutover | Actions runs 57 and 58 (Nov 23, Nov 24) `failure`; last success run 56 (Nov 22, Railway). Root cause: C1 (`/cron/penny-daily` is fail-closed). | No Penny daily review. |
| C7 | Max decay has no scheduled caller; separate secret | `/internal/cron/max-decay` authenticates with **`MAX_DECAY_CRON_SECRET`** (Bearer), not `CRON_SECRET` (`utils/maxDecayCron.js:122`). The Railway cron service `max-decay-daily` was not replaced; no `[max_decay_cron]` log line since the cutover. | Decay not applied. Presence of `MAX_DECAY_CRON_SECRET` UNVERIFIED. |
| C8 | Max attention cycle has no caller | `/cron/max-attention-cycle` exists (`routes/cron.js` `handleMaxAttentionCycleCron`). No workflow or log evidence of a caller (also none found pre-cutover). | Attention items are not re-evaluated on a schedule. |
| C9 | Levi weekly GitHub fallback never fires | `levi-scout-weekly.yml` exists only on `migration/render-current-main`. Scheduled workflows run from the default branch (`main`), where it is absent. The in-process weekly clock is the only trigger (startup log: "Levi Scout weekly research clock started"). | Single point of failure for the tenant-21 weekly cycle (next: Mon 2026-11-30 00:15Z). |
| C10 | `content_recommendations` relation missing | App log 13:54:53Z. Created only by the separately applied migration `migrations/2026-08-16-specialist-direction.sql`, never by startup DDL. The rehearsal reported an identical catalog, so this is **probably pre-existing on Railway**, not a cutover regression (not provable without database access). | Command Deck spatial content recommendations fail. |
| C11 | Publicly reachable pgAdmin 4 | `GET /` → 302 `/login?next=/`; `GET /login` → 200, title "pgAdmin 4". Service `pgAdmin-dpg-dbtgtloae00c73dd8040-a`, created 2026-11-24 00:00Z, `ipAllowList 0.0.0.0/0`, auto-deploys from `github.com/render-examples/pgadmin` `main`. No login was attempted. | Internet-facing admin UI with database network access. Upstream commits redeploy it automatically. Owner decision required (§3). |

Re-checked and healthy: `GET /healthz` → 200. Anonymous `/api/me`, `/api/prospects` and `POST /api/v1/max/workspace/ask` → 401. `/admin/scout-research-approval` → 302 to `/login`.

---

## 2. Changes implemented and their evidence

All changes are on branch `ccr-006e6a0d-ntlnuf`, which is based on the deployed `357e747`. PR target: `migration/render-current-main`.

| Change | Implemented | Tested | Deployed | Prod-verified |
|---|---|---|---|---|
| Fail-closed legacy cron auth (`utils/cronAuth.js`; 13 sites in `routes/cron.js`, 1 in `routes/knowledgeAdmin.js`) | Yes (`c082132`) | Yes | No | No |
| Cron route-enumeration regression test (`test/cronAuthFailClosed.test.js`) | Yes | 7/7; fails on the previous code (handlers ran → 500/422) | n/a | n/a |
| Full regression on push/PR for `migration/render-current-main` + `test:amo` ratchet job | Yes (`ddd5f87`) | Ratchet passes on the current tree, exits 1 on an injected regression; baseline deterministic across 2 runs | Takes effect on merge | No |
| Release gate (`render-release-gate.yml`, `scripts/ci/verifyReleaseCandidate.js`) | Yes (`ddd5f87`) | Decision unit tests 7/7 (`test/releaseGateRatchet.test.js`) | Inert until the operator adds a secret | No |
| Audit session-reference correction (follow-up commit, no force-push) | Yes (`1b3aa2a`) | n/a | n/a | n/a |

**Regression evidence (local, deployed tree plus these changes):**
- All 23 test files touching cron or `CRON_SECRET`: **250 pass / 4 fail**. That is the same 4 failing tests as the unmodified baseline (byte-identical failure set).
- `npm run test:global-smoke`: 10/10.
- `npm run test:max:unit`: 2,925/2,925.
- Workflow-lint and CI suites (`ciPlanPr`, `ciPostgresProvisioning`, `revenueWiring`, `executeAnchorOneOutbound`, `anchorCanonicalOutboundRecovery`): 117/117.

**Test change disclosed:** `test/anchorResearchProviderPauseOperators.test.js` previously asserted that an **unset** `CRON_SECRET` lets an unauthenticated caller run the Scout Places diagnostic (HTTP 200). That assertion encoded the vulnerability. It now asserts 401 with the diagnostic not invoked. Two route harnesses (`maxConversationContextCleanupCron`, `maxAttentionCronRoute`) gained the new dependency in their allowlists.

---

## 3. Remaining operator-dependent blockers

| # | Requirement | Owner | Why it cannot be done here |
|---|---|---|---|
| O1 | **Set `CRON_SECRET` on Render** to one value shared by every legitimate caller: GitHub repository secret `CRON_SECRET`, the cron-job.org jobs (`/cron/anchor-max-outbound-control`, `/cron/emmett`, `/cron/riley`, `/cron/warm_signal`, `/cron/paige`), and any other scheduler. Render redeploys on env change. | Owner | Creating credentials is reserved. Choosing a value without matching cron-job.org would break those callers silently. |
| O2 | **Then deploy the fail-closed fix** (merge this PR → release gate or manual deploy of the merge SHA). Order matters: O1 first, or the legitimate callers stop with the fix, which is the safe direction but a visible outage. If O1 is delayed, deploying the fix alone is still recommended: it closes the anonymous exposure. | Owner | Merge and deploy require approval. |
| O3 | Set `ANTHROPIC_API_KEY` on Render. Set `GEMINI_API_KEY` too if absent. | Owner | Credential. |
| O4 | Set Riley Gmail credentials (`GMAIL_CREDENTIALS` and token, or `GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET` + refresh token). Re-confirm the OAuth redirect URIs for the Render host. | Owner | Credential plus Google Console. |
| O5 | Set `MAX_DECAY_CRON_SECRET` and choose a decay caller. Option: cron-job.org with a Bearer header. A GitHub schedule works only once the workflow is on `main`. | Owner | Credential. |
| O6 | Name-by-name reconciliation of the ~150 Railway variable names (listed during cutover) against Render. Render's API, as exposed here, cannot list env names. | Owner | No read access to Render env names. |
| O7 | pgAdmin: suspend it, or restrict `ipAllowList` to operator IPs and disable auto-deploy from the third-party repo. Confirm who created it and when it was last used. | Owner | Infrastructure change requiring explicit approval. Not performed. |
| O8 | Release-gate enforcement: add secret `RENDER_DEPLOY_HOOK_URL`; enable branch protection on `migration/render-current-main` (require PRs plus the `PR CI gate` check); deploy only through `render-release-gate.yml`. Verify that Render deploy hooks honour `&ref=<sha>`; if they don't, deploy the branch tip only. | Owner | Repository and Render settings. |
| O9 | Production database read access for the checks in §6 (a SELECT-only role, plus an IP allowlist entry or a Render shell). | Owner | The Render Postgres allowlist is empty by design. |
| O10 | Make the default branch match production (merge draft PR #1146 after this PR, or change the default branch) so scheduled workflows (`levi-scout-weekly.yml`, any decay/attention workflow) run. | Owner | Branch policy decision. |

---

## 4. Security: fail-open routes and exposure

**Confirmed open to anonymous callers until O1 or O2 lands.** These are guarded by `if (process.env.CRON_SECRET && secret !== …)`:

| Route (GET and/or POST) | What an anonymous caller can start | Downstream gates that still apply |
|---|---|---|
| `/cron/:agent` (any of 30 `CRON_MODULES`, e.g. `emmett`, `sam`, `cal_batch`, `scout`, `paige`, `riley`, `tenant_outreach_executor`) | Agent runs, including SMS (Sam), AI calls (Cal Batch), paid research (Scout) and email (Emmett) | `isAgentEnabledForClient` per tenant, research pause, the tenant-21 pilot policy, the Emmett governor, DNC, outbound kill switches |
| `/cron/execute-anchor-one-outbound` | The production one-send script with `confirmProduction: true`. The script mints its own `APPROVE_EXECUTION` (`scripts/executeAnchorOneOutbound.js:564`). | Pre-send "green" probes; tenant-10 authorization (observed `authorization_expired` on Nov 9) |
| `/cron/tenant-outreach-executor` | `executeDueScheduledSends` | Scheduled-send authorization and sending identity |
| `/cron/recover-anchor-canonical-outbound`, `/cron/inspect-anchor-canonical-outbound`, `/cron/continue-anchor-str-scout-investigation` | Anchor recovery, inspection and investigation scripts | Script-level gates |
| `/cron/seed-direct-mail-ao`, `/cron/scoutExpansion`, `/cron/pulse-health`, `/cron/scout-places-diagnostic`, `/cron/tenant-mailbox-poll`, `/cron/signal-shadow`, `/cron/knowledge-outbox` | Seeding, Scout expansion, Claude spend, Places diagnostics, mailbox polling, Signal shadow, knowledge outbox drain | Varies |

**Whether any anonymous call has occurred is UNVERIFIED.** Render request logs are not available through this API. `agent_log` (`triggered_by: 'cron'`) and provider ledgers would show it, and require O9. **Recommended immediately after O9:** a read-only review of `agent_log`, `api_usage_events`, `acquisition_outbound_items` and Brevo/Twilio/Bland activity since 2026-11-22 19:30Z for invocations no known scheduler explains.

**Fail-closed routes behave correctly**: they refuse everyone, so nothing anonymous gets through. Examples: Clarity, Anchor governed, Levi (`cee40f8`), context cleanup, Penny daily, max-attention-cycle, `/api/emmett/autorun`, `server.js` GBP lookup.

---

## 5. Worker and scheduler inventory

Last-success data from production tables is UNVERIFIED (O9); the figures below come from logs and Actions history.

### 5a. In-process background workers (all disabled by `PF_DISABLE_BACKGROUND_WORKERS=1`)

| Worker | Tenant scope | External side effects | Replay / duplication risk | Execution permission | Restore class |
|---|---|---|---|---|---|
| Signal v1 shadow scheduler | Signal (separate product) | None (shadow) | Low | n/a | Read-only. Restorable after the Signal owner confirms. |
| Signal optional pilot controller / Signal operator | Signal | Possible trading / SMS staging (ADR-109) | Medium | Signal-specific gates | **Side-effecting.** Excluded from Max P0. |
| Mira transcription / classifier / router / digest | Mira (owner) | Model calls; digest messages | Digest could re-send after an outage | Mira client state | Transcription/classifier: internal. Digest: outbound to the owner. |
| Warm routing scheduler | Prospect routing | Writes routing; may notify setters | Backlog flush on restart | Setter rules | Internal writes. Restore after the backlog review below. |
| Anchor unenriched enrichment scheduler (tenant 10) | 10 | **Paid enrichment providers** | Re-enrichment cost | Research pause / grants | **Paid.** Hold. |
| Anchor governed scheduler (poll → tick → Max control) | 10 | Mailbox poll (read), **email sends** (tick), Max control decisions | Stage-level locks and provider receipts exist. Durable stage scheduling not deployed. | Program authorization (expired on Nov 9), maintenance/kill switch | **Outbound.** Hold. Controlled plan §6. |
| Governed outbound execution clock (mailbox tenants ≠ 10, from `GOVERNED_OUTBOUND_TENANT_IDS`; Babrun is likely 13) | listed tenants | **Email sends**, mailbox polls | Same as above | Per-tenant program authorization | **Outbound.** Hold. |
| Levi Scout weekly clock | 21 | Free public open-data requests (SODA). No paid providers, no outbound (pilot policy). | Weekly claim, advisory lock (R2); unique index not applied | Research-only flag (on, per the startup log) | **Running.** Independent of the flag. |

### 5b. External schedulers

| Job | Caller | Target route | Auth | Current state |
|---|---|---|---|---|
| Max context cleanup (hourly) | GitHub Actions (`main`) | `/cron/max-conversation-context-cleanup` | Bearer `CRON_SECRET`, fail-closed | **Failing 401** (C1) |
| Penny daily | GitHub Actions (`main`) | `/cron/penny-daily` | query secret, fail-closed | **Failing 401** since Nov 23 |
| Levi weekly | GitHub Actions | `/cron/scout_levi` | Bearer, fail-closed | **Never fires** (not on `main`); also 401 until O1 |
| Full regression (daily) | GitHub Actions | — | — | Runs on `main` only. Fixed for the deploy branch by `ddd5f87` once merged. |
| Paige (daily 12:00Z), Riley (13:30Z), Emmett (observed 14:30Z, 16:15Z) | cron-job.org (per the cutover record; not visible here) | `/cron/paige`, `/cron/riley`, `/cron/emmett` | query secret, **fail-open** | **Reaching Render only because auth is open.** They will 401 after O2 unless O1 matches cron-job.org's secret. |
| Anchor Max outbound control (every 15 min), warm signal | cron-job.org | `/cron/anchor-max-outbound-control`, `/cron/warm_signal` | Bearer fail-closed / query fail-open | No app-log trace on Render. UNVERIFIED (owner: cron-job.org history). |
| Max decay (daily) | none since the cutover | `/internal/cron/max-decay` | Bearer `MAX_DECAY_CRON_SECRET` | **Not running** (C7) |
| Max attention cycle | none | `/cron/max-attention-cycle` | query, fail-closed | **Not running** (C8) |

**Outstanding backlog:** UNVERIFIED (needs O9). For each held worker, the queries to run before restoration are: due `governed_outbound_stage_*`/scheduled sends, `scout_unenriched` pending rows, warm-routing pending, the Mira queue, and `agent_actions` pending.

---

## 6. Controlled restoration plan

Principle: restore read-only work first, internal writes second, side-effecting work last. Each step is separately approved, uses the existing gates, and never flips `PF_DISABLE_BACKGROUND_WORKERS` globally.

**Phase R0 — close exposure (no functionality restored)**
1. O1 (`CRON_SECRET`), then O2 (deploy the fail-closed fix).
2. Verify: the probe from §1 C1 now returns 401. Context-cleanup and Penny workflows go green. The cron-job.org jobs still succeed.
3. O9, then the anonymous-invocation review (§4).

**Phase R1 — read-only and non-side-effecting (safe after R0)**
- Max context cleanup: green automatically after O1. Bounded DELETE of expired rows only.
- Max decay: O5. Internal score decay; no external effect. Run once manually with the verifier (`npm run max:decay:history`) before scheduling it.
- Max attention cycle: explicit `client_id` per call; a zero-item cycle first. It writes internal decisions only.
- Riley **poll only** (after O4): reads the inbox and classifies. It already writes touchpoints/status. It does not send.
- Paige drafting (after O3): writes `pending_comments` only. Publishing stays behind human approval.

**Phase R2 — internal writes with backlog risk (each needs a backlog query and approval)**
- Warm routing; Mira transcription/classifier. Restore one at a time, behind a per-worker flag rather than the global flag. This requires a small code change to split `PF_DISABLE_BACKGROUND_WORKERS` into per-worker switches. **Not implemented**: it changes server startup, which is deferred to the next package unless explicitly authorized.

**Phase R3 — workers that contact prospects or spend money (activation plan only; not authorized)**

| Worker | Preconditions before activation | First activation | Abort / rollback |
|---|---|---|---|
| Anchor governed scheduler (poll → tick → control) | Program authorization renewed by the operator; maintenance/kill-switch state read back; mailbox credentials valid; due-work backlog reconciled (uncertain provider outcomes quarantined, not replayed); **lock-leak fix from the next package deployed**; single instance confirmed | Poll only (read). Then a single tick on an approved, bounded tenant-10 canary with `maxSends: 1`. Then readback of provider id and canonical record. | Stop the worker; leave quarantine in place; reconcile per `docs/reliability/durable-stage-scheduling.md` §"Required reconciliation" |
| Governed outbound clock (Babrun / mailbox tenants) | As above, per tenant; `GOVERNED_OUTBOUND_TENANT_IDS` read back | Poll only, then a bounded canary | Same |
| Anchor unenriched enrichment | Research grant and spend cap confirmed; `api_spend_policies` row read | One bounded cycle under the existing grant | Research pause |
| Sam / Cal Batch / Emmett sends via cron | Enabled-agents review per tenant; DNC; governor | Only through governed paths, never `/cron/:agent` | Disable agent for tenant |

---

## 7. Deployment branch reconciliation status

| Item | Status |
|---|---|
| Authoritative production branch | `migration/render-current-main` (Render `autoDeploy: no`, manual/API trigger) |
| Deployed commit | `357e747` (`dep-dc2p9obm8hqs7386utd0`, live 2026-11-24 13:46:13Z) |
| `main` | `76a1059`: behind by 18 commits. Its only unique content is `docs/migration/final-export-request.json` (one line, a historical request; its workflow triggers only on the retired cutover branch, so it is inert). |
| Vehicle | Draft PR #1146 (`migration/render-current-main` → `main`) already exists |
| Plan | 1. Merge this PR into `migration/render-current-main`; full regression + `test:amo` ratchet run on the push (new trigger). 2. Deploy via the release gate (O8). 3. Undraft #1146 and merge into `main`, taking the deploy branch's JSON content; full regression runs on `main`'s push. 4. Then either switch Render to deploy from `main` with branch protection, or keep the deploy branch and make it the GitHub default (O10). Never deploy a branch that has not passed full regression at its tip. |
| Status | **Planned, not executed** (merges need approval). |

## 8. CI enforcement status

| Control | Status |
|---|---|
| Full regression on the deployment candidate | Implemented (trigger on push/PR for `migration/render-current-main`). Active after merge. |
| `test:amo` in CI | Implemented as a ratchet (72 known failures baselined; new failures fail the job). Active after merge. |
| Release gate refusing failed checks | Implemented (`render-release-gate.yml`). **Not enforceable** until O8: deploy-hook secret, branch protection, and no direct Render deploys. Render itself cannot see GitHub checks while auto-deploy is off. |
| Historical breach | `f414087` and `0728ba2` were deployed with failing PR CI (MAX-P0-001 E8). |

---

## 9. Tenant readiness (read-only)

Production database access is unavailable (O9). Everything below is from code, logs and HTTP probes.

| Tenant | Check | Result |
|---|---|---|
| Anchor (10) | Outbound / research state | UNVERIFIED (database). Governed scheduler and enrichment are off (C4). Last known (Nov 9, Railway): `authorization_expired`, research grant revision mismatch. **Read-only validation only.** |
| Babrun (13) | Outbound / research state | UNVERIFIED. Its mailbox execution clock is off (C4). **Read-only validation only.** |
| Levi (21) | Pilot policy deployed | **Yes, deployed**: `357e747` contains `e592578` (`utils/tenantPilotPolicy.js`). Prod-verified: **No** (step-4 refusal check needs an admin session). |
| | Pre-invite SQL (`docs/audits/levi-pilot-preinvite.sql`) | **Not run.** UNVERIFIED (O9). |
| | Post-deploy checks (release-readiness §7 step 4) | Startup log does **not** contain the expected `replay_blocked` catch-up line for `levi-week-2026-11-23`. Only "weekly research clock started" appears. The admin `POST /api/run/scout?client_id=21` → 403 check was **not run** (needs an admin session; it would write a `dispatch_blocked` audit row). |
| | Authentication readiness | Code path tested in CI (`leviTenant21ClientAccessPostgres`, 88/88 per the readiness doc). Production: `/login` 200, anonymous APIs 401. **Whether Levi's account exists, or whether he has signed in, is UNVERIFIED.** Credential delivery does not establish either. |
| | Onboarding restrictions | For a `client` user, Max stays locked until the Blueprint is approved **and** the AIM published (`services/pilotOnboarding.maxAcquisitionReply`). |
| | Blueprint status | UNVERIFIED (database). Last recorded state: no approved numeric financing range (`docs/P0-SCOUT-LEVI-001-READINESS.md`). Financing range **not** approved here and must not be. |
| | Weekly cycle | In-process clock running; GitHub fallback never fires (C9). Next cycle Mon 2026-11-30 00:15Z. |

**Exact checks for the operator (read-only, after O9):**
1. `psql "$AUDIT_DATABASE_URL" -X -f docs/audits/levi-pilot-preinvite.sql` (go criteria in `docs/audits/LEVI-PILOT-LOGIN-SMOKE-TEST.md` §1).
2. `node scripts/auditLeviTenant21PostPr1157.js --tenant 21 --confirm-read-only --expect-database=pulseforge_production`.
3. `SELECT id, email, role, client_id, active, last_login_at, password_change_required FROM users WHERE client_id = 21;` This answers "has Levi logged in" (`last_login_at`).
4. `SELECT action, error_msg, ran_at FROM agent_log WHERE client_id = 21 ORDER BY ran_at DESC LIMIT 20;`

---

## 10. Scout revalidation requirements

Scout's production reliability is reopened. Before it can be declared verified again:
1. **Exposure review:** confirm no anonymous `/cron/scout*`, `/cron/:agent?agent=scout`, `scoutExpansion` or `scout-places-diagnostic` invocation since 2026-11-22 19:30Z (`agent_log`, `api_usage_events` for `google_places`, `places_api_requests`).
2. **Spend controls:** read the `api_spend_policies` rows (Places and others) and the day's ledger; confirm the research pause and grant state for tenant 10.
3. **Schedulers:** decide which Scout paths should run on Render (Anchor enrichment, governed control). Today only the Levi clock runs.
4. **Levi:** first post-deploy weekly cycle (Nov 30) observed end to end: claim, discovery, review-only persist, no paid provider, no outbound. Then run the post-PR-1157 audit.
5. **Data integrity:** `setter-review-hold-impact.sql` (R8, deployed with #1159), run per tenant.
6. **Acceptance:** Scout portions of MAX-P0-001 T-DISCOVERY on an acceptance tenant, after R0.

---

## 11. Updated P0 acceptance register

| ID | Item | State |
|---|---|---|
| D1/C4 | Background workers off | Open, operator plan (§6) |
| C1 | `CRON_SECRET` unset → anonymous cron access | **Code fix implemented and tested; not deployed.** Config fix O1 open. |
| D2/C5 | Context cleanup 401 | Root cause identified (C1). Open until O1. |
| D3/C2 | Anthropic credential | Open (O3) |
| D4/C3 | Riley Gmail | Open (O4) |
| D5/C7 | Max decay | Open (O5); separate secret identified |
| C6 | Penny daily failing | Root cause C1. Open until O1. |
| C8 | Attention cycle no caller | Open (R1) |
| C9 | Levi weekly fallback never fires | Open (O10) |
| C10/D13 | `content_recommendations` | Reclassified: likely pre-existing (migration never applied). Open, owner decision. |
| C11/S-3 | Public pgAdmin | **Confirmed.** Open (O7) |
| D9 | CI not testing the deployment candidate | **Implemented** (`ddd5f87`), active on merge. Enforcement open (O8). |
| D6 | Advisory-lock leak | Open; deferred to the next package (per directive) |
| D7/D8 | Durable ownership / verification readback | Open; next package |
| D10 | Routing defects R2, R3, R7, R9, R10 | Open (baselined in the ratchet) |
| D11 | Silent-failure alerting | Open |
| D14 | Docs still describe Railway | Partially addressed here; CLAUDE.md not yet updated |
| R1–R10, P1–P4 (register) | Prior register | Unchanged; failing tests now tracked by the `test:amo` ratchet baseline |

**P0 is not closed.**
