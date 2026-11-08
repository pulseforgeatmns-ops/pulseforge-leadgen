# P0 Max Reliability — Release Checklist (PR #1065)

Status: **release candidate, not authorized.** Companion to
`docs/SPEC-MAX-RELIABILITY-004.md`, `docs/P0-MAX-PRODUCTION-ACCEPTANCE.md` and
`docs/P0-MAX-RESPONSE-INTEGRITY.md`.

Base: `main` @ `f129c74` (merged into the PR branch 2026-11-07). The release
candidate is the PR HEAD whose GitHub checks are all green (record the exact
SHA in §6 at sign-off).

## R. Release-candidate addendum (2026-11-07)

Commits added after `5b2db0c`/`f49e4fe` (all reviewed in `docs/P0-MAX-RESPONSE-INTEGRITY.md`):
`9829760` response integrity, `d849a49` execution-intent routing, `6bfc3b0`
review-bound execution approval, `33ed2d9` complete-content approval binding,
then a merge of `main` (`f129c74`, Pipeline UI only), then preflight corrections:
a distinct `billing_unavailable` model-error code, and the P0 PostgreSQL recovery test
moved from `package.json` into explicit CI workflow steps (no `package.json` change remains).

**R.1 Migrations.** None. No migration file, no new table or column, startup DDL unchanged.
New approval fields (`preparedContentDigest`) and pending-decision fields
(`missionId`, `preparedArtifactRevision`, `preparedContentDigest`) are additive JSON in
existing `acquisition_mission_contributions.payload` / `acquisition_missions.payload`.
The unapplied operator migrations noted in §0.2 are pre-existing and not required.

**R.2 Production configuration.** None. No environment variable, feature flag or
Railway setting changes.

**R.2a Services affected by merging the final commit** (Railway project `charming-trust`,
environment `production`; source settings read 2026-11-08, read-only):
| Service | Source | Watch patterns | Deploys on merge? |
|---|---|---|---|
| `pulseforge-leadgen` | `main`, `checkSuites:false` | none (every commit) | **Yes** |
| `max-decay-daily` (cron `0 8 * * *`) | `main`, `checkSuites:false` | none (every commit) | **Yes** (next cron run uses the new build) |
| `signal-dashboard` | `main`, `checkSuites:false` | signal paths, `/package.json`, `/package-lock.json` | **No.** The final diff vs `main` (66 files) touches no watched path. An earlier PR revision changed `package.json` and would have redeployed it and re-run its pre-deploy schema step; that change was removed. |
| `signal-historical-backfill` | `codex/signal-opportunities-v1` | n/a | No (different branch) |
| `telegramCallerFeed` | `codex/signal-opportunities-v1` | n/a | No (different branch) |
Re-check this table if any later commit touches `package.json`, `package-lock.json` or a signal path.

**R.3 API behaviour change.** `APPROVE_EXECUTION` from any human-facing source (chat,
approval button, command deck, voice, API) must carry `expectedPreparedArtifactRevision`
and `expectedPreparedContentDigest` (from `executionReview`). Audit of production callers:
| Caller | Status |
|---|---|
| Max chat | supplies both from the package this session rendered |
| Mission Workspace UI (`public/acquisition-missions.html`) | has no execution-approval button, so unaffected |
| `POST /api/v1/amo/missions/:id/execute` (external/manual) | must pass both; no in-repo caller |
| Governed daily outbound / Anchor email recovery (`services/governedOutboundAdapters.js`) | exempt: passes `governedApproval`, keeps its snapshot-hash + manifest binding; REST cannot inject `governedApproval` |
| `scripts/executeAnchorOneOutbound.js`, `regenerateAnchorCapacityRevision.js`, `regenerateAnchorPaigeCopyRevision.js` | updated to bind to the state they just inspected |
| `services/anchorScopedResumeCrmPreparation.js` | allow-lists intents; never `APPROVE_EXECUTION` |
| `/api/actions/:id/execute` | marks an `agent_actions` row done; unrelated |

**R.4 Pre-deployment checks (read-only, authorized operator).**
1. GitHub: PR CI gate and every planned job green on the exact HEAD to merge.
2. Confirm production still runs the rollback target (§6) and no deploy is in progress.
3. Read-only SQL (acceptance §1.2/§1.3): schema present; `api_spend_policies` row for `google_places` recorded.
4. Read-only inventory of missions at READY with a consumed execution approval (they keep
   legacy revision-only binding until re-approved; record them):
   (the `payload` column holds the whole contribution row; approval fields are under `payload->'payload'`)
   `SELECT mission_id, tenant_id, payload->'payload'->>'preparedArtifactRevision' AS revision FROM acquisition_mission_contributions WHERE specialist='operator' AND kind='approval' AND payload->'payload'->>'action'='execution_approved' AND payload->'payload'->>'preparedContentDigest' IS NULL AND COALESCE((payload->'payload'->>'invalidated')::boolean,false)=false;`
5. Governed outbound state recorded (last 3 days of `acquisition_outbound_events`), and outbound mode per tenant recorded.
6. Known pre-existing hydrate skip (`mission_30b36f10-…`) noted so it is not mistaken for a regression.

**R.5 Rollback.** Redeploy `pulseforge-leadgen` deployment `88837730-a603-48b8-bca5-2f5b3c183ef4`
(`f129c74`) and `max-decay-daily` deployment `002d91c2-c237-450b-bf63-6030557a373f` (`f129c74`)
in Railway. No data rollback: the new JSON fields are ignored by the previous code. After a
rollback, approvals created by this release remain valid under the previous revision-only
check, and chat/API approvals are no longer review-bound. Hold execution approvals until re-deploy.

---

## 0. Final production-readiness review (2026-11-06)

Read-only review against PR HEAD `5b2db0c` (plus a test-only commit that adds
one PostgreSQL regression test). No deploy, merge, database write or
configuration change was made.

### 0.1 Proven

| Item | Evidence |
|---|---|
| PR is current with `main` | merge-base = `main` tip `43427e7`; GitHub `mergeable_state = clean` |
| CI on `5b2db0c` | 15 check runs: all required green (PR CI gate, Max orchestration, Global startup smoke, Anchor governed outbound safety, Revenue PostgreSQL, Paige, Signal, Decision shadow, identity-workflow, portal); 4 path-gated jobs skipped by the planner |
| Local, Node 22 + PostgreSQL 16 (non-root) | `test:max` 1700/1700; `test:max:unit` 1681/1681; `test:max:expectations:postgres` 43/43 (44 with the new test); `test:max:attention:postgres` 33/33; AO + Anchor review PostgreSQL 5/5 |
| `test:amo` | 498/544 on the PR **and** 498/544 on `main`, with the same 46 failing suites (stale fixtures: unhandled `ALTER TABLE` in the memory pool, a pending-decision assertion). The failures predate the PR, and no workflow runs `test:amo` |
| Production DB identity | Railway project `charming-trust`, service `pulseforge-leadgen` (production) has no database service in its own project. Its network-flow log shows its only database egress goes to `66.33.22.231:43478`. Its DNS log resolves `shinkansen.proxy.rlwy.net` → `66.33.22.231`. Port `43478` is the TCP proxy of service `Postgres` in project `amused-harmony`, the only Postgres in the workspace with a public proxy. **The production database is `amused-harmony/Postgres` via `shinkansen.proxy.rlwy.net:43478`.** Credentials were not read |
| Production SHA (rollback target) | deployment `c773084a-a679-4402-91a1-b5620737b2d7` = `43427e7` (`canRollback: true`) |
| **Merge = deploy** | the service tracks `main` with `checkSuites: false`; #1069/#1070/#1071 each started a production deployment 2–3 s after the merge commit. Merging PR #1065 deploys it immediately, without waiting for CI |
| No new schema | the PR adds no migration and no startup DDL. Its only new production SQL is `SELECT tenant_id FROM acquisition_missions WHERE id = $1` (and the same query with `AND tenant_id = $2`), which uses the verified `id`/`tenant_id` columns |
| Places spend enforcement unchanged | the PR touches no Places, `apiSpendControl` or pause code. Fixture fallback now only replaces live results when a caller opts in, and it never adds Places calls (fallback runs after the live attempt) |

### 0.2 Schema compatibility

| Dependency | Status |
|---|---|
| `acquisition_missions` (id PK, tenant_id NOT NULL, payload JSONB NOT NULL); PR's id/tenant reads; payload carrying `authorizationConstraints` / `syntheticEvidence` | **Confirmed compatible** (operator inspection + code). Legacy payloads without the new fields mean "no restrictions" |
| Upsert `ON CONFLICT (id) DO UPDATE` (pre-existing) | Compatible. It does not update `tenant_id`, but it does overwrite `payload`. Cross-tenant ids are refused before the upsert (new PostgreSQL regression test). See §1.3 #10 |
| Other `acquisition_mission_*` tables, `missions`, `mission_audit_events` | **Unverified**, but created by `CREATE TABLE/INDEX IF NOT EXISTS` at runtime. **Behaviour change:** a failed generic mission-store init (`ensureMissionSchema`) now fails closed instead of using memory. No `Postgres store failed — using in-memory` line appears in production logs since 2026-10-30. That is consistent with a working init, but it is not proof (init is lazy) |
| AO CRM `prospects` columns | Operator confirmed `ao_current_status`, `ao_call_suppressed`, `ao_outreach_review_required`. `ao_next_action`, `ao_disqualification_reason`, `ao_paused`, `ao_last_touch_at` etc. are **unverified**. The PR adds no AO query. Production already runs the same AO code at `43427e7`, and no "column … does not exist" error appears in today's logs |
| `content_recommendations`, `max_spreadsheet_contacts` | **Confirmed absent / not applied** (production logs: `relation "content_recommendations" does not exist`, command deck). Pre-existing, from unapplied operator migrations (`2026-08-16-specialist-direction.sql`, `2026-10-07-max-spreadsheet-reliability.sql`). The PR does not reference them, so this neither blocks nor is fixed by the PR |
| Migration fingerprints | unchanged (no migration files in the diff) |

### 0.3 Places spending control (traced)
- Every Places HTTP call goes through `utils/placesApi.js#guardedAttempt`, which calls `reserveApiCall`. That takes a provider advisory lock and checks the `api_spend_policies` row for `google_places`.
- **Global, not per tenant.** The daily and monthly USD and request limits apply across all tenants. The Tenant 10 research pause (`ANCHOR_RESEARCH_PROVIDERS_PAUSED`) and the research-only grant meter apply to tenant 10 only, so an acceptance tenant is limited by the provider-wide row alone.
- **Fail-closed when configured.** A reservation error throws before any network call. If a limit is set, an unknown cost is refused (`api_cost_unknown`).
- **Fail-open when absent:** `enforcePolicy(null)` returns. **With no `google_places` row, there is no limit.**
- The operator's "budget" table search would not match `api_spend_policies`. `api_usage_events` exists, and it is created by the same migration file, so the policy table almost certainly exists. **Its row is unverified** (acceptance §1.3 query).

### 0.4 Production observations (read-only logs, not PR-caused)
- Anthropic access, chronologically (historical errors are not evidence of a current outage):
  - 2026-11-05 and 2026-11-06, 12:00 UTC Paige/Mira jobs: `401 invalid x-api-key` (credential).
  - 2026-11-07, 12:00 UTC: `400` credit balance too low (credential accepted, billing refused).
  - Afterwards, the operator replenished credits and a direct production-console Anthropic request returned
    HTTP 200 (operator-reported). This establishes credential **and** billing access at the time of that test.
  - It does not establish Max's complete model path (workspace → presentation → model → response). That
    remains acceptance gate 1: one authenticated Max workspace turn with `metadata.modelCompletion.ok === true`.
    A failed turn now reports a sanitized code (`provider_auth`, `billing_unavailable`, `rate_limited`, …).
- `[amo] hydrate skip inconsistent mission mission_30b36f10-…` (prepared outreach ready, execution approval not pending). The PR keeps skipping it; it does not fail the tenant's hydrate.

---

## 1. Final review — findings

### 1.1 Blocking defects (found in release review, fixed in `aeaced5`)

| # | Defect | Impact | Fix |
|---|---|---|---|
| B1 | Amendment endpoint accepted **impersonated** sessions | An admin impersonating another user could lift restrictions; breaks the established rule (Clarity, Anchor CRM preparation) | 403 before any step-up is consumed |
| B2 | **Mission id overwrite** via `POST /api/v1/amo/missions` (`...req.body` passthrough, pre-existing) | Re-creating with an existing id replaced the mission (restrictions dropped — full bypass of amendment controls) or moved it to the caller's tenant; durable upsert overwrote the payload cross-tenant | `Engine.create` refuses existing ids; `runtime.create` checks durable ids across tenants; REST create uses an allowlist (also closes `planApproved` passthrough) |
| B3 | Restrictions **inferred from objective text** in the engine core | Governed daily missions copy the approved program objective; phrasing like "no prior contact" could silently halt governed Anchor outbound | Restrictions only from operator words at the Max handoff and REST create |
| B4 | Post-commit failure reported as **"not created"** | False failure; retry with "create a new mission" could duplicate | Durable row check: committed → return mission; unconfirmed → outcome `unknown`, check-before-retry |

Each has a regression test that fails on the pre-fix code
(`packages/max/workspace/tests/p0DeploymentReadiness.test.js`).

### 1.2 Reviewed, no defect found
- **Tenant isolation:** engine reads are tenant-scoped (`amo_tenant_mismatch`); amendment, preview, router and TME all read through them; REST tenant comes from the session, never the body.
- **Authorization bypass:** restrictions enforced in `routeExecutionRequest` (pre-dispatch, audited) and inside `executeMissionStage` (every stage, incl. autonomous progression); no Max code path reaches `AuthorizationAmendment`; amendment requires admin/manager, loosening requires admin + single-use SQL-consumed step-up, bound to the reviewed hash (re-checked in-transaction), durable lock held via `pool`.
- **Synthetic data:** stamped in the same commit as the stage (rolled back with it); refused outreach (router + TME), CRM admission (before any query) and learning; governed production paths pin `allowFixtureFallback: false`; fixture fallback opt-in only.
- **Database init / restart:** mission engine fails closed, failed boot not cached (retries next call); AMO hydrate throws on load failure; restrictions, amendments and synthetic stamps survive restart (real PostgreSQL tests).
- **Duplicate execution:** amendment idempotent by `amendmentId`; outbound replay `already_executed`; create rollback on failed write; id collision refused.

### 1.3 Nonblocking improvements (post-release backlog)
1. Amendment replay with the same `amendmentId` but a **different** change returns the original (`alreadyApplied`) instead of a conflict error.
2. Casual phrasing ("don't email anyone *yet*") on a resumed mission creates a **permanent** restriction; only an admin amendment lifts it. Restriction language inside a lift request ("lift the review-only restriction") re-tightens (fail-closed but surprising).
3. Legacy `/api/max/ask` returns 500 for all questions while mission persistence is unavailable (previously silent in-memory). Auth sessions also need PostgreSQL, so impact is bounded.
4. A persistently failing mission-schema init retries DDL on every request (no backoff).
5. Evidence classification trusts handler-supplied `transactionId` / `providerMessageId`; provider acceptance ≠ delivery.
6. No recipient allowlist exists: acceptance S8 can only run when the bound queue holds exactly the test recipient (see acceptance doc).
7. Places spend cap is provider-wide (`api_spend_policies`), not per tenant.
8. Restriction / fallback detection is lexical.
9. Amendments have no UI (REST only).
10. `persistMission` upserts with `ON CONFLICT (id) DO UPDATE` and has no tenant predicate. Today the create path refuses cross-tenant ids first. Defence in depth: add `WHERE acquisition_missions.tenant_id = EXCLUDED.tenant_id` and treat 0 rows as a conflict. This needs separate approval because it changes the shared write path.
11. One production mission (`mission_30b36f10-…`) is skipped on every hydrate as inconsistent. Repair it through the governed path; never by hand-editing SQL.

---

## 2. GO / NO-GO

> **Merge and deploy are one event.** The production service auto-deploys
> `main` without waiting for CI (§0.1). Therefore every "GO for deploy" item
> below must hold **before** the merge, unless an operator first turns off
> auto-deploy for `pulseforge-leadgen`. Turning it off is a production
> configuration change and needs its own authorization.

**GO for merge** only when all hold:
- [ ] All required GitHub checks green on the exact HEAD being merged (PR CI gate = success).
- [ ] Human code review approval of PR #1065, including §1.1 fixes.
- [ ] No unresolved blocking findings.
- [ ] Every "GO for deploy" item below holds (merge deploys).

**GO for deploy** only when all hold:
- [ ] Deploy of that SHA authorized (A1).
- [ ] Read-only schema check (acceptance §1.2) passed: mission tables present, and the full AO CRM column set present (required by #1064, already on `main`).
- [ ] Places spend policy row recorded (acceptance §1.3), with a `daily_usd_limit` low enough to serve as the A3 cap, or the procedural cap accepted in writing.
- [ ] Anthropic key failure (§0.4) understood: either fixed by the operator, or Max workspace turns shown to work without it.
- [ ] Governed Anchor outbound state recorded (last 3 days of sends) for post-deploy comparison.
- [x] Rollback target recorded: deployment `c773084a-a679-4402-91a1-b5620737b2d7` = `43427e7`.

**NO-GO** if any:
- Any required check red or pending on the HEAD to be merged.
- Schema check shows any AO CRM column from `migrations/2026-09-28-ao-crm-001.sql` missing (then #1064, already on `main`, is itself broken in production; fix that first).
- Mission tables missing (fail-closed boot would disable missions).
- Any production mutation would be required to deploy (none is expected: this PR adds no migration).

**P0 closure** (separate decision): acceptance S1–S7 passed in production;
S8 passed or explicitly waived; S9 passed or recorded not reachable.

---

## 3. Deploy (authorized operator only)
1. Merge PR #1065 (squash or merge per repo convention).
2. Railway deploys the merged SHA (no variable or configuration change).
3. Confirm `GET /api/status` = 200 and no `failing closed` log line within 5 minutes.

## 4. Rollback
Trigger on any §5 rollback condition.
1. Redeploy the recorded previous production SHA in Railway (no config change).
2. Data compatibility — **no data rollback needed**:
   - No migrations; startup DDL unchanged.
   - New mission payload fields (`authorizationConstraints`, `syntheticEvidence`) and operator `constraints` contributions are additive JSON; previous code ignores them.
   - Test-only fixture changes have no production effect.
3. **Caveat:** after rollback, restrictions and synthetic stamps are no longer
   enforced (previous code ignores them). Until re-deploy, keep outreach on
   review-only missions under manual control and do not approve execution on
   missions that showed `syntheticEvidence`.
4. Record the rollback and the triggering evidence on PR #1065.

## 5. Post-deployment monitoring (first 24 h, then daily for 7 days)

| Signal | Source | Expected | Rollback trigger |
|---|---|---|---|
| `[missionRuntime] mission persistence unavailable — failing closed` | server log | none | any occurrence not explained by a DB incident |
| `[amo] hydrate failed — durable mission state unavailable` | server log | none | repeated (>3/h) |
| `[amo] mission committed; follow-on durable write failed` | server log | rare | sustained |
| 5xx on `/api/v1/max/workspace/ask`, `/api/max/ask`, `/api/v1/amo/*` | HTTP metrics | ≤ pre-deploy baseline | sustained increase |
| Governed Anchor daily outbound sends | `acquisition_outbound_events` / mailbox | same pattern as last 3 days | sends stop with `authorization_constraint_blocked` or `synthetic_evidence_blocked` |
| `[CER_EXECUTION_AUDIT]` outcomes `blocked_by_constraint`, `blocked_synthetic_evidence` | server log | only on missions known to be restricted / synthetic | any on a governed mission |
| `executionState = outcome_unknown` share | router audit / REST responses | low | sudden rise |
| "Not executed" turn rate | Max responses (`metadata.routingFallback`) | low; each traceable | operators report legitimate commands refused |
| Places spend | `api_usage_events` | ≤ baseline | unexpected increase |
| `amo_mission_exists` (409) | logs | ~0 | appears from UI flows (would indicate a client sending ids) |

## 6. Sign-off record
| Item | Value | Who / when |
|---|---|---|
| Release HEAD SHA | | |
| GitHub checks | | |
| Code review approval | | |
| Schema check result | | |
| Previous production SHA (rollback target) | `f129c74`: `pulseforge-leadgen` deployment `88837730-a603-48b8-bca5-2f5b3c183ef4`, `max-decay-daily` deployment `002d91c2-c237-450b-bf63-6030557a373f` (superseded `43427e7` / `c773084a-…`) | Railway metadata, 2026-11-07 |
| Production database | `amused-harmony/Postgres` via `shinkansen.proxy.rlwy.net:43478` (§0.1) | Railway network-flow + DNS logs, 2026-11-06 |
| Deploy authorization (A1) | | |
