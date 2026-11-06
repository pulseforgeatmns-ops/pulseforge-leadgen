# P0 Max Reliability — Release Checklist (PR #1065)

Status: **release candidate, not authorized.** Companion to
`docs/SPEC-MAX-RELIABILITY-004.md` and `docs/P0-MAX-PRODUCTION-ACCEPTANCE.md`.

Base: `main` @ `43427e7`. The release candidate is the PR HEAD whose GitHub
checks are all green (record the exact SHA in §6 at sign-off).

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

---

## 2. GO / NO-GO

**GO for merge** only when all hold:
- [ ] All required GitHub checks green on the exact HEAD being merged (PR CI gate = success).
- [ ] Human code review approval of PR #1065, including §1.1 fixes.
- [ ] No unresolved blocking findings.

**GO for deploy** only when all hold:
- [ ] Merge done; deploy of that SHA authorized (A1).
- [ ] Read-only schema check (acceptance §1.2) passed: AO CRM columns present (required by #1064, already on `main`), mission tables present.
- [ ] Places spend policy recorded (acceptance §1.3).
- [ ] Governed Anchor outbound state recorded (last 3 days of sends) for post-deploy comparison.
- [ ] A rollback target SHA (current production) recorded.

**NO-GO** if any:
- Any required check red or pending on the HEAD to be merged.
- Schema check shows missing `ao_current_status` / `ao_next_action` / `ao_disqualification_reason` (then #1064 — already on `main` — is itself broken in production; fix that first).
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
| Previous production SHA (rollback target) | | |
| Deploy authorization (A1) | | |
