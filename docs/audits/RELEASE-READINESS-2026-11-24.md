# Release readiness — PR #1159 and Levi (tenant 21) pilot — 2026-11-24

Scope: the reliability release (R1, R2 code, R8) and the Levi prospecting-only pilot login.
Nothing was merged, deployed, changed in production, sent, or run by Scout. No credentials were created.
Production database access is **unavailable** (the Render Postgres IP allowlist is empty; I re-checked today). Every production fact below is **UNVERIFIED**.

## 1. PR #1159 — CI and review

* Head `bc2936c`: **all 15 checks pass**, including the PR CI gate (16 runs; Max orchestration was skipped by the planner).
* No merge conflicts, no reviews and no comments yet.
* Merge and deploy are pending approval.

## 2. R8 production impact query — UNVERIFIED

`docs/audits/setter-review-hold-impact.sql` (read only, aggregates per tenant) could not be run.

What the code establishes:
* Tenant 10 is unchanged.
* Tenant 21: the R8 test fixtures model all 29 rows as review-held. Research-only rows score 65; review-only `contact_now` imports score 80. None become setter-visible under R8.
* Any other tenant with held rows that are currently setter-visible would lose them from the setter queue at their next re-evaluation.

The per-tenant counts are unknown until an operator runs the query.

## 3. Deployment readiness

| Item | Code + tests | Production | Ready? |
|---|---|---|---|
| R1 merge-before-persist | Levi suite 115/115; real-path regression | not deployed | Yes, after merge approval |
| R2 advisory-lock claim (code) | 30-way concurrency: exactly 1 claim; lock-wait proven | not deployed | Yes, after merge approval |
| R2 unique index migration | guarded, idempotent, rollback tested | not applied | **Separate approval.** Run the precheck first. Not required for correctness. |
| R8 global review-hold gate | 5 disposable-PG tests (tenants 21, 10, 1); shared setter suites pass | impact UNVERIFIED | Yes, once the impact query has been run and accepted |

Pre-existing failures, identical on the unmodified base:
* `walkthroughRoutes` Anchor site copy;
* `setterPreMigrationCompat` DNC pre-migration check.

## 4. Levi tenant 21 login readiness

New evidence: `test/leviTenant21ClientAccessPostgres.test.js`, run against disposable PostgreSQL with the real route modules and auth middleware. It passed, as did the existing auth and isolation suites (login identity, session persistence, tenant workspace, CIE isolation, registration): **88/88**.

| Area | Finding | Evidence |
|---|---|---|
| Account model | Levi needs `role = 'client'`, `client_id = 21`. Created by an admin in `/admin/users` with a temporary password; `password_change_required = TRUE` and `email_verified = TRUE` are forced. A client user without a tenant is refused at login. **There is no tokenized invite link**: the temporary password must be delivered out of band. | `routes/users.js` POST `/api/users`; `server.js` login |
| Authentication | bcrypt cost 12. Session ID is rotated at sign-in. Opt-in 30-day "remember me". Sessions are revalidated against `users` every 30s. A temporary-password session gets `403 password_change_required` on data APIs. | existing tests; new test asserts the 403 |
| Tenant isolation | The client tenant comes only from `users.client_id`; `?client_id=` and body overrides are ignored. Prospect list, pipeline, detail and touchpoints never return tenant-1 rows. `/api/me` reports tenant 21. | new test |
| Prospect visibility | `/api/prospects` lists tenant-21 prospects where `do_not_contact = false`. Both seeded rows (research-only and review-only import) are visible. Levi lands on `/command-deck`. | new test |
| Closed surfaces | `POST /api/run/scout` and `/api/run/emmett` (agent runs), `/api/clients`, `/api/users`, setter and closer APIs, approval and pending-comment APIs: 401/403. `/setter`, `/closer` and `/admin/users` pages redirect away. No `dashboard_trigger` was logged. | new test |
| Outbound | A client *can* call `POST /api/v1/tenant-outreach/schedules/authorize` and the EOI plan endpoints. Scheduling requires a sending identity owned by tenant 21; without one it is refused (`sending_identity_tenant_mismatch`, returned as HTTP 500 — a cosmetic mapping bug). The executor also refuses any review-held prospect, and all tenant-21 rows are held. Brevo, Emmett, Sam and the governed paths already honour the hold for every tenant. | new test; `tenantOutreachScheduler.js` at 724–727 |
| Max / missions | Max is locked for a client unless the Blueprint is approved **and** the AIM is published (`pilotOnboarding.maxAcquisitionReply`). If unlocked, a client acts as operator on its own missions and can approve plan and discovery (`/api/v1/amo/*`). | code review |

## 5. Pilot policy (branch `fix/levi-pilot-policy`, stacked on #1159) — closes the paid-provider and execution blocker

`utils/tenantPilotPolicy.js` declares tenant 21 prospecting-only.
* The list is a code constant. It reads no database, AIM, `enabled_agents`, request parameter or session, so publishing an AIM or editing client config cannot lift it.
* The environment can only *add* pilot tenants.
* Unknown capabilities fail closed.

| Capability | Tenant 21 | Enforced at |
|---|---|---|
| View prospects, evidence, pipeline | allowed | unchanged read routes (tenant-pinned) |
| Free research (Levi Scout, public SODA) | allowed | its own operator-only cron and weekly clock; untouched |
| Paid research (Places, SerpAPI, Prospeo, Hunter, Apollo, phone enrichment, Scout) | **denied** | `assertResearchProviderAllowed` / `withResearchProviderScope` (now independent of the Anchor pause), `placesApi.guardedPlacesCall`, `lib/serpapi`, `enrichProspects`. `leadgen.run` is refused at its first line. |
| Outbound (email, SMS, calls, scheduled sends, EOI) | **denied** | `callEligibility.withCallAuthorization` / `assertCallAllowed` (shared by Brevo `sendEmail`, Sam, Cal and Cal Batch), `dbClient.checkDNC`, tenant outreach scheduler (authorize and execute eligibility), EOI plan/approve/acknowledge routes |
| Agent dispatch | `max` only | `isAgentEnabledForClient`, used by `/api/run/:agent` and `/cron/:agent`. Applies whatever `enabled_agents` says. Each refusal is audited as `dispatch_blocked`. |
| Mission execution | only clarify/cancel/edit/apply-edits/clarity review | `routeExecutionRequest`, checking request, context and the mission's own tenant; plus TME refuses Scout, Paige and Emmett stages |

Evidence (disposable PostgreSQL; every outbound HTTP, `fetch`, axios and socket recorded and refused):
* `tenantPilotPolicyPostgres` (5 tests) and `tenantPilotPolicyDispatchPostgres` (1):
  * decisions for tenant 21 versus tenants 1, 2, 5 and 10;
  * env cannot remove the pilot;
  * every paid-provider and sender chokepoint refuses tenant 21 with **zero network attempts**;
  * other tenants pass the gates;
  * router and TME refuse all dispatching intents, including when a tenant-21 mission is referenced under another tenant's request;
  * with `enabled_agents = {scout,emmett,sam,cal,…}` for tenant 21, dispatch and cron are still refused and audited;
  * `leadgen.run({client_id:21})` is refused;
  * `/cron/scout_levi` is not refused by the policy.
* `leviTenant21ClientAccessPostgres`: prospects stay visible; EOI and scheduling return `403 tenant_pilot_policy_denied`.
* The Levi suite, including the real free-research cycle for tenant 21: **121/121**.
* Every existing test touching the changed modules (91 files, 1,860 tests) has no new failures against the baseline. The remaining 6 failing tests (15 counting subtests) fail identically on `bc2936c`: Anchor fresh mission audit, SPEC-071, SPEC-186, SPEC-118 Max Ask, Anchor Pilot 0 route wiring, and the setter pre-migration DNC check.
* Sandboxed guard tests that pin import lists still hold. The policy reaches `callEligibility` through `anchorOutreachReview`, and the provider gate loads it lazily.

Not exercised end to end: the Max chat surface. It dispatches only through the router, which is covered. The smoke test (step 7) observes it in production.

## 6. Max chat cannot bypass the policy

`packages/max/workspace/tests/p0TenantPilotMaxChat.test.js` drives the real `WorkspaceEngine.ask()` (12 tests, hermetic, no network).

For tenant 21 with discovery pending, these phrasings each reach **zero** Scout calls, leave the mission state unchanged, and never report execution: "Approved. Begin discovery.", "Go ahead and run Scout", "Let's run it", "Let's proceed", "Go ahead and approve", "Go ahead and send the emails", "Let's launch", "Let's execute".
* When the router refuses, Max answers "Not executed: this workspace is a prospecting-only pilot…" with `executionState: blocked`, instead of erroring.
* Plan approval is refused and audited.
* A read question still gets an answer.

Controls: tenants 10 and 1 still dispatch discovery on "Go ahead and run Scout".

Side fix: `test/maxInvestigationAcceptancePostgres.test.js` seeded a tenant-21 mission at `execution_approval` by running Scout, Paige and Emmett stages, which the policy now refuses. That broke CI on #1160's first commit (`e592578`). The test now builds the stages under a synthetic tenant and stores the identical record as tenant 21. This models a legacy mission that was pending before the policy, and it still proves that operational reads win over a pending approval: 3/3.

Side fix: the semantic-generalization eval used a synthetic "Harbor Dental Group" fixture that happened to carry tenant id 21. It now uses synthetic id 27, with its baseline keys remapped and nothing else changed. The eval passes exactly as on the base. The full Max workspace and AMO suites (203 files, 2,910 tests) show no new failures; the same 44 fail on the base.

## 7. Deployment order (each numbered step needs explicit approval)

0. **Read only, before any merge** (an operator with a SELECT-only credential on `pulseforge_production`):
   * `docs/audits/setter-review-hold-impact.sql` (R8 impact per tenant). Review `held_visible_now` and `held_in_setter_pipeline`.
   * `docs/audits/levi-pilot-preinvite.sql`. Record a baseline.
   * `migrations/2026-11-24-levi-scout-cycle-claim-unique.precheck.sql` (R2). It must show 0 duplicate groups.
1. **Merge #1160 into `fix/levi-scout-p0-reliability`.** #1159 then carries R1, R2 code, R8 and the pilot policy.
2. **Merge #1159 into `migration/render-current-main`.** Auto-deploy is off, so nothing ships yet.
3. **Manual Render deploy** of `pulseforge-api-migration`. Note the previous live deploy, `dep-dc2bfvs9v7es73bb1ij0` (commit `0728ba2`), for rollback.
4. **Post-deploy checks:**
   * Startup log shows the Levi clock catch-up returning `replay_blocked` for `levi-week-2026-11-23`.
   * Run the tenant-21 audit: `node scripts/auditLeviTenant21PostPr1157.js --tenant 21 --confirm-read-only --expect-database=pulseforge_production`.
   * Smoke step 2 (admin: `POST /api/run/scout?client_id=21` returns `403 tenant_pilot_policy_denied`, and is audited).
5. **R2 index migration** (separate approval; optional for correctness): run the precheck, apply, re-run the audit (`cycle.claim_race_protection` PASS).
6. **Credential issuance point.** Create Levi's `client` account **only after all of these hold**:
   * step 4 passed, which proves the pilot policy is live in production;
   * the pre-invite SQL passes, including `legacy_pin_set = false`, no sending identity, 29 held / 0 visible / 0 contacted, and one weekly claim.

   R2 does not gate this. Deliver the generated passphrase out of band.
7. **Login smoke test with Levi** (`docs/audits/LEVI-PILOT-LOGIN-SMOKE-TEST.md`, steps 4 to 5), then re-run the pre-invite SQL.
8. Finish steps 1 to 4 before **Mon 2026-11-30 00:15Z**, the next weekly cycle, so R1 protects it.

## 8. Rollback plan

| Situation | Action | Data impact |
|---|---|---|
| Deploy unhealthy, or the post-deploy audit fails | Render dashboard, **Rollback** to `dep-dc2bfvs9v7es73bb1ij0`; or `git revert` the #1159 merge on `migration/render-current-main` and redeploy | None. No schema change ships with #1159 or #1160. |
| Pilot policy rolled back for any reason **after** Levi's account exists | **First** deactivate Levi's account in `/admin/users` (this revokes his sessions), **then** roll back | Without the policy a `client` could approve paid discovery |
| R8 causes an unwanted setter-queue change for a tenant | Roll back the deploy. Rows hidden by R8 return at their next handoff/recompute under the old gate. | `setter_visible` values flipped by recomputes after deploy stay until recomputed |
| R2 index problem | `psql -f migrations/2026-11-24-levi-scout-cycle-claim-unique.rollback.sql` (`DROP INDEX CONCURRENTLY`) | None. The claim stays safe through the advisory lock. |
| A weekly cycle fails after deploy | Do not replay. Inspect the `research_cycle_diagnostic` row. The next ISO week claims normally. | None |

## 9. Remaining blockers

* Approval to run steps 0 to 7. I have no production database access, so step 0 is UNVERIFIED until an operator runs it.
* Residual risks, not blockers:
  * no platform-wide login throttling;
  * minimum password length 8 (mitigate with a generated passphrase of 20 or more characters);
  * `/client/:id` PIN portal (closed by `legacy_pin_set = false`).

## Recommendations

* **Reliability release (#1159 with #1160 merged into it): GO**, conditional on step 0 being run and accepted before deploy. R2 migration: separate GO after its precheck.
* **Levi prospecting-only pilot: GO at step 6**, i.e. only after the policy is verified live in production and the pre-invite SQL passes. NO-GO before that.
