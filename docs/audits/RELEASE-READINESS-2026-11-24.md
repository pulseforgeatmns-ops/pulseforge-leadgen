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

## 6. Remaining blockers for a secure invite this week

1. Approve, merge and deploy #1159 and `fix/levi-pilot-policy`.
2. Run the production pre-invite SQL (`docs/audits/levi-pilot-preinvite.sql`). It must pass, including `legacy_pin_set = false`.
3. Create the account and run the login smoke test (`docs/audits/LEVI-PILOT-LOGIN-SMOKE-TEST.md`), with the passphrase delivered out of band.

Residual risks, not blockers:
* There is no platform-wide login throttling.
* The minimum password length is 8; mitigate with a generated passphrase of 20 or more characters.
* The R8 impact query has not been run.

## Recommendations

* **Reliability release (#1159): GO, conditional.**
  * Conditions: the R8 impact query is run and accepted before deploy.
  * CI is green.
  * The R2 index migration is a separate approval.
* **Levi prospecting-only pilot: GO once `fix/levi-pilot-policy` is deployed and the pre-invite SQL and smoke test pass.** NO-GO before the policy is deployed.
