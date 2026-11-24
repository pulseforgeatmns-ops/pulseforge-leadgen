# Release readiness — PR #1159 and Levi (tenant 21) pilot — 2026-11-24

Scope: the reliability release (R1, R2 code, R8) and the Levi prospecting-only pilot login.
Nothing was merged, deployed, changed in production, sent, or run by Scout. No credentials were created.
Production database access is **unavailable** (the Render Postgres IP allowlist is empty; I re-checked today). Every production fact below is **UNVERIFIED**.

## 1. PR #1159 — CI and review

* Head `f94228e`; this report's commit follows. Base: `migration/render-current-main` (`2e5cc34`). No merge conflicts.
* CI at the time of writing:
  * **14 checks success**, among them `levi-scout`, `identity-workflow`, `portal`, Global startup smoke, Revenue PostgreSQL, Anchor canonical outbound recovery, Walkthrough notification safety and Signal V1 core.
  * 1 skipped by the planner (Max orchestration).
  * 1 still running (Anchor governed outbound safety).
* Reviews and comments: none yet.

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

## 5. Blockers for a secure invite this week

1. **Levi could trigger paid Scout discovery himself.** This was found by code review and is not proven end to end.
   * The research-provider pause (`utils/anchorResearchProviderPause.js`) applies only to tenant 10.
   * No `enabled_agents` check was found on the mission discovery path.
   * So if tenant 21's AIM is published, Levi, as a client, could approve a discovery stage and cause Places/SerpAPI/Prospeo calls. That would violate "no paid providers / no Scout without approval".
   * Proposed fix, which needs approval: extend the provider pause to an explicit tenant list including 21, or enforce a research-only authorization constraint on tenant-21 missions. Then add a disposable test proving that a client-approved discovery makes no provider call.
2. **Production pre-invite facts are unverified.** Run `docs/audits/levi-pilot-preinvite.sql` (read only). It must show:
   * `clients.active = true`, with `enabled_agents` limited to `max`;
   * **no legacy PIN** on tenant 21. The `/client/:id` 4-digit PIN portal has no rate limit and would be a weak parallel path to tenant-21 prospect data;
   * no tenant-21 sending identity, scheduled sends, outbound items or send plans;
   * 29 prospects, all held, 0 setter-visible, 0 contacted;
   * exactly one claim for `levi-week-2026-11-23`;
   * the existing tenant-21 accounts by role (to avoid duplicates).

Not blockers, but should be fixed or decided:
* There is no login throttling or lockout platform-wide. This predates this work and affects every account. Mitigate with a long generated temporary passphrase and a forced change at first login.
* The minimum password length is 8.
* The scheduled-send tenant-mismatch error returns 500 instead of 4xx.

## Recommendations

* **Reliability release (R1, R2 code, R8): GO, conditional.**
  * Conditions: the last CI job passes, and the R8 impact query is run and accepted before deploy.
  * The R2 index migration remains a separate approval, after the precheck.
* **Levi prospecting-only pilot: NO-GO this week as things stand.**
  * It becomes GO when blocker 1 is fixed (or tenant 21's AIM is confirmed unpublished, so Max and missions stay locked) and the pre-invite SQL passes, with a strong temporary passphrase delivered out of band.
  * No code-level tenant-isolation or outbound defect was found.
