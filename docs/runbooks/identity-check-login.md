# Existing-account identity-check login

## Purpose and selection

The opt-in `/login?workflow=identity-check` page uses the same existing-account
password login as the normal sign-in form, but a successful login lands on fixed
`/api/me` JSON instead of opening the dashboard. The page renders a constant
hidden form field. Normal visible credential entry and native form submission
preserve that field; no browser script or credential extraction is needed.

The exact scalar `identity-check` is the only recognized workflow. It is a
navigation choice, not an authentication, role, workspace or tenant grant.
Arbitrary `returnTo`, `next`, URLs, objects, arrays and unknown workflow values
cannot specify a redirect. Absent or invalid workflows retain default behavior.

Invalid credentials, inactive/unverified users and missing client workspaces
still fail the existing checks. Their retry links preserve the selected workflow.
Required password changes still take precedence and lead to the existing change
password form. Do not submit a password change under identity-check authorization.
The identity-check flow rejects the legacy no-user login fallback; it requires a
genuine existing account. The ordinary fallback remains unchanged.

## Request and side-effect boundary

1. Optional anonymous readiness observation: request `/anchor-portal.html` with
   no cookies, `Accept: text/html`, and redirects disabled. A `302` to `/login`
   proves auth and portal initialization completed for that responding process.
   A `503` means readiness is pending or failed. This does not prove every replica
   is ready or cover a later restart. The request itself starts no initializer.
2. Open `/login?workflow=identity-check`. Its form only renders HTML and a
   password-visibility button; it fetches no application APIs. It has existing
   external font resources. It does not save an uninitialized anonymous session.
3. Submit ordinary credentials through the secure login mechanism. The existing
   cached auth initialization Promise is awaited, the existing account is read,
   all ordinary credential/account checks run, and last_login_at is updated.
4. On success, an ordinary session cookie and session record are created/updated.
   The fixed redirect has no query string and goes to `/api/me`. The response is
   noncacheable JSON with no frontend shell or scripts.
5. `/api/me` runs existing session auth and a clients SELECT, then projects
   identity and tenant display fields. It does not run schema initialization,
   business workers, provider calls, tenant selection or impersonation actions.

This is business-inert, not literally write-free. Existing session save/touch
behavior, 24-hour expiry, and normal expired-session pruning remain. Startup
initializers and already-running schedulers are independent of this workflow and
are not disabled by it. A cached auth initialization failure remains a failure;
this change neither retries nor bypasses it.

## Scope validation after login

Check `/api/me` without tenant selectors. Require the expected real account ID,
expected role, and expected client context. Stop on unexpected impersonation,
legacy identity, required password changes or mismatched identity. `/api/me`
reflects the session; it is not proof that the account's current database role or
active status has remained unchanged since login.

This flow deliberately does not select tenant 10 or any other tenant. Normal
admin login chooses `user.client_id || 1`; managers retain their account binding.
The Max attention operator queue independently reloads current authenticated and
effective actors, validates their active state and admin/manager roles, and binds
its tenant from session/admin or current manager binding. A query `client_id=10`
is an equality assertion, not a tenant switch. Never use `/api/me?client_id=10`
as proof that the authenticated session selected tenant 10.

Do not automatically call `/api/clients/active`, attention health, dashboard,
command deck or broader application endpoints under this narrow scope. Tenant
selection is a separate mutation and includes workspace loading. The operator
queue's default ten-row result also does not prove a specific item is absent from
the entire unresolved queue.

## Existing security behavior, not changed here

The main login currently does not implement a CSRF-token/Origin check or rotate
its session ID at authentication. It also preserves valid pre-existing
impersonation state. The workflow marker provides none of those protections and
must not be represented as a security fix. The patch does not change cookie,
credential, role, authorization, session-ID or impersonation policies. Address
any separate hardening in a separately reviewed and authorized change.

## Local verification

- `node --test test/loginIdentityWorkflow.test.js`
- `node --test test/browser/loginIdentityWorkflowBrowser.test.js`
- Adjacent regressions: tenantWorkspace, registrationWorkspace,
  maxAttentionAuthorization, maxAoHelpReviewGuards, aoAdminImpersonation,
  anchorPortalStartupSafety and clientExperienceConvergence.

The route harness executes the real login, auth and `/api/me` code against
synthetic credentials and query recorders, without importing the production
server or starting its pool/initializers/workers. The native-browser fixture
allows loopback traffic only and blocks external resources. Browser execution
requires a functioning local Chromium sandbox; a launch failure is not a passed
browser test.

Publication, deployment and live credential submission require the relevant
separate approvals and verification. No authentication, account grants or tenant
selection may be fabricated to bypass a blocked step.
