# Identity-only Anchor tenant selection

This is an opt-in continuation of the existing identity-check login, not a new
login or grant. The only supported target is existing tenant 10. Existing login,
dashboards, tenant switchers, cookie/security configuration and roles are unchanged.

## Intended operator procedure

1. Verify the reviewed deployment and existing auth readiness. Use the previously
   approved `/login?workflow=identity-check` flow only if a new login is needed.
2. In the dedicated quiet browser context, read `/api/me`. Require an actual
   authenticated admin, no impersonation and no pending password change. Close
   other Pulseforge tabs and do not open a dashboard in this workflow.
3. Open `/identity-check/tenant` directly, with no query string. The page displays
   current tenant and the database name of fixed target 10, plus an explicit
   selection button and a Cancel link back to `/api/me`.
4. After approval covering the existing-session selection, click the real native
   submit button. The ordinary form submits `workflow=identity-tenant-check`,
   `client_id=10`, `confirm=select-anchor` and a hidden CSRF token. Do not expose,
   log, reconstruct or transfer that token or the session cookie.
5. Success is a fixed 303 to `/api/me`, with no query selectors. Read the returned
   account information and require `active_client_id === 10`, the expected actual
   admin and no impersonation before the approved tenant-scoped proof. `/api/me`
   is a session projection, not current database authorization: the operator queue
   must still execute its existing fresh database user/tenant checks.

The form uses no scripts, assets or automatic API calls. Its same-origin referrer
policy preserves the browser's native POST Origin while withholding cross-origin
referrers; no-referrer would incorrectly produce a null Origin. Cancel does not select a
tenant. Refresh or another GET replaces the prior form challenge; browser Back or
repeated submission is not an authorization to retry. The challenge expires at
five minutes or the session's existing expiry, whichever comes first.

## Exact effects and boundaries

- The router is mounted immediately after native `express-session`, before global
  body parsing, CORS and legacy active-client normalization. It does not call
  `requireAuth`, which has legacy-user synthesis and impersonation repair behavior.
  Instead it requires a real positive session user ID, authenticated admin snapshot,
  no impersonation state and fresh database validation of the same active admin.
- Both GET and POST lock the existing session row and read only the current user
  and fixed target client. Missing/expired sessions are never inserted or revived.
  Current password-change, verification, role and active-state denials stay intact.
- Target validation uses `id=10 AND active=true AND
  COALESCE(operator_switchable,true)=true`, matching existing NULL semantics.
  Missing schema columns fail closed; there is no permissive schema fallback.
- GET writes only a random-token digest and bounded identity/context snapshot in
  `session.sess.identityTenantSelection`. It does not change the active tenant.
- POST requires exact configured canonical Origin, matching Host, a bounded native
  URL-encoded form, exact fixed workflow/target/confirmation and the session-bound
  CSRF token. User/tenant IDs, role grants and redirect URLs cannot be supplied.
  Session and fresh database client bindings must still match the form snapshot.
- A row-locked transaction rechecks expiry after lock acquisition and again at
  update. Current user/client rows are held `FOR SHARE` through commit to prevent
  concurrent role/active changes within that operation. Lock waits and statements
  have local 3-second/5-second limits; these are not global database settings.
- POST atomically consumes the challenge and sets only `session.sess.active_client_id`
  to 10. Unrelated session JSON and the existing session expiry remain unchanged.
  No credentials, user/client rows, access grants, schema, tenant workspace, CRM,
  agent, outbound, audit-publisher or billing records are changed.
- The router sets `req.session = null` for every handled response. Under the
  existing express-session `unset=keep` default this prevents stale automatic
  save/touch and Set-Cookie, not logout or session deletion. This dependency is
  source-tested; changing `unset` would require revisiting this route.
- Existing session-store reads can still arm normal expired-session pruning.
  The final `/api/me` read retains normal session expiry maintenance. Existing
  process startup initializers are neither invoked nor disabled by this route.

## Failure, retry and concurrency

All known precommit failures roll back and never redirect success. Missing or
mismatched Origin/Host, malformed forms, inactive/missing/hidden target, demotion,
impersonation, changed bindings, expiry and replay fail closed. The response never
logs the body, token, cookie, session ID, or raw database error.

A COMMIT acknowledgement/network error is reported as `selection_outcome_unknown`
with HTTP503. It may have committed: read `/api/me` before doing anything else,
never automatically resubmit and never claim the session was unchanged. A browser
navigation timeout is likewise uncertain. Opening a new form is an explicit
new session-challenge write, not a read-only retry.

**Route-scoped guarantee, not global session hardening:** concurrent submissions
through independent instances of this router serialize and cannot consume the
same currently stored challenge twice. However, existing unrelated routes use
whole-session UPSERTs and can overwrite a later tenant/nonce with a stale snapshot.
This patch intentionally does not replace the global session store, add a sidecar
session/challenge record, or claim to prevent that baseline race. A regression test
reproduces the limitation. Use the quiet dedicated flow and verify active tenant10
immediately before the fresh-authorized proof. If other concurrent session writers
cannot be excluded, stop rather than broaden the proof or add architecture changes.

## Canonical origin deployment prerequisite

The existing `APP_URL` must parse as an HTTPS canonical origin and match the browser
origin and Host. HTTP is accepted only for loopback development. Neither request
Host nor forwarded headers can choose the authorized origin; missing/null/wrong
Origin cannot bypass POST checks. Missing/invalid configuration or alternate host
fails closed. This patch does not change APP_URL, proxy trust, cookies or grants.

At preparation time the successful browser origin was
`https://pulseforge-leadgen-production.up.railway.app`; the existing runtime APP_URL
match has **not** been verified. Runtime inspection was blocked by a provider access
denial. Do not use another route/account/worker to bypass that denial. Keep the
runtime prerequisite explicitly unverified until an authorized permitted check is
available, and do not change production configuration to make the form pass without
separate authorization. There has been no live tenant selection with this patch.

## Verification

`node --test test/identityTenantSelection.test.js test/loginIdentityWorkflow.test.js
 test/tenantSelectorSync.test.js test/tenantWorkspace.test.js
 test/maxAttentionAuthorization.test.js test/maxAoHelpReviewGuards.test.js`

The new suite uses actual disposable loopback PostgreSQL, native express-session
cookies and the pinned connect-pg-simple store. It does not import the application
server, `db.js`, credentials, providers or startup workers. It covers failure and
uncertain-commit behavior, two-router concurrency, session/nonce expiry during lock
waits, strict selectors/origin/CSRF, unchanged expiry and absence of store saves.
The existing login tests cover default role redirects and guards unchanged.

Local native-browser validation of this new form is not yet established: Chromium
startup hit the environment's Unix-socket restriction before any page ran. A
three-case browser fixture is authored for normal submit/Origin, Cancel, and DB
demotion; the dedicated CI runs it and the existing login browser fixtures with
the repository's established Chrome setup. No restriction was bypassed locally. Real HTTP
form/session tests are not a claim of browser validation. The dedicated Identity
workflow safety CI runs the focused and adjacent suites on PRs and main; release
requires those and all other required/selected checks to pass on the exact commit.
Publication, merge, deployment and live selection each remain within the parent's
current authorization/scope check; this document itself grants none.
