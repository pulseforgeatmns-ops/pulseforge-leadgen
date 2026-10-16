# Isolated authenticated synthetic portal

Prepared locally only. No service, database, credentials, deployment, customer access, or production changes are authorized or performed by this artifact. Do not host `scripts/demoExeterPortalLocal.js` or its test harness.

## Isolation boundary

This entry point imports only Express, express-session, connect-pg-simple, bcryptjs, pg, Node built-ins, and injected portal routes/service/schema/image validation. It never imports the main server, shared db singleton, auth/bootstrap module, agent workers, Signal, mail, AI, payment, or outreach providers. Only two portal assets are served; no repository-wide static hosting. Runtime startup only validates schema/data and reads sessions. The separate initializer is the only schema/fixture creator.

The included Dockerfile copies only this runtime and its explicit dependency closure. Its independent 89-package lock is derived from the reviewed repository lock; `npm ci --offline --dry-run` validates lock consistency. Full dependency installation/image build still requires an approved build environment. It does not run root npm scripts, main server, or root Railway configuration.

## Explicit configuration

Every setting is required; no credential defaults or fallbacks:

- PORTAL_MODE: `local` (127.0.0.1 listener) or `hosted` (0.0.0.0 behind trusted HTTPS ingress).
- PORT: selected listening port, supplied by hosting platform for hosted mode.
- PORTAL_ORIGIN: canonical origin only. Local must be `http://127.0.0.1:<PORT>`; hosted must be HTTPS.
- PORTAL_DATABASE_URL: dedicated PostgreSQL database named exactly `anchor_portal_synthetic`. No query parameters or fallback to DATABASE_URL. Local host must be 127.0.0.1. Hosted mode requires database authentication, a DNS hostname and verified TLS; IP-literal hosts are refused.
- PORTAL_SESSION_SECRET: independently provisioned random secret, at least 32 random bytes encoded as hex.
- PORTAL_DATABASE_CA: optional authentic public CA PEM for the dedicated database. Verified TLS remains mandatory; no private keys, hostname override or insecure fallback.
- PORTAL_SYNTHETIC_MARKER: `anchor-portal-synthetic-only-v1`.

Main-app database/auth/provider variables, generic credential variables and unknown PORTAL_* variables are rejected. Never copy production environment variables. Do not put secrets in commands, repository files, logs, chat or this README. HTTPS database connectivity must be established without disabling certificate verification.

## Provisioning gates

Before any deployment: obtain explicit approval for service/database creation, costs and exact target. Real credential/secret creation is a separate secure user handoff; no real credentials were generated here.

Provisioning is split into two explicit commands:

1. `node standalone/anchor-portal/initialize.js --schema-only` requires the exact marker, named database, public schema and an entirely empty database. It creates schema only, with zero account and location rows. The runtime refuses to listen with `Portal setup pending: approved unexpired demo accounts required.` This is infrastructure preparation, not a login-ready demo.
2. Only after explicit approval to create the two accounts, `node standalone/anchor-portal/initialize.js --provision-approved-demo` reads secure stdin JSON containing `hashes` (client and operator bcrypt cost-12 hashes) and `expires_at` (an explicit UTC timestamp in the approved hosting window, at most 48 hours from provisioning). It requires untouched schema-only state, creates exactly those two accounts and one labelled synthetic building with sample reports/issues/photos, and commits atomically. No plaintext passwords, generated defaults, foreign-tenant or attack-test accounts are accepted by this command.

The proposed accounts are client@exeter-fixture.invalid (facility_client, assigned to the single demo building) and operator@exeter-fixture.invalid (manager, synthetic tenant 10 only). No admin or cleaner account is required. Approval of infrastructure, database access or a session secret is not approval to create these accounts. Distinct account secrets must be supplied through a secure user provisioning step; do not put them in commands, logs, files, chat or this README.

The explicit window is checked before every HTTP request; expired deployments reject reads, login and writes. Account validity is checked at login and every authenticated request, and sessions cannot outlive the account deadline. Startup also refuses schema-only, missing, expired or invalid account state. A new window is not auto-created; reprovisioning or renewal requires separate approval. Infrastructure cleanup is a separately tracked operation; expiry blocks access but does not stop hosting charges.

Runtime performs no migration/seeding. Use a dedicated least-privilege runtime database role (portal reads/writes and session operations only), separate from the initializer's DDL role. Schema-only setup does not create database roles. Runtime validates marker, table inventory, additive upgrade fields, synthetic identities/location flags and readiness. No real customer or student data belongs here.

## Trusted Railway database certificates

The official PostgreSQL template creates deployment-specific certificates, not a universal public Railway CA. Current source stores public root.crt and server.crt under /var/lib/postgresql/data/certs and includes localhost plus RAILWAY_PRIVATE_DOMAIN in the server SAN. Source is not proof of a deployed certificate:
https://raw.githubusercontent.com/railwayapp-templates/postgres-ssl/main/init-ssl.sh

Through authenticated access to the exact approved demo deployment, retrieve only root.crt and server.crt using supported Railway SSH/read-only deployed-file access. Never retrieve root.key or server.key. Registering a new SSH key or creating ongoing access needs explicit approval. Record the deployment identity and public certificate fingerprint, verify chain and validity, and confirm the intended private database hostname appears in the actual server SAN. Put the authentic root PEM in PORTAL_DATABASE_CA. Node/pg still validates the live chain and actual connection hostname. Do not use the public proxy hostname if absent from SAN, substitute localhost, or disable validation.

Supported Railway access: https://docs.railway.com/cli/ssh
Node hostname validation: https://nodejs.org/api/tls.html#tlscheckserveridentityhostname-cert

If authentic retrieval or hostname coverage cannot be established, hold deployment and report the blocker. A certificate reissue or new trust/access change requires approval. Provider renewal may rotate the root; treat a new root as a new provenance verification, never automatically trust an unknown replacement.

## Hosted review before use

Select the dedicated Railway config `standalone/anchor-portal/railway.json`; do not change root production config. Build context is repository root; Dockerfile is `standalone/anchor-portal/Dockerfile`.

The app trusts exactly one ingress proxy. Verify that this ingress overwrites X-Forwarded-* headers, enforces the canonical HTTPS hostname and blocks direct origin access. Its bounded in-process login limiter assumes one replica, as configured. Host/header tests cover the application, not the actual Railway ingress. Verify this property before exposing the app.

Sessions are PostgreSQL-backed, rotated at login, HTTP-only/SameSite Strict, secure in hosted mode, with absolute 30-minute expiry and expired-row cleanup every 15 minutes. Mutations, including login/logout, require matching canonical Origin and a per-session CSRF token. Logout uses a confirmation page and POST. User activity and permissions are re-read on each authenticated request. No test login endpoint exists in the runtime.

Use only synthetic records and original illustrative images. The fixture has three labelled reports, three zone sections, open and resolved problems and resolution proof. Capacity, memory/disk limits and sustained concurrency have not been load-tested. Do not claim the proposed 0.5 GB / 0.5 vCPU service/database sizing is proven.

## Verification

- `npm run test:anchor-portal` for service/security/UI-safety/acceptance/startup regression.
- `npm run test:anchor-portal:standalone` for real bcrypt auth, independent sessions, logout/expiry, inactive-user revocation, CSRF/origin/host checks, roles/location isolation, dedicated schema/marker guards, hosted proxy secure cookies and successful mutations, and report/photo/problem/resolution roundtrip.
- `npm run test:anchor-portal:standalone:browser` for real browser login, report/photo view and logout. Requires Chromium, PostgreSQL and permitted local sockets.
- Existing `npm run test:anchor-portal:browser` exercises desktop/mobile portal workflows.

Tests always create their own disposable local PostgreSQL; random test passwords remain in memory and are never printed. Browser requests are restricted to the test loopback origin plus data/blob images. Local API/auth/regression verification passed. Browser verification is currently blocked by the execution sandbox's Chromium socket restriction; this is not a browser pass. Dedicated Docker build and browser login/report-photo/logout passed CI on the original PR925 head; each correction must pass those gates again. Actual hosted ingress/database TLS and phone testing remain unverified deployment gates.

## Explicit public access for the existing Exeter demo

`PORTAL_PUBLIC_DEMO=enabled` removes password entry only for the existing isolated
Exeter deployment. It requires hosted mode, the exact reviewed origin and the three
`PORTAL_PUBLIC_DEMO_PROJECT_ID`, `PORTAL_PUBLIC_DEMO_ENVIRONMENT_ID`, and
`PORTAL_PUBLIC_DEMO_SERVICE_ID` pins in public-demo.js. The guarded launch wrapper
must derive these values only after checking the actual Railway project,
environment and service IDs. Do not forward caller-supplied assertions unchecked.
The runtime additionally checks the dedicated database, synthetic marker, exact
client/operator IDs, emails, roles, tenant and original 2026-10-16 17:36 UTC deadline.
No expiry extension, account creation, credential change or production auth change
is performed. Public access must be explicitly approved before enabling it.

Browser entry creates a normal expiring DB session for the existing demo client;
the header's Demo operator / Demo client button switches only between the two
existing fixture accounts using an origin/CSRF-checked POST that rotates session
and CSRF. The real portal routes, persistence, assets and tenant checks are retained.
Password login is disabled only in public mode. Existing non-public sessions are
not adopted, and public sessions are rejected when this mode is disabled. API and
CSRF requests do not create public sessions. Session issuance is bounded per source
(20/minute) and process-wide (120/minute), including role switches; ordinary view
reloads reuse valid sessions. Writes are bounded at 60/minute/source, 120/minute process-wide and 200 total
per process lifetime (including rejected application attempts). These limits reset
on process restart; they are an abuse backstop, not a persistent spending limit.
At the ceiling, reads remain available and writes return HTTP 429; do not restart
automatically to evade the bound. Maps expire
entries and are capped. Existing 30-minute session TTL remains bounded by the
original demo deadline. This assumes the existing single trusted ingress proxy.
