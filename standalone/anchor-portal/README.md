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
- PORTAL_DATABASE_URL: dedicated PostgreSQL database named exactly `anchor_portal_synthetic`. No query parameters or fallback to DATABASE_URL. Local host must be 127.0.0.1. Hosted mode requires database authentication and verified TLS.
- PORTAL_SESSION_SECRET: independently provisioned random secret, at least 32 random bytes encoded as hex.
- PORTAL_SYNTHETIC_MARKER: `anchor-portal-synthetic-only-v1`.

Main-app database/auth/provider variables, generic credential variables and unknown PORTAL_* variables are rejected. Never copy production environment variables. Do not put secrets in commands, repository files, logs, chat or this README. HTTPS database connectivity must be established without disabling certificate verification.

## Provisioning gates

Before any deployment: obtain explicit approval for service/database creation, costs and exact target. Real credential/secret creation is a separate secure user handoff; no real credentials were generated here.

The one-off `initialize.js --initialize-synthetic-only` reads a JSON object of six externally provisioned bcrypt cost-12 hashes from secure stdin: client, operator, cleaner, unassigned, foreignOperator, foreignClient. It accepts no plaintext passwords, generates no fallback passwords, and prints no hashes. Use distinct secrets for each account; do not reuse the shared ephemeral hash pattern used by automated tests.

It requires the exact marker, named database, public schema and an entirely empty database, creates only synthetic users/locations/reports/issues/illustrative images plus sessions, and commits all fixtures atomically. A repeat or unexpected existing data refuses rather than overwriting. Runtime refuses missing/extra tables, shadow tables, upgrade drift, non-demo locations and non-synthetic user domains. This is a demonstration, not a mechanism for importing or processing real customer/student data.

Run the runtime with `node standalone/anchor-portal/runtime.js` only after provisioning and marker/schema verification. It performs no migration/seeding at startup. Use a dedicated least-privilege runtime database role (portal reads/writes and session operations only), separate from the initializer's DDL role; permission/credential provisioning needs approval.

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

Tests always create their own disposable local PostgreSQL; random test passwords remain in memory and are never printed. Browser requests are restricted to the test loopback origin plus data/blob images. Local API/auth/regression verification passed. Browser verification is currently blocked by the execution sandbox's Chromium socket restriction; this is not a browser pass. Docker build and real hosted ingress/database TLS remain unverified deployment gates.
