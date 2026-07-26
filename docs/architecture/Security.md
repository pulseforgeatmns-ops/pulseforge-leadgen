# Security

## AuthN / AuthZ

- Session auth (express-session + Postgres store)
- Roles: `admin`, `manager`, `viewer`, `setter`, `closer`, `sales` (+ client-scoped roles where applicable)
- `requireAuth` / `requireRole` gate protected routes
- Fallback `DASHBOARD_PASSWORD` only if users table empty (legacy safety)

## Secrets

Never commit:

- `.env`
- `gmail_credentials.json` / `gmail_token.json`
- `facebook_session.json` / `linkedin_session.json`
- API keys, intake secrets, OAuth refresh tokens

Railway injects secrets as env vars. Scrub secrets from logs, inquiry events, and API responses.

## Cron and webhooks

- `/cron/*` requires `CRON_SECRET` (staging/production: non-empty required at boot)
- Webhooks verify provider authenticity where implemented; treat payloads as untrusted input
- Inquiry intake: timing-safe secret check; identical `401` on failure modes

## Data protection rules

1. **Client scope** — no cross-tenant reads/writes on new paths; fail closed
2. **DNC** — honor `do_not_contact`
3. **Approval gates** — no silent public post or inquiry external send
4. **Shadow defaults** — Max mutating actions off by default
5. **PII minimization** — log summaries, not full secret-bearing payloads

## Deployment posture

See [Deployment.md](Deployment.md). Production does not auto-migrate or run Inquiry boot DDL.

## Reporting issues

Treat credential leaks as Sev-1: rotate keys, purge logs if needed, document in CHANGELOG/ops notes without republishing secrets.
