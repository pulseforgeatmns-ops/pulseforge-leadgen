# Levi (tenant 21) prospecting-only pilot — operator pre-invite and login smoke test

Every step is for an operator, and each one needs explicit approval. Nothing here sends to a prospect, runs Scout, or changes prospect data. The only writes are creating Levi's user account (step 3) and the session and audit rows that sign-in and refusals record.

Prerequisites:
* PR #1159 and the pilot-policy branch (`fix/levi-pilot-policy`) are merged and deployed to the Render service.
* The Render deploy shows a commit that includes `utils/tenantPilotPolicy.js`.

## 1. Read-only pre-invite checks (production)

```
psql "$AUDIT_DATABASE_URL" -X -v ON_ERROR_STOP=0 -f docs/audits/levi-pilot-preinvite.sql
```

Go only if every line holds:

| Check | Required |
|---|---|
| `current_database()` | `pulseforge_production` |
| `clients` id 21 | `active = true`, `legacy_pin_set = false`. Any `enabled_agents` value is tolerated under the policy, but `{max}` is expected. |
| `users` for client 21 | no `setter`, `closer`, `sales` or `ao`; no existing `client` account for Levi (otherwise reset it rather than create a duplicate) |
| sending identities, scheduled sends, outbound items, send plans, `email_events` | 0 rows each |
| prospects | 29 total, 29 held, 0 setter-visible, 0 contacted |
| `places_api_requests` for tenant 21 | 0 |
| weekly claims | exactly one, `levi-week-2026-11-23` |

If `legacy_pin_set = true`, clear the tenant-21 PIN first. That needs separate approval. The `/client/:id` PIN portal has no rate limit.

## 2. Confirm the policy in production without side effects

Use an admin session on the dashboard (an existing operator account) and the browser devtools console. Each call must return the response shown. None of them performs work, because the policy refuses before any store write or provider call.

```js
// Agent dispatch for tenant 21 (admin may target any tenant): expect 403, error "tenant_pilot_policy_denied".
await (await fetch('/api/run/scout?client_id=21', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })).json();
```

Afterwards, `SELECT action, error_msg FROM agent_log WHERE client_id = 21 ORDER BY ran_at DESC LIMIT 3;` shows `dispatch_blocked / tenant_pilot_policy_denied`.

## 3. Create Levi's account (separate approval)

In `/admin/users`, create the user with:
* role `client`;
* client = the tenant-21 workspace;
* a long, generated temporary passphrase (20 or more characters). Do not reuse one, and do not paste it into chat, email or tickets.

The account is created with `password_change_required = true` and `email_verified = true`. Deliver the passphrase out of band: by phone, or a one-time secret link, separately from the sign-in URL.

## 4. Login smoke test

Do this together with Levi, or with a test client account for tenant 21 that is deactivated afterwards.

1. Open the production `/login`. Sign in with the temporary passphrase. Expect a redirect to `/change-password`.
2. Before changing the password, any data API (e.g. `/api/prospects`) returns `403 password_change_required`.
3. Change the password. Sign in again. Expect a redirect to `/command-deck`.
4. `/api/me`: `client.id = 21`. `/api/me?client_id=1` must still show 21.
5. `/api/prospects`: the tenant-21 prospects where `do_not_contact = false`, and none from other tenants.
6. These must be refused (403 or a redirect away; JSON `error` where shown):
   * `/setter`, `/closer`, `/admin/users`: redirect away;
   * `POST /api/run/scout`: 403;
   * `POST /api/v1/eoi/plan` with `{}`: 403 `tenant_pilot_policy_denied`;
   * `POST /api/v1/tenant-outreach/schedules/authorize` with `{}`: 403 `tenant_pilot_policy_denied`.
7. In Max (Command Deck), ask "brief me on my prospects". Expect an operational read of existing records. If Max offers "Approve discovery", replying "Approved" must produce a refusal (`tenant_pilot_policy_denied`), with no Scout run and no new `places_api_requests` rows.
8. Sign out. `/api/prospects` returns 401.

## 5. After the smoke test (read only)

Re-run step 1. All counts are unchanged, apart from login timestamps and audit rows recording the refusals.
