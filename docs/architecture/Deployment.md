# Deployment

## Platform

- **Host:** Railway
- **Process:** `node server.js` (`railway.json`)
- **Database:** Railway PostgreSQL via `DATABASE_URL`

## Environments

| `NODE_ENV` | Notes |
|---|---|
| `development` | Local; Inquiry boot DDL helpers allowed |
| `test` | Automated suite |
| `staging` / `production` | Fail closed without `DATABASE_URL` + non-empty `CRON_SECRET`; no Inquiry boot DDL; no silent migrate on boot |

## Migrations

```bash
npm run db:migrate:status
npm run db:migrate:dry-run
npm run db:migrate
```

Tracked SQL in `migrations/`. Explicit operator action only in staging/production.

## Readiness

`/ready` should reflect database/migration readiness (Inquiry stack reports not-ready if schema missing in gated envs).

## Feature flags (examples)

Max orchestration uses env + `clients.max_orchestration_config` with shadow default. Do not enable non-shadow mutation flags without an approved ops plan.

## Production authorization boundaries

The following are **not** authorized merely because code exists:

- Inquiry Foundation external senders
- Operator Command Center as a client-facing product
- Max non-shadow lifecycle writes / auto outreach

See operational runbook: [`../DEPLOYMENT.md`](../DEPLOYMENT.md) (Inquiry Foundation detail).

## Deploy checklist (generic)

1. Confirm CURRENT_STATE / release notes for the version being deployed
2. Run migrations explicitly
3. Verify `/ready`
4. Smoke auth + one cron with secret
5. Confirm flags still safe (shadow defaults)
6. Watch logs for pool/auth/webhook errors
