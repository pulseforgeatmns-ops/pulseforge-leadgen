# One-shot Railway export: 2026-11-21

**Status:** prepared worker contract only. This is not an executed dump, restore, or cutover. Production writers stay off.

Create a disposable Railway service named `migration-offsite-export-20261121` in project `amused-harmony`, **production** environment. It runs the existing read-only exporter (`Dockerfile.migration-export` → `scripts/migration/exportRailwayToBucket.py`). It is not an application or agent container.

## Contract

| Item | Value |
|---|---|
| Service name | `migration-offsite-export-20261121` |
| Project / environment | `amused-harmony` / production |
| Image | `Dockerfile.migration-export` |
| Config as code | `railway.migration-export.json` (`restartPolicyType: NEVER`) |
| Dockerfile override | **Mandatory:** `RAILWAY_DOCKERFILE_PATH=Dockerfile.migration-export` |
| Approval | `EXPORT_ONCE_APPROVED=YES_READ_ONLY_TO_BUCKET` |
| Object key | `BACKUP_OBJECT_KEY=migration-backups/railway-20261121-full-pg18.dump` |

Railway's default `railway.json` / `Dockerfile` build the live application. Without `RAILWAY_DOCKERFILE_PATH=Dockerfile.migration-export`, this service would start the app against production. That is forbidden.

Restart policy is **NEVER**. The worker must not loop or retry after a successful or failed dump.

## Variables (reference only; never paste secrets)

Wire **reference variables** from the existing Postgres and MigrationBackup services. Do not paste connection strings, passwords, or bucket keys into Git, chat, or the service form as literals.

From **Postgres**:
- `PGHOST`
- `PGUSER`
- `PGDATABASE`
- `PGPASSWORD`
- `PGPORT`

From **MigrationBackup**:
- `S3_ENDPOINT`
- `S3_BUCKET`
- `S3_REGION`
- `S3_ACCESS_KEY_ID`
- `S3_SECRET_ACCESS_KEY`

Set on the worker itself:
- `EXPORT_ONCE_APPROVED=YES_READ_ONLY_TO_BUCKET`
- `BACKUP_OBJECT_KEY=migration-backups/railway-20261121-full-pg18.dump`
- `RAILWAY_DOCKERFILE_PATH=Dockerfile.migration-export`

The exporter refuses overwrite if the object key already exists, requires the private Railway Postgres host, and dumps database `railway` only.

## Out of scope

- No restore into Render.
- No retarget of `restoreGatewayRender.py` to `pulseforge_production`.
- No production writers, application workers, or cutover.
- Delete the one-shot service after the run. Keep the source Postgres and the `MigrationBackup` bucket.
