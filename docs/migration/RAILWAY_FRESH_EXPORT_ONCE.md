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
| Config file override | **Mandatory:** `RAILWAY_CONFIG_FILE=railway.migration-export.json` |
| Approval | `EXPORT_ONCE_APPROVED=YES_READ_ONLY_TO_BUCKET` |
| Object key | `BACKUP_OBJECT_KEY=migration-backups/railway-20261121-full-pg18.dump` |

Railway's default `railway.json` / `Dockerfile` build the live application. Without `RAILWAY_DOCKERFILE_PATH=Dockerfile.migration-export` and `RAILWAY_CONFIG_FILE=railway.migration-export.json`, this service would start the app against production. That is forbidden.

Restart policy is **NEVER**. The worker must not loop or retry after a successful or failed dump. Do **not** reuse service `migration-offsite-export-once` or object key `migration-backups/railway-20261117-full-pg18.dump`.

## Variables (reference only; never paste secrets)

Worker process env keys must be `S3_*` as required by `exportRailwayToBucket.py`. Railway bucket reference **names** are `ENDPOINT`, `BUCKET`, `REGION`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` — not `S3_*`. Postgres references are `PGHOST` / `PGUSER` / `PGPASSWORD` / `PGDATABASE` / `PGPORT`.

`PGDATABASE` must resolve to `railway` and `PGHOST` must end with `.railway.internal`. Hard-code those two non-secret values only if the references resolve wrong. Do not paste passwords, access keys, or connection URLs.

```
RAILWAY_DOCKERFILE_PATH=Dockerfile.migration-export
RAILWAY_CONFIG_FILE=railway.migration-export.json
EXPORT_ONCE_APPROVED=YES_READ_ONLY_TO_BUCKET
BACKUP_OBJECT_KEY=migration-backups/railway-20261121-full-pg18.dump
PGHOST=${{Postgres.PGHOST}}
PGPORT=${{Postgres.PGPORT}}
PGUSER=${{Postgres.PGUSER}}
PGPASSWORD=${{Postgres.PGPASSWORD}}
PGDATABASE=${{Postgres.PGDATABASE}}
S3_ENDPOINT=${{MigrationBackup.ENDPOINT}}
S3_BUCKET=${{MigrationBackup.BUCKET}}
S3_REGION=${{MigrationBackup.REGION}}
S3_ACCESS_KEY_ID=${{MigrationBackup.ACCESS_KEY_ID}}
S3_SECRET_ACCESS_KEY=${{MigrationBackup.SECRET_ACCESS_KEY}}
```

The exporter refuses overwrite if the object key already exists, requires the private Railway Postgres host, and dumps database `railway` only.

## Out of scope

- This worker does not restore into Render.
- After the export succeeds, a separate restore workflow may retarget `restoreGatewayRender.py` at empty `pulseforge_production`. That workflow must preserve `pulseforge_production_migration`, keep writers off, leave PR #1146 unmerged, and fail closed if the 10 GB disk cannot hold the full source plus the snapshot.
- No production writers, application workers, or cutover.
- Delete the one-shot service after the run. Keep the source Postgres and the `MigrationBackup` bucket.
