# Max pre-shadow database recovery

The recorded archive must be copied to durable, encrypted, access-controlled storage before it is considered fully verified. A path under `/private/tmp` is local evidence only.

1. Verify the SHA-256 against the readiness record.
2. Run `pg_restore --list ARCHIVE` and confirm the required operational tables are present.
3. Provision an isolated PostgreSQL instance matching or exceeding PostgreSQL 18.4.
4. Restore into the isolated database with `pg_restore --clean --if-exists --no-owner --no-privileges --dbname TARGET ARCHIVE`.
5. Validate row counts and application-critical constraints in the isolated database.
6. Stop. Never restore over production without a separate incident authorization, a maintenance window, and a tested rollback plan.

The production database is never a target of the verification procedure in this repository.
