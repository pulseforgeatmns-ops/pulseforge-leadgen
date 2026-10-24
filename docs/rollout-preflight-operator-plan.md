# Rollout preflight operator plan

Scope: [SPEC-PF-ACCEL-001 rollout slice](specs/SPEC-PF-ACCEL-001_Rollout_Preflight.md), PR897 workbook and PR899 Signal. This command inspects prerequisites; it does not grant permission to merge, migrate, deploy or send.

## Commands and evidence

- `npm run rollout:preflight`: default dry run, no database connection or .env loading. Expected exit 2 (unknown).
- `npm run rollout:preflight -- --local-config`: inspect only this process's token-length requirement. Does not show the token/length/hash or contact either service.
- Set `PREFLIGHT_TEST_DATABASE_URL` privately through the test environment, then run `npm run rollout:preflight -- --test-database`. Do not paste a connection URL into a command argument, log, issue or chat. `DATABASE_URL` and ambient pg password sources are not used. The target variable must identify a disposable/local test database; the flag is an operator assertion, not proof that a hostname is nonproduction.
- `npm run test:rollout-preflight`: isolated unit/security suite.
- `npm run test:rollout-preflight:postgres`: non-skipping disposable PostgreSQL gate. Requires initdb and pg_ctl; creates and removes its own synthetic fixture. Never points at an externally supplied database. Test fixture DDL belongs only to this gate, not the preflight command.

Connection URLs require an explicit username, database and host, with no query parameters or fragment. The implementation fixes read-only settings and short deadlines. Loopback uses plaintext for disposable fixtures; non-loopback connections require valid TLS certificates. A connection error is reported without its raw message, URL or credential. No SSL-warning bypass is offered.

Exit 0 means the inspected schema prerequisites (and local token check if requested) are ready. Exit 1 means an observed blocker. Exit 2 means evidence is missing, noncanonical, timed out or could not be safely established. Always inspect `schema`, `local_config` and `rollout` separately: `rollout.status` stays unknown. A zero exit is never an overall deployment green light.

## Workbook gate before merge or migration

1. Preserve an exact reviewed-head regression result for PR897 and this preflight. The copied base/migration fixtures must match the current reviewed PR897 contract before relying on them.
2. Check all prerequisites, including the composite unique indexes on users(client_id,id) and prospects(client_id,id), new table PK/FKs, column types/defaults, validated application_in_progress status CHECK, and immutable proposal trigger. If a schema differs from the reviewed expression, review it explicitly; do not interpret unknown as compatible.
3. Separately authorize production catalog inspection and any migration. This implementation assignment does not authorize either. Use approved credential handling and a least-privilege metadata reader. Do not reuse application startup, health or migration commands as a diagnostic shortcut.
4. Plan the reviewed PR897 migration before new consumers reach production. Current known missing safety/source/website columns and all six migration tables are blockers; rerun current metadata evidence after an authorized change rather than relying on historical observations.
5. Rehearse the actual 16-table SHARE ROW EXCLUSIVE commit lock under representative concurrent tenant/outbound activity in an approved staging environment. Record throughput, lock waits, timeout recovery and provider authorization outcomes. Existing bounded correctness tests are not load evidence. This CLI never acquires those locks or generates traffic.

## Signal gate before coordinated deployment

1. Confirm >=32-character token validity in both main and caller services without disclosing values. Local validity does not establish secure entropy or matching values. Masked presence alone is insufficient.
2. Securely verify both service values match under a separately approved operator procedure. This command does not compare across services or emit stable token hashes.
3. Keep the updated bearer-auth collector and authenticated feed compatible across rollout order and rollback. Verify the exact deployed revision of each service.
4. Obtain approval for any endpoint smoke test: the existing /health and /feed handlers can poll Telegram, so even an HTTP GET is not a side-effect-free diagnostic. This command never calls them, sends alerts, or bootstraps a service.

## Deployment and rollback gate

Review actual Railway main/caller auto-deploy and Wait for CI settings, predeploy/migration sequencing, rollback procedure, exact commit checks and service health through authorized means. Main was auto-deploying with CI waiting disabled and no predeploy migration at review time; these facts must be rechecked before relying on them. Keep merges/deployments and all production writes behind explicit approval. Schema presence alone does not certify runtime grants, entire-application schema integrity, historical data validity, live traffic load or feed compatibility.

## Validation for this isolated preparation

- Unit/security: 38/38 passed, including every query-stage failure/timeout, read-only-setting rejection, error/argument/catalog redaction, composite indexes, tenant FKs, trigger checks and dry-run isolation.
- Existing CI selection/global startup regression: 11/11 passed.
- Real PostgreSQL: attempted and blocked locally by unavailable initdb/pg_ctl. It failed rather than skipped. No real-PG pass is claimed; the PR's Revenue PostgreSQL job runs the new gate after publication is separately approved.
- No production read, production DDL/DML, provider call, push, PR publication, merge or deployment was performed for this slice.
