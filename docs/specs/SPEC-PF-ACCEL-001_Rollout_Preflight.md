# SPEC-PF-ACCEL-001 — Read-only rollout preflight slice

Status: In progress, user-approved implementation slice, 2026-10-13.
Scope: the Oct 13–Nov 2 acceleration plan's PR897 workbook / PR899 Signal rollout safeguards only. This document does not replace the broader plan or authorize a production action.

## Objective and vision references

Prevent main/caller auto-deploy from overtaking schema or paired-feed readiness. Preserve [approval-gated execution](../adr/ADR-003_Human_Approval.md) and [product principles](../vision/Product_Thesis.md).

## Problem

At review time the main prospects schema lacked both workbook safety flags, source address and website fields, and all six workbook migration tables. Its status check excluded application_in_progress. The pending workbook transaction locks 16 tables globally. Bounded contention correctness evidence does not prove staging load suitability. Signal's reviewed feed requires a >=32-character bearer token in both services and the matching collector change; masked presence alone proves neither validity nor equality. Current deployment configuration must be independently checked before any merge.

## Scope and architecture

A standalone CLI adapts the existing catalog-validation approach, without importing its shared database/app dependencies. Default is no connection. Explicit --test-database reads PREFLIGHT_TEST_DATABASE_URL only; no .env loading or DATABASE_URL fallback. The flag asserts a separately selected test target; hostnames cannot prove an environment is safe. URLs are never printed, URL parameters cannot override safety settings, and remote TLS verifies certificates.

One owned pg.Client uses read-only startup settings, BEGIN READ ONLY, fixed parameterized SELECTs on pg_catalog, and ROLLBACK/close. Connect, query, lock, idle-transaction and cleanup deadlines are bounded. Raw errors, definitions, credentials and customer records never enter the JSON report. No arbitrary SQL, migrations, app bootstrap, provider calls or customer-data reads.

Checks cover required table kinds and typed/nullability/default column shapes, immediate valid nonpartial composite unique prerequisites, new PK/FK tenant boundaries, validation and enabled FK enforcement triggers, exact reviewed status/channel checks, and enabled immutable-trigger definition. Different/extra check definitions yield unknown rather than speculative SQL interpretation. All output fields are fixed allowlisted identifiers and explanations.

Optional local-config checks reproduce PR899's local token-length requirement. They do not prove entropy, equality, service deployment or feed compatibility. Deployment readiness remains unknown even when inspected schema prerequisites are ready.

## Dependencies and migration strategy

Reviewed source: PR897 migration 2026-10-07-max-spreadsheet-reliability.sql and commit-lock set; PR899 feedAuth and operatorJsonFeedCollector. Copied migration/base SQL is test-only under test/fixtures. No migration is introduced, executed, or wired into deployment by this slice. Reconcile the fixture against any subsequent reviewed PR897 change.

## Testing and acceptance

- Default, import and help paths require no application dependencies or network.
- Absent/partial/full schemas classify blocked/blocked/ready; noncanonical evidence is unknown.
- Wrong/null safety fields, invalid or partial unique keys, missing/unvalidated/mis-scoped FK/PKs, restrictive checks and disabled/changed triggers cannot pass.
- Fixed metadata SELECTs only, plus read-only transaction boundaries; no customer-data reads.
- Authentication/permission/query failures, timeouts and cleanup failures emit sanitized unknown, close the dedicated connection, and never bootstrap jobs.
- Real PostgreSQL gate is non-skipping: the actual reviewed migration must pass catalog deparsing, DML must be rejected, timeouts must close connections and pg_stat_activity must show no leaked backends.
- Operator plan explicitly preserves staging contention, paired Signal deployment, CI gating, rollback and production approval requirements.

## Out of scope / future work

No production execution, migration/config changes, deployment, merge, provider/Telegram polling, load generation, cross-service secret comparison, whole-application schema certification, runtime privilege audit or publication without separate approval. See [operator plan](../rollout-preflight-operator-plan.md).
