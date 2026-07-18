# Revenue Phase 1.5 — Productionization and Reconciliation Report

**Date:** 2026-07-18
**Production writes:** none
**Verdict:** implementation complete; production definition of done remains blocked on the separately approved Anchor canary.

## Delivered

- Disposable PostgreSQL integration harness applying both revenue migrations from a fresh prerequisite schema.
- Full customer → opportunity → job → completion → payment → refund lifecycle test.
- Concurrent idempotent replay proof with one durable mutation and one replay response.
- Transaction rollback proof after deliberately failing the final audit insert.
- Cross-tenant reference rejection proof.
- Database permission and trigger tests rejecting ordinary-role `UPDATE` and `DELETE` on `revenue_events`.
- Projection rebuild CLI with dry-run, compare-only, apply, tenant/all-tenant, and date-range modes.
- Three-way ledger/projection/source reconciliation with exact financial and count totals.
- Five independent two-key operational feature flags, all default off.
- Internal tenant-scoped revenue health output and operator audit view.
- Required pull-request PostgreSQL CI job and production runbook.

## Reconciliation metrics

Each tenant report contains booked, delivered, collected, refunded, net-collected revenue; job, payment, and refund counts; unattributed outcomes; and disputed outcomes. Any unequal ledger/projection/source value or unexplained projection event sets status `failed` and causes the CLI to exit nonzero.

The disposable production-shaped rehearsal passed with 14 ledger events, one projected outcome, zero mismatches, and zero unexplained events. Ledger, projection, and source totals were: booked 30,000 cents; delivered 30,000; collected 30,000; refunded 5,000; net collected 25,000; one job; one payment; one refund; zero unattributed; zero disputed.

## Remaining production gate

The controlled Anchor canary was intentionally not executed. Phase 1.5 cannot be declared production-complete until a named operator authorizes a bounded window, the current production backup is independently restore-tested, the exact revision and flags are verified, one controlled Anchor lifecycle is recorded, and the post-canary report reconciles with zero unexplained differences.

Retention automation and customer expansion remain Phase 2 and are not authorized.

## Verification result

- Required disposable PostgreSQL check: 1 passed, 0 failed.
- Full repository suite: 261 tests; 257 passed, 0 failed, 4 skipped.
- The skipped revenue integration test in the generic suite is intentional; CI invokes it separately with `REVENUE_TEST_POSTGRES=true`, where it passed.
- Repository workflow defines the required check name. Branch protection must still be configured in GitHub to require that check before merge.
