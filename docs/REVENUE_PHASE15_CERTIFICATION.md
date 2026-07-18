# Revenue Phase 1.5A — Governance Enforcement Certification

**Date:** 2026-07-18  
**Repository:** `pulseforgeatmns-ops/pulseforge-leadgen`  
**Production writes:** none  
**Governance verdict:** **COMPLETE**  
**Phase 1.5 production-closure verdict:** **BLOCKED — controlled Anchor canary and production backup/restore evidence remain separately unauthorized**

## Enforced protection

`main` now requires a pull request and the exact GitHub check context `revenue-postgresql-required`; the solo-founder policy requires zero approvals. Required checks are strict (the PR branch must be current with `main`). Force pushes and branch deletion are disabled. Administrators are included in enforcement; there is no routine administrative or repository-role bypass.

The exact requirement was identified from the real PR check run:

```text
Workflow: Revenue PostgreSQL Integration
Job / check context: revenue-postgresql-required
```

## PR gate evidence

Verification PR [#19](https://github.com/pulseforgeatmns-ops/pulseforge-leadgen/pull/19) passed the required check and merged through protected `main` at `2026-07-18T12:42:57Z` (`fb21bb658b2d7c7f102f72b08458f554da5f5d99`). Temporary negative verification PR [#20](https://github.com/pulseforgeatmns-ops/pulseforge-leadgen/pull/20), closed without merge, established the gate behavior under the live zero-approval policy:

- While the required check was in progress, PR #20 was `BLOCKED`.
- Its intentionally failing required check was `BLOCKED`; the temporary PR was closed without merge.
- The production workflow then passed on PR #19: [run 29644474973](https://github.com/pulseforgeatmns-ops/pulseforge-leadgen/actions/runs/29644474973), job `revenue-postgresql-required`, and PR #19 merged through the protected path.
- A direct push to `main` by a repository administrator was rejected with `GH006` under the former one-approval setting. The active rule now has zero approvals but keeps administrator enforcement and the required check.

## Workflow integrity

The required workflow has no PR path filters, so it cannot be bypassed by changing a file outside a matching path. It installs PostgreSQL and invokes the disposable harness with `REVENUE_TEST_POSTGRES=true`. That harness creates and destroys a fresh database, applies all revenue migrations, and tests lifecycle transitions, concurrent idempotent replay, transaction rollback, tenant isolation, append-only enforcement, projection reconstruction and determinism, and three-way reconciliation.

The required run passed its one integration test with zero skips. The generic test suite's intentionally skipped PostgreSQL integration path is not the required workflow path.

## Reconciliation and test evidence

The disposable rehearsal reconciled exactly: 14 ledger events, one projected outcome, zero mismatches, and zero unexplained events. Ledger, projection, and source values matched exactly. See [the rehearsal artifact](../artifacts/revenue/phase15-disposable-postgres-rehearsal.json).

- `npm run test:revenue:postgres`: 1 passed, 0 failed, 0 skipped.
- GitHub required workflow: 1 passed, 0 failed, 0 skipped.
- Full repository suite: 261 tests, 257 passed, 0 failed, 4 skipped. The skips are unrelated to the required PostgreSQL workflow.

The machine-readable branch-protection and gate evidence is in [phase15-branch-protection-certification.json](../artifacts/revenue/phase15-branch-protection-certification.json).

## Scope boundary and next authorization

This certification did not apply migrations, change production feature flags, run a production canary, create revenue events, touch Anchor runtime capabilities, send messages, process refunds, or start retention automation.

Phase 1.5A governance enforcement is complete and the repository is ready to receive a separately approved Phase 1.6 production authorization. Production remains blocked until the authorization names the window and operator, verifies and restore-tests the backup, and completes the bounded Anchor client `10` canary with exact post-canary reconciliation.
