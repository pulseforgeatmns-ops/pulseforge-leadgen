# Revenue Phase 1.6B — Implementation Report

**Status:** blocked production attempt investigated; root cause fixed offline; new authorization prepared unsigned and non-executable.
**Unsigned draft:** `artifacts/revenue/phase16b-production-authorization-draft.json`
**Unsigned-draft canonical hash:** `d41a233b9a8c91d292404ffac4e23a9b6e69f959abd4c60d03008a66612e90c2`
**Current authorization ID:** `3808a9f7-b8e4-467f-917f-5021dfb7d485`
**Retired authorization ID (consumed, permanently non-reusable):** `ce9005f1-6d6f-46fd-a52d-4081a79ed02f`

## Blocked production attempt (2026-07-21T04:02Z) — root cause and fixes

The first production execution under authorization `ce9005f1…` was **BLOCKED SAFELY**: production was contacted, but no migration checkpoint completed, no revenue or feature-flag tables exist, no flags changed, and no Eliza/ledger/financial records were created. Failed-attempt evidence is preserved at `artifacts/revenue/phase16b-production-execution-blocked-20260721T0402Z.json`; the retired signed authorization at `artifacts/revenue/phase16b-production-authorization-signed-retired-ce9005f1.json`.

### Root cause (initiating first error)

- **Error:** `there is no unique constraint matching given keys for referenced table "companies"` — SQLSTATE **42830** (`invalid_foreign_key`).
- **Location:** `migrations/2026-07-18-anchor-closed-loop-revenue-phase1.sql`, first `CREATE TABLE customers` statement, composite tenant FK `FOREIGN KEY (client_id, company_id) REFERENCES companies(client_id, id)`.
- **Why:** production `companies` has only `PRIMARY KEY (id)` and production `prospects` has only `PRIMARY KEY (id)` + `UNIQUE (email)` — neither carries `UNIQUE (client_id, id)` — and production has **no `campaigns` table at all**. The old test fixture (`test/fixtures/revenueBaseSchema.sql`) invented those composite keys and a campaigns table, so every disposable rehearsal passed while production could not.
- **Transaction boundary:** the migration file opens an explicit `BEGIN`; the 42830 error aborted that transaction, PostgreSQL rolled the work back (no partial schema), and the session remained in the aborted-transaction state.
- **Rollback:** effective — no revenue object survived in production.
- **Query parameters:** none (pure DDL; no parameter values involved).
- **Reproduced offline:** byte-exact, against a clone of the Phase 1.6A production restore (`phase16a_revenue_restore_validation`, PostgreSQL 18.4 — the same version the production backup restore was validated on) and now permanently pinned by a regression test on a production-faithful disposable schema.

### Why the evidence surfaced 25P02 instead of 42830

The runner's `catch` did capture the initiating 42830, but the `finally` block then ran the write-shutdown prover on the **same aborted connection**. Its first probe raised **25P02** (`current transaction is aborted`), and that handler **overwrote** `evidence.failure` with the 25P02 shutdown message and reported `writes_disabled: false` — misleading, because `revenue_feature_flags` never existed, so writes could never have been enabled. 25P02 was a secondary symptom, not the root cause.

### Fixes (all offline; reproduced first, then fixed)

1. **Migration production-compatibility** (`migrations/…-phase1.sql`): idempotent `DO` blocks provision `UNIQUE (client_id, id)` on `companies` and `prospects` before any dependent table (both columns NOT NULL; `id` already unique, so this cannot fail on data), and the `opportunities → campaigns` composite FK is now attached only when a campaigns table with a `(client_id, id)` unique key exists (production: skipped; `campaign_id` stays a plain nullable UUID; the canary never sets it). New certified phase1 SHA-256: `c11740daa17a4d8495daa428134effdffaa0d64d8b343e044739a23d28fa6495`. The consumed migration bytes are preserved for regression at `test/fixtures/consumedPhase16bMigrationPhase1.sql`.
2. **First-error preservation** (`services/revenuePhase16bRunner.js`): the first database error — full SQLSTATE, detail, hint, position, table, constraint, routine, migration file, checksum, and best-effort statement location — is the primary failure and is never overwritten by cleanup or shutdown-proof errors (`failure_is_first_database_error: true`).
3. **Immediate rollback:** the runner issues `ROLLBACK` immediately after any failed statement; the rollback outcome (success or its own error) is reported separately in `evidence.rollback`. No verification query ever runs on an aborted transaction.
4. **Fresh-connection write-shutdown proof:** `verifyWritesDisabledOnFreshConnection` opens a new connection; if `revenue_feature_flags` is absent it reports `writes_disabled: true` with reason `feature_flag_table_absent_pre_migration`; if present it forces the client-10 flag off, re-verifies it, and reports any non-Anchor tenant with writes enabled (never mutating other tenants). The legacy same-connection prover is retained only as the pinned defective behavior for the regression test.
5. **Structured migration diagnostics:** checkpoints for production identity verification, pre-migration baseline, each migration transaction open/commit (or failure), and post-migration structural verification — each with UTC timestamp, correlation ID, migration checksum, transaction state, database role, and search path. Ambiguous migration state fails closed with `PHASE16B_AMBIGUOUS_MIGRATION_STATE`.
6. **Production-faithful fixture:** `test/fixtures/revenueBaseSchema.sql` now mirrors the actual production key shapes (no composite tenant keys, no campaigns table), so every disposable rehearsal exercises the real production precondition.
7. **Authorization retirement:** `ce9005f1…` is in `RETIRED_AUTHORIZATION_IDS` and is rejected by the validator even if perfectly re-signed; its correlation ID and all eight idempotency keys are likewise non-reusable. The new authorization uses fresh values throughout.

## Authoritative component status

| Component | Status | Path |
| --- | --- | --- |
| Validator | **Implemented** | `utils/revenuePhase16b.js` |
| Historical-date handling | **Implemented** | `utils/historicalTimestamp.js` |
| Runner | **Implemented** | `services/revenuePhase16bRunner.js` |
| Reconstruction verifier | **Implemented** | `services/revenueReconstruction.js` |
| Execution CLI | **Implemented** | `scripts/executeRevenuePhase16b.js` |
| Offline finalizer CLI | **Implemented** | `scripts/finalizeRevenuePhase16bAuthorization.js` |
| Disposable PostgreSQL harness | **Implemented** (collision-proof) | `test/helpers/disposablePostgres.js` |
| Unit / integration / stress / finalizer tests | **Implemented** | `test/revenuePhase16b.test.js`, `test/revenuePhase16bFinalizer.test.js`, `test/disposablePostgresHarness.test.js` |

### Stale exploration conclusions (superseded)

Earlier exploration concluded that the Phase 1.6B validator, runner, historical-date derivation, and non-destructive reconstruction verifier were still missing. **That conclusion is obsolete.** Those components exist, are covered by disposable-PostgreSQL tests with zero skips, and are wired into the required CI check `revenue-postgresql-required`. Tenant-remediation Phase 16B scripts remain out of scope and must not be reused.

## Confirmed operator facts (not yet written into the unsigned draft)

The following facts are confirmed by the operator for use at finalization time. They are **not** applied to the unsigned draft; the offline finalizer is the only path that may write them into a separate signed file.

| Fact | Value |
| --- | --- |
| Scheduled-start local date | `2026-07-14` |
| Completion local date | `2026-07-14` |
| Payment-received local date | `2026-07-14` |
| Timezone | `America/New_York` |
| Precision | `day` |
| Operator confirmed | `true` |
| Payment method | `stripe_card` |

## Remaining operator facts (required for signing)

1. Operator attestation (Jacob Maynard).
2. Founder approval attestation (Jacob Maynard).
3. Actual UTC `approved_at` at finalization (defaults to current UTC when the finalizer runs unless `--approved-at` is supplied).
4. Final canonical authorization hash recomputed by the finalizer after the above values are fixed.

The three historical dates and payment method above are confirmed and ready to pass into the finalizer; they remain outstanding relative to the unsigned draft until that finalization runs.

## Remaining production gates

1. Protected pull-request merge to `main` with mandatory `revenue-postgresql-required` passing on the exact release commit (solo-founder policy: zero approvals; administrators enforced; force pushes and deletion disabled). Independent human review is **not** a required gate under the certified solo-founder policy.
2. Offline finalization producing a separate signed authorization file (unsigned draft must remain unchanged).
3. Selection of a future two-hour UTC execution window (not chosen yet; do not reuse a past window).
4. Fresh protected-main / Railway deployment identity observation immediately before execution.
5. Explicit environment gate `REVENUE_PHASE16B_PRODUCTION_ENABLED=true` only after the signed authorization has passed final validation inside the active window.
6. Runner invocation only via `scripts/executeRevenuePhase16b.js --production --confirm=3808a9f7-b8e4-467f-917f-5021dfb7d485`.
7. Release identity and window in the unsigned draft must be re-bound (and the hash recomputed) to the fresh protected-main commit and Railway deployment after the root-cause-fix PR merges.

## Offline finalizer interface

```bash
npm run revenue:phase16b:finalize -- \
  --operator-attestation-file <path> \
  --approver-attestation-file <path> \
  --scheduled-date 2026-07-14 \
  --completion-date 2026-07-14 \
  --payment-date 2026-07-14 \
  --payment-method stripe_card
```

Optional: `--approved-at <UTC ISO-8601>` (defaults to the finalization instant). Optional: `--output <path>` (defaults to `artifacts/revenue/phase16b-production-authorization-signed.json`). The finalizer rejects any altered authorization ID, correlation ID, idempotency key, financial value, prohibition, or stop condition; validates day-level historical precision; never modifies the unsigned draft in place; never opens a database connection; and proves the signed file is non-executable outside its window.

## Harness guarantees

Every disposable PostgreSQL instance receives a collision-proof unique:

- data directory (`mkdtemp` root + `/data`);
- Unix socket directory (separate short `mkdtemp` path);
- TCP port (OS-assigned ephemeral + cross-process `wx` port lock);
- log path;
- postmaster process identifier;
- cleanup root directory.

Cleanup is proven after successful completion, test-body failure, `initdb` failure, `pg_ctl start` failure, concurrent multi-instance execution, and concurrent child-process crash (process-exit handler).

## Base-versus-candidate failure classification

Compared during release-candidate stabilization:

- **Base / protected main:** `cd6ed74abde896eb504db1fd70709d0bc67229b3` (clean) — `# tests 284 / pass 272 / fail 0 / skipped 12`
- **Dirty original worktree** (Phase 1.6B + unrelated WIP) — `# tests 386 / pass 368 / fail 6 / skipped 12`
- **Clean Phase 1.6B release branch** — must report **zero failures**; the six dirty-worktree failures are excluded from this PR

The earlier seventh failure (concurrent `initdb` resource collision) is **gone** after the harness stabilization.

The six repository-wide failures observed in the dirty original worktree are **unrelated WIP**. Those test files were present only in the original worktree, are **absent from protected main**, and are **excluded from the Phase 1.6B release PR**. They are preserved on branch `wip/preserve-unrelated-anchor-tenant-2026-07-21` and must not be treated as Phase 1.6B regressions.

| # | Test name | File | Exact error | On protected main? | In release PR? | Classification |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | follow-up draft is manual-only copy and Setter has no provider request | `test/anchorSetterCampaign.test.js` | Assertion: source matched `/api\.brevo\.com\|sendCloserHandoffEmail\|axios\.post/` (expected no match) | Absent | Excluded | Unrelated Anchor setter WIP (original worktree only) |
| 2 | callback defaults use the next business day and nurture defaults to 90 days | `test/anchorSetterCampaign.test.js` | `TypeError: Cannot read properties of null (reading 'getTime')` | Absent | Excluded | Unrelated Anchor setter WIP (original worktree only) |
| 3 | manual opportunity bridge is feature-gated, deterministic, and does not map Setter booking to revenue booked | `test/anchorSetterCampaign.test.js` | Assertion: source did not match `/source: 'outbound_phone'/` | Absent | Excluded | Unrelated Anchor setter WIP (original worktree only) |
| 4 | the Anchor schema remains migration-gated and rollback refuses to erase structured history | `test/anchorSetterCampaign.test.js` | Assertion: source matched `/ADD COLUMN IF NOT EXISTS details/i` (expected no match) | Absent | Excluded | Unrelated Anchor setter WIP (original worktree only) |
| 5 | Cal batch rejects mixed-client records before provider payload creation | `test/anchorTenantIntegrity.test.js` | `AssertionError: Missing expected exception.` | Absent | Excluded | Unrelated tenant-integrity WIP (original worktree only) |
| 6 | production agents do not capture runtime client selection at module load | `test/tenantRemediation.test.js` | Assertion: `veraAgent.js` matched `/const\s+CLIENT_ID\s*=\s*getRuntimeClientId/` (expected no match) | Absent | Excluded | Unrelated tenant-remediation WIP (original worktree only) |
| 7 | (previously reported) concurrent disposable PostgreSQL `initdb` resource collision | shared harness / concurrent suites | `initdb` / bind / socket collision under concurrent suite execution | N/A | Fixed in harness | Environment-related — fixed by collision-proof shared harness |

**Phase 1.6B introduced failures: zero.** Clean release-branch repository suite must be zero-fail.

## Clean test counts (post-root-cause-fix verification)

| Suite | Pass | Fail | Skip |
| --- | --- | --- | --- |
| Phase 1.6B unit + disposable PostgreSQL + regression + failure modes (`test/revenuePhase16b.test.js`) | 26 | 0 | 0 |
| Offline finalizer (`test/revenuePhase16bFinalizer.test.js`) | 7 | 0 | 0 |
| Concurrent harness stress (`test/disposablePostgresHarness.test.js`) | 5 | 0 | 0 |
| Combined revenue PostgreSQL suites (`test:revenue:postgres` + `test:revenue:phase16b`) | 34 | 0 | 0 |
| Repository-wide suite (all PostgreSQL gates enabled) | 322 | **0** | 0 |
| Syntax (`node --check` on Phase 1.6B surfaces) | all ok | — | — |
| `git diff --check` | clean | — | — |

New failure-mode coverage: production 42830 reproduction on the consumed migration bytes; legacy same-connection prover defeated by 25P02 (pinned old defect); first-statement and mid-migration failures with the initiating error primary; rollback success and simulated rollback failure reported separately; transactional migration leaving no partial schema; fresh-connection shutdown proof with the flag table absent, present-off, present-unexpectedly-on (forced off), and enabled-for-another-tenant (reported, never mutated); ambiguous migration state failing closed; retired-authorization rejection at validation before any database connection.

Phase 1.6B integration tests: **zero skips**.

## Non-goals of this release candidate

- No production access of any kind.
- No authorization signature.
- No execution-window selection.
- No Eliza canary.
- No feature-flag change.
- No migration application.
