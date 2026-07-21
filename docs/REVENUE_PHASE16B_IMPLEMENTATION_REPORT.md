# Revenue Phase 1.6B — Implementation Report (Release Candidate)

**Status:** offline implementation complete; authorization remains unsigned and non-executable.
**Production access during this work:** none. No Railway access, no production PostgreSQL connection, no migration application, no feature-flag change, no Eliza canary, no authorization signature.
**Protected-main base:** `cd6ed74abde896eb504db1fd70709d0bc67229b3`
**Unsigned draft:** `artifacts/revenue/phase16b-production-authorization-draft.json`
**Unsigned-draft canonical hash:** `add3646e275aeba07969b8e21b32d52e9b7831085efc10cad1f9da5a63ac447f`

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
6. Runner invocation only via `scripts/executeRevenuePhase16b.js --production --confirm=ce9005f1-6d6f-46fd-a52d-4081a79ed02f`.

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

## Clean test counts (release-candidate verification)

| Suite | Pass | Fail | Skip |
| --- | --- | --- | --- |
| Phase 1.6B unit + disposable PostgreSQL (`test/revenuePhase16b.test.js`) | 19 | 0 | 0 |
| Offline finalizer (`test/revenuePhase16bFinalizer.test.js`) | 7 | 0 | 0 |
| Concurrent harness stress (`test/disposablePostgresHarness.test.js`) | 5 | 0 | 0 |
| Combined revenue PostgreSQL suites | 20 | 0 | 0 |
| Syntax (`node --check` on Phase 1.6B surfaces) | all ok | — | — |
| `git diff --check` | clean | — | — |
| Clean release-branch repository-wide suite | 303 | **0** | 12 |

Phase 1.6B integration tests: **zero skips**.

## Non-goals of this release candidate

- No production access of any kind.
- No authorization signature.
- No execution-window selection.
- No Eliza canary.
- No feature-flag change.
- No migration application.
