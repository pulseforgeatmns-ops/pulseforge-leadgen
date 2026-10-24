# Email verification retry failure observability

This additive diagnostic slice preserves retry eligibility, verifier behavior,
deliverability outcomes, aggregate counters, and the existing optimistic write
guards. It adds no provider call, query, retry, schema migration, or send authority.

## Persisted counters

The existing `max_outbound_control` event now includes
`verificationRetryFailureReasons`, a fixed seven-key object:

| Key | Observed branch |
| --- | --- |
| `verification_failed` | The existing outcome classifier returned `failed`. This includes unknown/unverified results; it does not establish a provider outage. |
| `adapter_unavailable` | The configured verifier is not a function. This does not assert that a remote provider is unavailable. |
| `candidate_ineligible` | The loaded candidate fails the existing tenant, DNC, or already-verified guard. Existing attempted/failed aggregate accounting is preserved. |
| `persistence_not_confirmed` | The guarded update did not report exactly one affected row. No specific changed field or deletion is inferred. |
| `verification_exception` | Calling or awaiting the verifier threw. |
| `persistence_exception` | Preparing or awaiting the guarded update threw, including possible serialization failures. |
| `unexpected_exception` | Another exception occurred within the existing per-attempt processing boundary, such as a null verifier result. |

Each newly attempted failure increments the unchanged `verificationRetryFailed`
counter and exactly one reason. Successful valid/risky/invalid writes keep their
existing totals and do not add a failure reason. Skipped unsafe candidates still
do not invoke verification.

`verificationRetryLoadFailed` separately counts candidate-read failures before
any attempt. It does not increment attempted/failed counters or failure reasons.

The reason object uses only fixed keys and nonnegative safe-integer values.
Unknown keys and invalid count values are dropped at merge/snapshot boundaries;
merged values are capped at `Number.MAX_SAFE_INTEGER`. No candidate identity,
email, URL, raw provider reason, payload, exception message, or credential is
copied into this new telemetry.

Older aggregate-only telemetry remains readable and retains its original totals.
Its missing reason counts are zero, not reconstructed. Thus category sums equal
the failed aggregate for newly emitted telemetry; historical/legacy merged totals
may contain additional uncategorized failures. An old zero reason map does not
prove that there were no failures.

## Limits and safety

- This is branch-level evidence, not a diagnosis of the current production
  failure cause. Historical verifier responses cannot explain newer cycles.
- The provider adapter and its existing logging are unchanged. Its missing-fetch
  path remains masked as timeout; that condition is not inferred from exception
  text. The privacy guarantee above applies to the new recovery telemetry,
  not pre-existing upstream provider logs.
- The CAS result is deliberately not followed by a diagnostic read. Tenant,
  address, DNC, verification snapshot, company, timestamp precision and row-count
  guards remain unchanged. No contact classification or provenance is rewritten.
- Publication, deployment and live retry invocation require separate approval.

## Fixture validation

- Recovery, control snapshot, tenant, preparation and CI-selection suites:
  106/106 passed, zero skipped, including disposable PostgreSQL 17.11.
- Existing `test:anchor-outbound` Node test command: 128/128 passed, zero skipped,
  including reply suppression, DNC, authorization and real-PostgreSQL regressions.
- Independent differential review: 756 synthetic scenarios matched main's
  aggregate results, verifier calls/arguments and SQL calls/parameters exactly.
- Provider exceptions/raw responses containing fake credentials do not appear in
  recovery logs or the new telemetry. External network was denied during runs;
  only disposable local PostgreSQL TCP was allowed. No production retry ran.

The focused command is `node --test test/emailVerificationRecovery.test.js
test/emailVerificationRecoveryPostgres.test.js test/anchorDailyOutboundMaxControl.test.js
test/babrunMaxOutboundPreparation.test.js test/scoutReplenishmentTenantGeneralization.test.js
test/governedOutboundContextInvariant.test.js test/governedOutboundContractGuard.test.js
test/ciPlanPr.test.js`. PostgreSQL fixtures require local `initdb` and `pg_ctl`;
they never use a production database fallback.
