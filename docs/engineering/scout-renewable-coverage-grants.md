# Renewable execution grants for finite Scout legacy coverage

## Scope

This change separates durable finite allocation from temporary execution permission in the existing Scout controller/provider/page/ticket/admission path. It does not add a paid-test runner, enable discovery, change the research pause or recovery authorization, deploy a migration, install a production grant, or authorize spending.

Version 1 remains immutable by design: its receipt binds roots, approval reference and window together. A one-root short-window receipt therefore cannot later expand or renew. Version 2 uses a stable allocation and separately approved grants; it does not edit or reinterpret existing version 1 receipts.

## Two identities, one persistent page history

- Allocation: tenant + unchanged mission fingerprint + epoch 1, with at most 54 distinct legacy roots cumulatively and 3 pages per root. Its hash excludes approval reference, roots selected by an individual grant, and dates.
- Execution grant: exact sorted root subset, approval reference, start/end window and explicit `maxNewPages`. Every changed scope/window/cap requires a new approval reference and a new immutable grant hash. Reusing an approval reference with changed content fails.
- Root/page identities remain the existing `epochRootIdentity` and `pageIdentity`. A renewed grant cannot reopen a reserved, uncertain, blocked, exhausted or three-page-complete chain. It does not reset tickets, attempts, source payloads or any previously consumed page allowance.
- New roots may be explicitly approved in a later grant, but their cumulative mapped union cannot exceed 54. Merely planning an eligible root maps it against that lifetime limit; mapping itself does not reserve a page or call a provider.
- An allocation governs legacy refresh only. Ordinary previously untried mission queries and out-of-scope tenants/fingerprints retain their existing behavior. Derived epoch keys cannot evade grant checks by dropping metadata.

Version 2 uses the existing opt-in epoch configuration input. The exact manifest keys are:
`version` (2), `epoch` (1), `approvalRef`, `tenantId`, `fingerprint`, `rootQueryKeys`, `notBefore`, `expiresAt`, `maxNewPages`.

`maxNewPages` must be an integer from 1 through three times that grant's root count. Times must be canonical UTC timestamps. There is no default approval, automatic renewal, automatic root expansion or environment-setting side effect.

## Atomic reservation and revocation

The store acquires the stable scope advisory lock, then allocation and grant row locks. After waiting, it rechecks grant identity, root scope, time and `revoked_at`, and checks the durable grant-page count. The actual page INSERT independently checks the grant, legacy handled state/version, unchanged root mapping, absence of original-root continuation data and remaining page allowance. Page reservation and grant debit commit together. Lost commit acknowledgement is reported as uncertain; subsequent runs discover the durable reservation rather than refetch.

A grant may be revoked by a separately authorized operator setting its `revoked_at`. Runtime code never clears it or rewrites a grant. Expiry/revocation/config removal gates NEW page reservations, not cancellation of an already-reserved page or every HTTP retry. In-flight work may complete and stage its paid payload. This is not a per-request revocation system.

Fetched payload remains discoverable with permission absent or expired. Existing ticket transfer, deduplication and admission remain unchanged. A malformed enabled configuration still fails the existing discovery-readiness gate: already-ticketed work can drain, but fetched-page transfer waits until the configuration is removed or corrected. No broader recovery behavior is claimed.

Only the provider's existing proof that ALL attempts had no network permits refund. The unused page and its grant debit are removed in one transaction. A cleanup failure rolls both back. Any prior HTTP, details failure, transport uncertainty or unknown outcome retains the consumed page/history. A budget denial after an earlier HTTP retry cannot refund that page.

## Version compatibility and migration

The additive migration creates allocation, grant, root-mapping and grant-page tables. It does not backfill, delete or convert old receipts or inventory. Two small triggers on version 1 receipts and version 2 allocations use the same scoped transaction advisory lock and reject mixed formats, including concurrently executing old binaries. Existing version 1 scopes continue on their unchanged version 1 path; version 2 on such a scope requires a separately reviewed reconciliation rather than guessing. Unmapped historical derived pages/checkpoints also require reconciliation.

The migration is transactional and repeatable for an identical schema, not an incompatible-schema repair. It creates/replaces a trigger function and drops/recreates its two named triggers. Before a future approved migration, inspect name collisions and schema compatibility. Trigger installation takes brief DDL locks on the existing receipt table; use approved bounded lock/statement timeouts and stop on contention. There is no startup or provider-side DDL. Partial/absent schema or missing format guards fails closed for version 2.

No production migration, grants, flags, deployment or paid test is part of this draft.

## What the bounds mean

The54 × 3 page ceiling and existing single-page provider behavior imply conservative structural upper bounds of 486 search attempts and 3,240 details attempts across the finite allocation, assuming unchanged provider code. They are not a new dollar budget, invoice guarantee or permission to spend that amount. Existing shared per-attempt API policy/reservation accounting is unchanged. A future one-page paid test still needs separately reviewed transport/spending controls, current pricing including applicable fees/taxes, explicit activation and approval.

A provider result can rediscover an existing business. Queue admission is not proof of a fresh verified contact. The existing deduplication and contact-verification policies remain authoritative.

## Test coverage

- Strict immutable version 2 manifest validation, stable allocation hash and changed grant hashes.
- Disposable PostgreSQL: concurrent one-page quota, later explicit root expansion, approval-reference reuse rejection, expiry after lock waits, revocation, metadata substitution, cumulative 54-root limit, three-page exhaustion, fetched replay without permission, uncertain commits and orphan-history refusal.
- Mixed-version compatibility with the migration installed; both old-first and new-first concurrent allocation attempts allow only one format.
- Atomic zero-network cleanup and rollback; actual provider HTTP retry followed by budget denial retains its page.
- Actual Scout controller → synthetic Places HTTP → durable page → ticket → admission pipeline: successive one-page grants add new synthetic admissions, repeated grant cannot fetch again, disabled permission replays staged work, fourth grant cannot reset three-page history. Existing version 1 pipeline remains covered.
- Existing checkpoint, continuation, ticket, clean-inventory and epoch regression suites. External providers are denied in local validation.

PR CI routes all new code, migration and tests to the native Scout domain. The PR workflow explicitly runs both new grant suites, and the broad regression workflow runs them with the actual pipeline test. These tests establish code behavior, not live activation or paid-test readiness.
