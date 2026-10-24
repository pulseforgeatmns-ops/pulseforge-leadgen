# Scout website-evidence cursor: local implementation review

This implements the bounded continuation proposed in `scout-evidence-cursor-design-20261018.md`. That earlier document records the diagnosis and pre-implementation review; this document specifies the implemented contract. Runtime publication and native PostgreSQL execution were not performed during local preparation. The final local integration is based on main `f7bc4b2ca54d9db10ae4d265cf4940549d2949ed`, which already contains the approved preparation-gate diagnostics and tenant recovery guard.

## Result and scope

The governed Scout persistence callback can now rotate its existing 20 website investigations across currently unowned, unclassifiable normalized business identities. Candidate arrays, main-loop processing order, query checkpoint payloads, search fingerprints, query plan versions, discovery limits and acknowledgment conditions are unchanged. The cursor is not a claim of new inventory, qualified contacts, exhausted queries or exhausted markets.

Only `OWNERSHIP_KINDS.CLEAR` candidates enter cursor selection. A read-only ownership prepass checks them, and the original loop independently rechecks ownership immediately before the website branch. Failed or unknown ownership never earns a slot. Duplicate normalized-domain aliases share one selection key and one actual investigation slot; source rows are not deduplicated or reordered. Tenant bindings in the store, Scout context, governed context, transport and program must agree, and a conflicting candidate tenant fails closed.

The cursor selects a stable ring of up to 20 SHA-256 identity keys after the durable endpoint. It records the ring endpoint rather than the last main-loop callback. Removed endpoints, changed candidate sets and wrapping remain safe. The constant cursor version is independent of the existing query-plan version; no query checkpoint is invalidated or refetched by this change.

## Durable storage and concurrency

`services/scoutBusinessEvidenceCursor.js` uses the existing `acquisition_outbound_events` table, without a migration. Every read and confirmation is bound to exact tenant, existing search fingerprint and cursor version. Deterministic event IDs hash event type, tenant, fingerprint, cursor version and sequence. Receipts contain a monotonic sequence and predecessor; timestamps do not determine causal order.

1. Validate primitive tenant and fingerprint inputs before SQL. Read and validate the current receipt using the ordinary pool, without retaining a client.
2. Run the existing persistence callback with the selection predicate. Keep ordinary website, ownership, ingestion and already-classifiable recovery behavior on its existing pool. A callback interruption closes the predicate and does not append a receipt.
3. After callback success, acquire a client and start a short transaction. Try a scope-specific advisory transaction lock in namespace `261021`, distinct from the existing outbound lock `261020`.
4. On the same client, reread the receipt and require its sequence to equal the snapshot predecessor. Append the next deterministic event, read its exact scoped ID back, and validate all receipt fields. Only a confirmed successful COMMIT reports advancement.
5. Busy locks, stale snapshots, corrupt receipt data, failed insert/confirmation, or uncertain commit outcomes throw. CandidateUniverse then retains fetched checkpoints and reports persistence failure. Roll back where possible; destroy the client when cleanup cannot be confirmed. A restart reads any durable receipt after an uncertain commit rather than resetting to zero.

The full callback intentionally does **not** hold a cursor client. Existing same-company recovery reaches helpers that close over the shared DB pool; pinning an extra connection throughout the callback could deadlock small/saturated pools. Short compare-and-set transactions avoid introducing that dependency. Concurrent direct callers may repeat bounded work from one snapshot, but a delayed writer cannot overwrite one or more newer receipts. This is not an exactly-once website-fetch guarantee.

## Recovery and retry boundary

This patch depends on the separately reviewed tenant-10 alternate-contact recovery guard (`2847b7340ed53c32e907cee6ae9458815ba20dac`, local cherry-pick `e5505a2e`). That guard is necessary because recovery currently contains tenant-10-specific SQL. The cursor also enforces a stricter independent no-expanded-recovery invariant: ambiguous same-company candidates are deferred instead of receiving newly rotated website evidence. Already-classified same-company recovery stays in its original loop position. Tests compare recovery-call identities against baseline, including within correctly bound tenant 10.

This intentionally narrows website investigation for ambiguous existing-company candidates, including those previously in the first 20. It does not mark them handled; they remain pending for a separately authorized recovery design. Queue duplicates and `attempt_limit_hold` rows retain their original attempt and cooldown state. The patch neither resets attempts nor relaxes admission, geography, ownership, quota or provider controls.

## Observable progress

The existing sanitized query-progression log/event and control-cycle diagnostic now expose only a fixed status, selected count, investigated count, confirmed advancement and sequence. The statuses distinguish `not_observed`, `no_candidates`, `advanced`, `busy`, `stale` and `failed`. Candidate keys, raw fingerprints, websites and contacts are not included in these diagnostics. The cursor receipt remains separate from checkpoint acknowledgment; unresolved evidence or deferred recovery still prevents whole-batch acknowledgment.

## Verification and release gate

Pure/mock tests cover 21/41/157 identities, duplicate aliases, changed candidate sets, scope/version isolation, stale overlapping writers, conflicting confirmation, lock contention, unknown commit outcomes, closed pass handles, partial ingestion crashes, acknowledgment failure, queue replay, tenant conflicts, no added recovery calls, max1 pool availability and sanitized failure diagnostics. The baseline diagnosis/model is retained separately.

The authored native test uses only a disposable local PostgreSQL cluster and extracts the actual existing event-table DDL from its migration. It covers scope/version isolation, tied timestamps, causal sequences, overlapping/stale writers, max1 callback queries, real separate-connection advisory-lock contention, failed confirmation rollback and restart. It does not read an environment database URL. Native PostgreSQL was syntax-checked only here; that is not a database integration pass.

The outbound domain CI selector now includes cursor runtime/tests/helper and the diagnosis fixture. Its explicit steps run the pure/mock cursor suites with the network-denial preload and the native suite with `SCOUT_EVIDENCE_CURSOR_REQUIRE_POSTGRES=true`. Missing PostgreSQL prerequisites fail this required gate rather than silently skipping. No other workflow domain or policy is changed.

Local verification commands and final results are recorded with the implementation handoff. Required release evidence is a green native PostgreSQL gate on the published commit, followed by separately authorized production verification. This change does not promise to clear all pending queries or produce new leads if the remaining evidence stays unclassifiable. The append-only event lookup currently uses the existing tenant/time index; no new performance index or retention policy is introduced in this bounded patch.


### Final local offline command

On the original implementation branch, 224 tests passed. On the final integration with main `f7bc4b2`, the same command passed 269 tests, 0 failed and 0 skipped, including the newly merged preparation-gate diagnostic cases. All network access was denied by the preload:

```sh
NODE_PATH=../spreadsheet_reconcile/node_modules DATABASE_URL=postgresql://localhost:1/pulseforge_test_only DATABASE_SSL=false node --require ./test/helpers/denyWebsiteEvidenceNetwork.cjs --test test/scoutBusinessEvidenceCursor.test.js test/scoutBusinessEvidenceCursorRamp.test.js test/scoutCheckpointStallDiagnosis.test.js test/scoutQueryCheckpointRamp.test.js test/scoutQueryCheckpoints.test.js test/sameCompanyContactRecovery.test.js test/scoutReplenishmentTenantGeneralization.test.js test/outboundControlDiagnostics.test.js test/ciPlanPr.test.js test/scoutPromotionRetention.test.js test/scoutQueueAccounting.test.js test/outboundInventory.test.js test/maxScoutReplenishment.test.js
```

`node --check test/scoutBusinessEvidenceCursorPostgres.test.js` passed without running the file. The required future-CI command is `SCOUT_EVIDENCE_CURSOR_REQUIRE_POSTGRES=true node --test test/scoutBusinessEvidenceCursorPostgres.test.js`; it was not run locally.
