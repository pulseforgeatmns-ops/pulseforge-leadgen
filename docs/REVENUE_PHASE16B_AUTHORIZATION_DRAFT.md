# Revenue Phase 1.6B — Production Migration and Controlled Anchor Canary

**Status:** new authorization after the blocked 2026-07-21T04:02Z production attempt. Finalized, unsigned, non-executable. Window owners, IDs, keys, and deployment identity are recorded; signatures, approval timestamp, finalization, and post-merge identity/window rebinding remain outstanding.
**Machine-readable draft:** `artifacts/revenue/phase16b-production-authorization-draft.json`
**Implementation report (root cause and fixes):** `docs/REVENUE_PHASE16B_IMPLEMENTATION_REPORT.md`
**Production access or mutation performed while preparing this draft:** none.

## Retired predecessor

Authorization `ce9005f1-6d6f-46fd-a52d-4081a79ed02f` was consumed by the blocked production attempt at 2026-07-21T04:02:56Z (initiating error SQLSTATE 42830 in the phase1 migration; production unchanged) and is **permanently non-reusable**: the validator rejects it outright, and its correlation ID, idempotency keys, window, and signed document must never be reused. Evidence: `artifacts/revenue/phase16b-production-execution-blocked-20260721T0402Z.json`, `artifacts/revenue/phase16b-production-authorization-signed-retired-ce9005f1.json`.

## Finalized values

- Authorization ID: `3808a9f7-b8e4-467f-917f-5021dfb7d485`
- Window (recorded in draft; **re-bind to a fresh future window after the fix PR merges**): `2026-07-21T18:00:00Z` → `2026-07-21T20:00:00Z` (2:00–4:00 PM America/New_York)
- Operator: Jacob Maynard (`jacob@gopulseforge.com`)
- Approving authority: Jacob Maynard, Founder (`jacob@gopulseforge.com`)
- Freeze owner: Jacob Maynard; Rollback owner: Jacob Maynard (`jacob@gopulseforge.com`)
- Release identity recorded in draft: `bf65bb8f4492147b7343240c0d005e58d9ed1acf` / Railway deployment `adb2db69-7888-4fe2-bedb-53b6aca5d647` — **must be re-bound to the fresh post-merge deployment** and the hash recomputed before signing
- Correlation UUID: `bc286f24-f816-41bc-bdd0-a874da7b11eb`
- Eight immutable idempotency keys (all newly generated): recorded in the draft under `canary.operator_only_runtime_values.idempotency_keys`
- Certified phase1 migration checksum (post-fix): `c11740daa17a4d8495daa428134effdffaa0d64d8b343e044739a23d28fa6495`
- Human owner: Jacob Maynard
- Customer email, customer phone, service address, estimated direct cost, and actual direct cost: explicitly approved `null` (unknown; not fabricated)
- Draft canonical hash (recursively key-sorted unsigned-draft binding; final signing hash will supersede it): `d41a233b9a8c91d292404ffac4e23a9b6e69f959abd4c60d03008a66612e90c2`

## Authoritative implementation status

| Component | Status | Path |
| --- | --- | --- |
| Validator | Implemented | `utils/revenuePhase16b.js` |
| Historical-date handling | Implemented | `utils/historicalTimestamp.js` |
| Runner | Implemented | `services/revenuePhase16bRunner.js` |
| Reconstruction verifier | Implemented | `services/revenueReconstruction.js` |
| Execution CLI | Implemented | `scripts/executeRevenuePhase16b.js` |
| Offline finalizer CLI | Implemented | `scripts/finalizeRevenuePhase16bAuthorization.js` |
| Tests | Implemented | `test/revenuePhase16b.test.js`, `test/revenuePhase16bFinalizer.test.js`, `test/disposablePostgresHarness.test.js` |

Earlier exploration conclusions that claimed the validator, runner, or reconstruction verifier were still missing are **superseded**. Implemented components do not authorize execution.

## Fixed authorization scope

This draft is limited to:

1. Freshly validating the protected-main commit, active Railway production deployment, production environment identity, and the three migration checksums.
2. Applying the certified Revenue Phase 1 and Phase 1.5 migrations.
3. Verifying schema, constraints, permissions, append-only triggers, application/database health, and an all-flags-off initial state.
4. Enabling the revenue schema, operator reads, and read-only Max context for Anchor (`client_id: 10`) only.
5. Temporarily enabling Anchor operator writes for exactly one named Eliza Bulger canary outcome.
6. Disabling operator writes immediately after the canary attempt, including on failure.
7. Running recorded compare-only reconciliation and a non-destructive deterministic projection reconstruction comparison.
8. Producing Phase 1.6B certification evidence and stopping.

It never authorizes a refund, second canary, other-client write, autonomous Max mutation, communication, provider activity, follow-up recommendation/send, retention automation, historical backfill, or automatic continuation.

## Confirmed operator facts (ready for finalizer; not in unsigned draft)

```json
{
  "scheduled_start": {"local_date":"2026-07-14","timezone":"America/New_York","precision":"day","operator_confirmed":true},
  "completion_date": {"local_date":"2026-07-14","timezone":"America/New_York","precision":"day","operator_confirmed":true},
  "payment_received_at": {"local_date":"2026-07-14","timezone":"America/New_York","precision":"day","operator_confirmed":true},
  "payment_method": "stripe_card"
}
```

## Remaining operator-only values before signing

- operator signature/attestation;
- approving-authority signature/attestation;
- actual UTC approval timestamp at signing (defaults to current UTC in the finalizer unless supplied);
- final canonical authorization hash recomputed after signatures and all values are fixed;
- a **fresh future** two-hour UTC execution window (do not reuse the recorded draft window for a live execution).

## Remaining production gates

1. Root-cause-fix PR merged through protected `main` with mandatory `revenue-postgresql-required` passing on the exact release commit. Under the certified solo-founder policy, **zero approvals** are required; independent human review is **not** a required gate.
2. Fresh Railway deployment observation after the merge; re-bind the draft's release identity and window and recompute the unsigned hash.
3. Offline finalization producing a separate signed authorization file.
4. Fresh protected-main / Railway identity observation immediately before execution.
5. `REVENUE_PHASE16B_PRODUCTION_ENABLED=true` only after signed validation inside the active window.
6. Runner only via `scripts/executeRevenuePhase16b.js --production --confirm=3808a9f7-b8e4-467f-917f-5021dfb7d485`.

## Offline finalizer

```bash
npm run revenue:phase16b:finalize -- \
  --operator-attestation-file <path> \
  --approver-attestation-file <path> \
  --scheduled-date 2026-07-14 \
  --completion-date 2026-07-14 \
  --payment-date 2026-07-14 \
  --payment-method stripe_card
```

The finalizer loads the immutable unsigned draft, rejects any altered authorization ID / correlation ID / idempotency key / financial value / prohibition / stop condition, validates day-level historical precision, writes a separate signed file, recomputes the canonical final hash, validates offline without database connectivity, and never modifies the unsigned draft in place.

## Exact execution prompt (use only after offline finalization and a fresh future window)

Do not submit this prompt with any placeholder remaining. Do not choose or reuse an execution window until signing is intentionally scheduled.

```text
Finalize (offline), then validate and execute only Revenue Phase 1.6B — Production Migration and Controlled Anchor Canary using:

Unsigned draft (immutable; do not edit in place):
artifacts/revenue/phase16b-production-authorization-draft.json
Authorization ID (fixed): 3808a9f7-b8e4-467f-917f-5021dfb7d485
Unsigned-draft canonical hash: d41a233b9a8c91d292404ffac4e23a9b6e69f959abd4c60d03008a66612e90c2
Retired authorization (never reuse): ce9005f1-6d6f-46fd-a52d-4081a79ed02f

Offline finalization first:
npm run revenue:phase16b:finalize -- \
  --operator-attestation-file <OPERATOR_ATTESTATION_FILE> \
  --approver-attestation-file <APPROVER_ATTESTATION_FILE> \
  --scheduled-date 2026-07-14 \
  --completion-date 2026-07-14 \
  --payment-date 2026-07-14 \
  --payment-method stripe_card

The draft window and release identity must already have been re-bound to the fresh post-merge deployment and a fresh future two-hour UTC window (recomputing the unsigned hash) before finalization. Never reuse the retired ce9005f1 authorization, its window, correlation ID, or idempotency keys.

Use only `scripts/executeRevenuePhase16b.js --production --confirm=3808a9f7-b8e4-467f-917f-5021dfb7d485`. Do not use the tenant-remediation Phase 16B scripts. The production runner must remain gated by `REVENUE_PHASE16B_PRODUCTION_ENABLED=true`; never set that gate before the signed authorization has passed final validation inside the active window.

Before any production connection or mutation:
1. Re-read and validate every signed-authorization field. Fail on null, placeholder, signature, hash, or two-hour-window errors.
2. Re-derive current protected main, the active successful pulseforge-leadgen Railway production deployment, image/environment identity, and all migration SHA-256 values.
3. Require deployed commit == protected-main commit == the approved release commit. Stop on any drift.
4. Revalidate Phase 1.6A durable backup file/folder IDs, size, SHA-256, independent verification, retention, Keychain presence, isolated restore result, and evidence hashes.
5. Confirm Anchor is client 10, Scout-only, autosend disabled, health is normal, and all revenue environment flags are off.
6. Validate the signed authorization hash and active window before opening a production database connection.

Inside the active UTC window only:
1. Capture a read-only production identity and baseline evidence.
2. Apply only the certified Phase 1 and Phase 1.5 migrations with their approved checksums. Do not edit or concatenate SQL.
3. With all flags still off, verify schema, constraints, foreign keys, permissions, append-only trigger, indexes, no unexpected events/outcomes, active connections, application routes, and database/application health.
4. Enable revenue schema, operator reads, and read-only Max revenue context for client 10 only. Keep operator writes and follow-up recommendations off. Verify all other clients remain disabled.
5. Run baseline compare-only checks and bind the fixed deterministic-reconstruction event boundary.
6. Temporarily enable operator writes for client 10 only.
7. Execute exactly one Eliza Bulger lifecycle with the fixed facts in the signed authorization:
   - new residential customer, prospect_id null, source yelp, attribution confirmed;
   - service main-bedroom pet-spray inspection/treatment;
   - booked/delivered/collected 15000 cents; refunded 0; manual succeeded payment;
   - payment method stripe_card;
   - historical dates 2026-07-14 America/New_York day precision (derived local noon, clock_time_observed false);
   - exact approved job note;
   - exactly one outcome and exactly 12 ledger events.
8. Disable client-10 operator writes immediately after the canary attempt, including on failure. Prove writes are off before continuing.
9. Run:
   npm run revenue:rebuild -- --client-id=10 --compare-only --record
   Require exact integer-cent equality, one projected/source outcome, exactly 12 ledger events, zero mismatches, and zero unexplained events.
10. Run the non-destructive projection reconstruction comparison twice against the same fixed event boundary. Canonicalize away run IDs and timestamps. Require both reconstruction hashes to equal each other and the persisted projection hash.
11. Verify read-only Max context, operator audit, idempotency, append-only behavior, final flags, no other-client writes, and application/database health.
12. Produce redacted Phase 1.6B certification evidence and a COMPLETE or BLOCKED verdict. Do not continue automatically to any later phase.

Fixed prohibitions:
- no refunds;
- no second canary or replay that creates another effect;
- no writes for any client other than 10;
- no autonomous Max mutations;
- no external communications or provider activity;
- no follow-up recommendations or sends;
- no retention automation or historical backfill;
- no automatic continuation beyond Revenue Phase 1.6B.

Stop immediately on authorization/window/identity/checksum drift; any one-cent discrepancy; event count other than 12; outcome count other than 1; tenant mismatch; duplicate effect; append-only violation; unexplained event; source/ledger/projection mismatch; nondeterministic reconstruction; external side effect; failure to disable writes; ambiguous migration result; or application/database degradation.

Return:
- finalized authorization and validation result;
- deployment/environment and migration identity;
- bound Phase 1.6A backup/restore evidence;
- migration and post-migration structural/health evidence;
- complete Eliza lifecycle identifiers and 12-event evidence;
- operator-write disablement evidence;
- reconciliation and deterministic reconstruction evidence;
- final flag and no-side-effect evidence;
- all files/artifacts created;
- final verdict COMPLETE or BLOCKED.
```

## Current disposition

The draft carries its immutable authorization ID, named owners, recorded deployment identity, correlation UUID, all eight idempotency keys, Jacob Maynard as human owner, approved-null contact/address/cost fields, and an unsigned-draft canonical hash. It still has no signatures or approval timestamp. Confirmed dates (`2026-07-14`) and payment method (`stripe_card`) are ready for the offline finalizer and are not present in the unsigned draft. `approved`, `production_execution_permitted`, and `executable` remain `false`. No execution window has been chosen for live use. It cannot authorize production access or execution.
