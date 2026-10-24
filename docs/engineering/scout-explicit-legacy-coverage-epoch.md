# Explicit legacy Scout coverage epoch: local proposal and release boundary

## What this repairs

A candidate-only historical query checkpoint cannot reconstruct a missing Places next-page token. After its raw candidates have transferred to durable tickets, epoch 1 can reacquire one explicitly selected original query and retain the resulting continuation chain. The tenant and mission fingerprint stay the same. Original candidates, ticket memberships and attempt history are retained. Existing deferred or blocked candidate work does not prevent the query's durable handoff, and stays independently drainable.

This is finite replenishment, not a promise of unlimited daily inventory. Three pages per approved root is the hard provider-page ceiling. Empty pages, absent next tokens, the third page, failed provider outcomes and uncertain outcomes all stop further acquisition. Candidate queue admissions still need downstream owner attribution, exact-email verification and clean-inventory policy checks.

## Default-off activation contract

The existing durable-candidate gate and tenant 10/13 allowlist remain prerequisites. The additional gate is `SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED=true`, with a strict JSON object in `SCOUT_LEGACY_COVERAGE_EPOCH_JSON` containing exactly:

- `version`: numeric 1
- `epoch`: numeric 1; no other epoch is implemented
- `approvalRef`: the reviewed release/approval reference, 1–128 alphanumeric, period, underscore, colon or hyphen characters
- `tenantId`: exact string `10` or `13`
- `fingerprint`: the existing 64-character lowercase SHA-256 mission fingerprint
- `rootQueryKeys`: 1–54 distinct, exact 64-character lowercase SHA-256 original query keys
- `notBefore` and `expiresAt`: UTC ISO timestamps with milliseconds, with expiry strictly later

No example production hash or timestamp is supplied. Live mission/query identities are unavailable to this local task. A valid manifest belonging to another tenant/fingerprint grants no permission to this run. A malformed enabled manifest blocks acquisition while existing transferred tickets continue draining. The configuration is an enforcement mechanism; it does not itself constitute human approval for spending or production changes.

## Safe natural-run approval material

`createCheckpointSession().snapshot().coverageApprovalManifest` projects actually observed original checkpoint roots. It includes tenant, mission fingerprint and query hashes only, a maximum of 54 roots, an explicit `truncated` field, `observedOnly`, `legacyEligibilityRequiresRecheck`, and the maximum cumulative request allocation. It contains no query text, candidates, website/email data or continuation tokens. Observed roots can include roots that fail the later legacy-eligibility check. A projection is neither an activation manifest nor evidence that the complete market is exhausted.

Obtain this projection through the approved natural runtime diagnostics after the default-off release. There is no new production database read/export in this proposal. The prior 21/54-query logs do not provide the exact hashes needed to approve activation; fixture hashes must never be substituted.

## Cumulative allocation and immutable receipt

One scope receipt, keyed by the unchanged tenant/fingerprint, pins the complete canonical manifest and window. The first eligible root lookup creates it, even when that cycle has no remaining page allowance. Concurrent different manifests have one winner. A later disjoint root list, extended window or different approval reference cannot enlarge or renew this epoch. Root mappings must match that receipt at creation and again at reservation.

Each approved root has at most 3 pages. Each page has at most 3 Text Search HTTP attempts (including its two possible retries) and 20 Details HTTP attempts. Therefore the upper bound is 69 HTTP attempts per root: 9 searches plus 60 details. Examples are 1 root = 3 pages/69 attempts, 21 roots = 63 pages/1,449 attempts, and the maximum 54 roots = 162 pages/3,726 attempts. These are code-derived ceilings, not provider pricing or an authorization to spend. Existing shared provider cost/request reservations and the one-new-page-per-acquisition-session limit remain binding and may stop work much earlier. Every attempted request, including failures and retries, retains its existing spend accounting.

Approval windows must be sized for the intended ordinary schedule and budget. An unfetched root already mapped during planning is still bound to that original window. Expiry permanently stops new epoch1 reservations; there is no automatic renewal, rolling epoch or fingerprint reset. A further repair would require a separately reviewed design and approval.

## Crash, expiry and rollback behavior

Saved fetched pages remain discoverable through the durable mapping after the manifest expires or is removed. They can transfer to tickets without new HTTP. Original fetched checkpoints transfer first, even when current epoch permission is missing or expired. Reserved/uncertain or failed pages remain held; there is no automatic retry of an uncertain outcome.

The existing page-store exception releases only an unused reservation after a proven pre-network budget denial. It never releases an attempted/uncertain page, epoch receipt, legacy row, fetched payload or ticket history. The session's one-page allocation is still consumed by that denied attempt. Later valid-budget cycles may try the same unused page identity.

The additive migration is `2026-10-19-scout-query-coverage-epochs.sql`. Both new tables absent is supported while epoch config is off, preserving the prior durable rollout. Either partial schema, permission error or connection error fails closed; active config requires the migration. Rollback removes activation permission and retains schema/rows so staged payloads and candidate tickets can finish draining. Do not delete rows, change mission fingerprints or rotate epoch IDs.

## Local evidence and its limits

`test/scoutCoverageEpoch.test.js` exercises strict default-off parsing, exact scope, immutable canonicalization and bounded sanitized diagnostics. `test/scoutCoverageEpochPostgres.test.js` uses disposable PostgreSQL to exercise 21 old roots, durable handoff, unchanged raw/attempt history, receipt/mapping races, one-page session allocation, expiry/removal, zero-network budget denial, incompatible legacy state/version, crafted reservation identity and optional/partial schema handling. These store tests deliberately stage synthetic payloads; they alone do not prove provider-to-admission flow.

The separate integration fixture `test/scoutDurableEpochPipelinePostgres.test.js` composes the actual runtime, Places provider, page/ticket stores and queue admission with synthetic provider responses. It begins with 21 old roots, selects one exact fixture root, admits 19, 20 and 20 fresh businesses in three restarted cycles, while a repeated original identity keeps its previous handling-attempt count, then proves no repeat HTTP at finite exhaustion. Its crash case transfers a paid staged page after config removal. `test/scoutDurableCleanInventory.test.js` separately composes actual owner extraction, enrichment selection, promotion and clean classification with synthetic sources/verifier inputs. Neither fixture certifies live yield, actual paid enrichment/verifier behavior or total production spend.

## Approval sequence

1. Review the final combined code, required PostgreSQL/offline tests and migrations. Approve any publication/deployment separately.
2. Deploy default-off and apply additive migrations only under the approved release plan. Capture sanitized natural diagnostics; no denied database access is bypassed.
3. Review the exact tenant, existing fingerprint, selected original root keys, epoch1, approval reference, activation window, cumulative ceiling and provider spending limit. Start with a small explicitly approved subset if desired.
4. Activate only that approved manifest. Verify natural provider-call accounting, backlog, actual owner/verification yield and finite-stop statuses.
5. Remove permission to stop new reservations while preserving staged-page recovery and old ticket drain. Expired or exhausted allocations are not automatically refreshed.
