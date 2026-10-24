# Durable Scout candidate integration (local validation, 2026-10-19)

## Status and activation

Production-wired implementation, locally validated only. No production migration, provider call, publication, or deployment was performed. Activation remains default off: exact `SCOUT_DURABLE_CANDIDATES_ENABLED=true` and resolved tenant **10 or 13** are both required. Other tenants and flag-off behavior retain the prior path. This document supersedes the Oct 18 unwired event-protocol prototype for implementation purposes; that prototype remains historical design material.

Required additive migrations, in order:
1. Existing `2026-10-17-scout-query-checkpoints.sql`.
2. `2026-10-19-scout-candidate-tickets.sql` (two current-work/membership tables, indexes, transferred-payload retention trigger).
3. `2026-10-19-scout-query-pages.sql` (bounded page reservations and immutable continuation metadata).

The ticket and page migrations are rerunnable and transaction-wrapped. Apply only through the separately approved release workflow. Runtime performs read-only readiness checks and fails closed before new discovery when either new schema is absent. Existing enrichment/recovery that precedes discovery is unchanged. Quiesce old discovery workers before activation; the trigger protects already-transferred payloads from a late old-binary acknowledgment, but cannot recover legacy payloads erased before transfer. Roll back by disabling the flag and draining active workers; retain ticket/page/membership data and the payload-preservation trigger. Do not drop tables or reset queue attempts.

## Responsibility and crash boundaries

A short transaction locks the exact tenant/fingerprint/query/plan row, copies every original JSONB observation into exact query membership, and marks query responsibility transferred. It never erases saved payloads. Candidate identity is exact Google place ID where present; otherwise exact canonical raw payload. There is no guessed domain-based identity in the durable store. Tenant aliases in raw data must all match. Plan and ticket versions are explicit scope keys.

Candidate processing separately claims at most 100 due work items with `FOR UPDATE SKIP LOCKED`. Each claim persists a 15-minute lease and at least 1-hour crash reservation before work. No transaction or connection remains pinned during websites or global-pool helpers. Renew/finish validate token, exact complete member set and membership revision; stale workers cannot perform queue-write callbacks. The queue insert and completion receipt share a short transaction. Sorted domain/name advisory locks protect the existing OR-dedup predicate across different place IDs. An ambiguous commit can be retried without repeating the callback. No attempt/cooldown row is reset.

Material newly observed business facts can reopen blocked/handled work. Adding another query label, timestamp or identical observation cannot. Raw memberships always preserve provenance even when not new business facts. Conflicting domains or observed locations are held for identity repair, including cross-ticket aliases. Missing old fields can be supplied by a consistent new observation under the same exact provider identity. Ownership and business admission are still rechecked; saved query labels cannot substitute for business evidence. New Places payloads explicitly record the actual discovery query; legacy Google Places `industry` fields are reconstructed as query provenance without mutating the original payload.

## Independent dispatch and budgets

The dispatcher runs even when query discovery is backed off, returns no candidates, is exhausted, or fails. A single dispatcher attempt per control cycle prevents a persistence failure from causing a second provider pass. One shared website selection has the existing 20-investigation cap (each crawl retains its existing three-successful-page/six-request ceiling). At most one new Places page per control cycle is reserved across all workloads: at most 20 detail requests plus the unchanged search retry ceiling and existing API-spend reservation/settlement path. A second enrichment pass remains conditional on newly queued rows and uses the existing limits; tickets never create a second discovery/enrichment budget.

This is finite approved coverage. Up to three pages per actual approved query can advance only after prior raw-page responsibility transferred. Saved raw page results and next token publish atomically. Unknown reservation outcomes and invalid/denied continuation stop instead of automatically refetching. Legacy checkpoints without saved next tokens report `legacy_continuation_unknown`; no token, freshness query, geography, mission expansion or market exhaustion is invented.

Completed/blocked page checkpoints are durable provider backoff. Every bounded control cycle may read their status but performs no repeat paid call. When all observed queries have an actionable terminal/dependency status, a tenant/fingerprint/status-deduplicated `tick_blocked` event requests review of the remaining approved plan or recorded provider dependency. This is explicitly an **observed query set** report, never whole-market exhaustion. Due candidate tickets still drain.

## Outcome policy and remaining dependencies

- Successfully queued work is handled only with atomic confirmed queue persistence. Existing eligible rows count as confirmed queue responsibility without resetting attempts.
- Existing queue cooldowns use the database's actual one-hour/seven-day boundaries. Missing retry timestamps and unsupported verticals remain explicit dependency holds.
- Complete successful website crawls with no admissible facts become blocked `unresolved_business_evidence`. They do not automatically recrawl daily. Material evidence/version/dependency repair can reopen them. This is retained unresolved work, not terminal exclusion.
- Only explicitly transient HTTP/network outcomes receive `transient_business_evidence_failure` with a 24-hour retry. Partial/permanent/unknown crawl outcomes become `business_evidence_dependency`; no failed/absent regex match is terminal. Opt-in structured outcomes do not change legacy callers.
- Contact-policy and mutable ownership observations are reconsidered by a bounded 24-hour **read-only** check. Current holds are checked before websites. They are not permanent terminal proof of current contact eligibility.
- Durable alternate-contact recovery is deliberately held: tenant 13 `recovery_tenant_unsupported`; tenant 10 `recovery_reservation_required`. The legacy helper spends and writes outside the ticket fence. A future business-scoped provider reservation plus fenced canonical write is required before tickets may activate it. Other existing recovery paths are unchanged.
- Incomplete or conflicting identity/geography is held; positive evidence-backed business-type/segment exclusions may be terminal. No owner qualification, email verification, provenance or contact repair is fabricated.

`scoutCandidateWork` is returned in the control summary, persisted, and emitted as a sanitized log with due/claimed/state/reason counts and processing-failure status. Page diagnostics expose only known reason counts and the one-page allocation; tokens/raw payloads never enter logs or cost attribution.

## Local evidence

- Store tests use real disposable PostgreSQL for concurrent transfer/claims, exact JSONB numbers, ambiguous COMMIT, backend termination/rollback, version/tenant fencing, raw-binding aliases, immutable legacy acknowledgment, ref mutation, changed evidence and unchanged-query no-rework.
- Runtime tests use real PostgreSQL, actual durable dispatcher, website extractor and queue admission.21 unresolved candidates consume 20+1 investigations, then fresh distinct candidates continue across restarted cycles. Backoff, failed discovery, concurrent ramps, held queue clocks, identity repair, geography conflict and no timed recrawl are tested.
- `scoutDurablePipelinePostgres.test.js` runs the actual approved 21-workload coverage plan, Places facade (synthetic HTTP), checkpoint/page/ticket stores, CandidateUniverse and governed queue for 63 restarted acquisition cycles. It admits 1,259 distinct eligible queue businesses and retains one unresolved candidate; three subsequent exhausted cycles have zero provider calls. Exact simulated accounting:63 text searches,1,260 detail requests,1,323 spend reservations, estimated 34.02 USD using existing estimator, and one website investigation. These are **simulated costs**, not incurred spend.
- Queue-stage flow is not verified-contact quality or observed live clean-inventory yield. The separate controlled qualification simulation and live rollout proof must be reported independently.
- `scoutDurableCleanInventory.test.js` uses actual promotion and clean-inventory filtering with synthetic owner and exact-address verification responses. The actual enrichment selector retains attempt-limit, cooldown, unsupported-vertical and other-tenant rows untouched. Three restarted cycles each add one eligible founder: 12 CRM promotions produce only 3 clean contacts. Employee, role, unverified, changed-address, suppression, ownership and DNC cases remain excluded. Three exhausted cycles repeat no Places, enrichment or verification calls. This validates pipeline mechanics, not live owner availability or verifier quality.

Required local command (available PostgreSQL binaries on PATH):

```sh
SCOUT_CANDIDATE_REQUIRE_POSTGRES=true SCOUT_CHECKPOINT_REQUIRE_POSTGRES=true node --test \
  test/scoutCandidateTickets.test.js test/scoutCandidateTicketsPostgres.test.js \
  test/scoutCandidateTicketDispatchPostgres.test.js test/scoutDurablePipelinePostgres.test.js \
  test/scoutQueryContinuationPostgres.test.js
node --require ./test/helpers/denyWebsiteEvidenceNetwork.cjs --test \
  test/scoutCandidateTicketDispatch.test.js test/scoutQueryContinuation.test.js \
  test/scoutSmallBusinessWebsiteEvidence.test.js
```

CI explicitly requires disposable PostgreSQL and does not accept skipped gates. Real live sustained yield remains unproven and requires separately approved deployment/migrations/activation and bounded observation across cycles. Monitor distinct new verified clean inventory, repeated identities, each provider's reserved/actual calls and estimated spend, unresolved dependency counts and unchanged queue attempt/cooldown values.

### Recorded local gates

- Required disposable-PostgreSQL integration command: 38 passed, zero skipped (store, dispatcher, complete coverage pipeline, controlled clean-inventory simulation, existing checkpoint and page contracts).
- Network-denied affected evidence/protocol regressions: 320 passed, zero skipped.
- Governed outbound/Babrun regression command with external transports denied and literal-loopback test DB: 218 passed, zero skipped.
- Bounded diagnostic/stall regression command: 77 passed, zero skipped.

Counts are per command and overlap; they are not a distinct-test total. The complete pipeline uses synthetic HTTP and exact fixture owner/verifier responses. Initial broad-suite guard runs rejected a localhost-named test DB; rerunning with a literal 127.0.0.1 test URL passed, without permitting external DNS or providers.


## Explicit one-time legacy coverage epoch (separate local follow-on)

An additional default-off path now permits a specifically approved first-page reacquisition of exact legacy roots that cannot continue because their next tokens were never saved. It requires `2026-10-19-scout-query-coverage-epochs.sql`, the durable gate above, `SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED=true`, and a valid `SCOUT_LEGACY_COVERAGE_EPOCH_JSON` manifest. The manifest binds version 1 / epoch 1, a bounded approval reference, one exact allowed tenant, the existing SHA-256 mission fingerprint, at most 54 exact root hashes, and explicit UTC start/expiry timestamps. A valid manifest for another tenant/fingerprint grants no permission to this run. Malformed enabled permission blocks acquisition with a sanitized actionable event while already-owned tickets still drain. Existing enrichment and ticket evidence budgets remain active; the epoch switch is not a global provider kill switch.

A scope-wide receipt pins the entire root list and window on the first eligible lookup; a later disjoint root list or extended window cannot expand the same epoch. This planning receipt may precede the first paid request. The controller does not change the mission fingerprint, original query rows, original payloads, membership, candidate responsibility, or queue attempt/cooldown history. Fetched legacy payloads transfer first. A deterministic epoch 1 root is mapped to the original root only after the original is handled and has no current page metadata. Mapping/hash/window are immutable through runtime; no automatic epoch rotation, renewal, or permission extension exists. Page reservation rechecks permission and exact legacy scope. The existing one-new-page-per-cycle limit and provider spend reservations still apply. At most 3 pages per selected root, 3 search attempts per page, and 20 detail attempts per page produce a maximum 69 Places HTTP attempts per selected root; successful no-retry fixture pages use 63. These ceilings exclude separately bounded existing website/enrichment/verification work and do not authorize production spending.

An immutable mapping remains readable when the manifest expires or is removed. A saved fetched epoch payload can still transfer to the same candidate scope with no new HTTP. Only new epoch reservations require active matching permission. Unknown paid outcomes remain held. Disabling the whole durable feature stops its dispatcher as before; retain all tables and the transferred-payload retention trigger on rollback.

Natural query-progress telemetry now includes a sanitized `coverageApprovalManifest` proposal using only persisted original roots actually observed in that cycle, sorted SHA-256 identities, tenant/fingerprint, an observed-only/truncation marker, and derived cumulative ceilings. It is not authorization and does not prove all historical roots were observed or are legacy-eligible. Epoch reservation independently rechecks legacy eligibility. No raw queries, candidates, tokens, arbitrary approval references, or secrets enter this projection. Production approval must identify the exact proposed root list, tenant/fingerprint, time window and aggregate provider allocation; no production manifest has been activated by this work.

`scoutDurableEpochPipelinePostgres.test.js` exercises the actual 21-workload coverage plan starting with 21 fetched legacy roots. Its synthetic manifest selects one exact root. Existing candidates transfer before reacquisition; three new pages add 19, 20 and 20 previously unseen businesses across restarted cycles. A deliberately repeated first-page legacy identity remains at one candidate handling attempt while retaining new query provenance. Repeated exhausted cycles make no provider calls. A second crash scenario saves raw epoch results before a simulated transfer failure, removes permission, then transfers them without HTTP and drains another original-scope ticket even under malformed permission. `scoutCoverageEpochRamp.test.js` tests runtime gating, out-of-scope behavior, invalid-permission drain and telemetry redaction. These are local simulations, not evidence of sustained live contact yield.

The epoch does not repair the existing multiword-geography query compiler. Its observed root identities are the actual current queries, not a newly interpreted market. Before approving a Babrun nationwide refresh, confirm those roots represent the approved geography; the known `United States` truncation remains a separate query-generation dependency. Do not treat the Manchester-based fixture as validation of that nationwide plan.


Final combined epoch gates: 61/61 required PostgreSQL and protocol tests, 329/329 network-denied affected regressions, and 82/82 diagnostic/controller regressions passed with zero skips. Counts overlap. The epoch fixture uses the tracked PostgreSQL pool teardown helper. Saved raw epoch pages replay on the next normal acquisition pass; discovery backoff may delay that lookup until its existing retry boundary. Already transferred candidate tickets continue draining during backoff. No new pending-page scanner or provider budget is introduced.
