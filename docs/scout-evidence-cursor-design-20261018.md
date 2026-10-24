# Scout evidence progression: historical bounded design review

Base: `bf236b662f946a0c08764cd59f3cfa82f70eb1fb`. This records the original diagnosis and model review before runtime implementation. For the subsequently implemented contract, see `scout-evidence-cursor-implementation-20261018.md`.

## Confirmed cause

- `services/maxOutboundControlLoop.js:629-641` re-investigates the first 20 unclassifiable candidates in the same resolved order. It remembers neither a visited candidate nor a continuation cursor. Any unresolved candidate sets the whole batch's `checkpointComplete` to false.
- `packages/max/scoutAcquisition/CandidateUniverse.js:407-415` only acknowledges all observed staged queries after that single completion flag and zero discovery errors. An unrelated blocked candidate can keep an empty query pending.
- Recovery cooldown/failure also blocks the whole batch (`maxOutboundControlLoop.js:651-673`). Queue duplicates, including `attempt_limit_hold`, do not independently block checkpoint completion. Resetting enrichment attempts is not a checkpoint fix.
- `services/scoutQueryCheckpoints.js` stores immutable fetched results and all-or-nothing acknowledgment. It has no cursor storage field. Do not append metadata objects to the candidates array, mutate fetched hits, or change `PLAN_VERSION` just to rotate work.

## Smallest next deliverable

Implement and review a website-evidence selection cursor. Do not change acknowledgment, provider query generation, query/page limits, retry controls, classification standards, geography, or recovery authorization. The cursor provides fair examination of already-fetched candidates; it does not guarantee qualified inventory or query-plan exhaustion.

1. Derive a deterministic candidate key from normalized business identity using existing canonical identity rules. Use a versioned hash in progress metadata. Deduplicate keys, but do not merge or remove actual companies or their observations. Enforce at most one actual website investigation per selected normalized identity within a pass, with an explicit consumed-key set or equivalent proof. Deduplicating selection keys alone is insufficient: duplicate raw company rows could otherwise consume the 20-call cap before every selected identity is reached and then be skipped by cursor advancement.
2. Load the latest cursor under exact `tenantId + searchFingerprint + cursorVersion`. Select at most 20 unresolved evidence candidates in stable key order beginning strictly after the cursor and wrapping once. Keep the original companies array and main processing order unchanged. A predicate at the existing website-investigation branch chooses the selected identities. Existing ownership and admission checks still apply before any request.
3. After the full persistence callback succeeds, append a single durable progress receipt to `acquisition_outbound_events` with the ring endpoint, fingerprint and cursor version. This existing table has an unconstrained event type and tenant/time index; a new SQL table is not inherently required. Use an idempotency key including tenant, fingerprint and unique cycle/run identity. Confirm the receipt is present before reporting cursor advancement. Do not repurpose or overwrite audit events.
4. Store the selected ring endpoint, not an array offset or the last callback's identity: callback order stays unchanged and wraparound differs from ring order. Endpoint/key validation and deterministic tie-breaking are required. Do not use `created_at DESC LIMIT 1` alone or random event-ID ordering as causal cursor order. Concurrent runs must serialize the entire read-select-process-confirmed-append lifecycle under the existing tenant lock, with a verified lock lifetime; otherwise use a separately designed versioned compare-and-set. Receipts need a durable monotonic sequence or equivalent causal predecessor ordering. A slower stale run must not publish a cursor that replaces a newer one, and tied timestamps must be covered by disposable-PostgreSQL tests. An append-only event table avoids a new schema only if these ordering and confirmation requirements can be met through the existing locking/storage contract; this is not yet proven by the model.
5. A crash before the receipt can repeat some bounded work; a crash after it cannot lose candidates because every original fetched replay payload remains. A cursor is never an ingestion acknowledgment or a reason to drop deferred work. Cursor read/write failure must be explicit, not falsely reported as progression. Missing cursor storage must not trigger new provider discovery.
6. Preserve `checkpointComplete:false` and `pending_query_work` while any existing blocker remains. Keep progress telemetry separate: candidates selected, actually investigated, cursor advancement confirmed. An exhausted selection window means only that those identities were visited.

## Recovery caveat that must block a careless rollout

The caller can reach same-company recovery for any tenant, but `services/sameCompanyContactRecovery.js` currently hardcodes tenant/client 10 in its load, attempt-history and admission SQL (approximately lines 172-190, 416 and 448). The cursor must not expand recovery execution for Babrun or other tenants. An explicit tenant-10 recovery guard or a separately reviewed tenant-safe implementation is required before newly visited candidates may enter that path.

Preserving candidate loop order alone does not prove unchanged recovery provider use: a newly investigated ambiguous company may become eligible for alternate-contact recovery. The current model has no same-company recovery candidates, so it makes no such claim. The no-extra-recovery-work invariant is independent of tenant binding: even correctly tenant-bound Anchor recovery must not gain new provider attempts from cursor expansion. A strict no-additional-recovery-work first release must either schedule only candidates proven `OWNERSHIP_KINDS.CLEAR` while preserving the baseline allocation for same-company candidates, or separately defer newly exposed same-company recovery and retain its checkpoint work. Review this boundary explicitly and test provider-call identity and count against the baseline, including within tenant 10; do not silently expand provider calls while calling the change a cursor-only fix.

## Model and evidence

`test/scoutCheckpointStallDiagnosis.test.js` contains three synthetic tests:

1. Three real persistence passes make 60 site-investigation calls to only the same 20 identities. The admissible twenty-first candidate is never investigated; it inserts successfully when supplied alone.
2. One unresolved candidate prevents acknowledgment of an independently empty query.
3. A test-only in-memory predicate seam in the real persistence function models cursor selection. All 41 unresolved normalized identities, represented by 42 raw rows including a case/WWW/path alias, are visited within three passes, at exactly 20 distinct-identity investigations per pass. The duplicate raw row consumes no extra slot. The input company order is unchanged, raw fetched candidates are unchanged, and checkpoint completion stays false. The model stores receipts in memory; it is not proof of PostgreSQL durability or a completed implementation.

Validation: 23/23 focused tests passed, 0 skipped, with the repository network-denial preload and synthetic DB objects. Command:

`NODE_PATH=../spreadsheet_reconcile/node_modules DATABASE_URL=postgresql://localhost:1/pulseforge_test_only DATABASE_SSL=false node --require ./test/helpers/denyWebsiteEvidenceNetwork.cjs --test test/scoutCheckpointStallDiagnosis.test.js test/scoutQueryCheckpointRamp.test.js test/scoutQueryCheckpoints.test.js`

## Required implementation checks

- Cursor round-trip through disposable PostgreSQL; restart and idempotent append.
- Tenant/fingerprint/version isolation, corrupt cursor handling, equal timestamps, concurrent stale writers, confirmed append/sequence ordering, and changing candidate sets.
- Crash before persistence, after a partial insertion, before cursor write, after cursor write, and acknowledgment failure; payloads always retained until existing acknowledgment conditions hold.
- 21/41/157-candidate fair visitation with capped website requests and unchanged main-loop order.
- Stable overlapping/deduplicated query identities, one actual call per normalized selection identity, and zero Places calls on replay.
- Unknown ownership and cross-tenant recovery fail closed; independently prove no expanded alternate-recovery provider work even for correctly tenant-bound tenant 10.
- Unchanged queue attempt counts, retry timestamps, ICP/geography/ownership gates, quota denials, and whole-batch deferred behavior.

## Explicit limits

This cannot eliminate permanent unknowns or alternate-contact backoff, and it intentionally does not clear all pending checkpoints. Per-candidate durable disposition and independently acknowledged queries would be a larger second design. Coverage currently executes its finite generated plan; skipping a handled query does not invent a new approved query or pagination cursor. Never call visited queries proof of whole-market exhaustion.


## Subsequently implemented contract

The local runtime patch uses a snapshot followed by a short same-client transaction, a scope-specific advisory transaction lock, and sequence-keyed compare-and-set receipts. It does not pin a client throughout website/recovery work. Only currently CLEAR identities enter the capped website selection, with one actual call per normalized identity; ambiguous same-company evidence remains pending to avoid additional recovery calls. Confirmed cursor receipts do not acknowledge query checkpoints. Explicit outbound CI steps cover the network-denied regressions and mandatory disposable PostgreSQL gate. The native PostgreSQL cases are authored and syntax-checked, but were not executed during local preparation. See the implementation document for dependency, concurrency, crash and intentional deferral details.
