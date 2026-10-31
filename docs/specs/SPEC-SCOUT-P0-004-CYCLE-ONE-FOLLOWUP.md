# SPEC-SCOUT-P0-004: cycle-one classification and diagnostic follow-up

Status: approved for implementation, testing and PR review only. No merge,
deployment, paid search, grant creation, cycle release or production record
change is authorized by this follow-up.

## Findings

The controlled first cycle retained three manufacturing facilities, but its
offline review found a factory historical-marker listing classified as an
operating manufacturer. It was held by a separate identity gate. A complete
website identity must not make the same kind of listing admissible later.

The research sink also incremented the generic `admittedToEnrichment` counter.
The durable research receipt correctly reported zero enrichment admissions,
but the generic ramp diagnostic showed three. These represent different writes
and need separate accounting.

The generic `provider_failed` diagnostic was reproduced with synthetic data:
hypothesis discovery returned facility evidence while optional adapters were
unavailable. The unavailable branch supplied only a message, and provider-report
normalization discarded structured error causes. This reproduction explains a
concrete ambiguity; it does not reconstruct an unrecorded historical HTTP error.
The complete Scout result can also report `insufficient_coverage` after a bounded
partial search. That limit must remain distinct from an actual provider failure,
including when optional adapters are unavailable in the same result.

The full native scheduler additionally attempts non-identity evidence work after
its one allowed page. The existing identity-only gate refuses that work before
HTTP. Its exception was flattened to `provider_error`. Preserve the fixed
`facility_research_identity_only` cause in the bounded receipt. A completed cycle
with refused work or truncated provider observations must not be relabeled healthy.
No additional evidence type or provider operation is authorized by this change.
Clear the previous provider execution receipt at each invocation so an exception
cannot reuse an earlier invocation's HTTP counts. Missing current measurements
remain unknown; they do not imply either zero spending or another paid call.

## Changes and constraints

- Recognize explicit historical-marker/landmark names and structured place types
  before factory-word matching. Reject them as unsuitable service premises even
  when a name, address and website are present. A nearby marker in descriptive
  text or a search query must not exclude an observed operating facility.
- Version new facility decisions as `anchor-facility-research-v2`. Preserve query
  fingerprints, existing evidence and parked-ticket dependencies. Do not rearm,
  promote, reconcile or rewrite historical records.
- Count research-sink inserts in `retainedForResearch`, with zero corresponding
  enrichment admissions. Preserve ordinary enrichment accounting.
- Preserve typed unavailable-adapter and thrown-provider causes through report
  normalization. Save and log the bounded, sanitized provider receipt with each
  new research cycle. No arbitrary error text, company identifiers, request URLs,
  credentials or session data belongs in these diagnostic projections.
- For completed research discovery with an observed completed provider report,
  optional unavailability can be a nonblocking issue. Denials, quotas, real
  provider errors, unknown failures, partial/blocked outcomes and explicit
  blockers must remain visible. A mixed optional-unavailable/real-failure report
  must not be labeled healthy. Do not infer HTTP calls or spending from status.

The classifier flag, research-only permissions, one-page limit, durable search
memory, Google reservation controls, maintenance, suppression, tenant scope and
atomic per-cycle allowance are unchanged. No migration is required.

## Validation

Use synthetic fixtures with network-denial guards. Cover complete-identity
historical listings, positive operating facilities, structured categories,
ambiguous geography and the Manchester England regression. Exercise typed
provider causes through the real hypothesis path, including malformed/private
error data and mixed failures.

The disposable PostgreSQL research journey must reject the synthetic marker,
retain suitable facilities, report zero enrichment, persist provider receipts,
buy at most one simulated page, and refuse an unreleased next cycle. Existing
release concurrency/retry, tenant, recovery and outbound regressions remain
required. No production exports may be committed as test fixtures.

## Deployment and rollback

Keep cycle two unreleased. Merge and deployment need separate approval. A later
deployment changes the revision pin and must fail closed against an older grant;
do not rewrite a grant or expand its ceiling to resume work.

Rollback is a code revert with no schema or historical-data rollback. Keep the
research grant unreleased or revoke it through the supported admin action during
rollback. Preserve maintenance, downstream restrictions and the append-only
release/spending audit. Reverting the marker exclusion restores its known quality
defect, so do not resume research merely because older code is running.
