# Anchor Scout discovery memory

P0 reliability repair for the existing Max-directed durable Scout path, tenant
10 only. Search knowledge does not grant research, enrichment, verification,
preparation or sending permission. The existing research pause, recovery
interlocks, source checks and shared provider spending meter remain in force.
No source fingerprint, historical payload, candidate attempt or grant is reset.

## Repeated searches

The original mission fingerprints and query/page keys remain audit identities.
A separate tenant-wide query owner prevents another mission from purchasing the
same query. Its identity normalizes case/spacing and preserves geography, while
sharing coverage across the identity/website output-contract bit. The actual
mission checkpoint keys retain that bit. Existing checkpoints and page records
are consulted too, including history predating the new tables.

The shared owner is committed before the ordinary page reservation. Concurrent
missions cannot each reserve a fresh copy. A reservation with no confirmed
outcome is a reconciliation dependency, never an expiring lease that permits
automatic research again. Completed searches are skipped; the provider continues
through the remaining queries generated from the same approved search definition.
The existing one-page cycle allowance still applies across concurrent workloads.

Pagination remains owned by its original source. Another mission cannot borrow
its continuation token or treat stored history as broader source authorization.

## Repeated business details

Before a detail request, Scout checks durable observations, candidate memberships
and retained checkpoint payloads for the exact Google Place ID within tenant 10.
Known business facts can enter the ordinary query/ticket/admission path without
another detail request. Old ticket references and query labels are not reused as
authority. Each current query receives its own provenance. Legacy facts have
unknown detail observation time; copying them does not make them fresh.

Unknown IDs receive a durable unique detail reservation before HTTP. A successful
business response is retained immediately, before page/ticket admission; an
interrupted or uncertain request remains consumed. A second process cannot fetch
an unfinished ID. Newly reserved detail requests disable the transient cache so
the stored observation time cannot misrepresent an older cached response.

Matching is by exact Place ID, not fuzzy names or shared domains. Separate sites
are not collapsed. Periodic fact refresh and repair of uncertain observations
require separate explicit execution scope; there is no automatic expiry/reset.

## Search yield and operator evidence

Retained page payloads mark reused detail observations. If a handled page has at
least five candidates and at least 80% reused details, further pagination is
deferred in favor of fresh approved roots. This conservative prioritization rule
is explicit and tested, not an estimate of whole-market exhaustion. Max receives
`tenant_query_low_novelty` as a remaining-coverage review dependency.

Query progression exposes only numeric history/reuse/reservation counters. After
candidate work drains, `scout_tenant_search_yield` records `newQueueAdmissionsThisCycle`, the actual queue-write
count for the cycle, including any previously owned backlog. It is not a claim
of per-query historical yield or verified contact inventory. An unconfirmed
admission pass reports a null count rather than claiming zero. Admission,
ownership, deduplication and downstream controls remain authoritative.

## Installation and acceptance

Apply `migrations/2026-10-27-scout-tenant-search-memory.sql` separately while
research is paused. It creates two Anchor-only tables, immutable query ownership,
single-completion observation guards and three read indexes. It is transactional
and create-once; inspect an uncertain result rather than retrying blindly.
Runtime readiness performs SELECTs only and fails closed without the schema.

The existing `SCOUT_DURABLE_CANDIDATES_ENABLED=true` path is required. This change
does not enable that flag, open recovery/research/send controls, alter other
tenants, or change the separate explicitly approved bounded-refresh CLI.
Keep those controls closed during deployment and catalog/readiness verification.
Rollback keeps all memory/audit rows and holds research; do not delete ownership
or reset an unfinished detail reservation to make another attempt possible.

Offline acceptance uses synthetic HTTP with external network denied and native
disposable PostgreSQL. It covers restarts, mission replacement, output-contract
changes, historical data, shared query/detail concurrency, uncertain commits,
scope isolation, low-novelty deferral and actual Max-to-raw-queue admission.
A future bounded production experiment still needs its own current source,
authority, spend limits, receipt and revocation. Passing these tests does not
establish sustained autonomous replenishment reliability.
