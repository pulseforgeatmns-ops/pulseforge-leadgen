# Outbound diagnostic accuracy

Local/test-only patch on October 15, 2026. No publication, deployment, production query or inventory repair is included.

The intermediate Scout ramp previously logged `buildReplenishmentYield` without a measured inventory-growth snapshot. Its default pre/post counts appeared as zero. The new logging-only projection explicitly reports these as null, retains genuine measured admission/enrichment zeros, and includes validated tenant/cycle correlation. It deliberately leaves numeric yield contracts and control/backoff decisions unchanged.

Scout diagnostic output contains fixed categories only; raw admission decisions, company names, addresses, provider messages and source URLs are excluded. Unknown codes map to unknown rather than being copied or guessed. Categories are diagnostic evidence, not permission to retry/change providers.

A second summary is emitted only after the existing control event write completes successfully (a duplicate may be a no-op). It uses measured inventory-before/after counts. A provenance flag distinguishes a successful preparation-envelope read from a legacy fallback zero; unavailable preparation counts are null in the summary. Send counts are shown only when the infrastructure snapshot supplies a numeric count. These additional flags do not alter control decisions or existing numeric fields. Logging failures cannot abort execution.

Existing Scout-ramp log shape changes intentionally. Consumers must use the new structured `discovery` categories and read authoritative inventory from the measured summary or durable event. The persisted yield schema remains compatible. Neither summary substitutes for last-send receipt reconciliation or full contact eligibility inspection.

See `babrun-read-only-diagnostic-snapshot.md` for the separate, unexecuted production verification plan. Full production runtime and live provider behavior remain unverified.

## Validation

Final focused diagnostic, inventory, Babrun sustainable inventory, tenant generalization, refill, CI-selector and startup set: 64 passed, zero failed/skipped with external network disabled. Independent review approved after raw malformed-send-count and event-write/no-op corrections; its focused offline set passed 31 tests. An additional route suite was blocked by the reviewer's network-denial guard at loopback DNS/listen, not counted as a pass. Full repository suite, live provider behavior and production runtime were not tested. Existing raw warning/error logging outside these new projections remains unchanged; this is not a global logging sanitization claim.

## Durable provider observations

The existing `max_outbound_control` event now includes `scoutProviderReceipt`, a
bounded projection of the current cycle's Scout result. It keeps tenant/cycle
correlation, fixed status/provider/error-code categories, numeric counts, valid
UTC timestamps, and at most 20 providers / 20 queries / 20 errors per collection.
No raw queries, messages, contacts, URLs, payloads or credentials are retained.
Missing reports and numeric fields remain unknown; explicit zero remains zero.

This does not install the optional specialist tables or change the ephemeral
Scout store. It records only an observation of the already-returned result into
the existing control event. Duplicate event writes may still no-op under the
existing store contract. No provider lookup, retry, cache lifetime, contact gate,
criteria, sending decision or rate is changed.

`executed` preserves the provider's Boolean, not proof of network activity or
paid usage. Current Places attempts do not carry cache metadata, so origin is
`unknown`; explicit cacheHit true/false is reported as cache/uncached only if
present. Neither status nor HTTP status proves a fresh request. Existing provider
raw/mapped result counts may be initialized defaults on failure and must be read
alongside execution/errors; a zero alone is not successful zero-result discovery.
A backoff/no-provider cycle never carries a previous cycle's receipt.

The same event also carries `scoutAdmissionReceipt`. Admission is marked measured
only after the existing persistence/admission callback completes successfully;
preallocated zero counters on backoff or incomplete discovery remain unknown.
Fixed numeric rejection keys include outside_geography and the existing fit,
ownership and offer-exclusion reasons. Their original values are not rewritten.
`recordedRejectedTotal` sums the actual recorded numeric reason values;
unrecognized reasons contribute only to an aggregate count without exposing
arbitrary keys. This total is not automatically equal to evaluated minus fit:
already-usable, queued, recovery and backoff paths have distinct counts. Missing
fields stay null and invalid counts invalidate the aggregate rather than imply
zero. No retrospective reason is invented for historical events.
