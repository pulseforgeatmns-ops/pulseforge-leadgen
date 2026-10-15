# Outbound diagnostic accuracy

Local/test-only patch on October 15, 2026. No publication, deployment, production query or inventory repair is included.

The intermediate Scout ramp previously logged `buildReplenishmentYield` without a measured inventory-growth snapshot. Its default pre/post counts appeared as zero. The new logging-only projection explicitly reports these as null, retains genuine measured admission/enrichment zeros, and includes validated tenant/cycle correlation. It deliberately leaves numeric yield contracts and control/backoff decisions unchanged.

Scout diagnostic output contains fixed categories only; raw admission decisions, company names, addresses, provider messages and source URLs are excluded. Unknown codes map to unknown rather than being copied or guessed. Categories are diagnostic evidence, not permission to retry/change providers.

A second summary is emitted only after the existing control event write completes successfully (a duplicate may be a no-op). It uses measured inventory-before/after counts. A provenance flag distinguishes a successful preparation-envelope read from a legacy fallback zero; unavailable preparation counts are null in the summary. Send counts are shown only when the infrastructure snapshot supplies a numeric count. These additional flags do not alter control decisions or existing numeric fields. Logging failures cannot abort execution.

Existing Scout-ramp log shape changes intentionally. Consumers must use the new structured `discovery` categories and read authoritative inventory from the measured summary or durable event. The persisted yield schema remains compatible. Neither summary substitutes for last-send receipt reconciliation or full contact eligibility inspection.

See `babrun-read-only-diagnostic-snapshot.md` for the separate, unexecuted production verification plan. Full production runtime and live provider behavior remain unverified.

## Validation

Final focused diagnostic, inventory, Babrun sustainable inventory, tenant generalization, refill, CI-selector and startup set: 64 passed, zero failed/skipped with external network disabled. Independent review approved after raw malformed-send-count and event-write/no-op corrections; its focused offline set passed 31 tests. An additional route suite was blocked by the reviewer's network-denial guard at loopback DNS/listen, not counted as a pass. Full repository suite, live provider behavior and production runtime were not tested. Existing raw warning/error logging outside these new projections remains unchanged; this is not a global logging sanitization claim.
