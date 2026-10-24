# Scout query completion checkpoints

This change is local and unpublished. The SQL migration is not applied automatically.

## Contract

Only the governed replenishment ramp opts in. Each market Places request is keyed by tenant, mission search fingerprint, plan version, actual query (preserving geography), and website requirement. Entity investigation remains unchanged.

A successful bounded provider query stages sanitized business hits in an immutable `fetched` row. Stage failure cannot acknowledge completion. A restarted run replays that payload without another provider call. Same-run overlapping queries retain self-contained payloads while reusing existing detail hits.

The existing candidate pipeline must complete, and its explicit durable `persistCompanies` callback must resolve with `checkpointComplete: true` before `acknowledgeDiscovery` updates staged units to `handled`. This includes successful empty outcomes. Discovery errors, interrupted persistence, missing acknowledgement flags and deferred candidate handling leave units replayable. Partial ingestion is replayed through existing tenant-scoped duplicate/admission gates. An acknowledgement failure may replay already ingested candidates; it must never discard them.

Handled units are skipped, allowing remaining approved queries to execute. The session reports only the observed request set. `observed_query_set_handled` does not claim that an entire market or approved plan is exhausted. Existing 20-result limits, retry ceilings, cache lifetimes, spend reservations and geography/ICP/contact gates are unchanged.

Missing migration/read failure blocks the checkpointed discovery path before a provider fetch. There is no fall-back to uncheckpointed paid discovery. Query staging uses ordinary business hit fields, not provider credentials, page tokens or raw error payloads. Handled rows discard replay payloads.

## Release verification

- Run offline `node --test test/scoutQueryCheckpoints.test.js` and governed ramp regressions.
- Run `SCOUT_CHECKPOINT_REQUIRE_POSTGRES=true node --test test/scoutQueryCheckpointsPostgres.test.js` in CI with local PostgreSQL binaries. This uses a disposable local cluster, never an externally supplied database URL. A skipped local run is not evidence of PostgreSQL durability.
- Review and approve migration/publication separately. No production schema change is authorized by these tests.
- Verify missing-schema fail-closed behavior before activation, then migration and stage/replay/ack behavior in the approved deployment environment.

Limitations: a crash after a provider fetch but before staging may repeat that fetch under existing cache and spend controls. Concurrent sessions can both fetch a previously unseen query; immutable staging makes them converge on one replay payload, but this change is not a distributed provider-request lease.

The acknowledgement is deliberately conservative for the whole acquisition batch. Unclassifiable business evidence (including the 20-site investigation cap or fetch failure), alternate-contact recovery cooldown, and unsuccessful alternate recovery return `checkpointComplete: false`. All staged query units remain replayable, including candidates already admitted in that batch; the existing queue/ownership gates deduplicate them. This prevents permanent loss of deferred candidates but is not a per-candidate work scheduler and does not solve website-investigation starvation. No automatic refresh or additional categories are authorized when the observed queries have been handled.

The PR outbound job runs the disposable PostgreSQL contract with `SCOUT_CHECKPOINT_REQUIRE_POSTGRES=true`; absent binaries are a failure there, not a successful skip. The app change opts the governed ramp into checkpointing, so the separately approved migration must precede application rollout. A code-only deployment before the migration would intentionally block external discovery.
