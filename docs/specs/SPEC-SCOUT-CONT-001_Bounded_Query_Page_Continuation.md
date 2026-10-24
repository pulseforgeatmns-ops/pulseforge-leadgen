# SPEC-SCOUT-CONT-001 — Bounded query-page continuation

Status: authorized local/test proposal; publication, migrations and activation require separate approval.

## Purpose

Continue the same approved Places query across provider pages after durable responsibility for the previous page has been transferred. Do not invent a new ICP, geography, query rotation, reset, cache lifetime, provider, or spending authorization. This is a bounded source of additional business candidates, not a guarantee of usable owner contacts, verified email, or indefinite daily inventory.

References: SPEC-153 Discovery Coverage Engine, SPEC-181 Evidence-Native Execution, SPEC-184 Provider Execution Continuity; ADR-076 Coverage Before Conclusion and ADR-099 Sensor Observation Continuity.

## Evidence and boundaries

Current production-code fixtures produce 21 unique Babrun founder-led-business query identities in every generation 0–3. Generation changes do not diversify that audience. A representative four-segment Anchor fixture produces 54 queries per generation and 216 distinct query identities across four rotations. These are offline source fixtures, not a reconstruction of live mission records. The available natural-run logs show 21 and 54 replayed queries per cycle, not total stored coverage across all generations.

The existing provider accepts at most 20 businesses per query invocation. If the first page supplies that many, it drops the continuation token when staging the candidate-only checkpoint. Consequently acknowledging an old fetched query cannot reveal the next page. Existing fetched/handled rows have no trustworthy token and must remain intact; they become `legacy_continuation_unknown` rather than silently resetting or refetching. Resuming those rows requires a separately reviewed refresh decision or trustworthy previously captured continuation evidence. The known multiword-geography truncation is not repaired here and is not asserted to be the present live root cause.

Google documents up to 20 results per page, three pages/60 results per Legacy Text Search and a short token-activation delay: https://developers.google.com/maps/documentation/places/web-service/legacy/search-text . That is a finite provider cap, not proof that the market contains no further businesses.

## Local design

- `createCheckpointSession({store,ticketStore,pageStore})` is opt-in. Without `pageStore` the old bounded query behavior is retained. The caller owns the default-off rollout gate.
- Page zero retains its existing query identity and `places-market-query-v1` checkpoint version. Later pages derive immutable keys from the root key and ordinal 1–2. Tenant and mission fingerprint remain part of every database predicate.
- `scout_query_pages` records an immutable scoped page reservation, opaque request/next-page token, sanitized raw result identifiers and availability time. Tokens never enter receipts, logs, candidate fields, membership keys or cost attribution; they are not API credentials.
- A short atomic reservation before HTTP ensures concurrent workers cannot both acquire the same page. There is no database connection held during provider requests.
- Candidate checkpoint and continuation metadata are staged in the same short transaction under the exact reservation owner. Missing schema, stale ownership, conflicting checkpoint or uncertain confirmation fail closed.
- `ticketStore.transferQuery(actualPageKey)` must atomically retain exact raw candidate membership and take responsibility before the page becomes handled. The session verifies its receipt. A failed transfer leaves the staged page replayable without new provider calls.
- One new page total is allocated across the entire acquisition-session cycle, including concurrent workloads. Other queries remain pending. At most 20 business detail requests and the existing search retry ceiling occur for that allocation. Every actual HTTP request continues through the existing shared API spend reservation and settlement code; no budget is created, increased or bypassed.
- A second page cannot execute in the same session, even after immediate transfer. A subsequent cycle can proceed only after durable handling, the token activation delay, and an exact parent token/state check in the reservation SQL.
- Partial provider/details failures emit no unstaged candidates. Oversized provider pages fail closed before details; they never silently truncate and advance. Staging/commit uncertainty retains the reservation and stops automatic refetch. This conservative stop requires operator reconciliation; elapsed time does not authorize retry of an unknown paid outcome.
- A proven pre-network budget denial releases only the unused reservation. Real failed attempts, invalid tokens and unknown transport outcomes retain their ordinary spend accounting and durable page stop.

## Finite outcomes and required operator action

- `finite_query_exhausted`: provider supplied no next page for this observed query.
- `provider_page_limit_reached`: configured three-page ceiling reached; no wider-market claim.
- `legacy_continuation_unknown`: prior checkpoint has no continuation evidence; review refresh/source coverage and cost before acquiring more.
- `page_token_not_ready`: no call; wait for an ordinary later cycle.
- `cycle_page_budget_reached`: no call; remaining approved query is eligible in a later cycle.
- `page_fetch_in_progress` / `page_fetch_outcome_unknown`: another reservation or unconfirmed execution; no automatic takeover.
- `continuation_token_invalid`, `page_provider_denied`, `page_provider_failure`: durable stop pending an explicit recovery/coverage decision.

The controller must preserve these counts, drain independently retained candidate tickets under its existing investigation cap, and alert on exhausted or blocked current coverage with a finite backlog. It must not repeatedly treat unchanged handled pages as new work. A whole-market or all-generation exhaustion claim requires an exact approved-plan/key comparison, which available logs alone do not establish.

## Acceptance and release gates

The same offline acceptance fixture fails on untouched main `8cbeba9d` in cycle two because it repeats an already seen business. With continuation enabled, three reconstructed sessions each admit 20 distinct eligible businesses through the real replenishment admission function, followed by three exhausted sessions with zero further calls. This is 60 business queue admissions, not verified outbound prospects. The fixture observes 63 reserved requests: three searches and 60 detail requests. Its $1.62 sum uses this repository's existing synthetic gross-upper-bound rate inputs; it is not a live price quote or actual billing evidence.

Tests cover cross-query and same-session concurrency, process-session restart, transfer crash, stage uncertainty, budget denial, invalid/early tokens, legacy checkpoints, tenant mismatch, missing schema, partial details failure, oversized results and immutable page boundaries. Required PostgreSQL tests cover real atomic staging, concurrent page reservation, stale/forged parent checks, scope isolation, rollback and uncertain commit recovery.

Release remains blocked until the integrated ticket/controller path is independently reviewed, all required tests pass against the exact combined commit, the additive migration is reviewed/applied through the approved process, and activation/any necessary legacy refresh and extra cumulative spending are explicitly approved. No production calls, data changes, publication or rollout are part of this local proposal.
