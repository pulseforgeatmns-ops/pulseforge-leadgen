# Places request savings

Identical overlapping Legacy Text Search and Place Details calls now share one
in-flight provider request. Only its owner reserves and settles spending. Other
callers record `cache_hit=true` with `memory_inflight`, preserving their attribution
without counting another paid attempt. A failed shared request retains the owner's
uncertain reservation and releases the in-flight entry so a later call can retry.
Explicit `useCache=false` bypasses both the cache and request sharing.

Request identity includes the API account, exact search geography, pagination,
detail field mask, and timeout. API keys and page tokens appear only as hashes in
cache identities. Request sharing does not add durable response storage or extend
the existing cache lifetime. Google status failures cannot become cached successes.

Discovery uses the rating already returned by search instead of requesting the
rating field again in Place Details. This avoids requesting the Atmosphere SKU
for that detail call. Website and phone enrichment, business identity, geography,
and the search rating remain available. Rejected place IDs are visited once per
evidence collection, so overlapping queries do not repeat their detail lookup.
Pagination stops once the caller's requested eligible prospect count is satisfied.

This is not a billing reconciliation or a numerical spending-policy change.
The usage dashboard retains conservative gross estimates. Actual cost changes
must be checked against provider reports by SKU and matching charge dates.

## Validation

`test/placesRequestSavings.test.js` checks overlapping requests, account and field
isolation, pagination, geography, retry after shared failure, quota recovery,
explicit bypass, pre-network budget denial, and preserved prospect fields.
The API usage workflow also runs PostgreSQL budget/concurrency tests and existing
Places and Paige regressions.

## References

- https://developers.google.com/maps/documentation/places/web-service/legacy/details
- https://developers.google.com/maps/documentation/places/web-service/legacy/search
- https://developers.google.com/maps/documentation/places/web-service/place-id
