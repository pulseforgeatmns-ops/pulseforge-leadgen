# Babrun replenishment observation repair

Local-only slice on `bb996bcdbc84d7652b93888da0b425fec8a2a9e6`, under accepted SPEC-184 / ADR-099.

## What is established

The October 16 11:15 Scout summary supplied for this investigation reports partial / insufficient coverage and zero discovery, fit, admission, enrichment and promotions. The 11:30 control summary reports zero clean/prepared/sent inventory. These summaries do not identify whether sources genuinely returned no candidates, a provider failed, or a failure was reused from cache. No underlying provider receipt was accessible for this investigation. Production database access was not used, and no discovery or control run was triggered.

Source review proves two observability defects:

1. `legacyTextSearch` already records cache origin internally, but omitted it from its return. `PlacesProvider` consequently could not preserve it into the canonical provider execution and the newer sanitized control receipt.
2. The sanitized receipt is stored in the normal control event, but the natural control log only exposed a coarse blocker. Operators without DB access could not use the existing receipt.

The backoff uses three zero-inventory-yield events, rotates through a maximum search generation of three, then waits until its cooldown expires. These are inventory yield observations, not proof that the market or provider source is exhausted. No operational evidence establishes whether that backoff caused the supplied run.

## Focused repair

- Return per-caller cache-hit and allowlisted strategy metadata from legacy text search; preserve it through Places attempts and sanitized provider receipts.
- Preserve cache origin independently for the original requester, in-flight sharers and later cache readers. Do not mutate shared/cache result objects.
- Emit the existing sanitized provider receipt only after normal control-event write completion. Logging failure remains non-fatal.
- Rename the exhausted label to `repeated_zero_yield_generation_limit_reached`; the threshold, generation limit, retry timestamp, cooldown and all decision branches remain identical.
- No raw query, provider prose, address, URL, contact identity or credentials are added to runtime logs. Historical metadata absence stays unknown.

Scope is text-search origin. Place-detail origins and transport failures without a returned result remain unmeasured; search cache provenance must not be used to infer the origin of a separate detail failure. These fields remain observational, never qualification evidence or a spending authorization.

## Verification

With a network-denial preload blocking sockets, DNS, HTTP(S), TLS and global fetch; all provider fixtures inject synthetic responses and all changed-path DB operations use fake stores:

- Focused cache, receipts, spend attribution/savings, Babrun inventory, tenant replenishment, recovery, global startup and CI selector regression: 118 passed, zero failed/skipped.
- Additional six-suite discovery regression: 52 tests, 50 passed, two failed. Both Scout summary-wording assertion failures reproduce against pristine `bb996bc` with the same strict denial preload.
- The legacy AUDIT-059 suite is not a valid offline gate here: it leaves an operation pending when socket connections are synchronously denied; that run was interrupted. An earlier weaker network guard completed with five assertions failing identically on baseline and patch. It is not counted as passing or fully validated.
- `git diff --check` passed. Existing CI selector selects governed outbound and global startup for the patch paths.

Full repository tests, full PostgreSQL governed-outbound gate, remote CI and live production behavior remain unverified. No branch publication, deployment, provider call, cache eviction, production write, filter broadening or contact repair occurred.

## Remaining verification after separate publication/deployment approval

Observe the next already-authorized natural run; do not invoke a replacement run. Correlate its tenant and cycle identifiers with the sanitized provider receipt and measured control summary. Distinguish REQUEST_DENIED / quota / unavailable from successful ZERO_RESULTS; read search cacheHit and cacheStrategy independently from provider status. Missing reports or detail origins remain unknown. If actual candidates appear, use measured admission and inventory evidence before attributing any gain to a fix.

The current live replenishment root cause and recovery yield remain unknown. This patch enables a narrower diagnosis; it does not restore prospect supply by itself.
