# SPEC-SCOUT-CONT-002 — Explicit bounded legacy coverage epoch

Status: authorized local implementation and tests only; no production activation, calls or publication.

## Objective and problem
Legacy candidate-only checkpoints cannot supply an omitted Places continuation token. After durable transfer of that old work, a separately approved, finite reacquisition of the exact same query can capture new continuation metadata without changing the mission fingerprint or overwriting historical work.

## Scope and authority
Default off. An explicit manifest selects tenant 10 or 13, the existing mission fingerprint, at most 54 exact root query hashes, epoch 1, an approval reference and an activation/expiry window. Only epoch 1 is implemented; there is no timer-created epoch, epoch rotation, implicit reset or new geography/query. Configuration is a technical gate, not user authorization. Any production configuration, cumulative spending and rollout require separate approval.

## Architecture and data model
A scope-wide immutable receipt pins the complete approved root set and window, preventing disjoint manifests from expanding the same epoch. An additive immutable mapping associates each scoped legacy root with a derived epoch root. The existing page table and immutable candidate checkpoint store handle that epoch root using their ordinary three-page ceiling. The old root and its candidates, tickets and attempt history remain untouched. Legacy fetched work transfers before an epoch can begin. One new page per entire acquisition session and normal API spending reservations still apply; the maximum cumulative allocation is three pages per listed root, up to three search attempts and 20 details per page.

The mapping remains discoverable when configuration expires or is removed. Saved fetched epoch payloads still replay and transfer; new reservations require the exact still-valid manifest. Unknown in-flight or failed outcomes stop rather than reacquire. Unique mapping/page identities fence concurrent workers.

## Diagnostics and rollout
Natural checkpoint-session diagnostics may project actually observed original-root hashes with tenant/fingerprint hashes only, capped at 54 and explicitly marked observed-only. It includes a finite cumulative request ceiling; it contains no candidate data, query text or continuation tokens and is not an automatically approved manifest. Eligibility is rechecked before each epoch reservation.

Stage reviewed code/migrations default-off, obtain a sanitized natural-run manifest, approve the exact bounded subset/window/cumulative allocation, then activate through the approved release process. No production DB access or paid refresh is performed by this local task.

## Acceptance targets and evidence
Real disposable PostgreSQL fixtures begin with 21 legacy roots and retained candidate work. Missing/unapproved configuration causes zero new epoch calls for exhausted legacy roots. Ordinary approved untried queries and existing enrichment retain their existing budgets. An explicit fixture manifest permits one initial reacquisition and ordinary continuation per selected root, produces fresh distinct business admissions over restarted cycles, and reaches finite exhaustion without unchanged rework. Tests cover config validation, expiry/removal replay, immutable manifest identity, concurrency, budget denial, existing ticket drain and preservation of old rows/attempts. The integrated clean-inventory simulation remains separately bounded by owner and verification gates.

## Operational constraints and evidence

Planning permanently pins the manifest, including roots not fetched because of the one-page session budget. No renewal or window extension exists. Both epoch tables absent is supported while the epoch flag is off; a partial schema or inaccessible schema fails closed. Existing fetched payloads remain recoverable after permission removal. Only the inherited proven pre-network budget-denial path releases an unused page reservation; attempted, fetched, historical and ticket rows are retained.

Store-only tests prove persistence contracts; the separate actual-runtime epoch pipeline fixture proves three-cycle synthetic admission flow. See [ADR-102](../adr/ADR-102_Explicit_Immutable_Legacy_Coverage_Epoch.md) and the [rollout/evidence boundary](../engineering/scout-explicit-legacy-coverage-epoch.md) for precise limits, approval fields and cumulative ceilings including retries.
