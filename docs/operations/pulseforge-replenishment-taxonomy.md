# SPEC-PF-ACCEL-001: bounded cashflow replenishment taxonomy

## Status

Local code-only repair based on main `697995ce1adf6d7eab8742dc37655676c5403db7` (PR #910). Prepared on `fix/cashflow-replenishment-taxonomy` for a separately approved draft PR. This work does not publish, merge, deploy, bootstrap a source, change a program, activate a mailbox, or authorize contact. There are no migrations or new dependencies.

## Verified failure

On base main, this pure repro returns `[]`:

```js
const r = require('./utils/replenishmentVertical');
const offer = require('./utils/pulseforgeCashflowOffer');
r.missionCompatibleVerticals({ missionSegments: offer.PULSEFORGE_TARGET_INDUSTRIES });
```

The approved segments are `plumbing`, `heating`, and `hvac`. Standalone plumbing/heating were not canonical enrichable categories. HVAC and plumbing were treated as contradictory contractor evidence outside the broad founder-led audience. A scalar unsupported segment also fell back to the broad default audience, unlike its compound equivalent.

Separate pure-code inspection verified that Scout's semantic planning replaced the exact approved trades with the parent founder-led SMB model. All 26 reviewed cities were also discarded in favor of the string “Southern New Hampshire.” These planning failures could waste discovery capacity even though later safety gates blocked sends.

## Repair and boundaries

- Shared exact service-trade aliases and concrete query terms cover plumbing, heating, HVAC, and explicit compounds. Unsupported parts fail closed. Plumbing/heating are not added to Babrun's founder-led roster.
- Classification uses actual business names, descriptions, categories, and declared trade fields. Discovery terms, city/query slugs, and mission membership cannot supply missing business evidence. Generic contractors, home services, adjacent contractor types, suppliers, software, and insurance are not substitutes for the target trades.
- A business explicitly described as plumbing and heating can match either evidenced trade. A plumbing-only business cannot become heating merely because the mission approves both.
- Scout coverage, semantic projection, observed-fit matching, tenant-1 inventory qualification, and Paige preparation share the trade contract. The trade-only semantic model has no adjacent markets or invented buying signals; learned nontrade terminology cannot expand it.
- Explicit reviewed cities survive both direct and semantic search planning and create per-city NH workloads. Candidate locations remain observed data; query geography is never filled in as location evidence.
- The legacy enrichment default and Anchor retry-worker list are unchanged. Only tenant 1 with a source that passes the existing immutable offer/industry/city/exclusion validator can select the three cashflow trades for enrichment. Tenants 10/13/17 retain their existing default selection. Known mixed legacy segment lists remain valid.
- Tenant-1 target checks, including Done Right, Overflow, and Clear All exclusions, now occur before alternate-contact recovery as well as insertion. Existing ownership/suppression/contact-classification, approval, sender-health, and send-boundary gates remain in place.
- Search fingerprints/backoff versions, schema, mission contents, policy hashes, production settings, and historical manual contact records are unchanged.

## Local verification

Environment: Node v24.19.0, npm 11.9.0. Reused the existing same-workspace dependencies without installing packages. All intentional database execution used the nonproduction sentinel `DATABASE_URL=postgresql://localhost:1/pulseforge_test_only`, `DATABASE_SSL=false`; new integration fixtures mock database/enrichment/provider operations.

- Core taxonomy, cashflow, tenant replenishment, inventory recovery, and legacy reconciliation: 83 passed, 0 failed/skipped.
- Semantic/hypothesis/geography, prior-learning, Substral, startup, and CI-planner contracts: 94 passed, 0 failed/skipped.
- `npm run test:max`: 1,349 passed, 0 failed/skipped on the repaired implementation.
- Independent focused review: 67 passed, 0 failed/skipped, including cashflow/taxonomy, Anchor worker, inventory recovery, Substral outbound, and governed execution-status guards. Two review findings were fixed and rechecked: mixed legacy audiences and unintended default enrichment expansion.
- A wider focused run included `test/scoutAcquisitionIntelligence.test.js`: one existing summary-wording assertion failed identically on unmodified base `697995ce`.
- `npm run test:amo`: 448 passed, 41 failed. An isolated untouched checkout of 697995ce and the final patch were then rerun with an external-network-denying preload. Both had the same 41 failing test names and locations, with no blocked network attempts in either run. This confirms no AMO failure delta; the aggregate suite still does not pass. See `pulseforge-replenishment-taxonomy-validation.json`.
- The broad `npm test` attempt did not complete. Auto-review stopped an existing test's request to an untrusted business website because it could transmit internal discovery criteria. The blocked run was not retried or routed around. No full-suite pass is claimed.
- Final taxonomy/startup/CI smoke: 22 passed; the final CI-selector-only recheck also passed 9/9.
- `git diff --check` passes. The new regression file is included in `test:pulseforge-cashflow` and the outbound PR CI job; taxonomy and Scout boundary paths select that job.

Focused tests are evidence of the local contract, not live inventory yield, mailbox readiness, provider delivery, or production acceptance. CI uses Node 22; that exact runtime and any database-dependent integration jobs still need verification on a separately authorized published commit.

## Read-only production verification plan

Do not run activation, bootstrap, outbound cron, Scout enrichment, migration, or provider-send commands as part of verification.

1. Read deployment metadata and confirm the exact approved commit is running. Read tenant-1 flags and program status without exposing credentials. Do not modify environment flags.
2. Read the intended tenant-1 source and program bindings. Validate the copied source payload locally with `pulseforgeMissionReason`. It must be the immutable $297 review, exact three industries, exact reviewed cities, and current first-touch exclusion version. Old $500 records and hashes remain historical.
3. Read sender/mailbox health, source/policy/approval bindings, finite caps/expiry, current suppressions, and contact classifications. Report missing or stale evidence; do not “repair” it during this read-only check.
4. Read current tenant-scoped `scout_unenriched` counts by canonical vertical and recent `acquisition_outbound_events` rejection counters for tenants 1/10/13/17. Capture a baseline. Do not initiate replenishment merely to create a result.
5. After a separately authorized normal replenishment cycle exists, inspect its receipts: exact tenant, source, business-evidence provenance, canonical trade, observed in-scope NH location, ownership/suppression result, and enrichment classification. Confirm manual first-touch exclusions remain held.
6. Compare other tenants' audiences and enrichment defaults with the baseline. No new plumbing/heating inclusion should result from this patch for their default selectors.
7. Stop with an actionable hold if there is no reviewed source, no authorized cycle, no business-fit evidence, no attributable geography, a suppression, an unverified contact, unhealthy sender, or missing approval. A missing outcome is not proof of readiness.

Production inspection is still pending. Publication, production changes, activation, and any external communication each require the operator's applicable approval.
