# Babrun standard Scout evidence preservation

Local preparation only, based on `49735e8cb7c884d1bf3e7b326e17bc028cc84ce8`.
No publication, deployment, production migration, backfill, recovery run or live provider operation is authorized by this change.

## Problem and bounded change

Standard Scout retained the chosen address and verification columns but discarded the email source URL during its final lead projection and omitted `job_title`, `acquisition_metadata.contactResolution` and `enrichment_provenance` from its prospect INSERT. A verified owner could therefore fail the existing Babrun contact-classification gate.

The patch changes three enrichment-result capture sites (Prospeo, Hunter and website scrape), one final projection, and one INSERT path. It binds the selected result's address, provider, source URL, contact name, title and queried company domain before combining any display/discovery fields. Tenant 13 carries that receipt through projection. Its INSERT uses only receipt-bound supported identity, persists the original acquisition source and verification receipt, and calls the unchanged classifier. The provider search preference and aggregate discovery-source list never establish founder identity. Unsupported, stale or ambiguous evidence remains held.

The classifier's existing ten pure functions and constants move without behavioral edits into `utils/babrunContactClassification.js`. The legacy contact-resolution module re-exports them. Promotion changes only its import target; its adapter, INSERT, duplicate recovery and UPDATE behavior are unchanged. The new intake adapter additionally treats an explicit verifier rejection as authoritative over contradictory positive flags, without changing legacy classifier/promotion semantics.

A valid owner/founder role, supported selected-result attribution and the existing name/address-match rule are all required for founder classification. The standard scraper currently supplies no named founder evidence, so a scraped address is not upgraded merely because it matches a person's name elsewhere. Verification establishes deliverability, not ownership.

## Boundaries

- Tenants 1, 10 and 17 keep the same waterfall mutation, projection and 29-column standard INSERT shape.
- Separate LinkedIn intake, verification recovery, existing-row classification, outbound policy, schedules, capacity and provider code are unchanged.
- Company duplicates still skip; email conflicts still use `ON CONFLICT (email) DO NOTHING`. Neither path writes replacement evidence onto an existing prospect.
- Contact eligibility is only an inventory prerequisite. DNC, mission/geography, human ownership, prior contact, suppression, prior execution, active authorization and dispatch windows still apply.
- No migration is added. Before a separately approved deployment, confirm the already-existing job-title and JSONB evidence columns on the target schema.

## Verification

Commands below require Node dependencies and local PostgreSQL `initdb`/`pg_ctl`. The PostgreSQL suite creates its own disposable server and never consumes an external database URL. Its synthetic schema uses UUID company/prospect identities, JSONB metadata and real unique conflicts; it is not a full production-schema certification.

- `npm run test:babrun-scout-intake`: 66 passed, zero failed/skipped. Covers actual waterfall/projection/save function bodies, mixed-person/source receipts, missing/unsupported/ambiguous evidence, role, pattern, changed address/company, risky/unknown/invalid verification, contradictory explicit rejection, other-tenant shape, legacy classifier/promotion parity, and real PostgreSQL INSERT/read-back/conflict and guard behavior.
- Final focused + CI selector + global startup acceptance: 79 passed, zero failed/skipped. The changed intake/classifier/promotion paths and test fixtures select the existing PostgreSQL-equipped governed-outbound CI job, which runs the new non-skipping gate.
- Broader offline regression: 201 tests, 199 passed, two failed. Both failures reproduce identically against pristine `49735e8` under the same network-denial harness: `babrunOperatorDelegatedMigration.test.js` expects `authorization_remaining_total` but gets `schedule_window_spacing`, and expects `operator_ceiling` but gets `schedule_window`. These existing policy assertions are not changed here.
- An earlier broad attempt included a legacy website-crawl test with an external fallback; that attempt was blocked and is not counted as a pass. It was not retried. The final broad set excludes that suite and denies external network/DNS while allowing only loopback for disposable PostgreSQL.
- Full repository `npm test`, remote CI, production schema/runtime, live provider behavior and deployment are not verified by this local slice.

## Current impact limits

This is preventive persistence for future standard Scout INSERTs. It does not repair historical rows, reclassify old inventory, refresh a verification, run Scout, prepare mail, or send anything. It changes zero production records. Current recoverable inventory and any eventual increase in clean/prepared inventory are unknown. Historical missing-classification/review-required counts and past zero-inventory control results are not fresh measurements or a promised recovery yield.

## Separate read-only production verification plan

This plan is not execution approval. Publication and deployment require their own approval; production inspection must use the approved read-only channel and target.

1. Confirm the deployed revision and the existing `prospects` column types from catalog metadata. Do not start the app, call a cron/health handler, import a migration helper, or invoke an enrichment/retry command as a diagnostic.
2. Through a bounded read-only connection (`BEGIN READ ONLY`, short statement/lock deadlines), select a tenant-13 aggregate baseline and the current outbound policy. Record the exact observation time and source revision. Keep the baseline's contact eligibility, clean inventory, prepared inventory and send capacity separate.
3. After an already-authorized routine Scout run, inspect a bounded sample of newly inserted tenant-13 rows. Scope by the run's time range and corroborating run evidence; `source='scout'` alone is insufficient because promotion uses the same label. The new adapter's contact-resolution `evidenceReason` field and provider-specific `discoveryMethod` can identify its footprint, but are not a substitute for deployment/run evidence.
4. Check exact normalized equality among the stored address, classification `bestEmail`, candidate email and provenance email; selected acquisition source/URL; supported name/title; and consistency between the nested verification receipt and existing verification columns. A missing/changed/unsupported receipt must be held. Never infer founder identity from verification or retrofit historical classifications during inspection.
5. Recompute contact-gate outcomes from the read-only snapshot using the unchanged policy. Apply the existing mission, DNC, ownership, prior-contact/execution and suppression checks when reporting clean inventory. Observe existing authorized control/preparation records separately. Do not trigger a control cycle, write an event, create an asset, reserve capacity or contact a provider.
6. Report observed future-row evidence retention, exclusion reasons and any actually measured inventory change, with timestamps. If no supported founder was acquired in the observed batch, state that founder-path production impact remains unobserved. Any old-row repair, new sourcing, retry, publication or operational change requires a separate approved scope.
