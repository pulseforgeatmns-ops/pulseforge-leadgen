# Tenant-10 research-provider pause: local operator proposal

Status: local implementation/review candidate under [SPEC-265](specs/SPEC-265_Anchor_Research_Provider_Pause.md). Publication, deployment and activation require separate approval. This document grants no provider or production authority.

## Control and exact scope

`ANCHOR_RESEARCH_PROVIDERS_PAUSED=true` stops new tenant-10 discovery, contact-enrichment and email-verification provider handoffs in the reviewed paths. The comparison is exact: an absent flag or any other value leaves the control inactive. It is a process-local pause, not a budget allocator, research-resume permission, email-send control, or all-provider shutdown.

Covered paid transports are shared Google Places, direct Places diagnostics/imports, SerpAPI, Prospeo, Hunter, Bouncer, and Apollo in the research/contact-enrichment helper. The independent weekday enrichment scheduler, cron/manual roots, canonical Scout, Max replenishment, mission-bound website discovery, promotion and verification retries carry trusted tenant context. The typed rejection is not an invalid contact, failed verification, empty discovery result, or enrichment retry. Completed earlier rows and existing reserved provider accounting are retained. The independent scheduler can retry after unpausing later in the same scheduling day; ordinary failures retain their prior scheduling behavior.

The control excludes field-route geocoding, email-send authorization, call/SMS authorization, unrelated presentation/operator LLM calls and other product operations. PageSpeed website-assessment calls are outside this paid-provider inventory; whether they are billed was not established. New known-tenant-10 research roots stop before entering their normal work, but the control does not promise to stop ordinary website/DNS work already in progress. Existing provider flags, recovery controls and approvals remain independent requirements.

## Required tenant context and compatibility

Trusted scope is captured once from the authenticated selected tenant, validated mission/program, selected CRM row, fixed-purpose script constant, or explicit operator tenant selection. It is immutable across asynchronous waits. Mutable leadgen configuration, a business name, an email domain and provider attribution records are not authority. A nested explicit mismatch rejects rather than replacing the original scope.

While the pause is armed, generic research/diagnostic calls without valid tenant context reject. In particular:

- `scripts/diagnoseScoutPlaces.js --client_id=<positive-integer>` and `phoneEnrich.js --client_id=<positive-integer>` need an explicit tenant. Both equals and separate argument forms are supported. Repeated conflicting or incomplete selections reject.
- The authenticated search and diagnostic HTTP routes bind the existing selected tenant and preserve their authentication/tenant checks. The cron diagnostic requires explicit tenant selection in its supported query/body input.
- Shared raw Places discovery, enrichment and verifier exports require a bound research context or their documented explicit scope. Non-geocode Places calls cannot evade this by omitting attribution metadata. Standalone field-route geocoding remains outside the control.
- Known non-Anchor Babrun/web-cohort utilities bind their existing tenant at their entrypoint. A mixed-tenant verification backfill checks each selected database row. Encountering a paused row stops that batch; prior completed rows remain completed.

This armed-mode context requirement is a compatibility change for previously unscoped operator utilities. It must be disclosed and approved with activation. It does not authorize inventing a tenant or claiming that a missing tenant belongs to another client.

## Later approval and rollout sequence

1. Approve publication/CI for the exact reviewed patch and its stated source footprint.
2. Approve deployment of that exact source with the new flag absent. No migration, control-row write or automatic activation is added.
3. Separately approve setting the flag to the exact value `true` on the intended app processes, including the operator-context compatibility requirement. Confirm all serving/worker replicas use the reviewed build and see that value. A rolling deployment/restart may be required for environment propagation; this implementation does not change another process's environment.
4. Establish the convergence checkpoint before claiming subsequent covered handoffs are paused. Already-started remote calls may finish and may be billed. No claim of zero total spend is supported without separate in-flight/drain evidence.
5. While a pause is required, keep a guard-aware build. Rolling back to pre-guard source would remove this protection even if the environment flag remained set. Separately approve unarming. Removing the flag or setting it to `false` restores only the pre-existing behavior of these paths. The unchanged recovery state may still disallow research. It grants no new provider quota, paid budget, email eligibility or sending authority.

No secret or credential changes, new public admin endpoint, database schema, recipient data or incident identifiers are part of this patch. Runtime error codes contain only the control reason. Do not use tenant deactivation or unrelated provider flags as a substitute.

## Evidence and limitations

Release evidence must bind the actual replayable patch, complete source-tree fingerprint, tests, reviewed caller inventory and independent review. Test fixtures use synthetic tenants/data and intercepted provider transports. Compatibility with an unmerged PR is separate evidence and does not silently change this patch's deployed-main base.

This is an effective pause for the enumerated research-provider boundaries, not sustained replenishment. A future finite research-resume/spend permission must be separately designed and approved, reuse the existing reviewed Places manifest/budget interface, address enrichment/verification budgets, and retain the permanent email quarantine. None is implemented here.
