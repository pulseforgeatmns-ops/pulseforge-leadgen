# SPEC-SCOUT-FACILITIES-001 — Legacy enrichment boundary repair

Status: In-progress local/test repair, 2026-10-20. Publication and production actions are not authorized.

## Objective and problem

Repair PR960 without redefining the existing tenant audiences. Canonical facilities recognition must not expand the shared enrichment retry defaults. Simply removing facilities from those defaults must not strand candidates admitted under an explicit Anchor facilities mission.

## Contract

- Preserve the exact legacy `ENRICHABLE_SCOUT_VERTICALS` list and Anchor scheduled retry selection.
- Keep dealership, school, campus, manufacturer and warehouse canonical for classification and queue identity. The one-time legacy reconciler must not treat canonical facilities or previously supported canonical service trades as query-slug contamination. This is recognition only, not enrichment or admission permission.
- Only tenant 10 with explicit managed-facilities or individual facility scope selects those categories for enrichment. Its reviewed eligible subsegments take precedence over its parent segment, retain restaurant FOH when included, and do not use the legacy catch-all.
- Unsupported mixed facilities scopes select no rows. Unscoped Anchor, existing Anchor missions and other tenant defaults remain unchanged. Pulseforge's reviewed trade gate remains unchanged.
- Admission, queue duplicate eligibility and enrichment share the same projected mission selection. Existing geography, ownership, DNC, provider controls and outbound approval gates remain intact.

## Scope and validation

Local code/test changes only, preserving the [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md) tenant/no-broadening contract. No schema, migration, activation, new provider request, outreach or deployment.

Reproduce the original strict legacy-default assertion without changing its expected audience. Add facility classification/admission, explicit Anchor mission, mixed scope, queue selection and cross-tenant regressions. Run focused suites and the applicable CI domain suites; report missing PostgreSQL infrastructure separately from code failures. Local tests do not establish live readiness.
