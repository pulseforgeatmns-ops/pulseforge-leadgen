# SPEC-MAX-RELIABILITY-001 — Evidence-Bound State Ingestion

Max ingests operator/AO/agent/file/system updates through one deterministic pipeline:

`RECEIVE → PRESERVE EVIDENCE → PARSE → RESOLVE → RECONCILE → VALIDATE → COMMIT → VERIFY → PROPAGATE`

## Entry points

- `POST /api/v1/max/ingest` — natural-language or structured claims (`services/maxStateIngestionService.js`)
- `POST /api/v1/max/ingest/spreadsheet` — row-independent spreadsheet ingestion
- Library: `packages/max/stateIngestion` (`ingestOperationalUpdate`, `ingestSpreadsheet`)

## Persistence

Migration: `migrations/2026-10-05-max-reliability-state-ingestion.sql`

Tables: `max_operational_ingestions`, `max_evidence_artifacts`, `max_ingestion_claims`, `max_applied_claims`, `max_ingestion_mutations`, `max_open_expectations`, `max_ingestion_conflicts`, `max_ingestion_evidence_links`.

Schema helper: `utils/maxStateIngestionSchema.js` (lazy on first Postgres ingest).

## Tests

All 20 acceptance scenarios: `test/maxStateIngestion.test.js` (`npm run test:max` includes this file).

## Authenticated API boundary

Operational understanding, ingestion, composer, voice and overdue-expectation
routes resolve their tenant through `utils/maxOperationalAuthorization.js` before
store initialization or provider work. The resolver reloads authenticated and
effective principals, requires current active roles and an active tenant, and
applies the same tenant policy as attention: admins use the selected workspace
(or an explicit current DB binding); managers and AOs use their current DB
binding. All supplied body/query `client_id` and `clientId` aliases must be valid
and agree. An omitted or malformed selector never defaults to tenant 1.

Only composer and voice retain the existing AO role. Ingestion provenance is
bound to the verified server actor, not a body-supplied actor or stale session
role. Spreadsheet proposal approval gates are unchanged. Legacy password-only
sessions and revoked access fail closed. Callers that formerly overrode their
workspace with a body selector must select the workspace through the established
tenant-selection flow. This is an authorization correction, not a new role,
grant, schema migration or permission to execute external work.

`test/maxOperationalAuthorization.test.js` exercises the actual routes and auth
middleware with inert business services, including conflicting aliases,
cross-tenant access, stale roles, impersonation, denied provider work and actor
provenance. These are local regression cases, not production-accuracy evidence.
