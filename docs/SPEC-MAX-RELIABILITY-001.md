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
