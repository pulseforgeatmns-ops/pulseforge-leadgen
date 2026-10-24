-- Rollback SPEC-MAX-RELIABILITY-001

BEGIN;

DROP TABLE IF EXISTS max_ingestion_evidence_links;
DROP TABLE IF EXISTS max_ingestion_conflicts;
DROP TABLE IF EXISTS max_open_expectations;
DROP TABLE IF EXISTS max_ingestion_mutations;
DROP TABLE IF EXISTS max_applied_claims;
DROP TABLE IF EXISTS max_ingestion_claims;
DROP TABLE IF EXISTS max_evidence_artifacts;
DROP TABLE IF EXISTS max_operational_ingestions;

COMMIT;
