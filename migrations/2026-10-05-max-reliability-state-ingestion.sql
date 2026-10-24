-- SPEC-MAX-RELIABILITY-001: Evidence-bound operational state ingestion

BEGIN;

SELECT pg_advisory_xact_lock(20261005, 1);

CREATE TABLE IF NOT EXISTS max_operational_ingestions (
  id TEXT PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  source_type TEXT NOT NULL CHECK (source_type IN (
    'OPERATOR_REPORTED', 'AO_REPORTED', 'SCOUT_DISCOVERED', 'AGENT_DERIVED',
    'FILE_IMPORTED', 'SYSTEM_OBSERVED', 'API_OBSERVED'
  )),
  source_actor TEXT,
  raw_source JSONB NOT NULL DEFAULT '{}'::jsonb,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  receipt_summary TEXT,
  telemetry JSONB NOT NULL DEFAULT '{}'::jsonb,
  pipeline_audit JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_max_operational_ingestions_client_received
  ON max_operational_ingestions (client_id, received_at DESC);

CREATE TABLE IF NOT EXISTS max_evidence_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ingestion_id TEXT NOT NULL REFERENCES max_operational_ingestions(id) ON DELETE CASCADE,
  artifact_type TEXT NOT NULL CHECK (artifact_type IN ('message', 'spreadsheet', 'structured_file', 'api_payload')),
  content_sha256 TEXT,
  filename TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_content JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_max_evidence_artifacts_ingestion
  ON max_evidence_artifacts (ingestion_id);

CREATE TABLE IF NOT EXISTS max_ingestion_claims (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ingestion_id TEXT NOT NULL REFERENCES max_operational_ingestions(id) ON DELETE CASCADE,
  claim_type TEXT NOT NULL,
  claim_fingerprint TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  resolution_status TEXT NOT NULL DEFAULT 'pending' CHECK (resolution_status IN (
    'pending', 'RESOLVED', 'PROVISIONALLY_RESOLVED', 'AMBIGUOUS', 'UNRESOLVED', 'CONFLICT', 'ALREADY_APPLIED'
  )),
  resolution JSONB NOT NULL DEFAULT '{}'::jsonb,
  safety_class TEXT CHECK (safety_class IN ('A', 'B', 'C')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ingestion_id, claim_fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_max_ingestion_claims_ingestion
  ON max_ingestion_claims (ingestion_id);

CREATE TABLE IF NOT EXISTS max_applied_claims (
  claim_fingerprint TEXT PRIMARY KEY,
  ingestion_id TEXT NOT NULL REFERENCES max_operational_ingestions(id),
  claim_id UUID REFERENCES max_ingestion_claims(id),
  target_entity_type TEXT,
  target_entity_id TEXT,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS max_ingestion_mutations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ingestion_id TEXT NOT NULL REFERENCES max_operational_ingestions(id) ON DELETE CASCADE,
  claim_id UUID REFERENCES max_ingestion_claims(id),
  entity_type TEXT NOT NULL,
  entity_id TEXT,
  field_name TEXT NOT NULL,
  intended_value JSONB,
  safety_class TEXT NOT NULL CHECK (safety_class IN ('A', 'B', 'C')),
  commit_status TEXT NOT NULL DEFAULT 'proposed' CHECK (commit_status IN (
    'proposed', 'committed', 'blocked', 'verification_failed', 'skipped_duplicate'
  )),
  observed_value JSONB,
  verification_status TEXT CHECK (verification_status IN ('VERIFIED', 'COMMIT_VERIFICATION_FAILED')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_max_ingestion_mutations_ingestion
  ON max_ingestion_mutations (ingestion_id);

CREATE TABLE IF NOT EXISTS max_open_expectations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  prospect_id UUID,
  company_id UUID,
  ao_id INTEGER REFERENCES users(id),
  expectation_type TEXT NOT NULL,
  description TEXT,
  expected_window JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'WAITING', 'RESOLVED', 'OVERDUE')),
  source_ingestion_id TEXT REFERENCES max_operational_ingestions(id),
  source_claim_id UUID REFERENCES max_ingestion_claims(id),
  source_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  resolved_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_max_open_expectations_client_status
  ON max_open_expectations (client_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_max_open_expectations_prospect
  ON max_open_expectations (prospect_id, status);

CREATE TABLE IF NOT EXISTS max_ingestion_conflicts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ingestion_id TEXT NOT NULL REFERENCES max_operational_ingestions(id) ON DELETE CASCADE,
  claim_id UUID REFERENCES max_ingestion_claims(id),
  conflict_type TEXT NOT NULL,
  existing_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  incoming_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  resolution_required BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS max_ingestion_evidence_links (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  field_name TEXT NOT NULL,
  ingestion_id TEXT NOT NULL REFERENCES max_operational_ingestions(id),
  claim_id UUID REFERENCES max_ingestion_claims(id),
  artifact_id UUID REFERENCES max_evidence_artifacts(id),
  source_record JSONB NOT NULL DEFAULT '{}'::jsonb,
  derivation TEXT,
  confidence NUMERIC(4, 3),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_max_ingestion_evidence_links_entity
  ON max_ingestion_evidence_links (client_id, entity_type, entity_id);

COMMIT;
