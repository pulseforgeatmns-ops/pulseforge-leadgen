-- SPEC-247 / SPEC-247A acquisition knowledge foundation.
-- Idempotent and additive. Canonical epistemic and validation states are
-- independent from legacy epistemic_kind/lifecycle_state projections.

CREATE TABLE IF NOT EXISTS acquisition_knowledge_objects (
  id TEXT PRIMARY KEY,
  external_key TEXT,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  mission_id TEXT,
  scope TEXT NOT NULL,
  object_type TEXT NOT NULL,
  title TEXT NOT NULL,
  content JSONB NOT NULL DEFAULT '{}'::jsonb,
  epistemic_state TEXT,
  validation_state TEXT,
  epistemic_kind TEXT NOT NULL,
  lifecycle_state TEXT NOT NULL,
  status TEXT NOT NULL,
  channel TEXT,
  experiment_id TEXT,
  tags TEXT[] NOT NULL DEFAULT '{}',
  confidence DOUBLE PRECISION,
  evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  derivation JSONB,
  relationships JSONB NOT NULL DEFAULT '[]'::jsonb,
  approved_by TEXT,
  created_from TEXT,
  created_by TEXT,
  version INTEGER NOT NULL DEFAULT 1,
  supersedes_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, external_key)
);

CREATE TABLE IF NOT EXISTS acquisition_knowledge_revisions (
  id BIGSERIAL PRIMARY KEY,
  knowledge_id TEXT NOT NULL REFERENCES acquisition_knowledge_objects(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  operation TEXT NOT NULL,
  actor_id TEXT,
  actor_role TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  rationale TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS acquisition_knowledge_decisions (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  mission_id TEXT,
  recommendation TEXT NOT NULL,
  question TEXT,
  knowledge_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  explanation JSONB NOT NULL DEFAULT '{}'::jsonb,
  actor_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE acquisition_knowledge_objects
  ADD COLUMN IF NOT EXISTS epistemic_state TEXT,
  ADD COLUMN IF NOT EXISTS validation_state TEXT,
  ADD COLUMN IF NOT EXISTS derivation JSONB,
  ADD COLUMN IF NOT EXISTS relationships JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE acquisition_knowledge_objects
SET epistemic_state = CASE
  WHEN epistemic_kind IN ('observed_fact', 'operator_preference', 'stakeholder_preference', 'canonical_truth') THEN 'OBSERVED'
  WHEN epistemic_kind IN ('hypothesis', 'validated_finding') THEN 'INFERRED'
  ELSE 'UNKNOWN'
END
WHERE epistemic_state IS NULL OR btrim(epistemic_state) = '';

UPDATE acquisition_knowledge_objects
SET validation_state = CASE
  WHEN lifecycle_state = 'STAKEHOLDER_VALIDATED' THEN 'STAKEHOLDER_VALIDATED'
  WHEN lifecycle_state IN ('MARKET_VALIDATED', 'CANONICAL') THEN 'MARKET_VALIDATED'
  ELSE 'UNVALIDATED'
END
WHERE validation_state IS NULL OR btrim(validation_state) = '';

ALTER TABLE acquisition_knowledge_objects
  ALTER COLUMN epistemic_state SET DEFAULT 'UNKNOWN',
  ALTER COLUMN epistemic_state SET NOT NULL,
  ALTER COLUMN validation_state SET DEFAULT 'UNVALIDATED',
  ALTER COLUMN validation_state SET NOT NULL;

DO $$
BEGIN
  ALTER TABLE acquisition_knowledge_objects
    ADD CONSTRAINT acquisition_knowledge_epistemic_state_check
    CHECK (epistemic_state IN ('OBSERVED', 'INFERRED', 'UNKNOWN'));
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE acquisition_knowledge_objects
    ADD CONSTRAINT acquisition_knowledge_validation_state_check
    CHECK (validation_state IN ('UNVALIDATED', 'STAKEHOLDER_VALIDATED', 'MARKET_VALIDATED'));
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS acquisition_knowledge_objects_tenant_idx
  ON acquisition_knowledge_objects (tenant_id, object_type, lifecycle_state);
CREATE INDEX IF NOT EXISTS acquisition_knowledge_objects_mission_idx
  ON acquisition_knowledge_objects (tenant_id, mission_id);
CREATE INDEX IF NOT EXISTS acquisition_knowledge_objects_tags_idx
  ON acquisition_knowledge_objects USING GIN (tags);
CREATE INDEX IF NOT EXISTS acquisition_knowledge_objects_canonical_state_idx
  ON acquisition_knowledge_objects (tenant_id, epistemic_state, validation_state);
