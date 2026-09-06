-- SPEC-247 Canonical Acquisition Knowledge
-- Additive only. Runtime also ensures this schema for first-run safety.

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
  epistemic_kind TEXT NOT NULL,
  lifecycle_state TEXT NOT NULL,
  status TEXT NOT NULL,
  channel TEXT,
  experiment_id TEXT,
  tags TEXT[] NOT NULL DEFAULT '{}',
  confidence DOUBLE PRECISION,
  evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
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

CREATE INDEX IF NOT EXISTS acquisition_knowledge_objects_tenant_idx
  ON acquisition_knowledge_objects (tenant_id, object_type, lifecycle_state);

CREATE INDEX IF NOT EXISTS acquisition_knowledge_objects_mission_idx
  ON acquisition_knowledge_objects (tenant_id, mission_id);

CREATE INDEX IF NOT EXISTS acquisition_knowledge_objects_tags_idx
  ON acquisition_knowledge_objects USING GIN (tags);
