-- SPEC-113 Acquisition Intelligence Compiler (v1)
-- Compile market knowledge into an approved AIM. Additive only.
-- Rollback: migrations/2026-08-18-acquisition-intelligence-compiler.rollback.sql

ALTER TABLE aim_models DROP CONSTRAINT IF EXISTS aim_models_status_check;
ALTER TABLE aim_models ADD CONSTRAINT aim_models_status_check
  CHECK (status IN ('draft', 'complete', 'published', 'superseded'));

CREATE TABLE IF NOT EXISTS aic_workspaces (
  id TEXT PRIMARY KEY,
  client_key TEXT NOT NULL,
  client_id INTEGER,
  status TEXT NOT NULL
    CHECK (status IN (
      'new',
      'ingesting',
      'extracted',
      'ontology_ready',
      'in_review',
      'approved',
      'published'
    )),
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  aim_id TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_operating_fact BOOLEAN NOT NULL DEFAULT FALSE,
  compiled_at TIMESTAMPTZ,
  approved_at TIMESTAMPTZ,
  approved_by TEXT,
  published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS aic_workspaces_client_key_idx
  ON aic_workspaces (client_key);

CREATE INDEX IF NOT EXISTS aic_workspaces_status_idx
  ON aic_workspaces (status);

CREATE TABLE IF NOT EXISTS aic_documents (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES aic_workspaces (id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  kind TEXT NOT NULL,
  filename TEXT,
  body TEXT NOT NULL,
  uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS aic_documents_workspace_idx
  ON aic_documents (workspace_id);

CREATE TABLE IF NOT EXISTS aic_concepts (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES aic_workspaces (id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  label TEXT NOT NULL,
  statement TEXT,
  confidence NUMERIC,
  status TEXT NOT NULL
    CHECK (status IN ('proposed', 'accepted', 'edited', 'merged', 'removed')),
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence_excerpt TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS aic_concepts_workspace_idx
  ON aic_concepts (workspace_id);

CREATE TABLE IF NOT EXISTS aic_edges (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES aic_workspaces (id) ON DELETE CASCADE,
  from_concept_id TEXT NOT NULL,
  to_concept_id TEXT NOT NULL,
  relation TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS aic_edges_workspace_idx
  ON aic_edges (workspace_id);

CREATE TABLE IF NOT EXISTS aic_reviews (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES aic_workspaces (id) ON DELETE CASCADE,
  concept_id TEXT,
  action TEXT NOT NULL
    CHECK (action IN ('accept', 'edit', 'merge', 'remove')),
  operator TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS aic_reviews_workspace_idx
  ON aic_reviews (workspace_id);
