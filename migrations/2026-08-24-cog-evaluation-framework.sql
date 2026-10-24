-- COG Cognitive Evaluation Framework — result persistence
-- Run manually or via utils/cogSchema.ensureCogSchema()

BEGIN;

CREATE TABLE IF NOT EXISTS cog_runs (
  id UUID PRIMARY KEY,
  suite_id TEXT NOT NULL,
  suite_version TEXT NOT NULL,
  cog_version TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ,
  overall_score NUMERIC(4,1),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cog_domain_results (
  id BIGSERIAL PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES cog_runs(id) ON DELETE CASCADE,
  domain_id TEXT NOT NULL,
  status TEXT NOT NULL,
  conversation_id TEXT,
  score NUMERIC(4,1),
  review_status TEXT NOT NULL DEFAULT 'pending',
  duration_ms INTEGER NOT NULL DEFAULT 0,
  transcript JSONB NOT NULL DEFAULT '[]'::jsonb,
  failures JSONB NOT NULL DEFAULT '[]'::jsonb,
  behavior_results JSONB NOT NULL DEFAULT '[]'::jsonb,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cog_runs_suite_started ON cog_runs (suite_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_cog_domain_results_run ON cog_domain_results (run_id);
CREATE INDEX IF NOT EXISTS idx_cog_domain_results_domain ON cog_domain_results (domain_id, created_at DESC);

COMMIT;
