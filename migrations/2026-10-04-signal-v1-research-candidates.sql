-- SIGNAL-V1-003 Phase B — research candidates + cohort freeze metadata

CREATE TABLE IF NOT EXISTS signal_research_candidates (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL,
  chain TEXT NOT NULL DEFAULT 'solana',
  discovered_from TEXT NOT NULL,
  earliest_known_call_at TIMESTAMPTZ,
  source_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  source_cluster_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  selection_category TEXT,
  selection_reason TEXT,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL,
  exclusion_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (token_address, discovered_from)
);

CREATE INDEX IF NOT EXISTS idx_signal_research_candidates_status
  ON signal_research_candidates (status);
CREATE INDEX IF NOT EXISTS idx_signal_research_candidates_token
  ON signal_research_candidates (token_address);

ALTER TABLE signal_research_cohorts
  ADD COLUMN IF NOT EXISTS frozen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS selection_version TEXT;
