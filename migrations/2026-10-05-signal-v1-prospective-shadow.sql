-- SIGNAL-V1-006 — Prospective empirical shadow mode (live observation, paper-only)

CREATE TABLE IF NOT EXISTS signal_source_registry (
  source_id TEXT PRIMARY KEY REFERENCES signal_sources(id) ON DELETE CASCADE,
  display_name TEXT NOT NULL,
  platform TEXT NOT NULL,
  external_ref TEXT,
  collector_id TEXT NOT NULL,
  source_role TEXT NOT NULL DEFAULT 'UNKNOWN',
  cluster_id TEXT REFERENCES signal_source_clusters(id),
  cluster_relationship_status TEXT NOT NULL DEFAULT 'UNKNOWN',
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signal_source_registry_active
  ON signal_source_registry (active);

CREATE TABLE IF NOT EXISTS signal_raw_caller_evidence (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  collector_id TEXT NOT NULL,
  source_id TEXT REFERENCES signal_sources(id),
  external_message_id TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw_reference TEXT,
  raw_text TEXT,
  extracted_ca TEXT,
  parser_version TEXT NOT NULL,
  token_address TEXT REFERENCES signal_tokens(token_address),
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (source_id, external_message_id, extracted_ca)
);

CREATE INDEX IF NOT EXISTS idx_signal_raw_caller_evidence_ingested
  ON signal_raw_caller_evidence (ingested_at DESC);

CREATE TABLE IF NOT EXISTS signal_prospective_research_jobs (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  observation_id TEXT REFERENCES signal_research_observations(id) ON DELETE CASCADE,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL,
  run_after TIMESTAMPTZ NOT NULL,
  target_delay_seconds INTEGER,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signal_prospective_jobs_status_run
  ON signal_prospective_research_jobs (status, run_after);

CREATE TABLE IF NOT EXISTS signal_collector_cursors (
  collector_id TEXT PRIMARY KEY,
  cursor_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_poll_at TIMESTAMPTZ,
  last_success_at TIMESTAMPTZ,
  last_error TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_token_research_episodes (
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  episode_kind TEXT NOT NULL,
  knowledge_at TIMESTAMPTZ NOT NULL,
  observation_id TEXT REFERENCES signal_research_observations(id),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (token_address, episode_kind, knowledge_at)
);

CREATE INDEX IF NOT EXISTS idx_signal_token_episodes_token
  ON signal_token_research_episodes (token_address, episode_kind);
