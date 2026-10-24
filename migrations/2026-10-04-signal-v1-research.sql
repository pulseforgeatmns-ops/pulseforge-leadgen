-- SIGNAL-V1-003 — Research observation layers + cohort evaluation

CREATE TABLE IF NOT EXISTS signal_research_observations (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  observation_type TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  feature_snapshot_id TEXT REFERENCES signal_feature_snapshots(id),
  trigger_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  definition_version TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (token_address, observation_type, definition_version)
);

CREATE INDEX IF NOT EXISTS idx_signal_research_obs_token_type
  ON signal_research_observations (token_address, observation_type);
CREATE INDEX IF NOT EXISTS idx_signal_research_obs_occurred
  ON signal_research_observations (occurred_at);

CREATE TABLE IF NOT EXISTS signal_research_cohorts (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  definition_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS signal_research_cohort_members (
  cohort_id TEXT NOT NULL REFERENCES signal_research_cohorts(id) ON DELETE CASCADE,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  inclusion_reason TEXT NOT NULL,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (cohort_id, token_address)
);

CREATE INDEX IF NOT EXISTS idx_signal_research_cohort_members_cohort
  ON signal_research_cohort_members (cohort_id);

CREATE TABLE IF NOT EXISTS signal_research_observation_outcomes (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES signal_research_observations(id) ON DELETE CASCADE,
  execution_delay_seconds INTEGER NOT NULL,
  data_availability TEXT NOT NULL,
  entry_price DOUBLE PRECISION,
  label TEXT,
  mfe DOUBLE PRECISION,
  mae DOUBLE PRECISION,
  time_to_2x_seconds DOUBLE PRECISION,
  time_to_minus_30_seconds DOUBLE PRECISION,
  return_15m DOUBLE PRECISION,
  return_1h DOUBLE PRECISION,
  return_6h DOUBLE PRECISION,
  return_24h DOUBLE PRECISION,
  horizon_hours INTEGER NOT NULL DEFAULT 24,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (observation_id, execution_delay_seconds)
);

CREATE INDEX IF NOT EXISTS idx_signal_research_outcomes_observation
  ON signal_research_observation_outcomes (observation_id);

CREATE TABLE IF NOT EXISTS signal_wallet_performance (
  id TEXT PRIMARY KEY,
  wallet_address TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  sample_size INTEGER NOT NULL DEFAULT 0,
  pass_rate DOUBLE PRECISION,
  fail_rate DOUBLE PRECISION,
  median_mfe DOUBLE PRECISION,
  median_mae DOUBLE PRECISION,
  median_time_to_2x_seconds DOUBLE PRECISION,
  score DOUBLE PRECISION,
  version TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (wallet_address, as_of, version)
);

CREATE INDEX IF NOT EXISTS idx_signal_wallet_performance_wallet
  ON signal_wallet_performance (wallet_address, as_of);
