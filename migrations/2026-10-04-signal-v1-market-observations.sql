-- SIGNAL-V1-002 — Ground-truth market observations + outcome simulation metadata

CREATE TABLE IF NOT EXISTS signal_market_observations (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  occurred_at TIMESTAMPTZ NOT NULL,
  price_usd DOUBLE PRECISION NOT NULL,
  market_cap_usd DOUBLE PRECISION,
  liquidity_usd DOUBLE PRECISION,
  volume_interval_usd DOUBLE PRECISION,
  interval_seconds INTEGER NOT NULL,
  provider TEXT NOT NULL,
  external_id TEXT,
  provider_timestamp TIMESTAMPTZ,
  observed_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (token_address, provider, occurred_at, interval_seconds)
);

CREATE INDEX IF NOT EXISTS idx_signal_market_observations_token_occurred
  ON signal_market_observations (token_address, occurred_at);

CREATE TABLE IF NOT EXISTS signal_market_ingestion_stats (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  provider TEXT NOT NULL,
  requested_start TIMESTAMPTZ NOT NULL,
  requested_end TIMESTAMPTZ NOT NULL,
  effective_resolution_seconds INTEGER,
  received_count INTEGER NOT NULL DEFAULT 0,
  inserted_count INTEGER NOT NULL DEFAULT 0,
  duplicate_count INTEGER NOT NULL DEFAULT 0,
  rejected_count INTEGER NOT NULL DEFAULT 0,
  missing_intervals INTEGER NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE signal_market_outcomes
  ADD COLUMN IF NOT EXISTS decision_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS execution_delay_seconds INTEGER,
  ADD COLUMN IF NOT EXISTS effective_execution_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS effective_price DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS data_resolution_seconds INTEGER,
  ADD COLUMN IF NOT EXISTS highest_price DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS lowest_price DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS outcome_timestamp TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_market_outcomes_delay
  ON signal_market_outcomes (token_address, decision_at, execution_delay_seconds);
