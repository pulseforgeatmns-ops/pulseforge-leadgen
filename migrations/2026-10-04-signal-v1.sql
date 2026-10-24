-- SIGNAL-V1 — Attention-driven market intelligence (paper-only)

CREATE TABLE IF NOT EXISTS signal_tokens (
  token_address TEXT PRIMARY KEY,
  chain TEXT NOT NULL DEFAULT 'solana',
  ticker TEXT,
  address_provenance TEXT NOT NULL DEFAULT 'unknown',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_source_clusters (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  cluster_type TEXT NOT NULL,
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_sources (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  source_type TEXT NOT NULL,
  external_handle TEXT,
  cluster_id TEXT REFERENCES signal_source_clusters(id),
  active BOOLEAN NOT NULL DEFAULT TRUE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_source_cluster_members (
  source_id TEXT NOT NULL REFERENCES signal_sources(id) ON DELETE CASCADE,
  cluster_id TEXT NOT NULL REFERENCES signal_source_clusters(id) ON DELETE CASCADE,
  PRIMARY KEY (source_id, cluster_id)
);

CREATE TABLE IF NOT EXISTS signal_events (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  chain TEXT NOT NULL DEFAULT 'solana',
  event_type TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source_type TEXT NOT NULL,
  source_id TEXT REFERENCES signal_sources(id),
  source_cluster_id TEXT REFERENCES signal_source_clusters(id),
  actor_id TEXT,
  wallet_address TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence DOUBLE PRECISION NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_signal_events_token_occurred
  ON signal_events (token_address, occurred_at);
CREATE INDEX IF NOT EXISTS idx_signal_events_source_occurred
  ON signal_events (source_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_signal_events_type_occurred
  ON signal_events (event_type, occurred_at);

CREATE TABLE IF NOT EXISTS signal_feature_snapshots (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  evaluated_at TIMESTAMPTZ NOT NULL,
  feature_version TEXT NOT NULL,
  features JSONB NOT NULL,
  evidence_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signal_feature_snapshots_evaluated
  ON signal_feature_snapshots (evaluated_at);
CREATE INDEX IF NOT EXISTS idx_signal_feature_snapshots_token_evaluated
  ON signal_feature_snapshots (token_address, evaluated_at);

CREATE TABLE IF NOT EXISTS signal_source_performance (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES signal_sources(id),
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
  UNIQUE (source_id, as_of, version)
);

CREATE TABLE IF NOT EXISTS signal_decisions (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  decided_at TIMESTAMPTZ NOT NULL,
  state TEXT NOT NULL,
  previous_state TEXT,
  score DOUBLE PRECISION NOT NULL,
  feature_snapshot_id TEXT REFERENCES signal_feature_snapshots(id),
  strategy_version TEXT NOT NULL,
  explanation JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signal_decisions_token_decided
  ON signal_decisions (token_address, decided_at);

CREATE TABLE IF NOT EXISTS signal_market_outcomes (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  observed_at TIMESTAMPTZ NOT NULL,
  entry_price DOUBLE PRECISION NOT NULL,
  label TEXT NOT NULL,
  mfe DOUBLE PRECISION,
  mae DOUBLE PRECISION,
  time_to_2x_seconds DOUBLE PRECISION,
  time_to_minus_30_seconds DOUBLE PRECISION,
  return_15m DOUBLE PRECISION,
  return_1h DOUBLE PRECISION,
  return_6h DOUBLE PRECISION,
  return_24h DOUBLE PRECISION,
  horizon_hours INTEGER NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS signal_paper_positions (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  opened_at TIMESTAMPTZ NOT NULL,
  entry_price DOUBLE PRECISION NOT NULL,
  entry_market_cap DOUBLE PRECISION,
  notional_usd DOUBLE PRECISION NOT NULL,
  remaining_pct DOUBLE PRECISION NOT NULL DEFAULT 100,
  realized_pnl_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  unrealized_pnl_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  entry_snapshot_id TEXT REFERENCES signal_feature_snapshots(id),
  closed_at TIMESTAMPTZ,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS signal_paper_transactions (
  id TEXT PRIMARY KEY,
  position_id TEXT NOT NULL REFERENCES signal_paper_positions(id) ON DELETE CASCADE,
  executed_at TIMESTAMPTZ NOT NULL,
  side TEXT NOT NULL,
  pct DOUBLE PRECISION NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  notional_usd DOUBLE PRECISION NOT NULL,
  fees_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  slippage_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS signal_replay_runs (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  start_time TIMESTAMPTZ,
  end_time TIMESTAMPTZ,
  feature_version TEXT NOT NULL,
  strategy_version TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  summary JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS signal_alerts (
  id TEXT PRIMARY KEY,
  token_address TEXT NOT NULL REFERENCES signal_tokens(token_address),
  state TEXT NOT NULL,
  previous_state TEXT,
  score DOUBLE PRECISION NOT NULL,
  reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  risks JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
