-- SPEC-117 Emmett Outbound Infrastructure Intelligence
-- Additive. Existing tenants start with no approved plan (fail closed).

CREATE TABLE IF NOT EXISTS emmett_inbox_snapshots (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  local_date DATE NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS emmett_inbox_snapshots_tenant_date_idx
  ON emmett_inbox_snapshots (tenant_id, local_date DESC);

CREATE TABLE IF NOT EXISTS emmett_send_plans (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  local_date DATE NOT NULL,
  status TEXT NOT NULL,
  recommended_capacity INTEGER,
  approved_capacity INTEGER,
  allow_legacy_sequences BOOLEAN NOT NULL DEFAULT FALSE,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  approved_at TIMESTAMPTZ,
  approved_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS emmett_send_plans_tenant_date_idx
  ON emmett_send_plans (tenant_id, local_date, status);

CREATE TABLE IF NOT EXISTS emmett_governor_acks (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  plan_id TEXT,
  outcome TEXT NOT NULL,
  operator_id TEXT,
  note TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS emmett_outbound_outcomes (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  prospect_id INTEGER,
  outcome_type TEXT NOT NULL,
  sinks JSONB NOT NULL DEFAULT '[]'::jsonb,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  event_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS emmett_outbound_outcomes_tenant_idx
  ON emmett_outbound_outcomes (tenant_id, event_at DESC);

CREATE TABLE IF NOT EXISTS emmett_outbound_learning (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  sink TEXT NOT NULL,
  outcome_id TEXT,
  outcome_type TEXT,
  statement TEXT,
  auto_applied BOOLEAN NOT NULL DEFAULT FALSE,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS emmett_outbound_learning_sink_idx
  ON emmett_outbound_learning (tenant_id, sink, created_at DESC);
