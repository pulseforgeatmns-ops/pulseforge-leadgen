-- SPEC-118 Acquisition Mission Orchestration
-- Additive. Existing tenants start with no missions.

CREATE TABLE IF NOT EXISTS acquisition_missions (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  stage TEXT NOT NULL,
  status TEXT NOT NULL,
  objective TEXT NOT NULL,
  target_segment TEXT,
  campaign TEXT,
  title TEXT,
  priority TEXT NOT NULL DEFAULT 'normal',
  confidence DOUBLE PRECISION,
  owner TEXT,
  created_by TEXT,
  orchestration_mission_id TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS acquisition_missions_tenant_idx
  ON acquisition_missions (tenant_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS acquisition_mission_events (
  id TEXT PRIMARY KEY,
  mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  specialist TEXT,
  label TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS acquisition_mission_events_mission_idx
  ON acquisition_mission_events (mission_id, at ASC);

CREATE TABLE IF NOT EXISTS acquisition_mission_contributions (
  id TEXT PRIMARY KEY,
  mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL,
  specialist TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS acquisition_mission_contrib_idx
  ON acquisition_mission_contributions (mission_id, at ASC);

CREATE TABLE IF NOT EXISTS acquisition_mission_observations (
  id TEXT PRIMARY KEY,
  mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL,
  specialist TEXT NOT NULL,
  observation TEXT NOT NULL,
  at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS acquisition_mission_outcomes (
  id TEXT PRIMARY KEY,
  mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  outcome_type TEXT NOT NULL,
  segment TEXT,
  prospect_id INTEGER,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS acquisition_mission_learning (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  mission_id TEXT,
  segment TEXT NOT NULL,
  sends INTEGER,
  replies INTEGER,
  reply_rate DOUBLE PRECISION,
  statement TEXT,
  auto_applied BOOLEAN NOT NULL DEFAULT FALSE,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS acquisition_mission_learning_tenant_idx
  ON acquisition_mission_learning (tenant_id, created_at DESC);
