-- SPEC-022 Mission Engine durable store
-- Additive only. Rollback: migrations/2026-07-27-mission-engine.rollback.sql

CREATE TABLE IF NOT EXISTS missions (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  type TEXT NOT NULL,
  status TEXT NOT NULL,
  objective_text TEXT NOT NULL,
  title TEXT,
  constraints JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by TEXT,
  priority TEXT DEFAULT 'normal',
  plan JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence DOUBLE PRECISION,
  duration_estimate_ms INTEGER,
  progress JSONB NOT NULL DEFAULT '{}'::jsonb,
  deliverables JSONB,
  review JSONB,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS missions_tenant_created_idx
  ON missions (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS missions_client_created_idx
  ON missions (client_id, created_at DESC);
CREATE INDEX IF NOT EXISTS missions_status_idx
  ON missions (status);

CREATE TABLE IF NOT EXISTS mission_audit_events (
  id TEXT PRIMARY KEY,
  mission_id TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
  at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  kind TEXT NOT NULL,
  capability_id TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS mission_audit_mission_at_idx
  ON mission_audit_events (mission_id, at ASC);
