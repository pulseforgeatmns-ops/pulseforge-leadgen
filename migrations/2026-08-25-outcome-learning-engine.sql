-- SPEC-166 — Outcome Learning Engine persistence.
-- Predictions, evaluations, and durable learnings. Never auto-applied.

CREATE TABLE IF NOT EXISTS acquisition_mission_predictions (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
  opportunity_id TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_amo_predictions_tenant ON acquisition_mission_predictions (tenant_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_amo_predictions_mission ON acquisition_mission_predictions (mission_id, captured_at DESC);

CREATE TABLE IF NOT EXISTS acquisition_mission_outcome_evaluations (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
  prediction_id TEXT NOT NULL,
  accuracy TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_amo_evaluations_tenant ON acquisition_mission_outcome_evaluations (tenant_id, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_amo_evaluations_mission ON acquisition_mission_outcome_evaluations (mission_id, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS acquisition_mission_outcome_learnings (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  mission_id TEXT,
  evaluation_id TEXT,
  kind TEXT NOT NULL,
  subject TEXT,
  direction TEXT,
  statement TEXT,
  auto_applied BOOLEAN NOT NULL DEFAULT FALSE,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_amo_outcome_learnings_tenant ON acquisition_mission_outcome_learnings (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_amo_outcome_learnings_mission ON acquisition_mission_outcome_learnings (mission_id, created_at DESC);
