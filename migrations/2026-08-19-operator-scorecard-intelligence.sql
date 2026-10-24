-- SPEC-116 Operator Scorecard Intelligence (v1 thin slice)
-- Max recommends metrics. Operators approve. Drafts are never runtime truth.
-- Additive. Rollback: migrations/2026-08-19-operator-scorecard-intelligence.rollback.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS operator_scorecards (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  status TEXT NOT NULL
    CHECK (status IN ('draft', 'in_review', 'approved', 'superseded')),
  version INTEGER NOT NULL DEFAULT 1,
  business_goal TEXT,
  business_stage TEXT,
  profile TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_runtime BOOLEAN NOT NULL DEFAULT FALSE,
  approved_at TIMESTAMPTZ,
  approved_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS operator_scorecards_tenant_status_idx
  ON operator_scorecards (tenant_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS operator_scorecards_client_status_idx
  ON operator_scorecards (client_id, status, updated_at DESC)
  WHERE client_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS operator_scorecard_learning (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  scorecard_id TEXT,
  metric_key TEXT,
  metric_name TEXT,
  action TEXT NOT NULL,
  reason TEXT,
  suppress BOOLEAN NOT NULL DEFAULT FALSE,
  prioritize BOOLEAN NOT NULL DEFAULT FALSE,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS operator_scorecard_learning_tenant_idx
  ON operator_scorecard_learning (tenant_id, created_at DESC);
