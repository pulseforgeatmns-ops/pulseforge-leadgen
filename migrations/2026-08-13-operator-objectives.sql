-- SPEC-095 Max Durable Operator Objectives (v1 thin slice)
-- Strategic objectives that survive fresh Max sessions — context only, not execution.
-- Distinct from missions: creating an objective never plans or executes a Mission.
-- Additive. Rollback: migrations/2026-08-13-operator-objectives.rollback.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS operator_objectives (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  scope TEXT NOT NULL
    CHECK (scope IN ('operator', 'client')),
  client_id INTEGER
    CHECK (
      (scope = 'operator' AND client_id IS NULL)
      OR (scope = 'client' AND client_id IS NOT NULL)
    ),
  title TEXT NOT NULL,
  objective_text TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'paused', 'completed', 'cancelled')),
  time_horizon TEXT,
  current_phase TEXT,
  context JSONB NOT NULL DEFAULT '{}'::jsonb,
  aliases TEXT[] NOT NULL DEFAULT '{}'::text[],
  created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS operator_objectives_tenant_status_idx
  ON operator_objectives (tenant_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS operator_objectives_tenant_scope_idx
  ON operator_objectives (tenant_id, scope, status);

CREATE INDEX IF NOT EXISTS operator_objectives_client_status_idx
  ON operator_objectives (client_id, status, updated_at DESC)
  WHERE client_id IS NOT NULL;

-- Production fixture: Public Max Launch (Pulseforge operator scope).
-- Idempotent — safe to re-run. Does not create Missions or trigger execution.
INSERT INTO operator_objectives (
  tenant_id,
  scope,
  client_id,
  title,
  objective_text,
  status,
  time_horizon,
  current_phase,
  context,
  aliases,
  created_by
)
SELECT
  '1',
  'operator',
  NULL,
  'Public Max Launch',
  'Build qualified attention around the ideas behind Pulseforge, progressively expose the problems we''re solving, then reveal Max and convert that attention into qualified demos.',
  'active',
  'Roughly three weeks, evidence-gated rather than date-forced.',
  'Thesis / problem exposure',
  jsonb_build_object(
    'owner', 'Max',
    'content_owner', 'Paige',
    'launch_trigger', 'evidence_gate',
    'notes',
      'Max owns the overall campaign. Paige owns content strategy and individual content experiments. SPEC-092 records content outcomes. SPEC-093 derives content learnings. Launch timing should respond to evidence rather than a fixed calendar date.'
  ),
  ARRAY[
    'max launch',
    'public max launch',
    'the launch',
    'launch campaign',
    'max launch campaign',
    'public launch'
  ]::text[],
  'spec_095_seed'
WHERE NOT EXISTS (
  SELECT 1
  FROM operator_objectives
  WHERE tenant_id = '1'
    AND scope = 'operator'
    AND lower(title) = 'public max launch'
    AND status = 'active'
);
