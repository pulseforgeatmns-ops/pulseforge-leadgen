-- SPEC-098 Max Specialist Delegation Contract
-- Durable Max → specialist delegations and specialist → Max results.
-- Additive. Rollback: migrations/2026-08-16-specialist-delegation.rollback.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS specialist_delegations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  specialist TEXT NOT NULL,
  capability TEXT NOT NULL,
  objective TEXT NOT NULL,
  reason TEXT NOT NULL,
  business_context JSONB NOT NULL DEFAULT '{}'::jsonb,
  target_context JSONB,
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  constraints JSONB NOT NULL DEFAULT '{}'::jsonb,
  authority TEXT NOT NULL
    CHECK (authority IN (
      'observe',
      'recommend',
      'draft',
      'execute_after_approval',
      'execute'
    )),
  expected_return JSONB NOT NULL DEFAULT '{}'::jsonb,
  requested_by TEXT NOT NULL DEFAULT 'max',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status TEXT NOT NULL
    CHECK (status IN (
      'created',
      'authorized',
      'rejected',
      'running',
      'completed',
      'partial',
      'blocked',
      'failed',
      'declined_policy'
    )),
  policy_events JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS specialist_delegations_tenant_created_idx
  ON specialist_delegations (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS specialist_delegations_tenant_status_idx
  ON specialist_delegations (tenant_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS specialist_delegations_tenant_specialist_idx
  ON specialist_delegations (tenant_id, specialist, capability);

CREATE TABLE IF NOT EXISTS specialist_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  delegation_id UUID NOT NULL REFERENCES specialist_delegations(id),
  tenant_id TEXT NOT NULL,
  specialist TEXT NOT NULL,
  capability TEXT NOT NULL,
  status TEXT NOT NULL
    CHECK (status IN (
      'completed',
      'partial',
      'blocked',
      'failed',
      'declined_policy'
    )),
  summary TEXT,
  observations JSONB NOT NULL DEFAULT '[]'::jsonb,
  actions_taken JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  artifact_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  confidence NUMERIC,
  uncertainties JSONB NOT NULL DEFAULT '[]'::jsonb,
  recommended_next_action JSONB,
  policy_events JSONB NOT NULL DEFAULT '[]'::jsonb,
  errors JSONB NOT NULL DEFAULT '[]'::jsonb,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS specialist_results_tenant_delegation_idx
  ON specialist_results (tenant_id, delegation_id, completed_at DESC);

CREATE INDEX IF NOT EXISTS specialist_results_tenant_created_idx
  ON specialist_results (tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS specialist_evaluations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  delegation_id UUID NOT NULL REFERENCES specialist_delegations(id),
  result_id UUID NOT NULL REFERENCES specialist_results(id),
  objective_satisfied BOOLEAN NOT NULL,
  material_change BOOLEAN NOT NULL DEFAULT false,
  warrants_operator_attention BOOLEAN NOT NULL DEFAULT false,
  warrants_another_delegation BOOLEAN NOT NULL DEFAULT false,
  suggested_priority_change JSONB,
  priority_applied BOOLEAN NOT NULL DEFAULT false,
  operator_direction_honored BOOLEAN NOT NULL DEFAULT true,
  accepted_as_ground_truth BOOLEAN NOT NULL DEFAULT false,
  explanation TEXT NOT NULL,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS specialist_evaluations_tenant_result_idx
  ON specialist_evaluations (tenant_id, result_id, created_at DESC);

CREATE INDEX IF NOT EXISTS specialist_evaluations_tenant_delegation_idx
  ON specialist_evaluations (tenant_id, delegation_id, created_at DESC);
