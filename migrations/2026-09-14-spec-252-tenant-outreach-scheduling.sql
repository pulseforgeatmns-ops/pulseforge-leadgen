-- SPEC-252 — Durable tenant outreach scheduling and execution.
-- Canonical scheduled-send records; executor claims due rows atomically.

BEGIN;

CREATE TABLE IF NOT EXISTS tenant_outreach_scheduled_sends (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  prospect_id TEXT NOT NULL,
  acquisition_knowledge_object_id TEXT,
  outreach_asset_id TEXT NOT NULL,
  outreach_asset_version TEXT,
  sending_identity_id TEXT NOT NULL,
  recipient_email TEXT NOT NULL,
  mission_id TEXT,
  thread_id TEXT,
  sequence_step INTEGER NOT NULL DEFAULT 1 CHECK (sequence_step >= 1 AND sequence_step <= 3),
  scheduled_for TIMESTAMPTZ NOT NULL,
  timezone TEXT NOT NULL DEFAULT 'America/New_York',
  status TEXT NOT NULL DEFAULT 'SCHEDULED',
  authorization_source TEXT NOT NULL,
  authorized_by TEXT NOT NULL,
  authorized_at TIMESTAMPTZ NOT NULL,
  authorization_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  past_due_policy TEXT NOT NULL DEFAULT 'execute_within_window',
  max_lateness_minutes INTEGER NOT NULL DEFAULT 30,
  idempotency_key TEXT NOT NULL,
  claim_token TEXT,
  claimed_at TIMESTAMPTZ,
  outbound_message_id TEXT,
  failure_code TEXT,
  failure_message TEXT,
  skip_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  executed_at TIMESTAMPTZ,
  cancelled_at TIMESTAMPTZ,
  CONSTRAINT tenant_outreach_scheduled_sends_status_check CHECK (
    status IN ('SCHEDULED', 'EXECUTING', 'SENT', 'PAUSED', 'CANCELLED', 'FAILED', 'SKIPPED')
  ),
  CONSTRAINT tenant_outreach_scheduled_sends_past_due_policy_check CHECK (
    past_due_policy IN ('execute_within_window', 'skip_past_due', 'execute_anytime')
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS tenant_outreach_scheduled_sends_idempotency_idx
  ON tenant_outreach_scheduled_sends (tenant_id, idempotency_key);

CREATE INDEX IF NOT EXISTS tenant_outreach_scheduled_sends_due_idx
  ON tenant_outreach_scheduled_sends (status, scheduled_for)
  WHERE status IN ('SCHEDULED', 'EXECUTING');

CREATE INDEX IF NOT EXISTS tenant_outreach_scheduled_sends_tenant_idx
  ON tenant_outreach_scheduled_sends (tenant_id, status, scheduled_for DESC);

CREATE INDEX IF NOT EXISTS tenant_outreach_scheduled_sends_prospect_idx
  ON tenant_outreach_scheduled_sends (tenant_id, prospect_id, sequence_step);

COMMIT;
