-- SPEC-254 — Emmett tenant-mailbox capacity envelopes for SPEC-252 authorization.

CREATE TABLE IF NOT EXISTS emmett_tenant_mailbox_capacity_envelopes (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  mailbox_integration_id TEXT NOT NULL,
  sending_identity_id TEXT NOT NULL,
  sender_email TEXT NOT NULL,
  sending_domain TEXT,
  local_date DATE NOT NULL,
  computed_at TIMESTAMPTZ NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL,
  valid_until TIMESTAMPTZ NOT NULL,
  max_sends_per_day INTEGER NOT NULL,
  max_sends_per_window INTEGER,
  minimum_spacing_minutes INTEGER NOT NULL DEFAULT 30,
  allowed_send_window JSONB NOT NULL DEFAULT '{"startHour":9,"endHour":17}'::jsonb,
  timezone TEXT NOT NULL DEFAULT 'America/New_York',
  ramp_stage TEXT,
  governor_state TEXT NOT NULL,
  current_sent_count INTEGER NOT NULL DEFAULT 0,
  current_scheduled_count INTEGER NOT NULL DEFAULT 0,
  current_executing_count INTEGER NOT NULL DEFAULT 0,
  remaining_capacity INTEGER NOT NULL DEFAULT 0,
  risk_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  emmett_contribution JSONB NOT NULL DEFAULT '{}'::jsonb,
  version INTEGER NOT NULL DEFAULT 1,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS emmett_tmb_capacity_envelopes_identity_date_idx
  ON emmett_tenant_mailbox_capacity_envelopes (tenant_id, sending_identity_id, local_date DESC);

CREATE INDEX IF NOT EXISTS emmett_tmb_capacity_envelopes_valid_idx
  ON emmett_tenant_mailbox_capacity_envelopes (tenant_id, sending_identity_id, valid_until DESC);

CREATE TABLE IF NOT EXISTS emmett_tenant_mailbox_capacity_reservations (
  id TEXT PRIMARY KEY,
  envelope_id TEXT NOT NULL REFERENCES emmett_tenant_mailbox_capacity_envelopes(id),
  tenant_id TEXT NOT NULL,
  sending_identity_id TEXT NOT NULL,
  schedule_id TEXT,
  status TEXT NOT NULL,
  scheduled_for TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS emmett_tmb_capacity_reservations_schedule_idx
  ON emmett_tenant_mailbox_capacity_reservations (tenant_id, schedule_id)
  WHERE schedule_id IS NOT NULL;
