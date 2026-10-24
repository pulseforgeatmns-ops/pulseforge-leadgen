-- SPEC-071 — Mission-bound outbound execution evidence (canonical EXECUTE path).

CREATE TABLE IF NOT EXISTS acquisition_mission_outbound_executions (
  id TEXT PRIMARY KEY,
  mission_id TEXT NOT NULL,
  tenant_id TEXT,
  prospect_id TEXT NOT NULL,
  prepared_artifact_revision TEXT NOT NULL,
  execution_approval_contribution_id TEXT,
  provider TEXT NOT NULL DEFAULT 'brevo',
  provider_message_id TEXT,
  status TEXT NOT NULL,
  provider_error_code TEXT,
  provider_error_message TEXT,
  execution_request_id TEXT,
  transaction_id TEXT,
  execution_identity TEXT NOT NULL,
  idempotency_key TEXT,
  attempted_at TIMESTAMPTZ,
  sent_at TIMESTAMPTZ,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS acquisition_mission_outbound_executions_identity_sent_idx
  ON acquisition_mission_outbound_executions (execution_identity)
  WHERE status = 'sent';

CREATE INDEX IF NOT EXISTS acquisition_mission_outbound_executions_mission_idx
  ON acquisition_mission_outbound_executions (mission_id, attempted_at DESC);

CREATE INDEX IF NOT EXISTS acquisition_mission_outbound_executions_prospect_idx
  ON acquisition_mission_outbound_executions (prospect_id, mission_id);

CREATE INDEX IF NOT EXISTS acquisition_mission_outbound_executions_provider_msg_idx
  ON acquisition_mission_outbound_executions (provider_message_id)
  WHERE provider_message_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS acquisition_mission_provider_events (
  id TEXT PRIMARY KEY,
  dedupe_key TEXT NOT NULL UNIQUE,
  mission_id TEXT NOT NULL,
  tenant_id TEXT,
  prospect_id TEXT NOT NULL,
  execution_record_id TEXT NOT NULL REFERENCES acquisition_mission_outbound_executions(id),
  prepared_artifact_revision TEXT NOT NULL,
  provider TEXT NOT NULL DEFAULT 'brevo',
  provider_message_id TEXT,
  event_type TEXT NOT NULL,
  event_category TEXT NOT NULL,
  raw_event_type TEXT,
  provider_event_id TEXT,
  occurred_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS acquisition_mission_provider_events_execution_idx
  ON acquisition_mission_provider_events (execution_record_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS acquisition_mission_provider_events_mission_idx
  ON acquisition_mission_provider_events (mission_id, occurred_at DESC);
