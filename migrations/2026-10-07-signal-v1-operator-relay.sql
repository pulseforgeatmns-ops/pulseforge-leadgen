-- Explicit opt-in only. Separate minimal operational evidence; never research rows.
CREATE TABLE IF NOT EXISTS signal_operator_events (
  id TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE signal_operator_alert_outbox DROP CONSTRAINT IF EXISTS signal_operator_alert_outbox_evidence_id_fkey;
ALTER TABLE signal_operator_alert_outbox DROP CONSTRAINT IF EXISTS signal_operator_alert_outbox_delivery_state_check;
ALTER TABLE signal_operator_alert_outbox ADD CONSTRAINT signal_operator_alert_outbox_delivery_state_check
  CHECK (delivery_state IN ('PENDING','SENDING','ACCEPTED','UNKNOWN','FAILED','DELIVERED'));
ALTER TABLE signal_operator_alert_outbox ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE signal_operator_alert_outbox ADD COLUMN IF NOT EXISTS first_attempt_at TIMESTAMPTZ;
ALTER TABLE signal_operator_alert_outbox ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE signal_operator_alert_outbox ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
ALTER TABLE signal_operator_alert_outbox ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ;
ALTER TABLE signal_operator_alert_outbox ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ;
ALTER TABLE signal_operator_alert_outbox ADD COLUMN IF NOT EXISTS displayed_at TIMESTAMPTZ;
ALTER TABLE signal_operator_alert_outbox ADD COLUMN IF NOT EXISTS last_error TEXT;
CREATE TABLE IF NOT EXISTS signal_operator_receipts (
  alert_id TEXT NOT NULL REFERENCES signal_operator_alert_outbox(id),
  stage TEXT NOT NULL CHECK(stage IN ('RECEIVED','DISPLAYED')),
  receipt TEXT NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(alert_id,stage)
);
ALTER TABLE signal_operator_alert_outbox DROP CONSTRAINT IF EXISTS signal_operator_alert_outbox_token_address_fkey;
ALTER TABLE signal_operator_alert_outbox ADD COLUMN IF NOT EXISTS operator_event_id TEXT REFERENCES signal_operator_events(id);
