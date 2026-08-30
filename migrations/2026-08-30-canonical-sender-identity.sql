-- AUDIT-085 / Canonical Single-Sender Enforcement
-- Quarantine foreign-domain provider events from Emmett reputation ingestion.

ALTER TABLE email_events
  ADD COLUMN IF NOT EXISTS sender_identity_status text,
  ADD COLUMN IF NOT EXISTS reputation_excluded boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS sender_identity_reason text;

CREATE INDEX IF NOT EXISTS email_events_reputation_scope_idx
  ON email_events (client_id, event_at)
  WHERE COALESCE(reputation_excluded, false) = false;
