BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'open_source') THEN
    CREATE TYPE open_source AS ENUM ('human', 'proxy', 'unknown');
  END IF;
END $$;

ALTER TABLE email_events
  ADD COLUMN IF NOT EXISTS open_source open_source NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS open_source_reason TEXT,
  ADD COLUMN IF NOT EXISTS open_source_classified_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS user_agent TEXT,
  ADD COLUMN IF NOT EXISTS ip_address TEXT;

CREATE INDEX IF NOT EXISTS email_events_open_source_idx
  ON email_events (client_id, prospect_id, event_at, open_source)
  WHERE event_type IN ('opened', 'open', 'opened_proxy');

CREATE INDEX IF NOT EXISTS email_events_message_event_idx
  ON email_events (client_id, prospect_id, brevo_message_id, event_type, event_at)
  WHERE brevo_message_id IS NOT NULL;

COMMIT;
