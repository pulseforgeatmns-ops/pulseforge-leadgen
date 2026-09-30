-- Apply before deploying the notification guard. Missing schema fails closed for
-- email delivery; the form still stores the request for the operator dashboard.
CREATE TABLE IF NOT EXISTS walkthrough_notification_claims (
  client_id INTEGER NOT NULL,
  fingerprint TEXT NOT NULL,
  claim_id UUID NOT NULL UNIQUE,
  action_id TEXT NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status TEXT NOT NULL DEFAULT 'claimed'
    CHECK (status IN ('claimed', 'sent', 'failed_or_uncertain')),
  provider_message_id TEXT,
  PRIMARY KEY (client_id, fingerprint)
);
