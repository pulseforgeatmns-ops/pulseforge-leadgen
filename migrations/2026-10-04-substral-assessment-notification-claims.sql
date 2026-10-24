-- Idempotent operator notification claims for Studio Substral assessment intake.
-- Missing table fails closed in production when email delivery is required.
CREATE TABLE IF NOT EXISTS substral_assessment_notification_claims (
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
