-- Rollback SPEC-068 import_intent columns

DROP INDEX IF EXISTS market_emails_import_intent_received_idx;

ALTER TABLE market_emails
  DROP CONSTRAINT IF EXISTS market_emails_import_intent_nonempty;

ALTER TABLE market_emails
  DROP COLUMN IF EXISTS import_intent;

ALTER TABLE market_intel_sync_state
  DROP COLUMN IF EXISTS import_intent;
