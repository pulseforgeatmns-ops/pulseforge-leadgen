-- SPEC-068 — market email import / source intent
-- Additive. Does not reinterpret existing rows as competitors.
-- Rollback: migrations/2026-08-04-market-intelligence-import-intent.rollback.sql

ALTER TABLE market_emails
  ADD COLUMN IF NOT EXISTS import_intent TEXT NOT NULL DEFAULT 'general_market_messaging';

ALTER TABLE market_emails
  DROP CONSTRAINT IF EXISTS market_emails_import_intent_nonempty;

ALTER TABLE market_emails
  ADD CONSTRAINT market_emails_import_intent_nonempty
  CHECK (char_length(btrim(import_intent)) > 0);

CREATE INDEX IF NOT EXISTS market_emails_import_intent_received_idx
  ON market_emails (import_intent, received_at DESC);

ALTER TABLE market_intel_sync_state
  ADD COLUMN IF NOT EXISTS import_intent TEXT NOT NULL DEFAULT 'general_market_messaging';

COMMENT ON COLUMN market_emails.import_intent IS
  'Acquisition context only (SPEC-068 allowlist). Never a factual claim that the sender is a competitor.';

COMMENT ON COLUMN market_intel_sync_state.import_intent IS
  'Import intent used by the most recent successful non-dry-run sync (acquisition context only).';
