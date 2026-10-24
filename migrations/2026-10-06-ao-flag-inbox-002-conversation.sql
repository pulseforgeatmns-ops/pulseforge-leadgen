-- AO-FLAG-INBOX-002: unify conversation reports with canonical ao_account_flags

BEGIN;

SELECT pg_advisory_xact_lock(20261006, 2);

ALTER TABLE ao_account_flags
  ALTER COLUMN account_id DROP NOT NULL;

ALTER TABLE ao_max_conversation_reports
  ADD COLUMN IF NOT EXISTS canonical_flag_id UUID REFERENCES ao_account_flags(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_ao_max_reports_canonical_flag
  ON ao_max_conversation_reports (canonical_flag_id)
  WHERE canonical_flag_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ao_account_flags_conversation
  ON ao_account_flags (client_id, conversation_id, status, created_at DESC)
  WHERE conversation_id IS NOT NULL;

COMMIT;
