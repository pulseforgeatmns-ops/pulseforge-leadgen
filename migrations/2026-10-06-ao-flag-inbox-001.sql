-- AO-FLAG-INBOX-001: durable AO flags inbox, unread state, operator notifications

BEGIN;

SELECT pg_advisory_xact_lock(20261006, 1);

ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'crm_account';
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS source_id UUID;
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS conversation_id UUID;
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS created_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS created_by_role TEXT;
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS assigned_to_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS unread BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS resolution_note TEXT;
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS source_context JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE ao_account_flags ADD COLUMN IF NOT EXISTS legacy_partial BOOLEAN NOT NULL DEFAULT false;

UPDATE ao_account_flags
SET source_id = account_id
WHERE source_id IS NULL AND account_id IS NOT NULL;

UPDATE ao_account_flags
SET created_by_user_id = ao_id
WHERE created_by_user_id IS NULL AND ao_id IS NOT NULL;

UPDATE ao_account_flags
SET unread = false
WHERE status IN ('resolved', 'dismissed');

UPDATE ao_account_flags
SET status = 'resolved',
    resolved_at = COALESCE(resolved_at, created_at),
    unread = false
WHERE status = 'dismissed';

ALTER TABLE ao_account_flags DROP CONSTRAINT IF EXISTS ao_account_flags_status_check;
ALTER TABLE ao_account_flags ADD CONSTRAINT ao_account_flags_status_check
  CHECK (status IN ('open', 'reviewed', 'resolved', 'dismissed'));

CREATE TABLE IF NOT EXISTS ao_flag_notifications (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  flag_id UUID NOT NULL REFERENCES ao_account_flags(id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  body TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  read_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ao_flag_notifications_user_unread
  ON ao_flag_notifications (client_id, user_id, created_at DESC)
  WHERE read_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_ao_account_flags_idempotency_active
  ON ao_account_flags (client_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL AND status IN ('open', 'reviewed');

CREATE INDEX IF NOT EXISTS idx_ao_account_flags_assignee_status
  ON ao_account_flags (client_id, assigned_to_user_id, status, created_at DESC);

COMMIT;
