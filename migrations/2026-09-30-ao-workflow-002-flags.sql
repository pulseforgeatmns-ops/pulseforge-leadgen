-- SPEC-AO-WORKFLOW-002: AO CRM account flags for Jake operator brief

BEGIN;

SELECT pg_advisory_xact_lock(20260930, 2);

CREATE TABLE IF NOT EXISTS ao_account_flags (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  account_id UUID NOT NULL,
  ao_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  reason TEXT NOT NULL,
  note TEXT,
  status TEXT NOT NULL DEFAULT 'open',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ,
  CONSTRAINT ao_account_flags_status_check CHECK (status IN ('open', 'resolved', 'dismissed'))
);

CREATE INDEX IF NOT EXISTS idx_ao_account_flags_client_open
  ON ao_account_flags (client_id, status, created_at DESC)
  WHERE status = 'open';

CREATE INDEX IF NOT EXISTS idx_ao_account_flags_account
  ON ao_account_flags (account_id, created_at DESC);

COMMIT;
