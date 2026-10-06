-- SPEC-AO-QUEUE-DEAD-001: AO queue mark-dead disposition (prospect + field lead)

BEGIN;

SELECT pg_advisory_xact_lock(20261005, 901);

ALTER TABLE prospects
  ADD COLUMN IF NOT EXISTS disposition_status TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS dead_reason TEXT,
  ADD COLUMN IF NOT EXISTS dead_note TEXT,
  ADD COLUMN IF NOT EXISTS dead_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS dead_by INTEGER REFERENCES users(id);

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_disposition_status_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_disposition_status_check
  CHECK (disposition_status IN ('active', 'dead'));

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_dead_reason_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_dead_reason_check
  CHECK (dead_reason IS NULL OR dead_reason IN (
    'business_not_found',
    'closed_or_inactive',
    'bad_address',
    'duplicate',
    'not_a_fit',
    'wrong_company_or_bad_data',
    'do_not_contact',
    'other'
  ));

ALTER TABLE ao_leads
  ADD COLUMN IF NOT EXISTS disposition_status TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS dead_reason TEXT,
  ADD COLUMN IF NOT EXISTS dead_note TEXT,
  ADD COLUMN IF NOT EXISTS dead_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS dead_by INTEGER REFERENCES users(id);

ALTER TABLE ao_leads DROP CONSTRAINT IF EXISTS ao_leads_disposition_status_check;
ALTER TABLE ao_leads ADD CONSTRAINT ao_leads_disposition_status_check
  CHECK (disposition_status IN ('active', 'dead'));

ALTER TABLE ao_leads DROP CONSTRAINT IF EXISTS ao_leads_dead_reason_check;
ALTER TABLE ao_leads ADD CONSTRAINT ao_leads_dead_reason_check
  CHECK (dead_reason IS NULL OR dead_reason IN (
    'business_not_found',
    'closed_or_inactive',
    'bad_address',
    'duplicate',
    'not_a_fit',
    'wrong_company_or_bad_data',
    'do_not_contact',
    'other'
  ));

CREATE INDEX IF NOT EXISTS idx_prospects_ao_disposition_active
  ON prospects (client_id, assigned_ao_id)
  WHERE disposition_status = 'active';

CREATE INDEX IF NOT EXISTS idx_ao_leads_disposition_active
  ON ao_leads (client_id, ao_owner_id)
  WHERE disposition_status = 'active';

COMMIT;
