-- SPEC-AO-ROSTER-REASSIGN-001: paused AO roster + transfer review buckets

BEGIN;

SELECT pg_advisory_xact_lock(20261005, 1);

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS ao_operational_status TEXT NOT NULL DEFAULT 'active';

ALTER TABLE users DROP CONSTRAINT IF EXISTS users_ao_operational_status_check;
ALTER TABLE users ADD CONSTRAINT users_ao_operational_status_check
  CHECK (ao_operational_status IN ('active', 'paused', 'inactive'));

ALTER TABLE prospects
  ADD COLUMN IF NOT EXISTS ao_review_bucket TEXT,
  ADD COLUMN IF NOT EXISTS ao_reassignment_prior_ao_id INTEGER REFERENCES users(id),
  ADD COLUMN IF NOT EXISTS ao_reassignment_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS ao_reassignment_reason TEXT;

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_ao_review_bucket_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_ao_review_bucket_check
  CHECK (ao_review_bucket IS NULL OR ao_review_bucket IN ('needs_reassignment', 'transferred_from_inactive_ao'));

CREATE INDEX IF NOT EXISTS idx_prospects_ao_review_bucket
  ON prospects (client_id, assigned_ao_id, ao_review_bucket)
  WHERE ao_review_bucket IS NOT NULL;

COMMIT;
