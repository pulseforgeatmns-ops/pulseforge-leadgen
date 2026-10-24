-- SPEC-256 — Separate publish state from approval state on Paige social artifacts.

ALTER TABLE paige_social_content_artifacts
  ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS business_id TEXT,
  ADD COLUMN IF NOT EXISTS media_refs JSONB,
  ADD COLUMN IF NOT EXISTS publish_state TEXT NOT NULL DEFAULT 'NOT_PUBLISHED',
  ADD COLUMN IF NOT EXISTS rejection_reason TEXT,
  ADD COLUMN IF NOT EXISTS publish_error TEXT,
  ADD COLUMN IF NOT EXISTS published_url TEXT,
  ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS rejected_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;

UPDATE paige_social_content_artifacts
   SET approval_state = 'APPROVED',
       publish_state = 'PUBLISHED',
       published_at = COALESCE(published_at, updated_at)
 WHERE approval_state = 'PUBLISHED';
