-- Rollback publish-state columns (approval_state PUBLISHED rows are not restored).

ALTER TABLE paige_social_content_artifacts
  DROP COLUMN IF EXISTS source_type,
  DROP COLUMN IF EXISTS business_id,
  DROP COLUMN IF EXISTS media_refs,
  DROP COLUMN IF EXISTS publish_state,
  DROP COLUMN IF EXISTS rejection_reason,
  DROP COLUMN IF EXISTS publish_error,
  DROP COLUMN IF EXISTS published_url,
  DROP COLUMN IF EXISTS approved_at,
  DROP COLUMN IF EXISTS rejected_at,
  DROP COLUMN IF EXISTS published_at;
