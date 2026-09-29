-- Additive. Existing unbound approvals must be reviewed and approved with a hash.
-- Never backfill authority from legacy APPROVED / PUBLISHED flags.
ALTER TABLE paige_social_content_artifacts
  ADD COLUMN IF NOT EXISTS approval_binding JSONB,
  ADD COLUMN IF NOT EXISTS publication JSONB NOT NULL DEFAULT '{}'::jsonb;
