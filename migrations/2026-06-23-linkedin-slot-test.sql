ALTER TABLE pending_comments
  ADD COLUMN IF NOT EXISTS slot INTEGER,
  ADD COLUMN IF NOT EXISTS format TEXT,
  ADD COLUMN IF NOT EXISTS posted_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS stats JSONB;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'pending_comments_slot_check'
  ) THEN
    ALTER TABLE pending_comments
      ADD CONSTRAINT pending_comments_slot_check
      CHECK (slot IS NULL OR slot IN (1, 2));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'pending_comments_linkedin_format_check'
  ) THEN
    ALTER TABLE pending_comments
      ADD CONSTRAINT pending_comments_linkedin_format_check
      CHECK (
        format IS NULL OR format IN ('punch', 'numbers', 'quote', 'stake', 'decision_log')
      ) NOT VALID;
  END IF;
END $$;
