-- Scout-supported personalization evidence for Anchor lifecycle messaging.
-- Evidence is stored on prospects.acquisition_metadata.scout_personalization.

ALTER TABLE prospects
  ADD COLUMN IF NOT EXISTS acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
