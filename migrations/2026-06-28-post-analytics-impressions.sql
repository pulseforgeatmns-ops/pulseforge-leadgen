BEGIN;

ALTER TABLE post_analytics
  ADD COLUMN IF NOT EXISTS impressions INT DEFAULT 0;

ALTER TABLE content_performance_summary
  ADD COLUMN IF NOT EXISTS avg_impressions NUMERIC(8,2) DEFAULT 0;

UPDATE post_analytics
SET impressions = reach,
    reach = NULL
WHERE channel IN ('linkedin_page', 'linkedin_personal')
  AND reach IS NOT NULL;

COMMIT;
