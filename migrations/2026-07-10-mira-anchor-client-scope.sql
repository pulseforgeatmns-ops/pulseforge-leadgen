ALTER TABLE daily_anchors
  ADD COLUMN IF NOT EXISTS client_id INT;

UPDATE daily_anchors
SET client_id = 1
WHERE client_id IS NULL;

ALTER TABLE daily_anchors
  ALTER COLUMN client_id SET DEFAULT 1;

ALTER TABLE daily_anchors
  ALTER COLUMN client_id SET NOT NULL;

ALTER TABLE daily_anchors
  DROP CONSTRAINT IF EXISTS daily_anchors_anchor_date_key;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'daily_anchors_client_date_key'
      AND conrelid = 'daily_anchors'::regclass
  ) THEN
    ALTER TABLE daily_anchors
      ADD CONSTRAINT daily_anchors_client_date_key UNIQUE (client_id, anchor_date);
  END IF;
END $$;
