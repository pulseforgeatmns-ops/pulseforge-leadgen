BEGIN;

ALTER TABLE companies
  ADD COLUMN IF NOT EXISTS places_locality TEXT,
  ADD COLUMN IF NOT EXISTS places_administrative_area_level_1 TEXT,
  ADD COLUMN IF NOT EXISTS places_postal_code TEXT;

UPDATE clients
SET service_area = array_append(service_area, 'Boston')
WHERE id = 1
  AND NOT (COALESCE(service_area, ARRAY[]::TEXT[]) @> ARRAY['Boston']::TEXT[]);

ALTER TABLE scout_expansion_queue
  DROP CONSTRAINT IF EXISTS scout_expansion_queue_status_check;

ALTER TABLE scout_expansion_queue
  ADD CONSTRAINT scout_expansion_queue_status_check
  CHECK (status IN ('pending', 'completed', 'failed', 'retired'));

UPDATE scout_expansion_queue
SET status = 'retired'
WHERE client_id = 1
  AND status = 'pending'
  AND location ~* '\s(?:WV|KY|TN|NH)$';

COMMIT;
