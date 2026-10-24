ALTER TABLE IF EXISTS scout_queue
  ADD COLUMN IF NOT EXISTS parked_at TIMESTAMPTZ;

DO $$
BEGIN
  IF to_regclass('public.scout_queue') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_scout_queue_active_selection
      ON scout_queue (client_id, status, saturated, vertical, prospect_count, id)
      WHERE parked_at IS NULL;
  END IF;
END $$;
