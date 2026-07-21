-- Phase B — structured lifecycle reasons rollback.
--
-- Guarded: refuses to drop the column once real (non-null) lifecycle reasons
-- have been recorded, because dropping it would destroy canonical history.

DO $$
DECLARE
  reason_count BIGINT;
BEGIN
  SELECT COUNT(*) INTO reason_count
  FROM prospect_lifecycle_events
  WHERE lifecycle_reason IS NOT NULL;

  IF reason_count > 0 THEN
    RAISE EXCEPTION 'Refusing rollback: % lifecycle events carry a structured lifecycle_reason. Dropping the column would destroy canonical history.', reason_count;
  END IF;

  ALTER TABLE prospect_lifecycle_events DROP COLUMN IF EXISTS lifecycle_reason;
END $$;
