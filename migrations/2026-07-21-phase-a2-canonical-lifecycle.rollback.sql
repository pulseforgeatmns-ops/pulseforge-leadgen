-- Rollback for 2026-07-21-phase-a2-canonical-lifecycle.sql.
-- Refuses to drop canonical history once real (non-synthetic) lifecycle
-- events or operator notes exist — mirror of the Anchor rollback guard.

BEGIN;

DO $guard$
DECLARE
  event_count INTEGER;
  note_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO event_count FROM prospect_lifecycle_events;
  SELECT COUNT(*) INTO note_count FROM prospect_notes;
  IF event_count > 0 OR note_count > 0 THEN
    RAISE EXCEPTION 'Rollback blocked: canonical lifecycle history exists (% events, % notes). Export or migrate before rolling back.',
      event_count, note_count;
  END IF;
END
$guard$;

DROP TABLE IF EXISTS prospect_lifecycle_events;
DROP TABLE IF EXISTS prospect_notes;
ALTER TABLE clients DROP COLUMN IF EXISTS setter_qualification_threshold;

COMMIT;
