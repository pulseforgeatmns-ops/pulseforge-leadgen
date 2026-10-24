\set ON_ERROR_STOP on
\pset pager off

\echo 'Starting closer_status correction transaction...'
BEGIN;

SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '2min';

-- Coordinate with the runtime closer-schema helper while this transaction runs.
SELECT pg_advisory_xact_lock(91720260517);

-- Snapshot the exact rows affected for in-session rollback reference.
CREATE TEMP TABLE closer_status_fix_snapshot ON COMMIT DROP AS
SELECT id, closer_status, booked_at, updated_at
FROM prospects
WHERE closer_status = 'booked'
  AND booked_at IS NULL;

\echo 'Pre-update scope:'
SELECT COUNT(*) AS rows_to_update
FROM closer_status_fix_snapshot;

\echo 'Legitimate booked rows preserved:'
SELECT COUNT(*) AS legitimate_booked_rows
FROM prospects
WHERE closer_status = 'booked'
  AND booked_at IS NOT NULL;

-- Stop future ordinary inserts from inheriting the invalid booked state.
ALTER TABLE prospects
ALTER COLUMN closer_status DROP DEFAULT;

-- Correct only false bookings captured in the transaction snapshot.
WITH corrected AS (
  UPDATE prospects p
  SET closer_status = NULL,
      updated_at = NOW()
  FROM closer_status_fix_snapshot s
  WHERE p.id = s.id
    AND p.closer_status = 'booked'
    AND p.booked_at IS NULL
  RETURNING p.id
)
SELECT COUNT(*) AS rows_updated
FROM corrected;

\echo 'Post-update status distribution (still uncommitted):'
SELECT closer_status, COUNT(*)
FROM prospects
GROUP BY closer_status
ORDER BY closer_status NULLS FIRST;

\echo 'Post-update booking integrity (still uncommitted):'
SELECT
  COUNT(*) FILTER (
    WHERE closer_status = 'booked' AND booked_at IS NULL
  ) AS invalid_booked_rows,
  COUNT(*) FILTER (
    WHERE closer_status = 'booked' AND booked_at IS NOT NULL
  ) AS legitimate_booked_rows,
  COUNT(*) FILTER (
    WHERE closer_status IS NULL AND booked_at IS NULL
  ) AS unbooked_null_rows,
  COUNT(*) AS total_prospects
FROM prospects;

\echo 'Column default after schema correction (still uncommitted):'
SELECT column_name, column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'prospects'
  AND column_name = 'closer_status';

\echo ''
\echo 'TRANSACTION IS OPEN. Review the output before deciding.'
\prompt 'Type COMMIT to persist this migration; anything else rolls it back: ' migration_decision

SELECT UPPER(BTRIM(:'migration_decision')) = 'COMMIT' AS migration_confirmed \gset

\if :migration_confirmed
  COMMIT;
  \echo 'Migration committed.'
\else
  ROLLBACK;
  \echo 'Migration rolled back.'
\endif
