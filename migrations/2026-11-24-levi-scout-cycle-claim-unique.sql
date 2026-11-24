-- OPERATOR MIGRATION. PROPOSED ONLY — apply only with explicit approval.
-- Not startup DDL. Run with psql OUTSIDE a transaction (CONCURRENTLY requires it):
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/2026-11-24-levi-scout-cycle-claim-unique.sql
-- Run 2026-11-24-levi-scout-cycle-claim-unique.precheck.sql first.
--
-- Purpose (P0-SCOUT-LEVI-001 R2): at most one weekly research-cycle claim per
-- (tenant, cycleId) for Levi Scout. The application already serializes claims
-- with an advisory lock; this index makes the database enforce it too.
-- Partial index: only scout_levi claim rows are covered, so no other agent_log
-- writer is affected. CONCURRENTLY: no write lock on agent_log.

\set ON_ERROR_STOP on

-- Guard 1: refuse to build over existing duplicates (never deletes anything).
DO $$
DECLARE dup integer;
BEGIN
  SELECT count(*) INTO dup FROM (
    SELECT client_id, payload->>'cycleId'
      FROM agent_log
     WHERE agent_name = 'scout_levi' AND action = 'research_cycle_claim'
     GROUP BY 1, 2 HAVING count(*) > 1) d;
  IF dup > 0 THEN
    RAISE EXCEPTION 'levi_claim_duplicates_present: % duplicate (client_id, cycleId) group(s); resolve with operator review before indexing', dup;
  END IF;
END $$;

-- Guard 2: refuse to continue over an INVALID leftover from a failed build
-- (IF NOT EXISTS would otherwise silently keep it).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
              WHERE c.relname = 'agent_log_scout_levi_cycle_claim_uniq' AND NOT i.indisvalid) THEN
    RAISE EXCEPTION 'levi_claim_index_invalid: run the rollback (DROP INDEX CONCURRENTLY) and retry';
  END IF;
END $$;

SET lock_timeout = '5s';
SET statement_timeout = '10min';

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS agent_log_scout_levi_cycle_claim_uniq
  ON agent_log (client_id, (payload->>'cycleId'))
  WHERE agent_name = 'scout_levi' AND action = 'research_cycle_claim';

-- Guard 3: verify the index is valid and live.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                  WHERE c.relname = 'agent_log_scout_levi_cycle_claim_uniq' AND i.indisvalid AND i.indisunique) THEN
    RAISE EXCEPTION 'levi_claim_index_not_valid_after_build';
  END IF;
END $$;
