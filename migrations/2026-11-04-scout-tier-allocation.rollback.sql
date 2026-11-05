-- Operator only. Refuses to drop recorded activations or qualifications.
-- Normal rollback is code-only: without an activation row the v1 planner applies.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
LOCK TABLE scout_query_allocation_activations, scout_candidate_commercial_qualifications IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM scout_query_allocation_activations)
    OR EXISTS(SELECT 1 FROM scout_candidate_commercial_qualifications) THEN
    RAISE EXCEPTION 'tier_allocation_schema_in_use_keep_records';
  END IF;
END $$;
DROP INDEX scout_anchor_page_created_lookup;
DROP TABLE scout_candidate_commercial_qualifications;
DROP TABLE scout_query_allocation_activations;
DROP FUNCTION scout_tier_allocation_permanent();
COMMIT;
