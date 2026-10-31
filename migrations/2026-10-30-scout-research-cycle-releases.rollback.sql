-- Operator only. Normal rollback keeps the ledger AND claim trigger: disable
-- research, revoke any open authority, roll back code, retain all evidence.
-- Never remove the database gate to make an older application execute again.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
LOCK TABLE scout_research_only_grants,scout_research_only_cycles,scout_research_cycle_releases IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM scout_research_cycle_releases)
    OR EXISTS(SELECT 1 FROM scout_research_only_cycles)
    OR EXISTS(SELECT 1 FROM scout_research_only_grants WHERE revoked_at IS NULL) THEN
    RAISE EXCEPTION 'research_cycle_release_schema_in_use_revoke_and_keep_guards';
  END IF;
END $$;
DROP TRIGGER scout_research_cycles_release_guard ON scout_research_only_cycles;
DROP TABLE scout_research_cycle_releases;
DROP FUNCTION scout_research_cycle_released();
DROP FUNCTION scout_research_release_forward();
COMMIT;
