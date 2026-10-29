-- Rollback for 2026-10-28-scout-research-only-grants.sql. Operator-run only.
--
-- Normal rollback is NOT this file: revoke the open grant (CLI --revoke or the
-- admin revoke route), set SCOUT_RESEARCH_ONLY_GRANTS_ENABLED=false and keep the
-- tables. Revocation is immediate and keeps the cycle and spend evidence.
--
-- This file removes the schema only while it has never been used. Any grant row
-- aborts the transaction: audit evidence of authority and Google spend is never
-- dropped, truncated or reset by a rollback. Research missions and retained
-- candidates count as use.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
LOCK TABLE scout_research_missions, scout_research_only_grants, scout_research_only_cycles, scout_research_google_spend,
  scout_research_candidates IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM scout_research_only_grants)
    OR EXISTS (SELECT 1 FROM scout_research_only_cycles)
    OR EXISTS (SELECT 1 FROM scout_research_google_spend)
    OR EXISTS (SELECT 1 FROM scout_research_missions)
    OR EXISTS (SELECT 1 FROM scout_research_candidates) THEN
    RAISE EXCEPTION 'research_only_schema_in_use_revoke_instead';
  END IF;
END $$;
DROP TABLE scout_research_candidates;
DROP TABLE scout_research_google_spend;
DROP TABLE scout_research_only_cycles;
DROP TABLE scout_research_only_grants;
DROP TABLE scout_research_missions;
DROP FUNCTION scout_research_candidates_reconcile_once();
DROP FUNCTION scout_research_google_spend_settle_only();
DROP FUNCTION scout_research_only_cycle_forward();
DROP FUNCTION scout_research_only_revoke_once();
DROP FUNCTION scout_research_only_permanent();
COMMIT;
