-- Rollback for 2026-11-24-levi-scout-cycle-claim-unique.sql. Operator approval required.
-- Run outside a transaction. Removes only the index; no rows are touched.
-- The application's advisory-lock claim stays correct without the index.
\set ON_ERROR_STOP on
SET lock_timeout = '5s';
DROP INDEX CONCURRENTLY IF EXISTS agent_log_scout_levi_cycle_claim_uniq;
