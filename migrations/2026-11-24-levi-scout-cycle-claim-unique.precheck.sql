-- READ ONLY precheck for 2026-11-24-levi-scout-cycle-claim-unique.sql.
-- Run first; apply the migration only if duplicate_claim_groups = 0 and invalid_index = 0.
BEGIN TRANSACTION READ ONLY;
SELECT current_database() AS database;
SELECT count(*) AS duplicate_claim_groups
  FROM (SELECT client_id, payload->>'cycleId'
          FROM agent_log
         WHERE agent_name = 'scout_levi' AND action = 'research_cycle_claim'
         GROUP BY 1, 2 HAVING count(*) > 1) d;
SELECT client_id, payload->>'cycleId' AS cycle_id, count(*) AS claims, min(ran_at), max(ran_at)
  FROM agent_log
 WHERE agent_name = 'scout_levi' AND action = 'research_cycle_claim'
 GROUP BY 1, 2 ORDER BY 1, 2;
SELECT count(*) AS null_cycle_claims
  FROM agent_log WHERE agent_name = 'scout_levi' AND action = 'research_cycle_claim' AND payload->>'cycleId' IS NULL;
SELECT c.relname AS index_name, i.indisvalid AS valid, count(*) OVER () AS present
  FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
 WHERE c.relname = 'agent_log_scout_levi_cycle_claim_uniq';
SELECT count(*) FILTER (WHERE NOT i.indisvalid) AS invalid_index
  FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
 WHERE c.relname = 'agent_log_scout_levi_cycle_claim_uniq';
ROLLBACK;
