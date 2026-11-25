-- MAX-P0-003 read-only PRE-MIGRATION check for
-- migrations/2026-11-25-max-p0-003-reliability-core.sql (run BEFORE any approval to apply it).
-- Runs entirely inside a READ ONLY transaction; writes are impossible. psql >= 10 (uses \if / \gset).
-- Render Shell (pulseforge-api-migration, cwd /app):
--   PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20s' \
--     psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -f scripts/max-reliability/pre-migration-check-readonly.sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
\pset footer off
\echo '== 1. session is read-only (must be: on)'
SELECT current_setting('transaction_read_only') AS read_only, current_database() AS database, now() AS observed_at;

\echo '== 2. reliability objects already present? (expected: all f on a never-migrated database)'
SELECT to_regclass('max_external_action_claims') IS NOT NULL AS claims_table,
       to_regclass('max_request_receipts') IS NOT NULL AS receipts_table,
       to_regclass('max_reliability_audit') IS NOT NULL AS audit_table,
       to_regclass('max_external_action_claims_one_outbound_per_subject') IS NOT NULL AS r7_index,
       EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'max_reliability_forbid_delete') AS guard_function
\gset
SELECT :'claims_table' AS claims_table, :'receipts_table' AS receipts_table, :'audit_table' AS audit_table,
       :'r7_index' AS r7_index, :'guard_function' AS guard_function;

\if :claims_table
\echo '== 3. claims table EXISTS: rows that would violate the R7 partial unique index (expected: 0 rows)'
SELECT tenant_id, mission_id, subject_ref, count(*) AS live_claims, array_agg(state ORDER BY updated_at) AS states
  FROM max_external_action_claims
 WHERE action_kind = 'amo_outbound_email' AND mission_id IS NOT NULL AND subject_ref IS NOT NULL
   AND state IN ('claimed','dispatch_attempted','provider_acknowledged','verified','outcome_unknown')
 GROUP BY tenant_id, mission_id, subject_ref HAVING count(*) > 1
 ORDER BY live_claims DESC LIMIT 50;
\echo '== 3b. claims table EXISTS: columns present (index needs action_kind, mission_id, subject_ref, state)'
SELECT column_name FROM information_schema.columns
 WHERE table_name = 'max_external_action_claims' AND column_name IN ('tenant_id','action_kind','mission_id','subject_ref','state')
 ORDER BY 1;
\else
\echo '== 3. claims table absent: the R7 unique index is created on an EMPTY table and cannot conflict.'
\endif

\if :receipts_table
\echo '== 3c. receipts table EXISTS: rows the delete guard would protect immediately (informational)'
SELECT state, count(*) FROM max_request_receipts GROUP BY state ORDER BY 1;
\endif

\echo '== 4. existing AMO outbound history the R7 record rule will act on (informational, no conflict)'
\echo '   4a. (tenant, mission, prospect) groups with SENT/ATTEMPTED under MORE THAN ONE revision (historical double sends)'
SELECT tenant_id, mission_id, prospect_id, count(DISTINCT execution_identity) AS revisions,
       array_agg(DISTINCT status) AS statuses, max(COALESCE(sent_at, attempted_at)) AS last_at
  FROM acquisition_mission_outbound_executions
 WHERE status IN ('sent','attempted')
 GROUP BY tenant_id, mission_id, prospect_id HAVING count(DISTINCT execution_identity) > 1
 ORDER BY last_at DESC NULLS LAST LIMIT 50;
\echo '   4b. prospects that already have a SENT/ATTEMPTED message per mission (a revised message to them will be refused)'
SELECT tenant_id, count(DISTINCT (mission_id, prospect_id)) AS mission_prospects_with_dispatch
  FROM acquisition_mission_outbound_executions WHERE status IN ('sent','attempted')
 GROUP BY tenant_id ORDER BY tenant_id;
\echo '   4c. pre-ledger ATTEMPTED records with no SENT/FAILED for the same identity (will be treated as outcome unknown, never re-sent)'
SELECT e.tenant_id, count(*) AS unresolved_attempts, min(e.attempted_at) AS oldest
  FROM acquisition_mission_outbound_executions e
 WHERE e.status = 'attempted'
   AND NOT EXISTS (SELECT 1 FROM acquisition_mission_outbound_executions f
                    WHERE f.execution_identity = e.execution_identity AND f.status IN ('sent','failed'))
 GROUP BY e.tenant_id ORDER BY e.tenant_id;

\echo '== 5. tenant mailbox history (informational): rows that pre-date the ledger and need reconciliation before any send is enabled'
\echo '   PENDING = process stopped around SMTP; FAILED rows recorded before the ledger have uncertain certainty'
SELECT tenant_id, status, count(*) AS messages, max(updated_at) AS last_at
  FROM tenant_outreach_messages
 WHERE direction = 'OUTBOUND' AND status IN ('pending','failed')
 GROUP BY tenant_id, status ORDER BY tenant_id, status;
\echo '   mailbox sequence steps per (tenant, mission, prospect): multiple steps are legitimate and are NOT constrained by the R7 index'
SELECT tenant_id, count(*) FILTER (WHERE steps > 1) AS prospects_with_multiple_steps, max(steps) AS max_steps
  FROM (SELECT tenant_id, mission_id, prospect_id, count(DISTINCT sequence_step_ref) AS steps
          FROM tenant_outreach_messages WHERE direction = 'OUTBOUND' AND status = 'sent'
         GROUP BY tenant_id, mission_id, prospect_id) s
 GROUP BY tenant_id ORDER BY tenant_id;

ROLLBACK;
