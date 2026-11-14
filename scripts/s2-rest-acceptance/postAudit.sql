-- P0 S2 — post-REST read-only audit, tenant 25, mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169.
-- Run only on the production endpoint recorded in the runbook (§1.2), as an
-- operator with authorized DB access. The transaction is READ ONLY: any write
-- fails. Replace :run_start with the run log's startedAt (UTC ISO timestamp).
--   psql "$DATABASE_URL" -v run_start="'2026-11-14T00:00:00Z'" -f scripts/s2-rest-acceptance/postAudit.sql

BEGIN TRANSACTION READ ONLY;

-- 1. Exactly one tenant-25 mission, still review-only, five prohibited classes, revision 0.
SELECT id, stage, status, updated_at,
       payload->'authorizationConstraints'->'reviewOnly'  AS review_only,
       payload->'authorizationConstraints'->'prohibited'  AS prohibited,
       payload->'authorizationConstraints'->'revision'    AS revision,
       payload->'pendingOperatorDecision'                 AS pending_decision,
       payload->'version'                                 AS version
  FROM acquisition_missions WHERE tenant_id = '25';
-- expect 1 row; review_only true; 5 classes; revision 0; pending_decision null;
-- updated_at earlier than :run_start (a blocked request commits nothing).

-- 2. Contributions unchanged: two, none at or after the run.
SELECT specialist, kind, at FROM acquisition_mission_contributions
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169' ORDER BY at;
SELECT count(*) AS contributions_since_run FROM acquisition_mission_contributions
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169' AND at >= :run_start::timestamptz;
-- expect 2 rows total; 0 since run.

-- 3. Execution audit: only the Max plan_lock commit; nothing since the run.
SELECT specialist, stage, commit_status, rollback_reason, error_class, at
  FROM acquisition_mission_execution_audit
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169' ORDER BY at;
SELECT count(*) AS execution_rows_since_run FROM acquisition_mission_execution_audit
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169' AND at >= :run_start::timestamptz;
-- expect one committed max/plan_lock row; 0 since run; no scout/paige/penny/emmett/operator rows.

-- 4. Mission events since the run (should be none).
SELECT kind, specialist, label, at FROM acquisition_mission_events
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169' AND at >= :run_start::timestamptz;

-- 5. No provider calls for tenant 25 (Places, Anthropic, Gemini) since the run, and none ever for Places.
SELECT provider, count(*) FROM api_usage_events
 WHERE tenant_id = 25 AND created_at >= :run_start::timestamptz GROUP BY provider;
SELECT count(*) AS tenant25_places_calls_all_time FROM api_usage_events
 WHERE tenant_id = 25 AND provider = 'google_places';
-- expect no rows since run; 0 Places calls all time.

-- 6. No outbound for tenant 25.
SELECT count(*) AS tenant25_touchpoints_since_run FROM touchpoints
 WHERE client_id = 25 AND created_at >= :run_start::timestamptz;
SELECT count(*) AS tenant25_pending_comments_since_run FROM pending_comments
 WHERE client_id = 25 AND created_at >= :run_start::timestamptz;
-- expect 0 and 0.

ROLLBACK;
