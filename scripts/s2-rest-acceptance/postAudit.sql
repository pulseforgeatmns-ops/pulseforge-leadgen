-- P0 S2 — post-REST read-only audit, tenant 25, mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169.
--
-- Run only on the production endpoint recorded in the runbook (§1.2), as an
-- operator with authorized DB access. Pass the run log's startedAt (UTC ISO):
--
--   psql "<authorized read-only connection>" -X \
--     -v run_start=2026-11-14T13:00:00Z -f scripts/s2-rest-acceptance/postAudit.sql
--
-- Safety:
--   * READ ONLY transaction, ended with ROLLBACK: any write fails.
--   * ON_ERROR_STOP: the first error stops the script. A failed query is never
--     followed by misleading output from an aborted transaction.
--   * Section 0 checks the LIVE schema for every table and column used below,
--     including the timestamp columns: acquisition_mission_* use "at";
--     api_usage_events, touchpoints and pending_comments use "created_at".
--     A missing column raises an error and halts before any audit query runs.

\set ON_ERROR_STOP on
\if :{?run_start}
\else
  DO $missing$ BEGIN
    RAISE EXCEPTION 'run_start is required: -v run_start=<run log startedAt, UTC ISO>';
  END $missing$;
\endif

BEGIN TRANSACTION READ ONLY;

-- 0. Schema preflight (live information_schema; halts on any missing column).
DO $preflight$
DECLARE
  required text[][] := ARRAY[
    ['acquisition_missions','id'], ['acquisition_missions','tenant_id'], ['acquisition_missions','stage'],
    ['acquisition_missions','status'], ['acquisition_missions','payload'], ['acquisition_missions','updated_at'],
    ['acquisition_mission_contributions','mission_id'], ['acquisition_mission_contributions','specialist'],
    ['acquisition_mission_contributions','kind'], ['acquisition_mission_contributions','at'],
    ['acquisition_mission_execution_audit','mission_id'], ['acquisition_mission_execution_audit','specialist'],
    ['acquisition_mission_execution_audit','stage'], ['acquisition_mission_execution_audit','commit_status'],
    ['acquisition_mission_execution_audit','rollback_reason'], ['acquisition_mission_execution_audit','error_class'],
    ['acquisition_mission_execution_audit','at'],
    ['acquisition_mission_events','mission_id'], ['acquisition_mission_events','kind'],
    ['acquisition_mission_events','specialist'], ['acquisition_mission_events','label'], ['acquisition_mission_events','at'],
    ['api_usage_events','provider'], ['api_usage_events','tenant_id'], ['api_usage_events','mission_id'],
    ['api_usage_events','created_at'],
    ['touchpoints','client_id'], ['touchpoints','created_at'],
    ['pending_comments','client_id'], ['pending_comments','channel'], ['pending_comments','status'],
    ['pending_comments','created_at']
  ];
  missing text := '';
  i int;
BEGIN
  FOR i IN 1 .. array_length(required, 1) LOOP
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_schema = 'public' AND table_name = required[i][1] AND column_name = required[i][2]
    ) THEN
      missing := missing || required[i][1] || '.' || required[i][2] || ' ';
    END IF;
  END LOOP;
  IF missing <> '' THEN
    RAISE EXCEPTION 'S2 audit schema preflight failed; missing columns: %', missing;
  END IF;
  RAISE NOTICE 'S2 audit schema preflight passed (% columns).', array_length(required, 1);
END
$preflight$;

-- 1. Exactly one tenant-25 mission: review-only, five classes, revision 0 present.
SELECT count(*) AS tenant25_missions FROM acquisition_missions WHERE tenant_id = '25';
SELECT id, stage, status, updated_at,
       payload->'authorizationConstraints'->'reviewOnly'         AS review_only,
       payload->'authorizationConstraints'->'prohibited'         AS prohibited,
       payload->'authorizationConstraints' ? 'revision'           AS revision_present,
       payload->'authorizationConstraints'->'revision'           AS revision,
       payload->'pendingOperatorDecision'                        AS pending_decision,
       payload->'version'                                        AS version
  FROM acquisition_missions WHERE tenant_id = '25';
-- expect count 1; status 'Plan approved (review only)'; review_only true;
-- prohibited = [content, outreach, outreach_authorization, paid_media, provider_research];
-- revision_present true; revision 0; pending_decision null (SQL NULL or JSON null);
-- updated_at earlier than run_start (a blocked request commits nothing).

-- 2. Contributions: two in total, none at or after run_start.
SELECT specialist, kind, at FROM acquisition_mission_contributions
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169' ORDER BY at;
SELECT count(*) AS contributions_since_run FROM acquisition_mission_contributions
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169' AND at >= :'run_start'::timestamptz;
-- expect 2 rows; 0 since run.

-- 3. Execution audit: only the committed Max plan_lock; nothing since run_start.
SELECT specialist, stage, commit_status, rollback_reason, error_class, at
  FROM acquisition_mission_execution_audit
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169' ORDER BY at;
SELECT count(*) AS execution_rows_since_run FROM acquisition_mission_execution_audit
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169' AND at >= :'run_start'::timestamptz;
-- expect one committed max / plan_lock row; 0 since run; no scout/paige/penny/emmett/operator rows.

-- 4. Mission events since run_start (expect none).
SELECT count(*) AS mission_events_since_run FROM acquisition_mission_events
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169' AND at >= :'run_start'::timestamptz;

-- 5. Provider calls. Tenant 25 since run_start (any provider), this mission ever,
--    and tenant 25 Places all time.
SELECT provider, count(*) AS calls_since_run FROM api_usage_events
 WHERE tenant_id = 25 AND created_at >= :'run_start'::timestamptz GROUP BY provider;
SELECT count(*) AS mission_provider_calls_all_time FROM api_usage_events
 WHERE mission_id = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169';
SELECT count(*) AS tenant25_places_calls_all_time FROM api_usage_events
 WHERE tenant_id = 25 AND provider = 'google_places';
-- expect no rows; 0; 0.

-- 6. Outbound and content queue for tenant 25 since run_start.
SELECT count(*) AS tenant25_touchpoints_since_run FROM touchpoints
 WHERE client_id = 25 AND created_at >= :'run_start'::timestamptz;
SELECT channel, status, count(*) AS tenant25_pending_comments_since_run FROM pending_comments
 WHERE client_id = 25 AND created_at >= :'run_start'::timestamptz GROUP BY channel, status;
-- expect 0; no rows.

ROLLBACK;
