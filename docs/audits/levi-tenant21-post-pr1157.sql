-- Levi tenant 21 — post-PR #1157 read-only audit (operator SQL).
-- Equivalent to scripts/auditLeviTenant21PostPr1157.js for operators who run psql directly.
-- Aggregates only: no names, emails, phones, addresses or URLs are selected.
-- Run with a SELECT-only credential:  psql "$AUDIT_DATABASE_URL" -v ON_ERROR_STOP=0 -f this-file.sql
-- Everything runs inside one READ ONLY transaction that is rolled back.

BEGIN TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '2s';

-- Q0 Target identity. Expect database = pulseforge_production, tx_read_only = on.
SELECT current_database() AS database, current_setting('transaction_read_only') AS tx_read_only,
       current_setting('server_version_num') AS server_version_num;

-- Q1 Inventory checkpoint. Expect total 29, scout_research_only 10.
SELECT source, count(*) AS n FROM prospects WHERE client_id = 21 GROUP BY 1 ORDER BY 2 DESC;
SELECT count(*) AS total,
       count(*) FILTER (WHERE source = 'scout_research_only') AS research_only
  FROM prospects WHERE client_id = 21;

-- Q2 City inference and provenance conflicts (research-only rows).
SELECT coalesce(acquisition_metadata->>'city', '<missing>')                       AS city,
       coalesce(acquisition_metadata->'cityInference'->>'source', 'not_recorded') AS inference_source,
       coalesce(acquisition_metadata->'cityInference'->>'conflict', 'not_recorded') AS conflict,
       count(*) FILTER (WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(coalesce(acquisition_metadata->'sourceUrls','[]')) u
                                       WHERE u LIKE 'https://data.cityofchicago.org/%')) AS chicago_host,
       count(*) FILTER (WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(coalesce(acquisition_metadata->'sourceUrls','[]')) u
                                       WHERE u LIKE 'https://data.cityofnewyork.us/%'))  AS nyc_host,
       count(*) FILTER (WHERE service_area_match ~* '(^|,\s*)chicago,\s*il\M')         AS chicago_service_area,
       count(*) FILTER (WHERE service_area_match ~* '(^|,\s*)new york,\s*ny\M')        AS nyc_service_area,
       count(*) AS n
  FROM prospects WHERE client_id = 21 AND source = 'scout_research_only'
 GROUP BY 1,2,3 ORDER BY 1,2,3;
-- FAIL if any row has a city with cityInference.conflict = false while its trusted
-- evidence (host or service area) names a different city or two cities.
SELECT count(*) AS inferred_despite_conflict
  FROM prospects p
 WHERE client_id = 21 AND source = 'scout_research_only'
   AND acquisition_metadata->'cityInference'->>'conflict' = 'false'
   AND (
     (acquisition_metadata->>'city' = 'Chicago' AND (service_area_match ~* 'new york,\s*ny' OR acquisition_metadata::text LIKE '%data.cityofnewyork.us%'))
  OR (acquisition_metadata->>'city' = 'New York' AND (service_area_match ~* 'chicago,\s*il' OR acquisition_metadata::text LIKE '%data.cityofchicago.org%'))
   );

-- Q3 Chicago SODA research evidence and completeness.
SELECT coalesce(acquisition_metadata->'contactResearch'->'lookup'->>'completeness', 'pre_1157_not_recorded') AS completeness,
       coalesce(acquisition_metadata->'contactResearch'->'lookup'->>'authoritativeZeroMatch', 'n/a')         AS authoritative_zero_match,
       max((acquisition_metadata->'contactResearch'->'lookup'->>'requests')::int)                            AS max_requests,
       count(*) FILTER (WHERE acquisition_metadata->'contactResearch'->'lookup'->>'ownersExactMatch' = 'true')   AS owners_match,
       count(*) FILTER (WHERE acquisition_metadata->'contactResearch'->'lookup'->>'licensesExactMatch' = 'true') AS licenses_match,
       count(*) FILTER (WHERE acquisition_metadata->'contactResearch'->>'verifiable' = 'true')                   AS verifiable,
       count(*) AS n
  FROM prospects
 WHERE client_id = 21 AND source = 'scout_research_only' AND acquisition_metadata->>'city' ILIKE '%chicago%'
 GROUP BY 1,2 ORDER BY 1,2;
-- Each of these must be 0.
SELECT
  count(*) FILTER (WHERE (acquisition_metadata->'contactResearch'->'lookup'->>'requests')::int > 4) AS over_budget,
  count(*) FILTER (WHERE acquisition_metadata->'contactResearch'->'lookup'->>'authoritativeZeroMatch' = 'true'
                     AND acquisition_metadata->'contactResearch'->'lookup'->>'completeness' <> 'authoritative') AS zero_match_on_incomplete,
  count(*) FILTER (WHERE acquisition_metadata->'contactResearch'->>'fabricated' = 'true'
                      OR acquisition_metadata->'contactResearch'->>'paidProvidersUsed' = 'true') AS fabricated_or_paid,
  count(*) FILTER (WHERE acquisition_metadata->'contactResearch'->>'verifiable' = 'true'
                     AND coalesce(acquisition_metadata->'contactResearch'->>'phone', 'null') = 'null'
                     AND coalesce(acquisition_metadata->'contactResearch'->>'email', 'null') = 'null'
                     AND coalesce(acquisition_metadata->'contactResearch'->>'address', 'null') = 'null') AS verifiable_without_value
  FROM prospects WHERE client_id = 21 AND source = 'scout_research_only';

-- Q4 Research-only classification. violations must be 0.
SELECT count(*) AS research_rows,
       count(*) FILTER (WHERE setter_visible IS TRUE
                           OR ao_outreach_review_required IS NOT TRUE
                           OR coalesce(acquisition_metadata->>'outreachAuthorized', 'missing') <> 'false'
                           OR coalesce(acquisition_metadata->>'importMode', 'missing') <> 'review_only'
                           OR acquisition_metadata->>'verifiedBorrowerIntent' = 'true'
                           OR coalesce(acquisition_metadata->>'permitDoesNotEstablishIntent', 'missing') <> 'true'
                           OR coalesce(discovery_method, 'missing') <> 'open_data_permit') AS violations
  FROM prospects WHERE client_id = 21 AND source = 'scout_research_only';
SELECT acquisition_metadata->>'sourceType' AS source_type, acquisition_metadata->>'bucket' AS bucket,
       acquisition_metadata->>'reviewState' AS review_state, count(*)
  FROM prospects WHERE client_id = 21 AND source = 'scout_research_only' GROUP BY 1,2,3;

-- Q5 Qualified-contact and setter-visibility gates. Expect 0 / 0 / 0.
SELECT count(*) FILTER (WHERE setter_visible IS TRUE)                              AS setter_visible,
       count(*) FILTER (WHERE icp_score >= 70)                                     AS icp_ge_70,
       count(*) FILTER (WHERE acquisition_metadata->>'bucket' = 'qualified_prospect') AS qualified_bucket,
       count(*) FILTER (WHERE setter_status IS NOT NULL)                           AS setter_status_set
  FROM prospects WHERE client_id = 21;
-- Contract qualification (evaluateProspect) cannot be expressed in SQL; the script computes it.

-- Q6 Outreach prevention and dedup. Every count must be 0 (a missing table = UNVERIFIED).
SELECT count(*) AS last_contacted FROM prospects WHERE client_id = 21 AND last_contacted_at IS NOT NULL;
SELECT channel, action_type, count(*) FROM touchpoints WHERE client_id = 21 GROUP BY 1,2;
SELECT action_type, count(*) FROM activity_log WHERE client_id = 21 GROUP BY 1;
SELECT coalesce(sum(sends), 0) AS email_sends FROM email_performance WHERE client_id = 21;
SELECT action_type, status, count(*) FROM agent_actions
 WHERE client_id = 21 AND action_type <> 'levi_scout_research_briefing'
   AND action_type ~* '(outreach|send|email|sms|call|handoff|enroll)' GROUP BY 1,2;
SELECT count(*) AS duplicate_name_company_groups FROM (
  SELECT lower(concat_ws(' ', p.first_name, p.last_name)), lower(coalesce(c.name, '')), count(*)
    FROM prospects p LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
   WHERE p.client_id = 21 GROUP BY 1,2 HAVING count(*) > 1) d;
SELECT count(*) AS duplicate_email_groups FROM (
  SELECT lower(email) FROM prospects WHERE client_id = 21 AND email IS NOT NULL GROUP BY 1 HAVING count(*) > 1) d;
SELECT count(*) AS duplicate_phone_groups FROM (
  SELECT regexp_replace(phone, '\D', '', 'g') FROM prospects WHERE client_id = 21 AND phone IS NOT NULL GROUP BY 1 HAVING count(*) > 1) d;

-- Q7 Weekly cycle state and bounded retry. Expect exactly 1 claim for levi-week-2026-11-23,
-- at most 1 diagnostic, no later cycle, outreachAttempted/paidProvidersUsed never true.
SELECT action, status, payload->>'cycleId' AS cycle_id, payload->>'status' AS payload_status,
       payload->>'reason' AS reason, payload->>'persisted' AS persisted,
       payload->>'outreachAttempted' AS outreach_attempted, payload->>'paidProvidersUsed' AS paid_providers_used,
       count(*), min(ran_at), max(ran_at)
  FROM agent_log WHERE client_id = 21 AND agent_name = 'scout_levi'
 GROUP BY 1,2,3,4,5,6,7,8 ORDER BY min(ran_at);
-- Replay attempts (each restart / cron retry in the week writes a 'replay_blocked' briefing card).
SELECT payload->>'cycleId' AS cycle_id, payload->>'status' AS cycle_status, status, count(*), min(created_at), max(created_at)
  FROM agent_actions WHERE client_id = 21 AND created_by = 'scout_levi' AND action_type = 'levi_scout_research_briefing'
 GROUP BY 1,2,3 ORDER BY min(created_at);
-- Claim race protection: expect >= 1 unique index covering the claim; 0 = concurrent double-claim possible.
SELECT indexname, indexdef FROM pg_indexes
 WHERE schemaname = current_schema() AND tablename = 'agent_log' AND indexdef ILIKE '%UNIQUE%';

ROLLBACK;
