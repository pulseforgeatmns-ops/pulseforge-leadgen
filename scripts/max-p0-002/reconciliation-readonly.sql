-- MAX-P0-002 final reconciliation — READ ONLY. Classifies the pre-ledger AMO
-- outbound attempts (status 'attempted' with no sent/failed for the same
-- execution identity) and the failed tenant-mailbox outbound record(s).
-- Nothing here can write: the session default and the transaction are READ ONLY.
-- No e-mail addresses, subjects or bodies are printed (recipients are md5-prefixed).
--
-- Render Shell (production web service, cwd /app):
--   PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=30s' \
--     psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=0 -f /tmp/p0002-reconciliation.sql
--
-- Outcome vocabulary (never upgraded without direct evidence):
--   SENT_CONFIRMED       direct evidence tied to this record (provider events on the record id,
--                        provider message id or provider response stored on the record)
--   NOT_SENT_DEFINITIVE  mailbox only: failure provably before the SMTP boundary
--   UNKNOWN              anything else; the sub-reason says what was (not) observed
\set ON_ERROR_ROLLBACK on
\pset footer off
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;

\echo '== 0. session (read_only must be on)'
SELECT current_setting('transaction_read_only') AS read_only, current_database() AS database, now() AS observed_at;

\echo '== 1. baseline re-confirmation (expected: unresolved_attempts total 20, duplicate_groups 0, failed_mailbox 1, p0003 tables f, advisory_locks 0)'
SELECT (SELECT count(*) FROM acquisition_mission_outbound_executions e
         WHERE e.status = 'attempted'
           AND NOT EXISTS (SELECT 1 FROM acquisition_mission_outbound_executions f
                            WHERE f.execution_identity = e.execution_identity AND f.status IN ('sent','failed'))) AS unresolved_attempts,
       (SELECT count(*) FROM (SELECT 1 FROM acquisition_mission_outbound_executions
                               WHERE status IN ('sent','attempted')
                               GROUP BY tenant_id, mission_id, prospect_id
                              HAVING count(DISTINCT execution_identity) > 1) g) AS duplicate_groups,
       (SELECT count(*) FROM tenant_outreach_messages WHERE direction = 'OUTBOUND' AND status = 'failed') AS failed_mailbox,
       (SELECT count(*) FROM tenant_outreach_messages WHERE direction = 'OUTBOUND' AND status = 'pending') AS pending_mailbox,
       to_regclass('max_external_action_claims') IS NOT NULL AS p0003_claims,
       to_regclass('max_request_receipts') IS NOT NULL AS p0003_receipts,
       (SELECT count(*) FROM pg_locks WHERE locktype = 'advisory') AS advisory_locks;

\echo '== 1b. unresolved attempts by tenant / provider'
SELECT e.tenant_id, e.provider, count(*) AS n, min(e.attempted_at) AS oldest, max(e.attempted_at) AS newest
  FROM acquisition_mission_outbound_executions e
 WHERE e.status = 'attempted'
   AND NOT EXISTS (SELECT 1 FROM acquisition_mission_outbound_executions f
                    WHERE f.execution_identity = e.execution_identity AND f.status IN ('sent','failed'))
 GROUP BY 1, 2 ORDER BY 1, 2;

\echo '== 2. quiescence: outbound-side writes in the last 24h (expected: all 0)'
SELECT (SELECT count(*) FROM acquisition_mission_outbound_executions WHERE updated_at > now() - interval '24 hours') AS amo_execution_writes,
       (SELECT count(*) FROM acquisition_outbound_items WHERE attempted_at > now() - interval '24 hours') AS governed_attempts,
       (SELECT count(*) FROM tenant_outreach_messages WHERE direction = 'OUTBOUND' AND created_at > now() - interval '24 hours') AS mailbox_outbound,
       (SELECT count(*) FROM tenant_outreach_scheduled_sends WHERE updated_at > now() - interval '24 hours') AS schedule_writes,
       (SELECT count(*) FROM tenant_outreach_scheduled_sends WHERE status IN ('SCHEDULED','EXECUTING')) AS live_schedules;
SELECT count(*) AS api_usage_events_24h FROM api_usage_events WHERE created_at > now() - interval '24 hours';

\echo '== 3. per-record classification: unresolved AMO attempts'
WITH u AS (
  SELECT e.* FROM acquisition_mission_outbound_executions e
   WHERE e.status = 'attempted'
     AND NOT EXISTS (SELECT 1 FROM acquisition_mission_outbound_executions f
                      WHERE f.execution_identity = e.execution_identity AND f.status IN ('sent','failed'))
), ev AS (
  SELECT u.id,
         u.provider_message_id IS NOT NULL AS has_pmid,
         (u.payload ? 'providerResponse' AND u.payload->'providerResponse' <> 'null'::jsonb) AS has_provider_response,
         pe.n AS provider_events, pe.types AS provider_event_types,
         sib.sent_other, sib.failed_other, sib.attempted_other,
         gi.items_near, gi.items_sent_near, gi.boundary_true, gi.boundary_false, gi.reasons AS governed_reasons,
         mb.n AS mailbox_sent_after, sc.sent_after AS schedule_sent_after, sc.live AS schedule_live
    FROM u
    LEFT JOIN LATERAL (SELECT count(*) AS n, array_agg(DISTINCT p.event_type) FILTER (WHERE p.event_type IS NOT NULL) AS types
                         FROM acquisition_mission_provider_events p WHERE p.execution_record_id = u.id) pe ON true
    LEFT JOIN LATERAL (SELECT count(*) FILTER (WHERE s.status = 'sent') AS sent_other,
                              count(*) FILTER (WHERE s.status = 'failed') AS failed_other,
                              count(*) FILTER (WHERE s.status = 'attempted') AS attempted_other
                         FROM acquisition_mission_outbound_executions s
                        WHERE s.mission_id = u.mission_id AND s.prospect_id = u.prospect_id AND s.id <> u.id
                          AND s.tenant_id IS NOT DISTINCT FROM u.tenant_id) sib ON true
    LEFT JOIN LATERAL (SELECT count(*) AS items_near,
                              count(*) FILTER (WHERE i.status = 'sent' OR i.provider_message_id IS NOT NULL) AS items_sent_near,
                              (SELECT count(*) FROM acquisition_outbound_events x
                                WHERE x.tenant_id = u.tenant_id AND x.item_id = ANY (array_agg(i.id))
                                  AND (x.payload->>'providerBoundaryCrossed' = 'true' OR x.payload ? 'providerMessageId')) AS boundary_true,
                              (SELECT count(*) FROM acquisition_outbound_events x
                                WHERE x.tenant_id = u.tenant_id AND x.item_id = ANY (array_agg(i.id))
                                  AND x.payload->>'providerBoundaryCrossed' = 'false') AS boundary_false,
                              array_agg(DISTINCT i.status || ':' || coalesce(i.reason, '-')) FILTER (WHERE i.id IS NOT NULL) AS reasons
                         FROM acquisition_outbound_items i
                        WHERE i.tenant_id = u.tenant_id AND i.prospect_id = u.prospect_id
                          AND i.attempted_at BETWEEN u.attempted_at - interval '15 minutes' AND u.attempted_at + interval '15 minutes') gi ON true
    LEFT JOIN LATERAL (SELECT count(*) AS n FROM tenant_outreach_messages m
                        WHERE m.tenant_id = u.tenant_id AND m.prospect_id = u.prospect_id AND m.direction = 'OUTBOUND'
                          AND (m.status = 'sent' OR m.provider_message_id IS NOT NULL)
                          AND m.created_at >= u.attempted_at - interval '2 minutes') mb ON true
    LEFT JOIN LATERAL (SELECT count(*) FILTER (WHERE s.status = 'SENT' OR s.outbound_message_id IS NOT NULL) AS sent_after,
                              count(*) FILTER (WHERE s.status IN ('SCHEDULED','EXECUTING')) AS live
                         FROM tenant_outreach_scheduled_sends s
                        WHERE s.tenant_id = u.tenant_id AND s.prospect_id = u.prospect_id
                          AND (s.status IN ('SCHEDULED','EXECUTING') OR coalesce(s.executed_at, s.updated_at) >= u.attempted_at - interval '2 minutes')) sc ON true
)
SELECT u.id AS record_id, u.tenant_id, u.mission_id, u.prospect_id, left(u.prepared_artifact_revision, 12) AS revision,
       u.provider, u.attempted_at, u.updated_at, left(md5(lower(coalesce(u.payload->>'email', ''))), 10) AS recipient_h,
       ev.provider_events, ev.provider_event_types, ev.sent_other, ev.failed_other, ev.attempted_other,
       ev.items_near AS governed_items_near, ev.items_sent_near AS governed_sent_near,
       ev.boundary_true, ev.boundary_false, ev.governed_reasons,
       ev.mailbox_sent_after, ev.schedule_sent_after, ev.schedule_live,
       CASE WHEN ev.provider_events > 0 OR ev.has_pmid OR ev.has_provider_response THEN 'SENT_CONFIRMED'
            ELSE 'UNKNOWN' END AS outcome,
       CASE WHEN ev.provider_events > 0 THEN 'provider_events_for_record'
            WHEN ev.has_pmid THEN 'provider_message_id_on_record'
            WHEN ev.has_provider_response THEN 'provider_response_on_record'
            WHEN ev.boundary_true > 0 THEN 'governed_attempt_crossed_provider_boundary'
            WHEN ev.items_sent_near > 0 OR ev.mailbox_sent_after > 0 OR ev.schedule_sent_after > 0 THEN 'unattributed_send_signal_near_attempt'
            WHEN ev.boundary_false > 0 THEN 'governed_pre_provider_signal_not_linked_to_record'
            ELSE 'no_local_post_attempt_evidence' END AS reason
  FROM u JOIN ev ON ev.id = u.id
 ORDER BY u.tenant_id, u.attempted_at;

\echo '== 3b. outcome totals'
WITH u AS (
  SELECT e.* FROM acquisition_mission_outbound_executions e
   WHERE e.status = 'attempted'
     AND NOT EXISTS (SELECT 1 FROM acquisition_mission_outbound_executions f
                      WHERE f.execution_identity = e.execution_identity AND f.status IN ('sent','failed'))
)
SELECT CASE WHEN EXISTS (SELECT 1 FROM acquisition_mission_provider_events p WHERE p.execution_record_id = u.id)
              OR u.provider_message_id IS NOT NULL
              OR (u.payload ? 'providerResponse' AND u.payload->'providerResponse' <> 'null'::jsonb)
            THEN 'SENT_CONFIRMED' ELSE 'UNKNOWN' END AS outcome, count(*) AS n
  FROM u GROUP BY 1 ORDER BY 1;

\echo '== 4. supplementary (informational, may be unavailable): CRM touchpoints / agent_log after each attempt'
WITH u AS (
  SELECT e.* FROM acquisition_mission_outbound_executions e
   WHERE e.status = 'attempted'
     AND NOT EXISTS (SELECT 1 FROM acquisition_mission_outbound_executions f
                      WHERE f.execution_identity = e.execution_identity AND f.status IN ('sent','failed'))
)
SELECT u.id AS record_id,
       (SELECT count(*) FROM touchpoints t WHERE t.prospect_id::text = u.prospect_id AND t.channel = 'email'
           AND t.created_at >= u.attempted_at - interval '2 minutes') AS email_touchpoints_after,
       (SELECT array_agg(DISTINCT coalesce(t.action_type, '-') || '/' || coalesce(t.outcome, '-'))
          FROM touchpoints t WHERE t.prospect_id::text = u.prospect_id AND t.channel = 'email'
           AND t.created_at >= u.attempted_at - interval '2 minutes') AS touchpoint_kinds,
       (SELECT count(*) FROM agent_log a WHERE a.prospect_id::text = u.prospect_id
           AND a.ran_at BETWEEN u.attempted_at - interval '2 minutes' AND u.attempted_at + interval '30 minutes') AS agent_log_near,
       (SELECT array_agg(DISTINCT a.agent_name || ':' || coalesce(a.action, '-') || ':' || coalesce(a.status, '-'))
          FROM agent_log a WHERE a.prospect_id::text = u.prospect_id
           AND a.ran_at BETWEEN u.attempted_at - interval '2 minutes' AND u.attempted_at + interval '30 minutes') AS agent_log_kinds
  FROM u ORDER BY u.tenant_id, u.attempted_at;

\echo '== 5. failed tenant-mailbox outbound record(s)'
SELECT m.id AS message_id, m.tenant_id, m.mission_id, m.prospect_id, m.failure_code,
       regexp_replace(left(coalesce(m.failure_message, ''), 240), '[^[:space:]<>"@]+@[^[:space:]<>"@]+', '<email>', 'g') AS failure_message_redacted,
       m.rfc_message_id IS NOT NULL AS has_rfc_id, m.provider_message_id IS NOT NULL AS has_provider_id, m.sent_at IS NOT NULL AS has_sent_at,
       m.metadata->>'providerAccepted' AS provider_accepted, m.created_at, m.updated_at,
       t.current_status AS thread_status, t.sequence_state,
       (SELECT count(*) FROM tenant_outreach_messages r
         WHERE r.tenant_id = m.tenant_id AND r.direction = 'INBOUND'
           AND (r.thread_id = m.thread_id
                OR (m.rfc_message_id IS NOT NULL AND (r.in_reply_to = m.rfc_message_id
                    OR strpos(coalesce(r.references_header, ''), m.rfc_message_id) > 0)))) AS inbound_replies,
       (SELECT count(*) FROM tenant_outreach_messages o
         WHERE o.tenant_id = m.tenant_id AND o.prospect_id = m.prospect_id AND o.direction = 'OUTBOUND'
           AND o.id <> m.id AND o.status = 'sent') AS other_sent_to_prospect,
       (SELECT array_agg(s.status) FROM tenant_outreach_scheduled_sends s WHERE s.outbound_message_id = m.id) AS linked_schedules,
       CASE
         WHEN m.provider_message_id IS NOT NULL OR m.sent_at IS NOT NULL THEN 'SENT_CONFIRMED'
         WHEN m.failure_code IN ('outreach_review_required', 'CALL_SUPPRESSED', 'tenant_pilot_policy_denied',
                                 'anchor_recovery_scope_invalid', 'EAUTH', 'EENVELOPE', 'EDNS')
           THEN 'NOT_SENT_DEFINITIVE'
         WHEN coalesce(m.failure_message, '') ~* '^(connect|getaddrinfo) E[A-Z]+' THEN 'NOT_SENT_DEFINITIVE'
         WHEN m.failure_code = 'EMESSAGE' AND coalesce(m.failure_message, '') ~ '(^|[^0-9])[45][0-9]{2}([^0-9]|$)' THEN 'NOT_SENT_DEFINITIVE'
         ELSE 'UNKNOWN'
       END AS outcome,
       CASE
         WHEN m.provider_message_id IS NOT NULL OR m.sent_at IS NOT NULL THEN 'provider_receipt_on_record'
         WHEN m.failure_code IN ('outreach_review_required', 'CALL_SUPPRESSED', 'tenant_pilot_policy_denied', 'anchor_recovery_scope_invalid')
           THEN 'authorization_refused_before_dispatch'
         WHEN m.failure_code IN ('EAUTH', 'EENVELOPE', 'EDNS') THEN 'smtp_definitive_' || lower(m.failure_code)
         WHEN coalesce(m.failure_message, '') ~* '^(connect|getaddrinfo) E[A-Z]+' THEN 'socket_connect_or_dns_failure'
         WHEN m.failure_code = 'EMESSAGE' AND coalesce(m.failure_message, '') ~ '(^|[^0-9])[45][0-9]{2}([^0-9]|$)' THEN 'smtp_reply_rejection'
         WHEN (SELECT count(*) FROM tenant_outreach_messages r
                WHERE r.tenant_id = m.tenant_id AND r.direction = 'INBOUND' AND m.rfc_message_id IS NOT NULL
                  AND (r.in_reply_to = m.rfc_message_id OR strpos(coalesce(r.references_header, ''), m.rfc_message_id) > 0)) > 0
           THEN 'reply_references_message_id_review_required'
         ELSE 'failure_not_provably_pre_boundary'
       END AS reason
  FROM tenant_outreach_messages m
  LEFT JOIN tenant_outreach_threads t ON t.id = m.thread_id
 WHERE m.direction = 'OUTBOUND' AND m.status = 'failed'
 ORDER BY m.created_at;

\echo '== 6. holds intact (expected: hold rows still present; no live re-contact path for affected prospects)'
WITH u AS (
  SELECT e.tenant_id, e.prospect_id FROM acquisition_mission_outbound_executions e
   WHERE e.status = 'attempted'
     AND NOT EXISTS (SELECT 1 FROM acquisition_mission_outbound_executions f
                      WHERE f.execution_identity = e.execution_identity AND f.status IN ('sent','failed'))
)
SELECT (SELECT count(*) FROM u) AS attempted_rows_still_attempted,
       (SELECT count(*) FROM acquisition_outbound_items i JOIN u ON u.tenant_id = i.tenant_id AND u.prospect_id = i.prospect_id
         WHERE i.status = 'pending') AS governed_pending_for_affected,
       (SELECT count(*) FROM tenant_outreach_scheduled_sends s JOIN u ON u.tenant_id = s.tenant_id AND u.prospect_id = s.prospect_id
         WHERE s.status IN ('SCHEDULED','EXECUTING')) AS live_schedules_for_affected;

ROLLBACK;
\echo '== done (rolled back; nothing written)'
