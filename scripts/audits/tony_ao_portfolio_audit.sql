-- =============================================================================
-- Tony (users.id = 24) · Anchor Cleaning (client_id / tenant_id = 10)
-- AO portfolio audit — READ ONLY query pack
-- =============================================================================
--
-- PURPOSE
--   Reconciles Tony's assigned prospects (prospects.assigned_ao_id = 24) and
--   owned field leads (ao_leads.ao_owner_id = 24) with every related evidence
--   source: companies, contacts, touchpoints, agent logs, outbound messages,
--   email events, scheduled sends, suppressions, quarantine / recovery control,
--   sender + capacity gates, AO activity / tasks / follow-ups and execution
--   records.
--
-- READ-ONLY GUARANTEES
--   * Every statement is a plain SELECT (or WITH … SELECT). There is no INSERT,
--     UPDATE, DELETE, MERGE, COPY, DDL, CALL, DO block, locking clause
--     (FOR UPDATE/SHARE), sequence call (nextval/setval) or function that writes.
--   * Statement 0 sets the session to read-only, as defence in depth. If your
--     client opens a fresh connection per run, it is harmless to skip — the
--     statements themselves cannot write.
--   * No temp tables, views or prepared statements are created.
--
-- CREDENTIAL HYGIENE
--   * No connection string, password or token appears in this file.
--   * users: only id/name/email/role/active/client_id (never password_hash).
--   * tenant_mailbox_integrations: secret-reference columns (*_secret_ref)
--     and SMTP/IMAP host details are deliberately NOT selected.
--   * anchor_scoped_resume_grants: the manifest body and session-bound data
--     are NOT selected; only identifiers, hashes and timestamps.
--
-- HOW TO RUN
--   Railway PostgreSQL query editor (Data tab):
--     paste ONE section (from one "-- §" header to the next) and run it.
--     Every section is self-contained; no psql meta-commands, variables or
--     macros are used. Sections that hit a table/column absent in production
--     will error on their own without affecting any other section.
--   psql:
--     psql "$DATABASE_URL" -X -f scripts/audits/tony_ao_portfolio_audit.sql
--     (autocommit: a failing statement does not stop later statements).
--   Never paste DATABASE_URL into chat, tickets or this file.
--
-- PORTFOLIO DEFINITION (repeated inline in every section that needs it)
--   pf   = 30 prospect rows  ∪ 17 ao_lead rows (linked via ao_leads.crm_prospect_id)
--   pids = DISTINCT linked prospect ids
--   IDs are compared as ::text so the pack works whether ids are uuid or integer.
-- =============================================================================


-- § 0 · Session safety (optional; run first in psql or a persistent session)
SET default_transaction_read_only = on;


-- § 1 · Identity check — Tony's account (no password hash)
SELECT id, name, email, role, active, client_id
FROM users
WHERE id = 24;


-- § 2 · Record counts + unique account count (expect 30 + 17 before dedup)
WITH pf AS (
  SELECT p.id::text AS prospect_id, NULL::text AS ao_lead_id, 'prospect' AS src
  FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text, l.id::text, 'ao_lead'
  FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
)
SELECT src,
       count(*)                     AS row_count,
       count(prospect_id)           AS rows_with_prospect,
       count(DISTINCT prospect_id)  AS distinct_prospects
FROM pf GROUP BY src
UNION ALL
SELECT 'UNIQUE_ACCOUNTS', count(*), NULL, NULL
FROM (SELECT COALESCE(prospect_id, 'lead:' || ao_lead_id) AS k FROM pf GROUP BY 1) u;


-- § 3 · Prospect master rows with company (all gate columns via p.*)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT p.*, c.name AS company_name, c.website AS company_website,
       c.industry AS company_industry, c.location AS company_location
FROM prospects p
JOIN pids ON pids.prospect_id = p.id::text
LEFT JOIN companies c ON c.id = p.company_id
ORDER BY c.name NULLS LAST, p.id;


-- § 4 · AO leads owned by Tony (including leads not linked to a prospect)
SELECT l.*
FROM ao_leads l
WHERE l.client_id = 10 AND l.ao_owner_id = 24
ORDER BY l.business_name;


-- § 5 · AO lead contacts
SELECT c.*
FROM ao_contacts c
JOIN ao_leads l ON l.id = c.lead_id
WHERE l.client_id = 10 AND l.ao_owner_id = 24
ORDER BY l.business_name;


-- § 6 · Duplicate detection across all 47 rows (by company name key + domain)
WITH pf AS (
  SELECT p.id::text AS prospect_id, NULL::text AS ao_lead_id, 'prospect' AS src
  FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text, l.id::text, 'ao_lead'
  FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), acct AS (
  SELECT pf.src, pf.prospect_id, pf.ao_lead_id, p.company_id,
         lower(regexp_replace(COALESCE(c.name, l.business_name, ''), '[^a-zA-Z0-9]', '', 'g')) AS name_key,
         lower(regexp_replace(regexp_replace(COALESCE(c.website, ''), '^https?://(www\.)?', '', 'i'), '/.*$', '')) AS domain_key
  FROM pf
  LEFT JOIN prospects p ON p.id::text = pf.prospect_id
  LEFT JOIN companies c ON c.id = p.company_id
  LEFT JOIN ao_leads  l ON l.id::text = pf.ao_lead_id
)
SELECT name_key, domain_key,
       array_agg(DISTINCT company_id)  AS company_ids,
       array_agg(DISTINCT prospect_id) AS prospect_ids,
       array_agg(DISTINCT ao_lead_id)  AS ao_lead_ids,
       array_agg(src)                  AS sources,
       count(*)                        AS n
FROM acct
GROUP BY name_key, domain_key
ORDER BY n DESC, name_key;


-- § 7 · Touchpoints (all channels)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT t.*
FROM touchpoints t
JOIN pids ON pids.prospect_id = t.prospect_id::text
ORDER BY t.prospect_id, t.created_at;


-- § 8 · Agent log (last 120 days by ran_at; payload truncated)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT a.prospect_id, a.agent_name, a.action, a.status, a.error_msg, a.ran_at,
       left(a.payload::text, 400) AS payload_head
FROM agent_log a
JOIN pids ON pids.prospect_id = a.prospect_id::text
WHERE a.ran_at > now() - interval '120 days'
ORDER BY a.prospect_id, a.ran_at;


-- § 9 · Tenant mailbox messages (outbound + inbound; bodies omitted)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT m.id, m.prospect_id, m.direction, m.subject, m.sender, m.recipients,
       m.sent_at, m.received_at, m.provider_message_id, m.mission_id,
       m.sending_identity_id, m.thread_id
FROM tenant_outreach_messages m
JOIN pids ON pids.prospect_id = m.prospect_id::text
WHERE m.tenant_id::text = '10'
ORDER BY m.prospect_id, COALESCE(m.sent_at, m.received_at);


-- § 10 · Classified replies
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT r.id, r.prospect_id, r.email, r.classification, r.classified_at,
       r.attempts, r.last_error, r.created_at
FROM acquisition_outbound_replies r
JOIN pids ON pids.prospect_id = r.prospect_id::text
ORDER BY r.prospect_id, r.created_at;


-- § 11 · Governed outbound items (envelope membership + send attempts)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT i.id, i.envelope_id, i.prospect_id, i.company_id, i.email, i.status,
       i.attempted_at, i.provider_message_id, i.reason
FROM acquisition_outbound_items i
JOIN pids ON pids.prospect_id = i.prospect_id::text
ORDER BY i.prospect_id, i.attempted_at;


-- § 12 · Governed outbound events (joined through items — events carry no prospect_id)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT i.prospect_id, e.id, e.event_type, e.envelope_id, e.item_id, e.created_at,
       left(e.payload::text, 300) AS payload_head
FROM acquisition_outbound_events e
JOIN acquisition_outbound_items i ON i.id = e.item_id
JOIN pids ON pids.prospect_id = i.prospect_id::text
ORDER BY i.prospect_id, e.created_at;


-- § 13 · Scheduled sends (any status)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT s.id, s.prospect_id, s.recipient_email, s.sequence_step, s.scheduled_for, s.status,
       s.authorization_source, s.authorized_by, s.authorized_at,
       s.sending_identity_id, s.mission_id
FROM tenant_outreach_scheduled_sends s
JOIN pids ON pids.prospect_id = s.prospect_id::text
WHERE s.tenant_id::text = '10'
ORDER BY s.prospect_id, s.scheduled_for;


-- § 14 · Email suppressions matching portfolio prospects (by email or contact_ref)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT p.id AS prospect_id, s.id, s.email, s.contact_ref, s.reason, s.source,
       s.status, s.created_at, s.revoked_at
FROM prospects p
JOIN pids ON pids.prospect_id = p.id::text
JOIN tenant_outreach_suppressions s
  ON s.tenant_id::text = '10'
 AND (lower(s.email) = lower(p.email) OR s.contact_ref = p.id::text)
ORDER BY p.id;


-- § 15 · Call suppressions (spreadsheet channel)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT s.id, s.prospect_id, s.contact_id, s.channel, left(s.data::text, 300) AS data_head
FROM max_spreadsheet_suppressions s
JOIN pids ON pids.prospect_id = s.prospect_id::text
WHERE s.client_id = 10;


-- § 16 · Email quarantine decisions (tenant-wide; decision records only)
SELECT * FROM anchor_email_quarantines;


-- § 17 · Recovery control (DB half of the kill-switch; manifest truncated)
SELECT singleton, maintenance_armed,
       manifest->>'kind'    AS manifest_kind,
       manifest->>'version' AS manifest_version,
       manifest->>'id'      AS manifest_id
FROM anchor_email_recovery_control;


-- § 18 · Scoped-resume grants (identifiers only — no manifest body / session data)
SELECT manifest_id, tenant_id, program_id, manifest_hash, approval_ref,
       operator_id, operator_role, installed_at
FROM anchor_scoped_resume_grants
ORDER BY installed_at DESC
LIMIT 10;


-- § 19 · Sender identities (no secrets)
SELECT id, mailbox_integration_id, sender_email, sender_display_name, status,
       verification_state, disabled_at, revoked_at
FROM tenant_sending_identities
WHERE tenant_id::text = '10';


-- § 20 · Mailbox integrations (secret refs and server hosts deliberately excluded)
SELECT id, provider_type, mailbox_address, display_name, status, verification_state,
       created_at, disabled_at, revoked_at
FROM tenant_mailbox_integrations
WHERE tenant_id::text = '10';


-- § 21 · Capacity envelopes (latest 5)
SELECT sender_email, local_date, max_sends_per_day, max_sends_per_window,
       minimum_spacing_minutes, allowed_send_window, ramp_stage, governor_state,
       valid_from, valid_until, computed_at
FROM emmett_tenant_mailbox_capacity_envelopes
WHERE tenant_id::text = '10'
ORDER BY computed_at DESC
LIMIT 5;


-- § 22 · Capacity reservations (last 3 days)
SELECT id, envelope_id, sending_identity_id, schedule_id, status, scheduled_for, created_at
FROM emmett_tenant_mailbox_capacity_reservations
WHERE tenant_id::text = '10' AND created_at > now() - interval '3 days'
ORDER BY created_at DESC;


-- § 23 · Inbox health
SELECT * FROM acquisition_outbound_inbox_health WHERE tenant_id::text = '10';


-- § 24 · AO prospect activity
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT a.*
FROM ao_prospect_activity a
JOIN pids ON pids.prospect_id = a.prospect_id::text
WHERE a.tenant_id::text = '10'
ORDER BY a.prospect_id, a.created_at;


-- § 25 · AO prospect tasks (assignment + recommended first action)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT t.*
FROM ao_prospect_tasks t
JOIN pids ON pids.prospect_id = t.prospect_id::text
WHERE t.client_id = 10;


-- § 26 · AO prospect updates (outcome / next action)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT u.*
FROM ao_prospect_updates u
JOIN pids ON pids.prospect_id = u.prospect_id::text
ORDER BY u.prospect_id, u.created_at;


-- § 27 · Field follow-up tasks (ao_leads path)
SELECT f.*
FROM ao_follow_up_tasks f
JOIN ao_leads l ON l.id = f.lead_id
WHERE l.client_id = 10 AND l.ao_owner_id = 24
ORDER BY f.due_date;


-- § 28 · Max-issued AO follow-up tasks for portfolio prospects
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT t.*
FROM max_ao_follow_up_tasks t
JOIN pids ON pids.prospect_id = t.prospect_id::text
WHERE t.client_id = 10
ORDER BY t.created_at;


-- § 29 · Account flags
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT f.*
FROM ao_account_flags f
JOIN pids ON pids.prospect_id = f.account_id::text
WHERE f.client_id = 10;


-- § 30 · Follow-up drafts (status + approval path; draft bodies truncated)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT d.id, d.account_id, d.assigned_ao_id, d.status, d.approval_path, d.approval_reason,
       d.subject_line, left(d.email_draft, 300) AS email_draft_head, d.warnings,
       d.created_at, d.updated_at
FROM ao_followup_drafts d
JOIN pids ON pids.prospect_id = d.account_id::text
WHERE d.tenant_id::text = '10'
ORDER BY d.created_at;


-- § 31 · Setter activity log (calls / emails / texts)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT a.*
FROM activity_log a
JOIN pids ON pids.prospect_id = a.lead_id::text
ORDER BY a.lead_id, a.created_at;


-- § 32 · agent_actions referencing portfolio prospects
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT aa.id, aa.created_by, aa.action_type, aa.title, aa.status, aa.executed_at,
       aa.created_at, left(aa.payload::text, 300) AS payload_head
FROM agent_actions aa
JOIN pids ON pids.prospect_id IN (aa.payload->>'prospect_id', aa.payload->>'prospectId')
WHERE aa.client_id = 10
ORDER BY aa.created_at;


-- § 33 · Mission outbound executions (execution evidence)
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT x.id, x.mission_id, x.prospect_id, x.provider, x.provider_message_id, x.status,
       x.provider_error_code, x.provider_error_message, x.attempted_at, x.sent_at, x.created_at
FROM acquisition_mission_outbound_executions x
JOIN pids ON pids.prospect_id = x.prospect_id::text
ORDER BY x.prospect_id, x.created_at;


-- § 34 · Emmett outbound outcomes
WITH pf AS (
  SELECT p.id::text AS prospect_id FROM prospects p WHERE p.client_id = 10 AND p.assigned_ao_id = 24
  UNION ALL
  SELECT l.crm_prospect_id::text FROM ao_leads l WHERE l.client_id = 10 AND l.ao_owner_id = 24
), pids AS (SELECT DISTINCT prospect_id FROM pf WHERE prospect_id IS NOT NULL)
SELECT o.id, o.prospect_id, o.outcome_type, o.event_at, o.created_at,
       left(o.payload::text, 300) AS payload_head
FROM emmett_outbound_outcomes o
JOIN pids ON pids.prospect_id = o.prospect_id::text
ORDER BY o.prospect_id, o.event_at;


-- § 35 · Institutional relationship signal — Anchor portal service locations
SELECT id, operator_client_id, name, slug, address, is_demo, next_scheduled_at, created_at
FROM anchor_portal_locations
ORDER BY name;

-- END OF PACK
