-- MAX-P0-003 §4 read-only schema report for the CURRENTLY DEPLOYED image (psql 18 is in it).
-- Run in Render Shell (pulseforge-api-migration):
--   PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20s' psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -f - <<'SQL'
--   ...this file's body...
--   SQL
-- Every statement runs in a READ ONLY transaction; the server rejects any write.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
\pset footer off
\echo '== server'
SELECT current_database() AS database, current_user AS role, split_part(version(), ' ', 2) AS pg_version,
       pg_size_pretty(pg_database_size(current_database())) AS size, current_setting('transaction_read_only') AS read_only, now() AS observed_at;
\echo '== catalog (rehearsal reference: 319 tables, 3 views, 80 triggers, 779 indexes, 3629 constraints, 84 functions, 56 sequences)'
SELECT (SELECT count(*) FROM pg_tables WHERE schemaname = 'public') AS tables,
       (SELECT count(*) FROM pg_views WHERE schemaname = 'public') AS views,
       (SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND NOT t.tgisinternal) AS triggers,
       (SELECT count(*) FROM pg_indexes WHERE schemaname = 'public') AS indexes,
       (SELECT count(*) FROM information_schema.table_constraints WHERE table_schema = 'public') AS constraints,
       (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public') AS functions,
       (SELECT count(*) FROM pg_sequences WHERE schemaname = 'public') AS sequences;
\echo '== Max / AMO / MAX-P0-003 core tables (present = t)'
SELECT t AS table_name, to_regclass(t) IS NOT NULL AS present
  FROM unnest(ARRAY['clients','users','session','prospects','companies','agent_log','agent_actions',
    'acquisition_missions','acquisition_mission_events','acquisition_mission_contributions','acquisition_mission_observations',
    'acquisition_mission_outcomes','acquisition_mission_execution_audit','acquisition_outbound_executions','max_conversation_context',
    'tenant_outreach_messages','max_external_action_claims','max_request_receipts','max_reliability_audit']) AS t;
\echo '== other schemas'
SELECT nspname AS schema FROM pg_namespace WHERE nspname NOT IN ('public','information_schema') AND nspname NOT LIKE 'pg\_%' ORDER BY 1;
\echo '== tenants (counts only)'
SELECT c.id, c.name, c.active, (SELECT count(*) FROM prospects p WHERE p.client_id = c.id) AS prospects
  FROM clients c ORDER BY c.id;
\echo '== missions by tenant'
SELECT tenant_id, count(*) AS missions, max(updated_at) AS last_updated FROM acquisition_missions GROUP BY tenant_id ORDER BY tenant_id;
\echo '== freshness'
SELECT 'agent_log' AS source, max(created_at)::text AS latest FROM agent_log
UNION ALL SELECT 'acquisition_mission_events', max(at)::text FROM acquisition_mission_events
UNION ALL SELECT 'session (expire)', max(expire)::text FROM session;
\echo '== pre-cutover copies on this server (names only)'
SELECT datname FROM pg_database WHERE datname LIKE 'pulseforge%' ORDER BY 1;
\echo '== advisory locks currently held (should be none, or only active workers)'
SELECT locktype, classid, objid, objsubid, granted, pid FROM pg_locks WHERE locktype = 'advisory';
ROLLBACK;
