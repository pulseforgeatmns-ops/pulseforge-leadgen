#!/usr/bin/env node
'use strict';

// SPEC-261 read-only, standalone local preflight. No db.js, dotenv, app bootstrap,
// DDL, provider SDK or mutation lock. A report is never activation approval.
const TABLES = Object.freeze(['prospects', 'companies', 'acquisition_outbound_items', 'acquisition_outbound_envelopes',
  'acquisition_mission_outbound_executions', 'anchor_email_recovery_control', 'anchor_email_quarantines', 'anchor_email_recovery_reservations']);
const REQUIRED = Object.freeze({
  prospects: ['id', 'client_id', 'company_id', 'email', 'ao_outreach_review_required'],
  companies: ['id', 'client_id', 'name'],
  acquisition_outbound_items: ['id', 'tenant_id', 'envelope_id', 'candidate_id', 'prospect_id', 'company_id', 'email', 'status', 'reason', 'attempted_at', 'provider_message_id'],
  acquisition_outbound_envelopes: ['id', 'tenant_id', 'program_id', 'mission_id', 'revision'],
  acquisition_mission_outbound_executions: ['id', 'tenant_id', 'mission_id', 'prospect_id', 'status', 'provider_message_id', 'sent_at', 'prepared_artifact_revision', 'payload'],
  anchor_email_recovery_control: ['singleton', 'maintenance_armed', 'manifest'],
  anchor_email_quarantines: ['id','tenant_id','item_id','envelope_id','program_id','mission_id','canonical_id','attempt_token','recipient_email','prospect_id','company_id','candidate_id','expected_revision','company_name_key','company_domain','expected_status','expected_reason','approval_ref','decision_hash','created_at'],
  anchor_email_recovery_reservations: ['manifest_id','item_id','attempt_token','prospect_id','company_id','recipient_email','used_at','dispatch_started_at'],
});
async function inspectPreflight(db) {
  const report = { status: 'blocked', readOnly: true, outcome: 'UNKNOWN', retryAllowed: false, activationAllowed: false,
    blockers: [], sharedProspectsCountsByTenant: [], unresolvedCountsByTenant: [], schema: {},
    reviewPolicy: 'No schema repair, backfill, hold release or admission policy is approved by this report.',
    deploymentGate: 'unverified: inspect deployed close-capable workers separately before any schema action',
    rollback: 'Retain the durable maintenance latch, quarantine, reservations and close-capable provider gate; clear only manifest under separate approval.' };
  const columns = (await db.query(`SELECT table_name,column_name,data_type,is_nullable,column_default
    FROM information_schema.columns WHERE table_schema='public' AND table_name=ANY($1::text[]) ORDER BY table_name,ordinal_position`, [TABLES])).rows;
  for (const [table, required] of Object.entries(REQUIRED)) {
    const known = columns.filter(row => row.table_name === table);
    report.schema[table] = { present: known.length > 0, missingColumns: required.filter(name => !known.some(row => row.column_name === name)) };
    if (!known.length) report.blockers.push(`missing_table:${table}`);
    else for (const name of report.schema[table].missingColumns) report.blockers.push(`missing_column:${table}.${name}`);
  }
  const review = columns.find(row => row.table_name === 'prospects' && row.column_name === 'ao_outreach_review_required');
  if (review && review.data_type !== 'boolean') report.blockers.push('review_column_not_boolean');
  const has = (table, column) => columns.some(row => row.table_name === table && row.column_name === column);
  if (has('prospects', 'client_id')) {
    const reviewCounts = review?.data_type === 'boolean'
      ? ',count(*) FILTER(WHERE ao_outreach_review_required IS TRUE)::int AS review_true,count(*) FILTER(WHERE ao_outreach_review_required IS FALSE)::int AS review_false,count(*) FILTER(WHERE ao_outreach_review_required IS NULL)::int AS review_unknown'
      : ',0::int AS review_true,0::int AS review_false,count(*)::int AS review_unknown';
    report.sharedProspectsCountsByTenant = (await db.query(`SELECT client_id::text AS tenant_id,count(*)::int AS total${reviewCounts}
      FROM prospects GROUP BY client_id ORDER BY client_id`)).rows;
  }
  if (['tenant_id','status','attempted_at'].every(name => has('acquisition_outbound_items', name))) {
    report.unresolvedCountsByTenant = (await db.query(`SELECT tenant_id,count(*)::int AS raw_unknown,
      count(*) FILTER(WHERE attempted_at IS NULL)::int AS missing_attempt_token
      FROM acquisition_outbound_items WHERE status IN ('attempted','uncertain') GROUP BY tenant_id ORDER BY tenant_id`)).rows;
  }
  if (report.schema.anchor_email_recovery_control.missingColumns.length === 0) {
    const rows = (await db.query('SELECT singleton,maintenance_armed,manifest IS NOT NULL AS manifest_present FROM anchor_email_recovery_control')).rows;
    report.maintenance = rows.length === 1 && rows[0].singleton === true && rows[0].maintenance_armed === true ? 'durably_armed' : 'unknown';
    if (report.maintenance !== 'durably_armed') report.blockers.push('maintenance_latch_missing_or_invalid');
  } else report.maintenance = 'unknown';
  const recoveryTables = TABLES.filter(name => name.startsWith('anchor_email_'));
  const triggerRows = (await db.query(`SELECT c.relname AS table_name,t.tgname AS name,t.tgenabled AS enabled,
    t.tgtype::int AS type,p.proname AS function_name,p.prosrc AS function_body
    FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid
    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
    WHERE n.nspname='public' AND c.relname=ANY($1::text[]) AND NOT t.tgisinternal`, [recoveryTables])).rows;
  const immutableBody = "BEGIN RAISE EXCEPTION 'anchor_recovery_permanent_record'; END";
  const dispatchBody = `BEGIN
    IF (NEW.manifest_id,NEW.item_id,NEW.attempt_token,NEW.prospect_id,NEW.company_id,NEW.recipient_email,NEW.used_at)
      IS DISTINCT FROM (OLD.manifest_id,OLD.item_id,OLD.attempt_token,OLD.prospect_id,OLD.company_id,OLD.recipient_email,OLD.used_at)
      OR OLD.dispatch_started_at IS NOT NULL OR NEW.dispatch_started_at IS NULL
    THEN RAISE EXCEPTION 'anchor_recovery_permanent_reservation'; END IF;
    RETURN NEW;
    END`;
  const compact = value => String(value || '').replace(/\s+/g, ' ').trim();
  for (const [table, name, type, body] of [
    ['anchor_email_recovery_control','anchor_recovery_control_permanent',42,immutableBody],
    ['anchor_email_quarantines','anchor_quarantine_permanent',58,immutableBody],
    ['anchor_email_recovery_reservations','anchor_reservation_permanent',42,immutableBody],
    ['anchor_email_recovery_reservations','anchor_reservation_dispatch_once',19,dispatchBody],
  ]) {
    const trigger = triggerRows.find(row => row.table_name === table && row.name === name);
    if (!trigger || !['O','A'].includes(trigger.enabled) || trigger.type !== type || compact(trigger.function_body) !== compact(body)) report.blockers.push(`missing_or_changed_invariant:${name}`);
  }
  const constraints = (await db.query(`SELECT r.relname AS table_name,c.contype AS kind,c.convalidated AS validated,
    c.condeferrable AS deferrable,pg_catalog.pg_get_expr(c.conbin,c.conrelid) AS expression,
    ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(num,ord)
      JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.num ORDER BY k.ord) AS keys
    FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class r ON r.oid=c.conrelid
    JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='public' AND r.relname=ANY($1::text[])`, [recoveryTables])).rows;
  for (const [table, kind, keys] of [
    ['anchor_email_recovery_control','p',['singleton']],
    ['anchor_email_quarantines','p',['id']],['anchor_email_quarantines','u',['item_id']],['anchor_email_quarantines','u',['decision_hash']],
    ['anchor_email_recovery_reservations','p',['manifest_id','item_id']],['anchor_email_recovery_reservations','u',['item_id']],
  ]) if (!constraints.some(c => c.table_name === table && c.kind === kind && c.validated === true && c.deferrable === false && JSON.stringify(c.keys) === JSON.stringify(keys))) report.blockers.push(`missing_unique_identity:${table}.${keys.join(',')}`);
  for (const column of ['singleton','maintenance_armed']) {
    if (!constraints.some(c => c.table_name === 'anchor_email_recovery_control' && c.kind === 'c' && c.validated === true && compact(c.expression).replace(/[()]/g,'') === column)) report.blockers.push(`missing_latch_constraint:${column}`);
  }
  for (const table of recoveryTables) for (const name of REQUIRED[table]) {
    const column = columns.find(c => c.table_name === table && c.column_name === name);
    if (!column) continue;
    const expectedType = ['singleton','maintenance_armed'].includes(name) ? 'boolean' : name === 'manifest' ? 'jsonb'
      : ['created_at','used_at','dispatch_started_at'].includes(name) ? 'timestamp with time zone' : 'text';
    if (column.data_type !== expectedType || (!['manifest','dispatch_started_at'].includes(name) && column.is_nullable !== 'NO')) report.blockers.push(`invalid_column_shape:${table}.${name}`);
  }
  if (!has('companies','domain') && !has('companies','website')) report.blockers.push('missing_authoritative_company_domain_source');
  // These are prerequisites only. No known schema implies live provider closure.
  report.schemaPrerequisitesSatisfied = report.blockers.length === 0;
  report.blockers.push('production_gate_and_separate_approvals_unverified');
  return report;
}
function localConfig(connectionString) {
  let url;
  try { url = new URL(connectionString); } catch { throw new Error('local_test_database_required'); }
  if (!['postgres:', 'postgresql:'].includes(url.protocol) || !['127.0.0.1', '[::1]'].includes(url.hostname)
      || !url.port || [...url.searchParams.keys()].length) throw new Error('local_test_database_required');
  return { connectionString, connectionTimeoutMillis: 3000, query_timeout: 5000,
    options: '-c default_transaction_read_only=on -c statement_timeout=4s -c lock_timeout=500ms', max: 1 };
}
async function main(args = process.argv.slice(2), env = process.env) {
  if (args.length === 1 && ['--help', '-h'].includes(args[0])) return { exitCode: 0, output: 'Read-only local preflight: --test-database uses explicitly selected loopback ANCHOR_RECOVERY_TEST_DATABASE_URL. Default does not connect. No apply mode.' };
  if (args.length === 0) return { exitCode: 2, output: JSON.stringify({ status: 'unknown', readOnly: true, connected: false, activationAllowed: false, reason: 'explicit_local_test_database_required' }) };
  if (args.length !== 1 || args[0] !== '--test-database') return { exitCode: 2, output: JSON.stringify({ status: 'unknown', reason: 'invalid_arguments' }) };
  let pool, db;
  try {
    const config = localConfig(env.ANCHOR_RECOVERY_TEST_DATABASE_URL);
    const { Pool } = require('pg');
    pool = new Pool(config); db = await pool.connect();
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const report = await inspectPreflight(db);
    await db.query('ROLLBACK');
    return { exitCode: 1, output: JSON.stringify(report, null, 2) };
  } catch (error) { return { exitCode: 2, output: JSON.stringify({ status: 'unknown', readOnly: true, activationAllowed: false, reason: 'preflight_incomplete', code: /^[A-Z0-9_]{5,64}$/.test(error.code || '') ? error.code : 'unverified' }) }; }
  finally { if (db) { try { await db.query('ROLLBACK'); } catch {} db.release(true); } if (pool) await pool.end(); }
}
if (require.main === module) main().then(({ output, exitCode }) => { console.log(output); process.exitCode = exitCode; }, () => { console.log('{"status":"unknown","activationAllowed":false}'); process.exitCode = 2; });
module.exports = { TABLES, REQUIRED, inspectPreflight, localConfig, main };
