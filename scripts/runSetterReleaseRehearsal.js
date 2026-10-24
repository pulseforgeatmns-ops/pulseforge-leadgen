'use strict';

// Phase 3F intentionally accepts only the GitHub Actions PostgreSQL service.
// It never reads DATABASE_URL, so a Railway or production connection cannot be
// selected accidentally.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Client } = require('pg');
const { applyProspectDisposition } = require('../utils/callDispositions');

const root = path.join(__dirname, '..');
const forwardPath = path.join(root, 'migrations/2026-07-19-setter-pilot-quality-control.sql');
const rollbackPath = path.join(root, 'migrations/2026-07-19-setter-pilot-quality-control.rollback.sql');
const reportPath = path.resolve(process.env.PHASE3F_REPORT_PATH || path.join(root, 'artifacts/phase3f/rehearsal-report.json'));
const adminUrl = process.env.PHASE3F_ADMIN_DATABASE_URL;
const rehearsalDatabase = process.env.PHASE3F_REHEARSAL_DATABASE;
const report = { phase: '3F', status: 'running', checks: [], timings_ms: {}, row_counts: {}, commits: {}, schema: {} };
const PHASE3D_TABLES = ['setter_callbacks'];
const PHASE3D_COLUMNS = {
  clients: ['setter_pipeline_v2_enabled', 'setter_pipeline_v2_configured_at', 'setter_review_sample_percent'],
  prospects: ['is_synthetic', 'synthetic_label', 'callback_completed_at', 'assigned_setter_id'],
  call_dispositions: ['structured_notes', 'activity_result', 'next_action', 'suppression_state', 'lifecycle_result', 'is_synthetic', 'review_required', 'review_status', 'idempotency_key'],
};
const PHASE3D_INDEXES = ['call_dispositions_idempotency_idx', 'setter_callbacks_one_pending_idx', 'setter_callbacks_due_idx'];
const PHASE3D_TRIGGERS = ['prospects_synthetic_suppression', 'prospects_suppression_cleanup'];

function check(condition, name, details = {}) {
  assert.ok(condition, name);
  report.checks.push({ name, passed: true, ...details });
}

function elapsed(start) {
  return Number(process.hrtime.bigint() - start) / 1e6;
}

function safeUrl(value, label) {
  check(Boolean(value), `${label} is configured`);
  const parsed = new URL(value);
  check(['127.0.0.1', 'localhost', 'postgres'].includes(parsed.hostname), `${label} host is CI-local`, { host: parsed.hostname });
  check(!/railway/i.test(value), `${label} contains no Railway URL`);
  return parsed;
}

async function snapshot(db) {
  // Sequential awaits only. A single pg Client (production Gate 2 and the
  // Phase 3F rehearsal Client) cannot safely run concurrent queries; Promise.all
  // on one client triggers the pg@8 overlapping-query deprecation.
  const tables = await db.query(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name`);
  const columns = await db.query(`SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position`);
  const indexes = await db.query(`SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' ORDER BY tablename, indexname`);
  const constraints = await db.query(`SELECT table_name, constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_schema = 'public' ORDER BY table_name, constraint_name`);
  const triggers = await db.query(`SELECT c.relname AS table_name, t.tgname AS trigger_name FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND NOT t.tgisinternal ORDER BY c.relname, t.tgname`);
  return { tables: tables.rows, columns: columns.rows, indexes: indexes.rows, constraints: constraints.rows, triggers: triggers.rows };
}

function hasTable(state, tableName) {
  return state.tables.some(table => table.table_name === tableName);
}

function hasColumn(state, tableName, columnName) {
  return state.columns.some(column => column.table_name === tableName && column.column_name === columnName);
}

function phase3dObjectState(state) {
  return {
    tables: Object.fromEntries(PHASE3D_TABLES.map(name => [name, hasTable(state, name)])),
    columns: Object.fromEntries(Object.entries(PHASE3D_COLUMNS).map(([table, columns]) => [table,
      Object.fromEntries(columns.map(column => [column, hasColumn(state, table, column)])),
    ])),
    indexes: Object.fromEntries(PHASE3D_INDEXES.map(name => [name, state.indexes.some(index => index.indexname === name)])),
    triggers: Object.fromEntries(PHASE3D_TRIGGERS.map(name => [name, state.triggers.some(trigger => trigger.trigger_name === name)])),
    callback_status_constraint: state.constraints.some(constraint => constraint.constraint_name === 'setter_callbacks_status_check' && constraint.constraint_type === 'CHECK'),
  };
}

function allPhase3dObjectsPresent(state) {
  return Object.values(state.tables).every(Boolean)
    && Object.values(state.columns).every(columns => Object.values(columns).every(Boolean))
    && Object.values(state.indexes).every(Boolean)
    && Object.values(state.triggers).every(Boolean)
    && state.callback_status_constraint;
}

async function counts(db, state) {
  const tableCounts = {};
  for (const tableName of ['clients', 'prospects', 'call_dispositions', 'setter_callbacks', 'setter_follow_up_drafts']) {
    if (!hasTable(state, tableName)) {
      tableCounts[tableName] = { exists: false, row_count: null };
      continue;
    }
    const result = await db.query(`SELECT count(*)::int AS count FROM ${tableName}`);
    tableCounts[tableName] = { exists: true, row_count: result.rows[0].count };
  }
  const prospectCounts = {};
  for (const [name, predicate] of Object.entries({
    synthetic_prospects: 'is_synthetic',
    suppressed_prospects: 'do_not_contact',
  })) {
    if (!hasTable(state, 'prospects') || !hasColumn(state, 'prospects', predicate)) {
      prospectCounts[name] = { exists: false, row_count: null };
      continue;
    }
    const result = await db.query(`SELECT count(*)::int AS count FROM prospects WHERE ${predicate}`);
    prospectCounts[name] = { exists: true, row_count: result.rows[0].count };
  }
  return { tables: tableCounts, prospect_filters: prospectCounts };
}

async function main() {
  check(!process.env.DATABASE_URL, 'DATABASE_URL is absent');
  check(!Object.keys(process.env).some(key => /^RAILWAY(?:_|$)/.test(key)), 'Railway production variables are absent');
  const parsedAdmin = safeUrl(adminUrl, 'phase3f admin database');
  check(/^phase3f_rehearsal_[a-z0-9_]+$/.test(rehearsalDatabase || ''), 'database name is rehearsal-specific', { database: rehearsalDatabase });
  check(parsedAdmin.pathname === '/postgres', 'admin connection uses only the service postgres database');

  const admin = new Client({ connectionString: adminUrl, ssl: false });
  await admin.connect();
  const version = (await admin.query('SHOW server_version')).rows[0].server_version;
  report.postgresql_version = version;
  check(version.startsWith('18.'), 'PostgreSQL major matches pinned production major', { version });
  await admin.query(`CREATE DATABASE ${rehearsalDatabase}`);
  await admin.end();

  const rehearsalUrl = new URL(adminUrl);
  rehearsalUrl.pathname = `/${rehearsalDatabase}`;
  safeUrl(rehearsalUrl.toString(), 'phase3f rehearsal database');
  const db = new Client({ connectionString: rehearsalUrl.toString(), ssl: false });
  await db.connect();
  try {
    await db.query(`
      CREATE EXTENSION pgcrypto;
      CREATE TABLE clients (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN NOT NULL DEFAULT true);
      CREATE TABLE users (id INTEGER PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id), role TEXT NOT NULL, active BOOLEAN NOT NULL DEFAULT true);
      CREATE TABLE prospects (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id),
        first_name TEXT, last_name TEXT, email TEXT, phone TEXT, status TEXT NOT NULL DEFAULT 'new',
        setter_status TEXT, setter_visible BOOLEAN NOT NULL DEFAULT true, do_not_contact BOOLEAN NOT NULL DEFAULT false,
        callback_at TIMESTAMPTZ, is_hot BOOLEAN NOT NULL DEFAULT false, notes TEXT, vertical TEXT,
        setter_updated_at TIMESTAMPTZ, last_contacted_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE TABLE call_dispositions (
        id SERIAL PRIMARY KEY, prospect_id UUID REFERENCES prospects(id), client_id INTEGER NOT NULL REFERENCES clients(id),
        disposition TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE TABLE setter_follow_up_drafts (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id), prospect_id UUID NOT NULL REFERENCES prospects(id),
        status TEXT NOT NULL DEFAULT 'draft', dismissed_at TIMESTAMPTZ, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      INSERT INTO clients (id, name) VALUES (10, 'Anchor rehearsal'), (20, 'Tenant two rehearsal');
      INSERT INTO users (id, client_id, role) VALUES (101, 10, 'setter'), (201, 20, 'setter');
      INSERT INTO prospects (id, client_id, first_name, callback_at) VALUES
        ('10000000-0000-4000-8000-000000000010', 10, 'Existing', now() + interval '1 hour'),
        ('20000000-0000-4000-8000-000000000020', 20, 'Other tenant', now() + interval '1 hour'),
        ('30000000-0000-4000-8000-000000000030', 10, 'Suppression target', now() + interval '1 hour');
    `);
    report.schema.before = await snapshot(db);
    report.schema.phase3d_before = phase3dObjectState(report.schema.before);
    report.row_counts.before = await counts(db, report.schema.before);
    check(report.schema.phase3d_before.tables.setter_callbacks === false, 'pre-migration setter_callbacks is absent as expected');
    check(report.row_counts.before.tables.setter_callbacks.exists === false && report.row_counts.before.tables.setter_callbacks.row_count === null,
      'pre-migration missing setter_callbacks is recorded without querying it');

    let started = process.hrtime.bigint();
    await db.query(fs.readFileSync(forwardPath, 'utf8'));
    report.timings_ms.forward = elapsed(started);
    report.schema.after_forward = await snapshot(db);
    report.schema.phase3d_after_forward = phase3dObjectState(report.schema.after_forward);
    report.row_counts.after_forward = await counts(db, report.schema.after_forward);
    check(allPhase3dObjectsPresent(report.schema.phase3d_after_forward), 'forward migration creates all expected Phase 3D tables, columns, indexes, constraints, and triggers');
    const flag = (await db.query('SELECT setter_pipeline_v2_enabled FROM clients WHERE id = 10')).rows[0];
    check(flag.setter_pipeline_v2_enabled === false, 'Anchor feature flag remains disabled');
    for (const name of ['setter_callbacks', 'prospects_synthetic_suppression', 'prospects_suppression_cleanup']) {
      const value = (await db.query('SELECT to_regclass($1) AS value', [`public.${name}`])).rows[0].value;
      if (name === 'setter_callbacks') check(value === 'setter_callbacks', 'expected table exists', { name });
      else check(value === null, 'trigger name does not masquerade as a table', { name });
    }
    const triggerCount = (await db.query("SELECT count(*)::int AS count FROM pg_trigger WHERE tgrelid = 'prospects'::regclass AND tgname IN ('prospects_synthetic_suppression', 'prospects_suppression_cleanup') AND NOT tgisinternal")).rows[0].count;
    check(triggerCount === 2, 'synthetic and DNC cleanup triggers exist');
    check(report.row_counts.after_forward.tables.setter_callbacks.row_count === 3, 'existing callbacks were backfilled exactly once');

    const syntheticId = '40000000-0000-4000-8000-000000000040';
    const inserted = await db.query('INSERT INTO prospects (id, client_id, first_name, is_synthetic, do_not_contact) VALUES ($1, 10, $2, true, false) RETURNING do_not_contact, is_synthetic', [syntheticId, 'Synthetic rehearsal']);
    check(inserted.rows[0].do_not_contact === true, 'synthetic prospect is forced DNC');
    const disposition = await db.query(`INSERT INTO call_dispositions (prospect_id, client_id, disposition, structured_notes, activity_result, next_action, suppression_state, lifecycle_result, is_synthetic, idempotency_key)
      VALUES ($1, 10, 'qualified', '{"summary":"Synthetic rehearsal only","next_step":"No real follow-up"}'::jsonb, 'decision_maker_conversation', 'none', 'suppressed', 'qualified', true, 'phase3f-synthetic-qualified') RETURNING id`, [syntheticId]);
    await applyProspectDisposition(db, { prospectId: syntheticId, clientId: 10, disposition: 'qualified' });
    await db.query("INSERT INTO setter_callbacks (client_id, prospect_id, source_disposition_id, due_at, is_synthetic) VALUES (10, $1, $2, now(), true)", [syntheticId, disposition.rows[0].id]);
    const synthetic = (await db.query('SELECT do_not_contact, is_synthetic, status FROM prospects WHERE id = $1', [syntheticId])).rows[0];
    check(synthetic.do_not_contact && synthetic.is_synthetic && synthetic.status === 'hot', 'synthetic setter flow stays suppressed while preserving lifecycle');
    const outboundEligible = (await db.query('SELECT count(*)::int AS count FROM prospects WHERE id = $1 AND NOT do_not_contact AND NOT is_synthetic', [syntheticId])).rows[0].count;
    check(outboundEligible === 0, 'synthetic flow creates no outbound-eligible prospect');
    const reportingEligible = (await db.query('SELECT count(*)::int AS count FROM call_dispositions WHERE client_id = 10 AND NOT is_synthetic')).rows[0].count;
    check(reportingEligible === 0, 'synthetic call is excluded from reporting candidates');

    await db.query("INSERT INTO setter_follow_up_drafts (client_id, prospect_id) VALUES (10, '30000000-0000-4000-8000-000000000030')");
    await db.query("UPDATE prospects SET do_not_contact = true WHERE id = '30000000-0000-4000-8000-000000000030' AND client_id = 10");
    const cancelled = (await db.query("SELECT status FROM setter_callbacks WHERE prospect_id = '30000000-0000-4000-8000-000000000030'")).rows[0].status;
    const dismissed = (await db.query("SELECT status FROM setter_follow_up_drafts WHERE prospect_id = '30000000-0000-4000-8000-000000000030'")).rows[0].status;
    check(cancelled === 'cancelled' && dismissed === 'dismissed', 'DNC cancels callbacks and dismisses pending drafts');
    const otherTenant = (await db.query("SELECT status FROM setter_callbacks WHERE prospect_id = '20000000-0000-4000-8000-000000000020'")).rows[0].status;
    check(otherTenant === 'pending', 'DNC cleanup is tenant-scoped');
    await assert.rejects(db.query("INSERT INTO setter_callbacks (client_id, prospect_id, due_at) VALUES (10, '10000000-0000-4000-8000-000000000010', now())"), { code: '23505' });
    report.checks.push({ name: 'one pending callback per tenant prospect', passed: true });
    const crossTenant = await db.query("UPDATE prospects SET setter_status = 'cross-tenant-attempt' WHERE id = '10000000-0000-4000-8000-000000000010' AND client_id = 20 RETURNING id");
    check(crossTenant.rowCount === 0, 'client-scoped access fails closed across tenants');
    report.schema.after_flow = await snapshot(db);
    report.row_counts.after_flow = await counts(db, report.schema.after_flow);

    await db.query('UPDATE clients SET setter_pipeline_v2_enabled = true WHERE id = 10');
    started = process.hrtime.bigint();
    await db.query(fs.readFileSync(rollbackPath, 'utf8'));
    report.timings_ms.rollback = elapsed(started);
    const rollbackFlag = (await db.query('SELECT setter_pipeline_v2_enabled FROM clients WHERE id = 10')).rows[0].setter_pipeline_v2_enabled;
    check(rollbackFlag === false, 'rollback restores legacy Pipeline selection');
    report.schema.after_rollback = await snapshot(db);
    report.schema.phase3d_after_rollback = phase3dObjectState(report.schema.after_rollback);
    report.row_counts.after_rollback = await counts(db, report.schema.after_rollback);
    check(allPhase3dObjectsPresent(report.schema.phase3d_after_rollback), 'rollback retains operational safety history by design');
    check(JSON.stringify(report.row_counts.after_rollback) === JSON.stringify(report.row_counts.after_flow), 'logical rollback preserves rehearsal data and callback history');

    started = process.hrtime.bigint();
    await db.query(fs.readFileSync(forwardPath, 'utf8'));
    report.timings_ms.forward_reapply = elapsed(started);
    const reapplyFlag = (await db.query('SELECT setter_pipeline_v2_enabled FROM clients WHERE id = 10')).rows[0].setter_pipeline_v2_enabled;
    check(reapplyFlag === false, 'forward migration is idempotent and does not enable Anchor');
    report.schema.after_reapply = await snapshot(db);
    report.schema.phase3d_after_reapply = phase3dObjectState(report.schema.after_reapply);
    report.row_counts.after_reapply = await counts(db, report.schema.after_reapply);
    check(allPhase3dObjectsPresent(report.schema.phase3d_after_reapply), 'forward reapply restores the expected Phase 3D schema');
    report.schema.diff = {
      added_tables: report.schema.after_forward.tables.filter(table => !report.schema.before.tables.some(before => before.table_name === table.table_name)),
      pre_migration_phase3d_objects: report.schema.phase3d_before,
      retained_after_logical_rollback: report.schema.phase3d_after_rollback,
      post_reapply_phase3d_objects: report.schema.phase3d_after_reapply,
    };
    report.status = 'passed';
  } finally {
    await db.end();
  }
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
}

if (require.main === module) {
  main().catch(error => {
    report.status = 'failed';
    report.error = error.stack || error.message;
    fs.mkdirSync(path.dirname(reportPath), { recursive: true });
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
    console.error(error.stack || error.message);
    process.exitCode = 1;
  });
}

module.exports = {
  allPhase3dObjectsPresent,
  counts,
  hasColumn,
  hasTable,
  phase3dObjectState,
  snapshot,
};
