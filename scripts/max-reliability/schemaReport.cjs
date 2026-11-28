#!/usr/bin/env node
'use strict';

/**
 * MAX-P0-003/P0-004 — read-only database schema and migration report.
 *
 * Run IN-NETWORK only (Render Shell on the API service, which already holds
 * DATABASE_URL on the private network). Never open the database to the
 * internet to run this.
 *
 *   node scripts/max-reliability/schemaReport.cjs > /tmp/schema-report.json
 *
 * Guarantees: one READ ONLY transaction (the server rejects any write), a
 * statement timeout, SELECTs against catalogs plus count/max aggregates only.
 * Output holds no row content: table names, counts, tenant ids/names, and
 * timestamps.
 */

const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

const REPO = path.join(__dirname, '..', '..');

/** Tables created by repository migrations (CREATE TABLE … in migrations/*.sql). */
function expectedFromMigrations() {
  const dir = path.join(REPO, 'migrations');
  const out = new Map();
  for (const file of fs.readdirSync(dir).filter((f) => f.endsWith('.sql')).sort()) {
    const sql = fs.readFileSync(path.join(dir, file), 'utf8');
    for (const m of sql.matchAll(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:public\.)?"?([a-z_][a-z0-9_]*)"?/gi)) {
      if (!out.has(m[1].toLowerCase())) out.set(m[1].toLowerCase(), file);
    }
  }
  return out;
}

/** Tables Max/AMO and MAX-P0-003 need at runtime. */
const MAX_CORE_TABLES = [
  'clients', 'users', 'session', 'prospects', 'companies', 'agent_log', 'agent_actions',
  'acquisition_missions', 'acquisition_mission_events', 'acquisition_mission_contributions',
  'acquisition_mission_observations', 'acquisition_mission_outcomes', 'acquisition_mission_execution_audit',
  'acquisition_mission_outbound_executions', 'max_conversation_context',
  'max_external_action_claims', 'max_request_receipts', 'max_reliability_audit',
  'max_durable_execution_tasks', 'max_durable_execution_task_events',
];

async function main() {
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is not set (run inside the service shell).');
  const ssl = /sslmode=require|render\.com/.test(process.env.DATABASE_URL) ? { rejectUnauthorized: false } : undefined;
  const client = new Client({ connectionString: process.env.DATABASE_URL, ssl, application_name: 'max-p0-003-schema-report' });
  await client.connect();
  const q = async (sql, params) => (await client.query(sql, params)).rows;
  try {
    await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    await client.query("SET LOCAL statement_timeout = '20s'");
    const [server] = await q(`SELECT current_database() AS database, current_user AS role, version() AS version,
      pg_database_size(current_database()) AS bytes, now() AS observed_at,
      current_setting('transaction_read_only') AS read_only`);
    const catalog = (await q(`SELECT
        (SELECT count(*) FROM pg_tables WHERE schemaname = 'public')::int AS tables,
        (SELECT count(*) FROM pg_views WHERE schemaname = 'public')::int AS views,
        (SELECT count(*) FROM pg_indexes WHERE schemaname = 'public')::int AS indexes,
        (SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = 'public' AND NOT t.tgisinternal)::int AS triggers,
        (SELECT count(*) FROM information_schema.table_constraints WHERE table_schema = 'public')::int AS constraints,
        (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public')::int AS functions,
        (SELECT count(*) FROM pg_sequences WHERE schemaname = 'public')::int AS sequences`))[0];
    const tables = await q(`SELECT c.relname AS name, c.reltuples::bigint AS estimated_rows
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND c.relkind IN ('r','p') ORDER BY c.relname`);
    const present = new Set(tables.map((t) => t.name));
    const expected = expectedFromMigrations();
    const missingFromMigrations = [...expected.entries()].filter(([t]) => !present.has(t)).map(([table, file]) => ({ table, file }));
    const maxCore = MAX_CORE_TABLES.map((t) => ({ table: t, present: present.has(t) }));
    const otherSchemas = await q(`SELECT nspname AS schema FROM pg_namespace
      WHERE nspname NOT IN ('public','information_schema') AND nspname NOT LIKE 'pg\\_%' ORDER BY 1`);
    const signalTables = tables.filter((t) => /^signal_/.test(t.name)).map((t) => t.name);
    const packageATablesPresent = present.has('max_durable_execution_tasks') && present.has('max_durable_execution_task_events');
    const packageAInvariants = packageATablesPresent ? (await q(`WITH attrs AS (
      SELECT (SELECT attnum FROM pg_attribute WHERE attrelid='max_durable_execution_tasks'::regclass AND attname='tenant_id' AND NOT attisdropped) AS root_tenant,
        (SELECT attnum FROM pg_attribute WHERE attrelid='max_durable_execution_tasks'::regclass AND attname='objective_id' AND NOT attisdropped) AS root_objective,
        (SELECT attnum FROM pg_attribute WHERE attrelid='max_durable_execution_task_events'::regclass AND attname='tenant_id' AND NOT attisdropped) AS wake_tenant,
        (SELECT attnum FROM pg_attribute WHERE attrelid='max_durable_execution_task_events'::regclass AND attname='task_id' AND NOT attisdropped) AS wake_task
    ), idx AS (
      SELECT c.relname, i.indisunique, i.indisvalid, i.indisready, i.indnkeyatts::int AS key_arity, i.indkey::text AS key_vector,
        pg_get_expr(i.indexprs, i.indrelid) AS expressions, pg_get_expr(i.indpred, i.indrelid) AS predicate
      FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
      WHERE c.relname IN ('max_durable_execution_tasks_one_objective_root','max_durable_execution_task_events_one_objective_wake')
    ) SELECT
      coalesce((SELECT indisunique AND indisvalid AND indisready AND key_arity=2 AND key_vector=format('%s %s', root_tenant, root_objective)
        AND regexp_replace(regexp_replace(lower(predicate), '::[a-z_ ]+', '', 'g'), '[^a-z0-9_]', '', 'g')='mission_idisnullandobjective_idisnotnullandspecialistmax_objectiveorexecutionscoperoleroot'
        FROM idx, attrs WHERE relname='max_durable_execution_tasks_one_objective_root'), false) AS canonical_root,
      coalesce((SELECT indisunique AND indisvalid AND indisready AND key_arity=3 AND key_vector=format('%s %s 0', wake_tenant, wake_task)
        AND regexp_replace(regexp_replace(lower(expressions), '::[a-z_ ]+', '', 'g'), '[^a-z0-9_]', '', 'g')='detailwakeidentity'
        AND regexp_replace(regexp_replace(lower(predicate), '::[a-z_ ]+', '', 'g'), '[^a-z0-9_]', '', 'g')='eventobjective_wakeanddetailwakeidentity'
        FROM idx, attrs WHERE relname='max_durable_execution_task_events_one_objective_wake'), false) AS wake_deduplication,
      coalesce((SELECT jsonb_build_object('unique', indisunique, 'valid', indisvalid, 'ready', indisready, 'keyArity', key_arity, 'keyVector', key_vector, 'expressions', expressions, 'predicate', predicate)
        FROM idx WHERE relname='max_durable_execution_tasks_one_objective_root'), '{}'::jsonb) AS canonical_root_evidence,
      coalesce((SELECT jsonb_build_object('unique', indisunique, 'valid', indisvalid, 'ready', indisready, 'keyArity', key_arity, 'keyVector', key_vector, 'expressions', expressions, 'predicate', predicate)
        FROM idx WHERE relname='max_durable_execution_task_events_one_objective_wake'), '{}'::jsonb) AS wake_deduplication_evidence`))[0]
      : { canonical_root: false, wake_deduplication: false, canonical_root_evidence: {}, wake_deduplication_evidence: {} };
    const packageAMigrationPreflight = packageATablesPresent ? (await q(`SELECT
      (SELECT count(*)::int FROM (SELECT 1 FROM max_durable_execution_tasks
        WHERE mission_id IS NULL AND objective_id IS NOT NULL AND (specialist='max_objective' OR execution_scope->>'role'='root')
        GROUP BY tenant_id, objective_id HAVING count(*) > 1) roots) AS duplicate_canonical_roots,
      (SELECT count(*)::int FROM (SELECT 1 FROM max_durable_execution_task_events
        WHERE event='objective_wake' AND detail ? 'wakeIdentity'
        GROUP BY tenant_id, task_id, detail->>'wakeIdentity' HAVING count(*) > 1) wakes) AS duplicate_objective_wakes`))[0]
      : { duplicate_canonical_roots: null, duplicate_objective_wakes: null };

    // Tenant and operational continuity (counts and timestamps only).
    const tenants = present.has('clients')
      ? await q(`SELECT c.id, c.name, c.active,
          (SELECT count(*) FROM prospects p WHERE p.client_id = c.id)::int AS prospects,
          (SELECT max(p.created_at) FROM prospects p WHERE p.client_id = c.id) AS last_prospect_at
        FROM clients c ORDER BY c.id`).catch((err) => [{ error: err.code || err.message }])
      : [];
    const missions = present.has('acquisition_missions')
      ? await q(`SELECT tenant_id, count(*)::int AS missions, max(updated_at) AS last_updated
          FROM acquisition_missions GROUP BY tenant_id ORDER BY tenant_id`)
      : [];
    const freshness = {};
    for (const [table, column] of [['agent_log', 'created_at'], ['touchpoints', 'created_at'], ['session', 'expire'],
      ['acquisition_mission_events', 'at'], ['acquisition_mission_outbound_executions', 'updated_at']]) {
      if (!present.has(table)) { freshness[table] = 'absent'; continue; }
      try {
        freshness[table] = (await q(`SELECT max(${column}) AS latest FROM ${table}`))[0].latest;
      } catch (err) {
        freshness[table] = `unreadable:${err.code || 'error'}`;
      }
    }
    await client.query('ROLLBACK');
    process.stdout.write(`${JSON.stringify({
      kind: 'max_reliability_schema_report',
      server,
      catalog,
      tableCount: tables.length,
      expectedFromMigrations: expected.size,
      missingFromMigrations,
      maxCore,
      reliabilityCoreInstalled: ['max_external_action_claims', 'max_request_receipts', 'max_reliability_audit'].every((t) => present.has(t)),
      durableExecutionOwnershipInstalled: ['max_durable_execution_tasks', 'max_durable_execution_task_events'].every((t) => present.has(t)),
      packageA: {
        canonicalRootInvariant: packageAInvariants.canonical_root === true,
        wakeDeduplicationInvariant: packageAInvariants.wake_deduplication === true,
        canonicalRootEvidence: packageAInvariants.canonical_root_evidence,
        wakeDeduplicationEvidence: packageAInvariants.wake_deduplication_evidence,
        migrationPreflight: {
          duplicateCanonicalRoots: packageAMigrationPreflight.duplicate_canonical_roots,
          duplicateObjectiveWakes: packageAMigrationPreflight.duplicate_objective_wakes,
          ready: packageATablesPresent && Number(packageAMigrationPreflight.duplicate_canonical_roots) === 0
            && Number(packageAMigrationPreflight.duplicate_objective_wakes) === 0,
        },
      },
      otherSchemas: otherSchemas.map((r) => r.schema),
      signalTables,
      tenants,
      missionsByTenant: missions,
      freshness,
      tables,
    }, null, 2)}\n`);
  } finally {
    await client.end().catch(() => {});
  }
}

main().catch((err) => {
  console.error(`[schema-report] ${err.code || 'error'}: ${err.message}`);
  process.exit(1);
});
