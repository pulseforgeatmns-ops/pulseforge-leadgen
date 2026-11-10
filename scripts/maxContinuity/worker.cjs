'use strict';

async function runWorker(config) {
  // No application imports before the egress barrier. Never print thrown errors.
  const { installGuard } = require('./egress.cjs');
  const guard = installGuard({ host: config.host, port: config.port }, { blockSubprocess: true });
  const checks = [];
  const cleanups = [];
  let db, cleanupDb, schema;
  let passed = false;
  let cleanup = false;
  try {
    guard.selfTest(); checks.push('egress_denial');
    const { Pool } = require('pg');
    const options = { connectionString: config.connectionString, ssl: false,
      connectionTimeoutMillis: 3000, query_timeout: 5000, statement_timeout: 4000,
      idle_in_transaction_session_timeout: 5000, max: 4 };
    // Verify the dedicated database/role before any schema write.
    cleanupDb = new Pool(options);
    cleanupDb.on('error', () => {});
    if (config.requireRestrictedRole) {
      const { rows } = await cleanupDb.query(`SELECT current_database() AS database, current_user AS username,
        rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls,
        EXISTS (SELECT 1 FROM pg_auth_members WHERE member=pg_roles.oid) AS member
        FROM pg_roles WHERE rolname=current_user`);
      const role = rows[0];
      if (!role || role.database !== 'max_continuity_rehearsal' || role.username !== 'max_continuity_rehearsal' ||
          ['rolsuper','rolcreatedb','rolcreaterole','rolreplication','rolbypassrls','member'].some(key => role[key])) {
        throw new Error('staging_role_rejected');
      }
    }
    schema = 'max_continuity_' + require('node:crypto').randomBytes(12).toString('hex');
    await cleanupDb.query(`CREATE SCHEMA "${schema}"`);
    db = new Pool({ ...options, options: `-c search_path=${schema}` });
    db.on('error', () => {});
    // Only the two fixture tables are created, inside this run's schema. No
    // migration runner, shared schema, production tables, or workers are loaded.
    await db.query(`CREATE TABLE max_conversation_context (
      tenant_id text NOT NULL, session_id text NOT NULL, revision text NOT NULL,
      context jsonb NOT NULL CHECK (jsonb_typeof(context)='object' AND octet_length(context::text)<=16384),
      expires_at timestamptz NOT NULL, PRIMARY KEY (tenant_id, session_id))`);
    const { runScenario } = require('./scenario.cjs');
    await runScenario({ db, guard, t: { after: fn => cleanups.push(fn) }, checkpoint: label => checks.push(label) });
    guard.assertClean(); passed = true;
  } catch (_) {
    // Assertion errors and database errors can contain cookies, URLs or content.
    passed = false;
  } finally {
    let cleanupFailed = false;
    for (const fn of cleanups.reverse()) {
      try { await fn(); } catch (_) { cleanupFailed = true; }
    }
    try { if (db) await db.end().catch(() => {}); } catch (_) { cleanupFailed = true; }
    try { if (schema) await cleanupDb.query(`DROP SCHEMA "${schema}" CASCADE`); } catch (_) { cleanupFailed = true; }
    try { if (cleanupDb) await cleanupDb.end(); } catch (_) { cleanupFailed = true; }
    try { guard.assertClean(); } catch (_) { cleanupFailed = true; }
    cleanup = !cleanupFailed;
    if (cleanupFailed) passed = false;
    // Keep denial installed until child exit, including any late async activity.
  }
  return { version: 1, scope: 'isolated-staging-rehearsal', result: passed && cleanup ? 'pass' : 'fail',
    checks, cleanup: cleanup ? 'complete' : 'failed', productionContinuity: 'unchanged',
    runtimeReplacement: 'fresh-objects', authenticationStore: 'fixture-memory', missionReferences: 'synthetic-sql' };
}
module.exports = { runWorker };
