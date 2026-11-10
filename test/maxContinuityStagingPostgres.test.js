'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { runIsolated } = require('../scripts/maxContinuity/run.cjs');

test('isolated staging worker runs HTTP recovery and cleans only its schema on real PostgreSQL', { timeout: 110000 }, async t => {
  const pg = await startDisposablePostgres('max-staging-');
  const admin = new Pool({ connectionString: pg.connectionString });
  let db;
  t.after(async () => { if (db) await db.end(); await admin.end(); await pg.stop(); });
  await admin.query("CREATE ROLE max_continuity_rehearsal LOGIN PASSWORD 'ci-fixture-only' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS");
  await admin.query('CREATE DATABASE max_continuity_rehearsal OWNER max_continuity_rehearsal');
  const config = { connectionString: `postgresql://max_continuity_rehearsal:ci-fixture-only@127.0.0.1:${pg.port}/max_continuity_rehearsal`,
    host: '127.0.0.1', port: pg.port, requireRestrictedRole: true };
  const fixture = path.join(__dirname, 'helpers/maxContinuityWorkerFixture.cjs');
  db = new Pool({ connectionString: config.connectionString });
  await db.query('CREATE TABLE public.cleanup_sentinel (value text)');
  await db.query("INSERT INTO public.cleanup_sentinel VALUES ('untouched')");
  const report = await runIsolated(config, fixture);
  if (process.env.CI === 'true') {
    const fs = require('node:fs');
    fs.mkdirSync('artifacts/max-continuity-staging', { recursive: true });
    fs.writeFileSync('artifacts/max-continuity-staging/ci-report.json', JSON.stringify({ evidenceKind: 'ci-disposable-postgres', ...report }, null, 2) + '\n');
  }
  assert.equal(report.result, 'pass', JSON.stringify(report));
  assert.equal(report.cleanup, 'complete');
  assert.deepEqual(report.checks, ['egress_denial','authenticated_http','durable_projection',
    'concurrent_recovery_and_tenant_isolation','expired','corrupt','missing-mission','database_outage_and_no_authority']);
  assert.doesNotMatch(JSON.stringify(report), /postgresql|ci-fixture|SECRET|cookie|password/i);
  assert.deepEqual((await db.query('SELECT * FROM public.cleanup_sentinel')).rows, [{ value: 'untouched' }]);
  assert.equal((await db.query("SELECT count(*)::int AS n FROM pg_namespace WHERE nspname LIKE 'max_continuity_%'")).rows[0].n, 0);
  // Reject an admin role before any fixture DDL, with no raw credential output.
  const rejected = await runIsolated({ ...config, connectionString: pg.connectionString.replace(/\/postgres$/, '/max_continuity_rehearsal') }, fixture);
  assert.equal(rejected.result, 'fail');
  assert.deepEqual(rejected.checks, ['egress_denial']);
  assert.equal(rejected.cleanup, 'complete');
  assert.equal((await db.query("SELECT count(*)::int AS n FROM pg_namespace WHERE nspname LIKE 'max_continuity_%'")).rows[0].n, 0);
  // Fixed JSON is suitable as CI evidence; no raw application logs leave the child.
  console.log('MAX_CONTINUITY_REHEARSAL_EVIDENCE', JSON.stringify(report));
});
