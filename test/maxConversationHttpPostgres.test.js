'use strict';
process.env.DATABASE_URL = 'postgresql://127.0.0.1:1/test_only';
delete process.env.ANTHROPIC_API_KEY;
const test = require('node:test');
const fs = require('node:fs');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { installGuard } = require('../scripts/maxContinuity/egress.cjs');

test('authenticated HTTP continuity survives runtime replacement using disposable PostgreSQL', { timeout: 60000 }, async t => {
  const pg = await startDisposablePostgres('max-http-context-');
  const guard = installGuard({ host: '127.0.0.1', port: pg.port });
  const { runScenario } = require('../scripts/maxContinuity/scenario.cjs');
  const db = new Pool({ connectionString: pg.connectionString, connectionTimeoutMillis: 500, query_timeout: 3000 });
  db.on('error', () => {});
  const cleanups = [];
  t.after(async () => {
    try { for (const fn of cleanups.reverse()) await fn(); }
    finally { await db.end().catch(() => {}); await pg.stop(); guard.restore(); }
  });
  await db.query(fs.readFileSync('migrations/2026-11-09-max-conversation-context.sql', 'utf8'));
  await runScenario({ db, t: { after: fn => cleanups.push(fn) }, guard });
});
