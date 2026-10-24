'use strict';
// Explicit future-CI opt-in only. Uses a disposable local cluster and never
// reads DATABASE_URL/TEST_DATABASE_URL. This test was not run during preparation.
const test = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID, createHash } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { createBusinessEvidenceCursorStore, CURSOR_VERSION, LOCK_NAMESPACE } = require('../services/scoutBusinessEvidenceCursor');

test('native PostgreSQL cursor CAS, scope isolation, tied timestamps, restart and max1 pool', {
  skip: process.env.SCOUT_EVIDENCE_CURSOR_REQUIRE_POSTGRES !== 'true'
    ? 'Requires explicit disposable-PostgreSQL authorization and SCOUT_EVIDENCE_CURSOR_REQUIRE_POSTGRES=true' : false,
  timeout: 60000,
}, async t => {
  const pg = await startDisposablePostgres('scout-evidence-cursor-');
  const pool = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 1 });
  const lockerPool = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 1 });
  t.after(async () => {
    try { await closePostgresPool(lockerPool); } finally { await closePostgresFixture(pool, pg); }
  });
  // Exercise the existing event-table DDL, without creating unrelated tables.
  const migration = fs.readFileSync(path.join(__dirname, '../migrations/2026-09-18-anchor-daily-outbound.sql'), 'utf8');
  const eventDdl = migration.match(/CREATE TABLE IF NOT EXISTS acquisition_outbound_events \([\s\S]*?\n\);/);
  assert.ok(eventDdl, 'existing event-table migration must remain available');
  await pool.query(eventDdl[0]);
  const fingerprint = randomUUID();
  const candidates = Array.from({ length: 41 }, (_, i) => ({ website: `https://fixture${i}.example` }));
  const cursor = (tenantId = '10', mission = fingerprint) => createBusinessEvidenceCursorStore({ pool, tenantId, fingerprint: mission });
  const run = (store = cursor(), after = async () => {}) => store.withPass(async selection => {
    selection.select(candidates);
    for (const company of candidates) selection.take(company);
    // Would deadlock max1 if callback ran with a pinned cursor connection.
    await pool.query('SELECT 1');
    await after();
    return { checkpointComplete: false };
  });
  assert.equal((await run()).progression.sequence, 1);
  assert.equal((await run()).progression.sequence, 2);
  assert.equal((await run(cursor('13'))).progression.sequence, 1);
  assert.equal((await run(cursor('10', randomUUID()))).progression.sequence, 1);
  await pool.query(`INSERT INTO acquisition_outbound_events(id,tenant_id,event_type,payload)
    SELECT 'alternate-version','10',event_type,payload || '{"cursorVersion":"future-version","sequence":999}'::jsonb
    FROM acquisition_outbound_events WHERE tenant_id='10' AND payload->>'searchFingerprint'=$1 LIMIT 1`, [fingerprint]);
  await pool.query("UPDATE acquisition_outbound_events SET created_at='2026-10-18T00:00:00Z'");
  let releaseSlow; let ready;
  const started = new Promise(resolve => { ready = resolve; });
  const blocked = new Promise(resolve => { releaseSlow = resolve; });
  const slow = run(cursor(), async () => { ready(); await blocked; });
  const stale = assert.rejects(slow, /snapshot is stale/);
  await started;
  assert.equal((await run()).progression.sequence, 3);
  assert.equal((await run()).progression.sequence, 4);
  releaseSlow(); await stale;
  assert.equal((await run()).progression.sequence, 5);
  const { rows } = await pool.query(`SELECT payload FROM acquisition_outbound_events
    WHERE tenant_id='10' AND payload->>'searchFingerprint'=$1
      AND payload->>'cursorVersion'='scout-business-evidence-v1' ORDER BY (payload->>'sequence')::int`, [fingerprint]);
  assert.deepEqual(rows.map(row => row.payload.sequence), [1, 2, 3, 4, 5]);
  assert.deepEqual(rows.map(row => row.payload.previousSequence), [0, 1, 2, 3, 4]);
  const before = Number((await pool.query('SELECT count(*) AS n FROM acquisition_outbound_events')).rows[0].n);
  await assert.rejects(run(cursor(), () => { throw Error('interrupted persistence'); }), /interrupted persistence/);
  assert.equal(Number((await pool.query('SELECT count(*) AS n FROM acquisition_outbound_events')).rows[0].n), before);
  // A failed confirmation must roll back its otherwise successful INSERT.
  const unconfirmedPool = { query: (...args) => pool.query(...args), connect: async () => {
    const db = await pool.connect();
    return { release: broken => db.release(broken), query: async (sql, args) => /WHERE id=\$1/.test(sql)
      ? { rows: [] } : db.query(sql, args) };
  } };
  const unconfirmed = createBusinessEvidenceCursorStore({ pool: unconfirmedPool, tenantId: '10', fingerprint });
  await assert.rejects(run(unconfirmed), /not confirmed/);
  assert.equal(Number((await pool.query('SELECT count(*) AS n FROM acquisition_outbound_events')).rows[0].n), before);
  assert.equal((await run()).progression.sequence, 6);
  // A separate connection really holds the same scope lock. The normal cursor
  // pool remains max1; contention must return busy, not append or wait forever.
  const scopeHash = createHash('sha256').update(JSON.stringify(['10', fingerprint, CURSOR_VERSION])).digest();
  const holder = await lockerPool.connect();
  const beforeBusy = Number((await pool.query('SELECT count(*) AS n FROM acquisition_outbound_events')).rows[0].n);
  try {
    await holder.query('BEGIN');
    await holder.query('SELECT pg_advisory_xact_lock($1,$2)', [LOCK_NAMESPACE, scopeHash.readInt32BE()]);
    await assert.rejects(run(), error => {
      assert.equal(error.cursorCode, 'busy');
      assert.equal(error.evidenceProgression.advanced, false);
      return true;
    });
    assert.equal(Number((await pool.query('SELECT count(*) AS n FROM acquisition_outbound_events')).rows[0].n), beforeBusy);
  } finally {
    try { await holder.query('ROLLBACK'); } finally { holder.release(); }
  }
  assert.equal((await run()).progression.sequence, 7);
});
