'use strict';
const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { randomUUID } = require('node:crypto');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { applyCanonicalAoFixture } = require('./helpers/canonicalAoPostgres');

function serviceWith(db) {
  const filename = path.join(__dirname, '../services/maxAttentionService.js');
  const module = { exports: {} };
  const dependencies = {
    '../db': db,
    '../packages/max/attention': require('../packages/max/attention'),
    '../packages/max/stateIngestion': require('../packages/max/stateIngestion'),
    '../packages/max/decisionExecution': require('../packages/max/decisionExecution'),
  };
  vm.compileFunction(fs.readFileSync(filename, 'utf8'), ['module', 'require'], { filename })(module, name => {
    assert.ok(Object.hasOwn(dependencies, name), `Unexpected dependency ${name}`);
    return dependencies[name];
  });
  return module.exports;
}

test('scheduler service checks readiness without migrations and respects bounded execution', { timeout: 60000 }, async t => {
  const instance = await startDisposablePostgres('max-scheduler-service-');
  const db = createTrackedPostgresPool({ connectionString: instance.connectionString });
  t.after(() => closePostgresFixture(db, instance));
  await applyCanonicalAoFixture(db);
  const service = serviceWith(db);
  const counts = async () => (await db.query(`SELECT
    (SELECT count(*)::int FROM max_attention_scheduler_runs) AS runs,
    (SELECT count(*)::int FROM max_operational_decisions) AS decisions,
    (SELECT count(*)::int FROM max_ao_follow_up_tasks) AS tasks`)).rows[0];

  await t.test('missing table fails before writes and never recreates it', async () => {
    const client = await db.connect();
    try {
      await client.query('BEGIN');
      await client.query('ALTER TABLE max_attention_items RENAME TO unavailable_attention_items');
      await assert.rejects(service.runAttentionScheduler(1, { db: client, limit: 1 }),
        { code: 'MAX_ATTENTION_SCHEMA_NOT_READY' });
      // The failing SELECT aborts the transaction; rolling back restores only our fixture change.
      await client.query('ROLLBACK');
      assert.deepEqual(await counts(), { runs: 0, decisions: 0, tasks: 0 });
    } finally { client.release(); }
  });

  await t.test('missing claim column fails closed without auto-altering schema', async () => {
    const client = await db.connect();
    try {
      await client.query('BEGIN');
      await client.query('ALTER TABLE max_attention_items RENAME COLUMN claim_token TO unavailable_claim_token');
      await assert.rejects(service.runAttentionScheduler(1, { db: client, limit: 1 }),
        { code: 'MAX_ATTENTION_SCHEMA_NOT_READY' });
      await client.query('ROLLBACK');
      assert.deepEqual(await counts(), { runs: 0, decisions: 0, tasks: 0 });
    } finally { client.release(); }
  });

  await t.test('missing heartbeat seed is not repaired implicitly', async () => {
    await db.query('DELETE FROM max_attention_heartbeats');
    await assert.rejects(service.runAttentionScheduler(1, { limit: 1 }),
      { code: 'MAX_ATTENTION_SCHEMA_NOT_READY' });
    assert.equal((await db.query('SELECT count(*)::int AS count FROM max_attention_heartbeats')).rows[0].count, 0);
    assert.deepEqual(await counts(), { runs: 0, decisions: 0, tasks: 0 });
    await db.query("INSERT INTO max_attention_heartbeats(id) VALUES ('global')");
  });

  await t.test('ready schema executes with no store init, DDL or client/user backfill', async () => {
    const types = [require('../packages/max/attention').PostgresAttentionStore,
      require('../packages/max/stateIngestion').PostgresStateStore,
      require('../packages/max/decisionExecution').PostgresDecisionStore];
    const original = types.map(Type => Type.prototype.init);
    for (const Type of types) Type.prototype.init = async () => { throw new Error('Forbidden store init'); };
    const before = (await db.query("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,column_name")).rows;
    try {
      const result = await service.runAttentionScheduler(1, { limit: 1 });
      assert.equal(result.claimed, 0);
      assert.equal(result.failed, 0);
      assert.deepEqual(await counts(), { runs: 1, decisions: 0, tasks: 0 });
      assert.deepEqual((await db.query('SELECT * FROM users ORDER BY id')).rows, [{ id: 10 }, { id: 11 }]);
      assert.deepEqual((await db.query('SELECT * FROM clients ORDER BY id')).rows, [{ id: 1 }, { id: 2 }]);
      assert.deepEqual((await db.query("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,column_name")).rows, before);
    } finally { types.forEach((Type, index) => { Type.prototype.init = original[index]; }); }
  });

  await t.test('library limit0 claims nothing even with due work; limit1 evaluates one tenant item', async () => {
    const { PostgresStateStore } = require('../packages/max/stateIngestion');
    const { PostgresAttentionStore } = require('../packages/max/attention');
    for (const clientId of [1, 1, 2]) {
      const prospectId = randomUUID();
      await db.query('INSERT INTO prospects(id,client_id,assigned_ao_id,status) VALUES ($1,$2,10,$3)', [prospectId, clientId, 'warm']);
      const exp = await new PostgresStateStore(db, { clientId }).createExpectation({ client_id: clientId,
        prospect_id: prospectId, ao_id: 10, expectation_type: 'inbound_call', status: 'OVERDUE',
        expected_window: { ends_at: '2026-10-01T00:00:00Z' } });
      await new PostgresAttentionStore(db, { clientId }).createAttention({ id: randomUUID(), client_id: clientId,
        subject_type: 'expectation', subject_id: exp.id, reason: 'Disposable test', status: 'WAITING',
        next_review_at: '2026-10-01T00:00:00Z', review_trigger: 'TIME', audience_tier: 'WAITING_SILENT', dedup_fingerprint: randomUUID() });
    }
    const itemsBefore = (await db.query('SELECT * FROM max_attention_items ORDER BY id')).rows;
    const empty = await service.runAttentionScheduler(1, { limit: 0, now: new Date('2026-10-20T00:00:00Z') });
    assert.equal(empty.claimed, 0); assert.equal(empty.evaluated, 0); assert.equal(empty.failed, 0);
    assert.deepEqual((await db.query('SELECT * FROM max_attention_items ORDER BY id')).rows, itemsBefore);
    assert.deepEqual(await counts(), { runs: 2, decisions: 0, tasks: 0 });
    assert.equal((await db.query("SELECT last_run_id FROM max_attention_heartbeats WHERE id='global'")).rows[0].last_run_id, empty.run_id);
    const result = await service.runAttentionScheduler(1, { limit: 1, now: new Date('2026-10-20T00:00:00Z') });
    assert.equal(result.claimed, 1); assert.equal(result.evaluated, 1); assert.equal(result.failed, 0);
    assert.deepEqual(await counts(), { runs: 3, decisions: 1, tasks: 1 });
    assert.equal((await db.query('SELECT count(*)::int AS count FROM max_attention_items WHERE last_reviewed_at IS NULL AND claim_token IS NULL')).rows[0].count, 2);
    assert.equal((await db.query('SELECT count(*)::int AS count FROM max_operational_decisions WHERE client_id=2')).rows[0].count, 0);
  });
});
