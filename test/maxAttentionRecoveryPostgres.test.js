'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { randomUUID } = require('node:crypto');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { applyCanonicalAoFixture } = require('./helpers/canonicalAoPostgres');
const { PostgresStateStore } = require('../packages/max/stateIngestion/store/postgresStore');
const { PostgresDecisionStore } = require('../packages/max/decisionExecution/store/postgresStore');
const { PostgresAttentionStore } = require('../packages/max/attention/store/postgresStore');
const { runAttentionCycle, evaluateAttentionItem } = require('../packages/max/attention/scheduler');

// Non-skipping CI gate. Only this harness's disposable local database is used;
// never DATABASE_URL, live routes, health/init endpoints, or provider credentials.
test('PostgreSQL attention lifecycle, fencing and atomic restart recovery', { timeout: 90000 }, async t => {
  const instance = await startDisposablePostgres('max-attention-');
  const db = createTrackedPostgresPool({ connectionString: instance.connectionString });
  // pool.end() can resolve before discarded/idle client sockets physically close.
  // Do not let synchronous pg_ctl stop manufacture late backend-termination errors.
  t.after(() => closePostgresFixture(db, instance));
  await applyCanonicalAoFixture(db);
  const now = new Date('2026-10-13T12:00:00Z');
  function stores(pool = db, clientId = 1) {
    return { clientId, now, stateStore: new PostgresStateStore(pool, { clientId }),
      decisionStore: new PostgresDecisionStore(pool, { clientId }),
      attentionStore: new PostgresAttentionStore(pool, { clientId }) };
  }
  async function reset() {
    await db.query(`TRUNCATE max_attention_items, max_attention_scheduler_runs,
      max_attention_heartbeats, max_operational_decisions, max_operational_action_intents,
      max_ao_follow_up_tasks, max_open_expectations CASCADE;
      INSERT INTO max_attention_heartbeats (id) VALUES ('global')`);
  }
  async function seed() {
    const prospectId = randomUUID();
    await db.query(`INSERT INTO prospects (id, client_id, assigned_ao_id, status, acquisition_metadata)
      VALUES ($1,1,10,'warm','{"maxStateIngestion":{"relationship_active":true}}')`, [prospectId]);
    const expectation = await stores().stateStore.createExpectation({ client_id: 1, prospect_id: prospectId,
      ao_id: 10, expectation_type: 'inbound_call', status: 'OVERDUE',
      expected_window: { ends_at: '2026-10-01T00:00:00Z' },
      source_evidence: { account_name: 'Local fixture', ao_name: 'AO fixture' } });
    const item = { id: `test-${randomUUID()}`, client_id: 1, subject_type: 'expectation',
      subject_id: expectation.id, reason: 'Local test review', status: 'WAITING',
      next_review_at: now.toISOString(), review_trigger: 'TIME', audience_tier: 'WAITING_SILENT',
      dedup_fingerprint: randomUUID() };
    await stores().attentionStore.createAttention(item);
    return item;
  }
  async function counts() {
    const tables = ['max_operational_decisions', 'max_operational_action_intents', 'max_ao_follow_up_tasks'];
    return Promise.all(tables.map(async table =>
      (await db.query(`SELECT count(*)::int AS count FROM ${table}`)).rows[0].count));
  }
  async function expire(id) {
    await db.query("UPDATE max_attention_items SET claimed_at = clock_timestamp() - interval '1 hour' WHERE id=$1", [id]);
  }

  await t.test('empty cycle has one completed record, advances heartbeat, and terminal retries are immutable', async () => {
    const context = stores();
    const result = await runAttentionCycle(context);
    const runs = (await db.query('SELECT * FROM max_attention_scheduler_runs')).rows;
    assert.equal(runs.length, 1);
    assert.equal(runs[0].id, result.run_id);
    assert.equal(runs[0].status, 'completed');
    const hb = await context.attentionStore.getHeartbeat();
    assert.ok(hb.last_successful_cycle_at);
    assert.equal(hb.last_run_id, result.run_id);
    await context.attentionStore.finishSchedulerRun({ ...runs[0], status: 'failed' });
    await context.attentionStore.recordSchedulerRun({ ...runs[0], status: 'running' });
    assert.equal((await context.attentionStore.getHeartbeat()).consecutive_failures, 0);
    assert.equal((await db.query('SELECT status FROM max_attention_scheduler_runs')).rows[0].status, 'completed');
  });

  await t.test('cycle failure persists one failed run and prior successful heartbeat', async () => {
    const context = stores();
    const before = await context.attentionStore.getHeartbeat();
    context.attentionStore.claimDueItems = async () => { throw new Error('injected claim failure'); };
    await assert.rejects(runAttentionCycle(context), /injected claim failure/);
    const failed = (await db.query("SELECT * FROM max_attention_scheduler_runs WHERE status='failed'")).rows;
    assert.equal(failed.length, 1);
    assert.equal(failed[0].error_message, 'injected claim failure');
    const hb = await context.attentionStore.getHeartbeat();
    assert.equal(hb.last_successful_cycle_at, before.last_successful_cycle_at);
    assert.equal(hb.consecutive_failures, 1);
  });

  await t.test('two workers produce one owner; restart reclaims an abandoned lease; old token cannot act', async () => {
    await reset();
    const item = await seed();
    const first = stores();
    const second = stores();
    const claims = await Promise.all([first, second].map((context, index) =>
      context.attentionStore.claimDueItems({ clientId: 1, now, limit: 1, claimToken: `worker-${index}` })));
    assert.equal(claims.flat().length, 1);
    const old = claims.flat()[0];
    await expire(item.id);
    await assert.rejects(evaluateAttentionItem({ ...first, item: old, claimToken: old.claim_token }),
      { code: 'ATTENTION_CLAIM_LOST' });
    assert.deepEqual(await counts(), [0, 0, 0]);
    const restarted = stores();
    const [current] = await restarted.attentionStore.claimDueItems({ clientId: 1, now, limit: 1, claimToken: 'restart' });
    assert.equal(current.id, item.id);
    assert.equal(await first.attentionStore.releaseClaim(item.id, { claimToken: old.claim_token,
      patch: { status: 'RESOLVED', next_review_at: null } }), null);
    await assert.rejects(evaluateAttentionItem({ ...first, item: old, claimToken: old.claim_token }),
      { code: 'ATTENTION_CLAIM_LOST' });
    const result = await evaluateAttentionItem({ ...restarted, item: current, claimToken: 'restart' });
    assert.equal(result.decision.execution_status, 'EXECUTED');
    assert.equal(result.decision.verification_status, 'VERIFIED');
    assert.deepEqual(await counts(), [1, 1, 1]);
    assert.equal((await restarted.attentionStore.findById(item.id)).claim_token, null);
  });

  await t.test('active expired transaction cannot be reclaimed; rollback then releases lock for retry', async () => {
    await reset();
    const item = await seed();
    const store = stores().attentionStore;
    await store.claimDueItems({ clientId: 1, now, limit: 1, claimToken: 'active' });
    await assert.rejects(store.withClaim(item.id, { claimToken: 'active', clientId: 1 }, async ({ db: connection }) => {
      await connection.query("UPDATE max_attention_items SET claimed_at = clock_timestamp() - interval '1 hour' WHERE id=$1", [item.id]);
      const other = await stores().attentionStore.claimDueItems({ clientId: 1, now, limit: 1, claimToken: 'other' });
      assert.deepEqual(other, []);
      throw new Error('worker rollback');
    }), /worker rollback/);
    await expire(item.id);
    assert.equal((await stores().attentionStore.claimDueItems({ clientId: 1, now, limit: 1, claimToken: 'after' })).length, 1);
  });

  await t.test('terminated worker connection rolls back effects and fresh store can reclaim', async () => {
    await reset();
    const item = await seed();
    const store = stores().attentionStore;
    await store.claimDueItems({ clientId: 1, now, limit: 1, claimToken: 'abandoned' });
    await assert.rejects(store.withClaim(item.id, { claimToken: 'abandoned', clientId: 1 }, async ({ db: connection }) => {
      // Actual server termination, not only a thrown JS exception. No external effects.
      const pid = (await connection.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
      await connection.query("UPDATE max_attention_items SET reason='uncommitted change' WHERE id=$1", [item.id]);
      await db.query('SELECT pg_terminate_backend($1)', [pid]);
      await connection.query('SELECT 1');
    }));
    assert.equal((await stores().attentionStore.findById(item.id)).reason, 'Local test review');
    await expire(item.id);
    const recovered = await runAttentionCycle(stores());
    assert.equal(recovered.evaluated, 1);
    assert.deepEqual(await counts(), [1, 1, 1]);
  });

  await t.test('failure after AO effect rolls back all writes; retry and replay create exactly one durable task', async () => {
    await reset();
    const item = await seed();
    let fail = true;
    const failingPool = { query: (...args) => db.query(...args), connect: async () => {
      const client = await db.connect();
      return { release: () => client.release(), query: async (sql, args) => {
        if (fail && /INSERT INTO max_operational_action_intents/.test(sql)) {
          fail = false;
          throw new Error('crash after AO task insert');
        }
        return client.query(sql, args);
      } };
    } };
    const result = await runAttentionCycle(stores(failingPool));
    assert.equal(result.failed, 1);
    assert.deepEqual(await counts(), [0, 0, 0]);
    const after = await stores().attentionStore.findById(item.id);
    assert.equal(after.claim_token, null);
    assert.equal(after.execution_failures, 1);
    assert.ok(new Date(after.next_review_at) > now);
    assert.equal((await db.query('SELECT status FROM max_attention_scheduler_runs WHERE id=$1', [result.run_id])).rows[0].status, 'failed');
    await db.query('UPDATE max_attention_items SET next_review_at=$2 WHERE id=$1', [item.id, now]);
    const retry = await runAttentionCycle(stores());
    assert.equal(retry.evaluated, 1);
    assert.deepEqual(await counts(), [1, 1, 1]);
    await db.query('UPDATE max_attention_items SET next_review_at=$2 WHERE id=$1', [item.id, now]);
    const replay = await runAttentionCycle(stores());
    assert.equal(replay.telemetry.duplicate_evaluations, 1);
    assert.deepEqual(await counts(), [1, 1, 1]);
  });

  await t.test('resolved and rescheduled rows are rechecked under the fence, never resurrected', async () => {
    await reset();
    for (const patch of [{ status: 'RESOLVED', next_review_at: null },
      { status: 'WAITING', next_review_at: '2026-10-14T12:00:00Z' }]) {
      const item = await seed();
      const context = stores();
      const [claimed] = await context.attentionStore.claimDueItems({ clientId: 1, now, limit: 1, claimToken: item.id });
      await context.attentionStore.updateAttention(item.id, patch);
      const result = await evaluateAttentionItem({ ...context, item: claimed, claimToken: item.id });
      assert.equal(result.skipped, true);
      const current = await context.attentionStore.findById(item.id);
      assert.equal(current.status, patch.status);
      assert.equal(current.next_review_at, patch.next_review_at && new Date(patch.next_review_at).toISOString());
      assert.equal(current.claim_token, null);
    }
    assert.deepEqual(await counts(), [0, 0, 0]);
  });

  await t.test('different-trigger active AO replay rejects a changed canonical owner', async () => {
    await reset();
    const item = await seed();
    await db.query("UPDATE max_attention_items SET status='OVERDUE' WHERE id=$1", [item.id]);
    assert.equal((await runAttentionCycle(stores())).evaluated, 1);
    await db.query('UPDATE prospects SET assigned_ao_id=11');
    await db.query('UPDATE max_attention_items SET next_review_at=$2 WHERE id=$1', [item.id, now]);
    const replay = await runAttentionCycle(stores());
    assert.equal(replay.failed, 1);
    assert.equal(replay.evaluated, 0);
    assert.deepEqual(await counts(), [1, 1, 1]);
    assert.equal((await db.query('SELECT owner_id FROM max_ao_follow_up_tasks')).rows[0].owner_id, 10);
    assert.ok(new Date((await stores().attentionStore.findById(item.id)).next_review_at) > now);
  });

  for (const { offsetMs, expectedStatus } of [
    { offsetMs: 0, expectedStatus: 'ACTIVE' },
    { offsetMs: -1, expectedStatus: 'OVERDUE' },
  ]) {
    await t.test(`claim boundary ${offsetMs}ms preserves ${expectedStatus}`, async () => {
      await reset();
      const item = await seed();
      await db.query("UPDATE max_attention_items SET status='ACTIVE', next_review_at=$2 WHERE id=$1",
        [item.id, new Date(now.getTime() + offsetMs)]);
      const [claimed] = await stores().attentionStore.claimDueItems({
        clientId: 1, now, limit: 1, claimToken: `boundary-${offsetMs}`,
      });
      assert.equal(claimed.id, item.id);
      assert.equal(claimed.status, expectedStatus);
      assert.deepEqual(await counts(), [0, 0, 0]);
    });
  }

  await t.test('legacy partial AO decision stays retryable without duplicating or inventing its task', async () => {
    await reset();
    const item = await seed();
    await runAttentionCycle(stores());
    await db.query('DELETE FROM max_ao_follow_up_tasks');
    await db.query('DELETE FROM max_operational_action_intents');
    await db.query("UPDATE max_operational_decisions SET execution_status='AUTHORIZED', verification_status=NULL");
    await db.query('UPDATE max_attention_items SET next_review_at=$2 WHERE id=$1', [item.id, now]);
    const replay = await runAttentionCycle(stores());
    assert.equal(replay.failed, 1);
    assert.equal(replay.evaluated, 0);
    assert.deepEqual(await counts(), [1, 0, 0]);
    const current = await stores().attentionStore.findById(item.id);
    assert.ok(new Date(current.next_review_at) > now);
    // Equality is due for review but is not strictly overdue; preserve ACTIVE.
    assert.equal(current.status, 'ACTIVE');
  });
});
