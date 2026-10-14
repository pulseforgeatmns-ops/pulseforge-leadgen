'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { MemoryAttentionStore, runAttentionCycle } = require('../packages/max/attention');
const { evaluateAttentionItem } = require('../packages/max/attention/scheduler');
const { MemoryStateStore } = require('../packages/max/stateIngestion');
const { MemoryDecisionStore } = require('../packages/max/decisionExecution');
const { PostgresAttentionStore } = require('../packages/max/attention/store/postgresStore');
const { PostgresStateStore } = require('../packages/max/stateIngestion/store/postgresStore');
const { PostgresDecisionStore } = require('../packages/max/decisionExecution/store/postgresStore');

const now = new Date('2026-10-13T12:00:00Z');
function item(id = 'attention-1') {
  return { id, client_id: 1, subject_type: 'prospect', subject_id: 'prospect-1',
    status: 'WAITING', next_review_at: now.toISOString(), dedup_fingerprint: id };
}
function stores(attentionStore = new MemoryAttentionStore({ clientId: 1 })) {
  return { attentionStore, stateStore: new MemoryStateStore({ clientId: 1 }),
    decisionStore: new MemoryDecisionStore({ clientId: 1 }), clientId: 1, now };
}
function run(id, status = 'running') {
  return { id, client_id: 1, started_at: now.toISOString(), status,
    completed_at: status === 'running' ? null : now.toISOString(), items_claimed: 0,
    items_evaluated: 0, items_failed: 0, telemetry: {} };
}

test('empty successful cycle records one terminal run and a successful heartbeat', async () => {
  const context = stores();
  const result = await runAttentionCycle(context);
  assert.equal(context.attentionStore.runs.length, 1);
  assert.equal(context.attentionStore.runs[0].id, result.run_id);
  assert.equal(context.attentionStore.runs[0].status, 'completed');
  assert.ok((await context.attentionStore.getHeartbeat()).last_successful_cycle_at);
});

test('failed cycle records one failed run and preserves prior success heartbeat', async () => {
  const context = stores();
  await context.attentionStore.updateHeartbeat({ last_successful_cycle_at: now.toISOString() });
  context.attentionStore.claimDueItems = async () => { throw new Error('claim database failure'); };
  await assert.rejects(runAttentionCycle(context), /claim database failure/);
  assert.equal(context.attentionStore.runs.length, 1);
  assert.equal(context.attentionStore.runs[0].status, 'failed');
  const hb = await context.attentionStore.getHeartbeat();
  assert.equal(hb.last_successful_cycle_at, now.toISOString());
  assert.equal(hb.consecutive_failures, 1);
});

test('terminal run is immutable and completion retry does not double-count heartbeat', async () => {
  const store = stores().attentionStore;
  await store.recordSchedulerRun(run('run-1'));
  await store.finishSchedulerRun({ ...run('run-1', 'failed'), items_failed: 1 });
  await store.finishSchedulerRun({ ...run('run-1', 'failed'), items_failed: 1 });
  await store.recordSchedulerRun(run('run-1'));
  assert.equal(store.runs.length, 1);
  assert.equal(store.runs[0].status, 'failed');
  assert.equal((await store.getHeartbeat()).consecutive_failures, 1);
});

test('abandoned lease is reclaimed and old token cannot release or evaluate', async () => {
  let time = now.getTime();
  const store = new MemoryAttentionStore({ clientId: 1, leaseMs: 1000, clock: () => time });
  await store.createAttention(item());
  const [old] = await store.claimDueItems({ clientId: 1, now, limit: 1, claimToken: 'old' });
  time += 1001;
  const [current] = await store.claimDueItems({ clientId: 1, now, limit: 1, claimToken: 'new' });
  assert.ok(current);
  assert.equal(current.claim_token, 'new');
  assert.equal(await store.releaseClaim(old.id, { claimToken: 'old', patch: { status: 'RESOLVED' } }), null);
  const context = stores(store);
  await assert.rejects(evaluateAttentionItem({ ...context, item: old, claimToken: 'old' }),
    { code: 'ATTENTION_CLAIM_LOST' });
  assert.equal(context.decisionStore.decisions.length, 0);
  assert.equal((await store.findById(old.id)).claim_token, 'new');
});

test('expired token fails closed even before another worker claims the item', async () => {
  let time = now.getTime();
  const store = new MemoryAttentionStore({ clientId: 1, leaseMs: 1000, clock: () => time });
  await store.createAttention(item());
  const [claimed] = await store.claimDueItems({ clientId: 1, now, limit: 1, claimToken: 'expired' });
  time += 1000;
  await assert.rejects(evaluateAttentionItem({ ...stores(store), item: claimed, claimToken: 'expired' }),
    { code: 'ATTENTION_CLAIM_LOST' });
  assert.equal(await store.releaseClaim(claimed.id, { claimToken: 'expired' }), null);
});

test('concurrent workers yield one owner and evaluating owner cannot be reclaimed', async () => {
  let time = now.getTime();
  const store = new MemoryAttentionStore({ clientId: 1, leaseMs: 1000, clock: () => time });
  await store.createAttention(item());
  const claimed = await Promise.all(['a', 'b'].map(claimToken =>
    store.claimDueItems({ clientId: 1, now, limit: 1, claimToken })));
  assert.equal(claimed.flat().length, 1);
  const owner = claimed.flat()[0];
  await store.withClaim(owner.id, { claimToken: owner.claim_token, clientId: 1 }, async () => {
    time += 1001;
    assert.deepEqual(await store.claimDueItems({ clientId: 1, now, limit: 1, claimToken: 'c' }), []);
  });
});

test('per-item failure is retry-eligible and recorded as a failed run', async () => {
  const context = stores();
  await context.attentionStore.createAttention(item());
  context.stateStore.readProspect = async () => { throw new Error('read failed'); };
  const result = await runAttentionCycle({ ...context, retryDelayMs: 1234 });
  assert.equal(result.failed, 1);
  assert.equal(context.attentionStore.runs.length, 1);
  assert.equal(context.attentionStore.runs[0].status, 'failed');
  const after = await context.attentionStore.findById('attention-1');
  assert.equal(after.claim_token, null);
  assert.equal(after.execution_failures, 1);
  assert.equal(after.next_review_at, new Date(now.getTime() + 1234).toISOString());
});

test('PostgreSQL evaluation rejects unsupported mixed stores before executing effects', async () => {
  let queried = false;
  const db = { query: async () => { queried = true; throw new Error('must not query'); } };
  const store = new PostgresAttentionStore(db, { clientId: 1 });
  await assert.rejects(evaluateAttentionItem({ ...stores(store), item: item(), claimToken: 'token' }),
    { code: 'ATTENTION_UNSAFE_STORE' });
  assert.equal(queried, false);
});

test('PostgreSQL transactional fence uses one connection and rolls back callback failure', async () => {
  const calls = [];
  const connection = {
    query: async (sql, args) => { calls.push([sql, args]); return { rows: /SELECT \*/.test(sql) ? [item()] : [] }; },
    release: () => calls.push(['RELEASE']),
  };
  const store = new PostgresAttentionStore({ connect: async () => connection }, { clientId: 1 });
  await assert.rejects(store.withClaim('attention-1', { clientId: 1, claimToken: 'owner' }, async scope => {
    assert.equal(scope.db.query instanceof Function, true);
    await scope.db.query('INSERT test_effect');
    throw new Error('crash after effect');
  }), /crash after effect/);
  assert.equal(calls[0][0], 'BEGIN');
  assert.ok(calls.some(([sql]) => /FOR UPDATE/.test(sql)));
  assert.ok(calls.some(([sql]) => /claim_token = \$2/.test(sql)));
  assert.ok(calls.some(([sql]) => /clock_timestamp/.test(sql)));
  assert.ok(calls.some(([sql]) => /statement_timeout/.test(sql)));
  assert.ok(calls.some(([sql]) => sql === 'ROLLBACK'));
  assert.equal(calls.at(-1)[0], 'RELEASE');
  assert.equal(calls.some(([sql]) => sql === 'COMMIT'), false);
});

test('PostgreSQL transaction-scoped evaluation rejects replaced methods and mismatched pools', async () => {
  const db = {};
  const attentionStore = new PostgresAttentionStore(db, { clientId: 1 });
  const stateStore = new PostgresStateStore(db, { clientId: 1 });
  const decisionStore = new PostgresDecisionStore({}, { clientId: 1 });
  await assert.rejects(evaluateAttentionItem({ clientId: 1, now, item: item(), claimToken: 'token',
    attentionStore, stateStore, decisionStore }), { code: 'ATTENTION_UNSAFE_STORE' });
  decisionStore.db = db;
  decisionStore.createAoTask = async () => { throw new Error('external executor'); };
  await assert.rejects(evaluateAttentionItem({ clientId: 1, now, item: item(), claimToken: 'token',
    attentionStore, stateStore, decisionStore }), { code: 'ATTENTION_UNSAFE_STORE' });
});

for (const patch of [{ status: 'RESOLVED', next_review_at: null },
  { status: 'WAITING', next_review_at: '2026-10-14T00:00:00Z' }]) {
  test(`changed claimed row is re-read and skipped: ${patch.status}/${patch.next_review_at}`, async () => {
    const context = stores();
    await context.attentionStore.createAttention(item());
    const [claimed] = await context.attentionStore.claimDueItems({ clientId: 1, now, limit: 1, claimToken: 'a' });
    await context.attentionStore.updateAttention(claimed.id, patch);
    const result = await evaluateAttentionItem({ ...context, item: claimed, claimToken: 'a' });
    assert.equal(result.skipped, true);
    assert.equal(context.decisionStore.decisions.length, 0);
    const after = await context.attentionStore.findById(claimed.id);
    assert.equal(after.status, patch.status);
    assert.equal(after.next_review_at, patch.next_review_at);
    assert.equal(after.claim_token, null);
  });
}

test('memory attention cannot bypass fencing for persistent state or decision stores', async () => {
  const context = stores();
  context.decisionStore = new PostgresDecisionStore({}, { clientId: 1 });
  await assert.rejects(evaluateAttentionItem({ ...context, item: item(), claimToken: 'a' }),
    { code: 'ATTENTION_UNSAFE_STORE' });
});

test('legacy incomplete AO replay is failed and retryable, never replayed into another task', async () => {
  const context = stores();
  context.stateStore.users = [{ id: 10, name: 'Fixture AO' }];
  context.stateStore.prospects = [{ id: 'prospect-1', client_id: 1, assigned_ao_id: 10,
    relationship_active: true, activities: [] }];
  context.stateStore.expectations = [{ id: 'exp-1', client_id: 1, prospect_id: 'prospect-1', ao_id: 10,
    status: 'OVERDUE', expectation_type: 'inbound_call', expected_window: { ends_at: '2026-10-01T00:00:00Z' } }];
  await context.attentionStore.createAttention({ ...item(), subject_type: 'expectation', subject_id: 'exp-1' });
  const initial = await runAttentionCycle(context);
  assert.equal(initial.evaluated, 1);
  assert.equal(context.decisionStore.aoTasks.length, 1);
  context.decisionStore.actionIntents = [];
  context.decisionStore.aoTasks = [];
  await context.attentionStore.updateAttention('attention-1', { next_review_at: now.toISOString() });
  const replay = await runAttentionCycle(context);
  assert.equal(replay.failed, 1);
  assert.equal(replay.evaluated, 0);
  assert.equal(context.decisionStore.decisions.length, 1);
  assert.equal(context.decisionStore.aoTasks.length, 0);
  assert.ok(new Date((await context.attentionStore.findById('attention-1')).next_review_at) > now);
});

test('PostgreSQL run completion updates existing identity and heartbeat within the same transaction', async () => {
  const calls = [];
  const terminal = run('pg-run', 'completed');
  const connection = {
    query: async (sql, args) => { calls.push([sql, args]); return { rows: /UPDATE max_attention_scheduler_runs/.test(sql) ? [terminal] : [] }; },
    release: () => calls.push(['RELEASE']),
  };
  const store = new PostgresAttentionStore({ connect: async () => connection }, { clientId: 1 });
  await store.finishSchedulerRun(terminal);
  assert.equal(calls[0][0], 'BEGIN');
  const transition = calls.find(([sql]) => /UPDATE max_attention_scheduler_runs/.test(sql));
  assert.match(transition[0], /status = 'running'/);
  assert.match(transition[0], /client_id IS NOT DISTINCT FROM \$2/);
  assert.ok(calls.some(([sql]) => /UPDATE max_attention_heartbeats/.test(sql)));
  assert.equal(calls.at(-2)[0], 'COMMIT');
  assert.equal(calls.at(-1)[0], 'RELEASE');
  assert.equal(calls.some(([sql]) => /INSERT/.test(sql)), false);
});

test('PostgreSQL heartbeat failure rolls terminal transition back', async () => {
  const calls = [];
  const terminal = run('pg-run', 'completed');
  const connection = {
    query: async sql => {
      calls.push(sql);
      if (/UPDATE max_attention_heartbeats/.test(sql)) throw new Error('heartbeat write failed');
      return { rows: /UPDATE max_attention_scheduler_runs/.test(sql) ? [terminal] : [] };
    },
    release() {},
  };
  const store = new PostgresAttentionStore({ connect: async () => connection }, { clientId: 1 });
  await assert.rejects(store.finishSchedulerRun(terminal), /heartbeat write failed/);
  assert.equal(calls.at(-1), 'ROLLBACK');
  assert.equal(calls.includes('COMMIT'), false);
});

test('active-duplicate AO replay checks current owner even when pipeline omits current snapshot', async () => {
  const context = stores();
  context.stateStore.users = [{ id: 10, name: 'Old AO' }, { id: 11, name: 'Current AO' }];
  context.stateStore.prospects = [{ id: 'prospect-1', client_id: 1, assigned_ao_id: 10,
    relationship_active: true, activities: [] }];
  context.stateStore.expectations = [{ id: 'exp-1', client_id: 1, prospect_id: 'prospect-1', ao_id: 10,
    status: 'OVERDUE', expectation_type: 'inbound_call', expected_window: { ends_at: '2026-10-01T00:00:00Z' } }];
  await context.attentionStore.createAttention({ ...item(), status: 'OVERDUE',
    subject_type: 'expectation', subject_id: 'exp-1' });
  assert.equal((await runAttentionCycle(context)).evaluated, 1);
  assert.equal(context.decisionStore.aoTasks[0].owner_id, 10);
  context.stateStore.prospects[0].assigned_ao_id = 11;
  context.stateStore.expectations[0].ao_id = 11;
  // Equal due time preserves ACTIVE, changing the trigger to SCHEDULED_REVIEW.
  // This deliberately exercises findActiveDecision, not exact idempotency replay.
  await context.attentionStore.updateAttention('attention-1', { next_review_at: now.toISOString() });
  const replay = await runAttentionCycle(context);
  assert.equal(replay.failed, 1);
  assert.equal(replay.evaluated, 0);
  assert.equal(context.decisionStore.aoTasks.length, 1);
  assert.equal(context.decisionStore.aoTasks[0].owner_id, 10);
  assert.ok(new Date((await context.attentionStore.findById('attention-1')).next_review_at) > now);
});

test('checked-out client termination fails pending work safely and invalidates late callback queries', async () => {
  const { EventEmitter } = require('node:events');
  const client = new EventEmitter();
  const queries = [];
  let discardedWith;
  const failure = new Error('terminating connection due to idle-in-transaction timeout');
  client.query = async sql => {
    queries.push(sql);
    if (sql === 'ROLLBACK') throw failure;
    return { rows: /SELECT \*/.test(sql) ? [item()] : [] };
  };
  client.release = error => { discardedWith = error; };
  const store = new PostgresAttentionStore({ connect: async () => client }, { clientId: 1 });
  let lateDb;
  await assert.rejects(store.withClaim('attention-1', { clientId: 1, claimToken: 'owner' }, async ({ db }) => {
    lateDb = db;
    setImmediate(() => client.emit('error', failure));
    await new Promise(() => {});
  }), /terminating connection/);
  assert.equal(discardedWith, failure);
  assert.equal(client.listenerCount('error'), 0);
  assert.equal(queries.includes('COMMIT'), false);
  const count = queries.length;
  await assert.rejects(lateDb.query('INSERT late_effect'), /terminating connection/);
  assert.equal(queries.length, count);
});

test('successfully released transaction rejects late queries too', async () => {
  let released = false;
  const client = { query: async sql => ({ rows: /SELECT \*/.test(sql) ? [item()] : [] }),
    release: () => { released = true; } };
  const store = new PostgresAttentionStore({ connect: async () => client }, { clientId: 1 });
  let lateDb;
  await store.withClaim('attention-1', { clientId: 1, claimToken: 'owner' }, async ({ db }) => { lateDb = db; });
  assert.equal(released, true);
  await assert.rejects(lateDb.query('INSERT late_effect'), { code: 'ATTENTION_TRANSACTION_CLOSED' });
});

for (const { offsetMs, expectedStatus } of [
  { offsetMs: 0, expectedStatus: 'ACTIVE' },
  { offsetMs: -1, expectedStatus: 'OVERDUE' },
]) {
  test(`claim boundary ${offsetMs}ms preserves ${expectedStatus}`, async () => {
    const context = stores();
    await context.attentionStore.createAttention({ ...item(), status: 'ACTIVE',
      next_review_at: new Date(now.getTime() + offsetMs).toISOString() });
    const [claimed] = await context.attentionStore.claimDueItems({
      clientId: 1, now, limit: 1, claimToken: `boundary-${offsetMs}`,
    });
    assert.equal(claimed.id, 'attention-1');
    assert.equal(claimed.status, expectedStatus);
    assert.equal(context.decisionStore.decisions.length, 0);
  });
}
