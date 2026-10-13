'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { MemoryStateStore, PostgresStateStore } = require('../packages/max/stateIngestion');
const { MemoryDecisionStore, evaluateOperationalDecision } = require('../packages/max/decisionExecution');
const { executeActionIntent } = require('../packages/max/decisionExecution/execute');
const { verifyActionIntent } = require('../packages/max/decisionExecution/verify');
const { attentionStatusFromDecision } = require('../packages/max/attention/syncFromDecision');
const { MemoryAttentionStore } = require('../packages/max/attention/store/memoryStore');

const exp = () => ({ id: 'expectation-1', client_id: 1, prospect_id: 'prospect-1', ao_id: 10,
  expectation_type: 'inbound_call', status: 'OVERDUE', expected_window: { ends_at: '2026-09-01T00:00:00.000Z' } });
const prospect = () => ({ id: 'prospect-1', client_id: 1, assigned_ao_id: 10,
  company_name: 'Example', activities: [{ kind: 'meeting_scheduled', at: '2026-10-05T08:00:00.000Z' }] });
const snapshot = () => ({ expectation: exp(), account: { id: 'prospect-1', client_id: 1, owner_id: 10 } });
const decision = () => ({ id: 'decision-1', client_id: 1, authority_class: 'A', idempotency_key: 'resolve-1' });
const state = () => new MemoryStateStore({ clientId: 1, expectations: [exp()], prospects: [prospect()] });
const scope = { clientId: 1, prospectId: 'prospect-1', aoId: 10 };

async function execute(stateStore, inputSnapshot = snapshot()) {
  return executeActionIntent({ stateStore, decisionStore: new MemoryDecisionStore(), decision: decision(),
    selected: { action_type: 'RESOLVE_EXPECTATION' }, snapshot: inputSnapshot });
}
async function verify(stateStore, intent, inputSnapshot = snapshot(), options = {}) {
  return verifyActionIntent({ stateStore, decisionStore: new MemoryDecisionStore(), intent,
    snapshot: inputSnapshot, options });
}
function assertFailed(intent, code) {
  assert.notEqual(intent.execution_status, 'EXECUTED');
  assert.equal(intent.verification_status, 'VERIFICATION_FAILED');
  if (code) assert.equal(intent.error_code, code);
}

for (const capability of ['neither', 'read only', 'update only', 'array only']) {
  test(`resolution rejects incapable adapter: ${capability}`, async () => {
    let writes = 0;
    const adapter = {};
    if (capability === 'read only') adapter.readExpectation = async () => exp();
    if (capability === 'update only') adapter.updateExpectation = async () => { writes += 1; return exp(); };
    if (capability === 'array only') adapter.expectations = [exp()];
    const { intent } = await execute(adapter);
    assertFailed(intent, 'EXPECTATION_STORE_UNSUPPORTED');
    assertFailed(await verify(adapter, intent, snapshot(), { skipExpectationRead: true }));
    assert.equal(writes, 0);
    if (adapter.expectations) assert.equal(adapter.expectations[0].status, 'OVERDUE');
  });
}

for (const change of ['missing', 'client_id', 'prospect_id', 'ao_id', 'status']) {
  test(`resolution rejects missing or mismatched persisted expectation: ${change}`, async () => {
    const store = state();
    if (change === 'missing') store.expectations = [];
    else store.expectations[0][change] = change === 'status' ? 'UNKNOWN' : 'other';
    let writes = 0;
    const update = store.updateExpectation.bind(store);
    store.updateExpectation = async (...args) => { writes += 1; return update(...args); };
    const { intent } = await execute(store);
    assertFailed(intent);
    assert.equal(writes, 0);
  });
}

for (const change of ['client_id', 'prospect_id', 'ao_id', 'account_client', 'no_expectation']) {
  test(`resolution rejects unscoped snapshot: ${change}`, async () => {
    const store = state();
    const input = snapshot();
    if (change === 'no_expectation') input.expectation = null;
    else if (change === 'account_client') input.account.client_id = 2;
    else input.expectation[change] = 'other';
    const { intent } = await execute(store, input);
    assertFailed(intent, 'EXPECTATION_SCOPE_INVALID');
    assert.equal(store.expectations[0].status, 'OVERDUE');
  });
}

for (const failure of ['zero rows', 'throws', 'stale return', 'wrong scope']) {
  test(`resolution fails closed on unsuccessful update: ${failure}`, async () => {
    const store = state();
    store.updateExpectation = async () => {
      if (failure === 'throws') throw Error('write failed');
      if (failure === 'zero rows') return null;
      if (failure === 'wrong scope') return { ...exp(), client_id: 2, status: 'RESOLVED', resolved_at: new Date().toISOString() };
      return exp();
    };
    const { intent } = await execute(store);
    assertFailed(await verify(store, intent));
    assert.equal(store.expectations[0].status, 'OVERDUE');
  });
}

test('resolution requires a fresh read after a write, including stale no-op success acknowledgments', async () => {
  const store = state();
  store.updateExpectation = async () => ({ ...exp(), status: 'RESOLVED', resolved_at: new Date().toISOString() });
  const { intent } = await execute(store);
  assert.equal(intent.execution_status, 'EXECUTED');
  assertFailed(await verify(store, intent, snapshot(), { skipExpectationRead: true }), 'EXPECTATION_READBACK_FAILED');
});

for (const failure of ['missing', 'throws', 'wrong tenant', 'wrong AO', 'unresolved', 'missing timestamp']) {
  test(`post-write readback fails closed: ${failure}`, async () => {
    const store = state();
    const { intent } = await execute(store);
    store.readExpectation = async () => {
      if (failure === 'throws') throw Error('read failed');
      if (failure === 'missing') return null;
      const row = structuredClone(store.expectations[0]);
      if (failure === 'wrong tenant') row.client_id = 2;
      if (failure === 'wrong AO') row.ao_id = 11;
      if (failure === 'unresolved') row.status = 'OVERDUE';
      if (failure === 'missing timestamp') row.resolved_at = null;
      return row;
    };
    assertFailed(await verify(store, intent, snapshot(), { skipExpectationRead: true }), 'EXPECTATION_READBACK_FAILED');
  });
}

test('Memory state resolves, reads back independently, and repeated resolution preserves timestamps', async () => {
  const store = state();
  const { intent } = await execute(store);
  assert.equal((await verify(store, intent)).verification_status, 'VERIFIED');
  const saved = await store.readExpectation(exp().id, scope);
  saved.status = 'WAITING';
  assert.equal(store.expectations[0].status, 'RESOLVED');
  const timestamp = store.expectations[0].resolved_at;
  let writes = 0;
  store.updateExpectation = async () => { writes += 1; throw Error('replay must not write'); };
  const replay = await execute(store);
  assert.equal((await verify(store, replay.intent)).verification_status, 'VERIFIED');
  assert.equal(store.expectations[0].resolved_at, timestamp);
  assert.equal(writes, 0);
});

test('Memory state methods cannot bypass the bound tenant or mutate other fields', async () => {
  const store = state();
  store.expectations.push({ ...exp(), id: 'other', client_id: 2 });
  assert.equal(await store.readExpectation('other'), null);
  assert.equal(await store.readExpectation('other', { clientId: 2 }), null);
  assert.equal(await store.updateExpectation('other', { status: 'RESOLVED' }, { ...scope, clientId: 2 }), null);
  assert.equal(await store.updateExpectation(exp().id, { status: 'RESOLVED' }), null);
  await assert.rejects(store.updateExpectation(exp().id, { status: 'WAITING' }, scope), /Unsupported/);
  await store.updateExpectation(exp().id, { status: 'RESOLVED', client_id: 2, ao_id: 11, source_evidence: {} }, scope);
  assert.equal(store.expectations[0].client_id, 1);
  assert.equal(store.expectations[0].ao_id, 10);
});

test('actual Postgres adapter emits scoped persisted reads/writes and cannot switch tenant', async () => {
  const calls = [];
  const store = new PostgresStateStore({ query: async (sql, args) => { calls.push({ sql, args }); return { rows: [] }; } }, { clientId: 1 });
  await store.readExpectation('expectation-1', scope);
  assert.match(calls[0].sql, /id = \$1 AND client_id = \$2/);
  assert.deepEqual(calls[0].args, ['expectation-1', 1, true, 'prospect-1', true, 10]);
  assert.equal(await store.updateExpectation('expectation-1', { status: 'RESOLVED' }, scope), null);
  assert.match(calls[1].sql, /id = \$1 AND client_id = \$2 AND prospect_id = \$3 AND ao_id = \$4/);
  assert.deepEqual(calls[1].args, ['expectation-1', 1, 'prospect-1', 10]);
  assert.equal(await store.readExpectation('expectation-1', { ...scope, clientId: 2 }), null);
  assert.equal(await store.updateExpectation('expectation-1', { status: 'RESOLVED' }, { ...scope, clientId: 2 }), null);
  assert.equal(calls.length, 2);
  const { intent } = await execute(store);
  assertFailed(await verify(store, intent));
  assert.equal(calls.length, 3, 'execution performed a durable read and stopped on missing record');
});

async function evaluate(store, decisionStore = new MemoryDecisionStore(), attentionStore = new MemoryAttentionStore({ clientId: 1 })) {
  return evaluateOperationalDecision({ clientId: 1, trigger: { type: 'NEW_EVIDENCE', payload: {} },
    stateStore: store, decisionStore, attentionStore, expectation: exp(), now: new Date('2026-10-05T08:05:00.000Z') });
}

for (const failure of ['write', 'readback']) {
  test(`pipeline persists ${failure} failure, leaves prior obligations open and attention unresolved`, async () => {
    const store = state();
    const decisionStore = new MemoryDecisionStore({ decisions: [{ ...decision(), id: 'prior', idempotency_key: 'prior',
      subject_type: 'expectation', subject_id: exp().id, execution_status: 'EXECUTED', selected_action: { action_type: 'ASK_AO_STATUS' } }],
    aoTasks: [{ id: 'task', decision_id: 'prior', status: 'open' }] });
    if (failure === 'write') store.updateExpectation = async () => null;
    else store.updateExpectation = async () => ({ ...exp(), status: 'RESOLVED', resolved_at: new Date().toISOString() });
    const attentionStore = new MemoryAttentionStore({ clientId: 1 });
    const result = await evaluate(store, decisionStore, attentionStore);
    assertFailed(result.decision);
    assert.equal(result.decision.execution_status, result.intent.execution_status);
    assert.equal((await decisionStore.findDecisionByIdempotency(1, result.decision.idempotency_key)).execution_status, result.intent.execution_status);
    assert.equal(decisionStore.decisions.find(d => d.id === 'prior').execution_status, 'EXECUTED');
    assert.equal(decisionStore.aoTasks[0].status, 'open');
    assert.ok(attentionStore.items.every(item => item.status !== 'RESOLVED'));
    assert.equal(result.telemetry.actions_verified, 0);
  });
}

test('verification failure propagates to decision for AO tasks too', async () => {
  const store = state();
  store.prospects[0].activities = [];
  const result = await evaluate(store, new MemoryDecisionStore({ simulateVerificationFailure: true }));
  assert.equal(result.decision.selected_action.action_type, 'ASK_AO_STATUS');
  assert.equal(result.decision.execution_status, 'ACTION_VERIFICATION_FAILED');
  assert.equal(result.decision.verification_status, 'VERIFICATION_FAILED');
});

test('restart and exact replay preserve verified decision and create no additional intent or effect', async () => {
  const store = state();
  const decisions = new MemoryDecisionStore();
  const first = await evaluate(store, decisions);
  assert.equal(first.decision.verification_status, 'VERIFIED');
  const restartedState = new MemoryStateStore(JSON.parse(JSON.stringify({ clientId: 1, prospects: store.prospects, expectations: store.expectations })));
  const restartedDecisions = new MemoryDecisionStore(JSON.parse(JSON.stringify({ decisions: decisions.decisions, actionIntents: decisions.actionIntents })));
  restartedState.updateExpectation = async () => { throw Error('must not repeat write'); };
  const replay = await evaluate(restartedState, restartedDecisions);
  assert.equal(replay.duplicate, true);
  assert.equal(replay.decision.id, first.decision.id);
  assert.equal(replay.decision.verification_status, 'VERIFIED');
  assert.equal(restartedDecisions.actionIntents.length, 1);
});

test('attention requires both successful execution and verification to resolve', () => {
  for (const action_type of ['RESOLVE_EXPECTATION', 'SUPERSEDE_PRIOR']) {
    for (const verification_status of [null, 'PENDING', 'VERIFICATION_FAILED']) {
      assert.equal(attentionStatusFromDecision({ selected_action: { action_type }, execution_status: 'EXECUTED', verification_status }), 'ACTIVE');
    }
    assert.equal(attentionStatusFromDecision({ selected_action: { action_type }, execution_status: 'EXECUTED', verification_status: 'VERIFIED' }), 'RESOLVED');
    assert.equal(attentionStatusFromDecision({ selected_action: { action_type }, execution_status: 'ACTION_VERIFICATION_FAILED', verification_status: 'VERIFIED' }), 'ACTIVE');
  }
});

test('intentional NO_ACTION and blocked actions stay non-mutating and not applicable', async () => {
  for (const execution_status of ['NO_ACTION_RECORDED', 'EXECUTION_BLOCKED']) {
    const result = await verify({}, { execution_status, action_type: 'NO_ACTION' });
    assert.equal(result.execution_status, execution_status);
    assert.equal(result.verification_status, 'NOT_APPLICABLE');
  }
});

for (const failure of ['changed state', 'missing row', 'read error', 'wrong tenant', 'legacy receipt', 'missing intent', 'unfinished intent']) {
  test(`duplicate resolution revalidates current state and never repeats effects: ${failure}`, async () => {
    const store = state();
    const decisions = new MemoryDecisionStore();
    const attention = new MemoryAttentionStore({ clientId: 1 });
    const first = await evaluate(store, decisions, attention);
    assert.equal(first.decision.verification_status, 'VERIFIED');
    if (failure === 'changed state') Object.assign(store.expectations[0], { status: 'OVERDUE', resolved_at: null });
    if (failure === 'missing row') store.expectations = [];
    if (failure === 'wrong tenant') store.expectations[0].client_id = 2;
    if (failure === 'legacy receipt') delete decisions.decisions[0].canonical_state_snapshot.expectation.client_id;
    if (failure === 'missing intent') decisions.actionIntents = [];
    if (failure === 'unfinished intent') Object.assign(decisions.actionIntents[0], { execution_status: 'EXECUTING', verification_status: null });
    let reads = 0;
    const read = store.readExpectation.bind(store);
    store.readExpectation = async (...args) => {
      reads += 1;
      if (failure === 'read error') throw Error('read unavailable');
      return read(...args);
    };
    store.updateExpectation = async () => { assert.fail('duplicate must not perform another resolution'); };
    const result = await evaluate(store, decisions, attention);
    assert.equal(result.duplicate, true);
    assertFailed(result.decision);
    if (!['legacy receipt', 'missing intent', 'unfinished intent'].includes(failure)) assert.equal(reads, 1);
    assert.equal(decisions.decisions[0].verification_status, 'VERIFICATION_FAILED');
    assert.equal(decisions.actionIntents.length, failure === 'missing intent' ? 0 : 1);
    if (decisions.actionIntents.length) assert.equal(decisions.actionIntents[0].verification_status, 'VERIFICATION_FAILED');
    assert.equal(attention.items.length, 1);
    assert.equal(attention.items[0].status, 'ACTIVE');
    assert.equal(attention.items[0].resolved_at, null);
    assert.deepEqual(attention.items[0].resolution_evidence, []);
  });
}

test('replay may repair unfinished decision bookkeeping from a verified durable intent, using readback only', async () => {
  const store = state();
  const decisions = new MemoryDecisionStore();
  await evaluate(store, decisions);
  Object.assign(decisions.decisions[0], { execution_status: 'AUTHORIZED', verification_status: null });
  let reads = 0;
  const read = store.readExpectation.bind(store);
  store.readExpectation = async (...args) => { reads += 1; return read(...args); };
  store.updateExpectation = async () => { assert.fail('replay must not re-execute'); };
  const replay = await evaluate(store, decisions);
  assert.equal(replay.duplicate, true);
  assert.equal(replay.decision.execution_status, 'EXECUTED');
  assert.equal(replay.decision.verification_status, 'VERIFIED');
  assert.equal(reads, 1);
  assert.equal(decisions.actionIntents.length, 1);
});

test('replay never reactivates a superseded resolution', async () => {
  const store = state();
  const decisions = new MemoryDecisionStore();
  await evaluate(store, decisions);
  decisions.decisions[0].execution_status = 'SUPERSEDED';
  store.updateExpectation = async () => { assert.fail('replay must not re-execute'); };
  const replay = await evaluate(store, decisions);
  assert.equal(replay.duplicate, true);
  assert.equal(replay.decision.execution_status, 'SUPERSEDED');
  assert.equal(decisions.decisions[0].execution_status, 'SUPERSEDED');
});
