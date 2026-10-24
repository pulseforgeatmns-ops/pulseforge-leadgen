const assert = require('node:assert/strict');
const test = require('node:test');
const { recordShadowDecision } = require('../utils/maxOrchestration');

function fakeClient(existingDecision = null, { failOn = null } = {}) {
  const calls = [];
  return {
    calls,
    releaseCount: 0,
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (failOn?.test(sql)) throw new Error('persistence failed');
      if (/SELECT \* FROM max_decisions/.test(sql)) return { rows: existingDecision ? [existingDecision] : [] };
      return { rows: [] };
    },
    release() { this.releaseCount++; },
  };
}

function fakePool(existingDecision = null, options = {}) {
  const client = fakeClient(existingDecision, options);
  return {
    client,
    connectCount: 0,
    async connect() { this.connectCount++; return client; },
  };
}

const prospect = {
  id: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
  client_id: 1,
  company_id: null,
};
const scoreResult = {
  score: 72,
  score_version: 'warmth-v1',
  components: [{ code: 'ICP_SCORE_80_PLUS', points: 30, description: 'ICP score is 85' }],
};
const decision = {
  current_state: 'heating',
  recommended_state: 'warm',
  transition_recommended: true,
  next_best_action: 'start_warm_sequence',
  operator_required: false,
  operator_priority: 'normal',
  reason_codes: ['WARM_THRESHOLD_CROSSED'],
  reason_summary: 'Recommend heating → warm with warmth score 72.',
  actions: ['pause_cold_sequence', 'start_warm_sequence'],
  decision_version: 'max-decision-v1',
};

test('shadow persistence records decision, unapplied transition, and skipped actions', async () => {
  const db = fakePool();
  const result = await recordShadowDecision({
    db,
    prospect,
    scoreResult,
    decision,
    config: { version: 'test' },
    triggerEvent: { id: 'event-1', event_type: 'email_human_opened', event_timestamp: new Date(), metadata: {} },
    key: 'key-1',
    startedAt: Date.now(),
    now: new Date('2026-07-15T12:00:00.000Z'),
  });
  assert.equal(result.duplicate, false);
  assert.equal(db.connectCount, 1);
  assert.equal(db.client.releaseCount, 1);
  assert.equal(db.client.calls.filter(call => /INSERT INTO max_decisions/.test(call.sql)).length, 1);
  const transition = db.client.calls.find(call => /INSERT INTO prospect_state_transitions/.test(call.sql));
  assert.ok(transition);
  assert.match(transition.sql, /TRUE,FALSE/);
  const actions = db.client.calls.filter(call => /INSERT INTO max_actions/.test(call.sql));
  assert.equal(actions.length, 2);
  assert.ok(actions.every(call => /'skipped'/.test(call.sql) && /'SHADOW_MODE'/.test(call.sql)));
  assert.equal(db.client.calls.some(call => /SET\s+status\s*=|agent_actions|cal_queue|email_sent/.test(call.sql)), false);
  assert.equal(db.client.calls.some(call => /UPDATE prospects[\s\S]*warmth_score/.test(call.sql)), true);
  assert.ok(db.client.calls.some(call => /INSERT INTO max_orchestration_metrics/.test(call.sql) && call.params.includes('max_decisions_total')));
  assert.equal(db.client.calls.filter(call => /^\s*(BEGIN|COMMIT)\s*$/i.test(call.sql)).length, 2);
});

test('duplicate decision returns existing audit without new writes', async () => {
  const existing = { id: 'existing', recommended_state: 'warm' };
  const db = fakePool(existing);
  const result = await recordShadowDecision({
    db, prospect, scoreResult, decision, config: {}, triggerEvent: null,
    key: 'same-key', startedAt: Date.now(), now: new Date(),
  });
  assert.equal(result.duplicate, true);
  assert.equal(result.decision.id, 'existing');
  assert.equal(db.client.calls.some(call => /INSERT INTO max_decisions|INSERT INTO max_actions|INSERT INTO prospect_state_transitions/.test(call.sql)), false);
  assert.equal(db.client.calls.filter(call => /^\s*COMMIT\s*$/i.test(call.sql)).length, 1);
  assert.equal(db.client.releaseCount, 1);
});

test('caller-owned connected client is reused without connection or transaction ownership', async () => {
  const client = fakeClient();
  client.connect = async () => { throw new Error('connect must not be called'); };
  const result = await recordShadowDecision({
    db: client, prospect, scoreResult, decision, config: {}, triggerEvent: null,
    key: 'caller-key', startedAt: Date.now(), now: new Date(),
  }, { client, transactionManagedByCaller: true });
  assert.equal(result.duplicate, false);
  assert.equal(client.releaseCount, 0);
  assert.equal(client.calls.some(call => /^\s*(BEGIN|COMMIT|ROLLBACK)\s*$/i.test(call.sql)), false);
  assert.ok(client.calls.some(call => /pg_advisory_xact_lock/.test(call.sql)));
  assert.ok(client.calls.some(call => /INSERT INTO max_decisions/.test(call.sql)));
});

test('caller-owned persistence failure propagates without rollback or release', async () => {
  const client = fakeClient(null, { failOn: /INSERT INTO max_decisions/ });
  await assert.rejects(recordShadowDecision({
    db: client, prospect, scoreResult, decision, config: {}, triggerEvent: null,
    key: 'caller-failure', startedAt: Date.now(), now: new Date(),
  }, { client, transactionManagedByCaller: true }), /persistence failed/);
  assert.equal(client.releaseCount, 0);
  assert.equal(client.calls.some(call => /^\s*ROLLBACK\s*$/i.test(call.sql)), false);
  await client.query('ROLLBACK');
  assert.equal(client.calls.filter(call => /^\s*ROLLBACK\s*$/i.test(call.sql)).length, 1);
});

test('internally owned connection rolls back errors and releases exactly once', async () => {
  const db = fakePool(null, { failOn: /INSERT INTO max_decisions/ });
  await assert.rejects(recordShadowDecision({
    db, prospect, scoreResult, decision, config: {}, triggerEvent: null,
    key: 'internal-failure', startedAt: Date.now(), now: new Date(),
  }), /persistence failed/);
  assert.equal(db.connectCount, 1);
  assert.equal(db.client.calls.filter(call => /^\s*ROLLBACK\s*$/i.test(call.sql)).length, 1);
  assert.equal(db.client.releaseCount, 1);
});

test('transaction context rejects a different db client', async () => {
  const client = fakeClient();
  await assert.rejects(recordShadowDecision({
    db: fakeClient(), prospect, scoreResult, decision, config: {}, triggerEvent: null,
    key: 'mismatch', startedAt: Date.now(), now: new Date(),
  }, { client, transactionManagedByCaller: true }), /same client/);
});
