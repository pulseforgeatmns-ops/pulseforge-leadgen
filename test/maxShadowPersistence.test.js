const assert = require('node:assert/strict');
const test = require('node:test');
const { recordShadowDecision } = require('../utils/maxOrchestration');

function fakeDb(existingDecision = null) {
  const calls = [];
  return {
    calls,
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (/SELECT \* FROM max_decisions/.test(sql)) return { rows: existingDecision ? [existingDecision] : [] };
      return { rows: [] };
    },
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
  const db = fakeDb();
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
  assert.equal(db.calls.filter(call => /INSERT INTO max_decisions/.test(call.sql)).length, 1);
  const transition = db.calls.find(call => /INSERT INTO prospect_state_transitions/.test(call.sql));
  assert.ok(transition);
  assert.match(transition.sql, /TRUE,FALSE/);
  const actions = db.calls.filter(call => /INSERT INTO max_actions/.test(call.sql));
  assert.equal(actions.length, 2);
  assert.ok(actions.every(call => /'skipped'/.test(call.sql) && /'SHADOW_MODE'/.test(call.sql)));
  assert.equal(db.calls.some(call => /SET\s+status\s*=|agent_actions|cal_queue|email_sent/.test(call.sql)), false);
  assert.equal(db.calls.some(call => /UPDATE prospects[\s\S]*warmth_score/.test(call.sql)), true);
  assert.ok(db.calls.some(call => /INSERT INTO max_orchestration_metrics/.test(call.sql) && call.params.includes('max_decisions_total')));
});

test('duplicate decision returns existing audit without new writes', async () => {
  const existing = { id: 'existing', recommended_state: 'warm' };
  const db = fakeDb(existing);
  const result = await recordShadowDecision({
    db, prospect, scoreResult, decision, config: {}, triggerEvent: null,
    key: 'same-key', startedAt: Date.now(), now: new Date(),
  });
  assert.equal(result.duplicate, true);
  assert.equal(result.decision.id, 'existing');
  assert.equal(db.calls.some(call => /INSERT INTO max_decisions|INSERT INTO max_actions|INSERT INTO prospect_state_transitions/.test(call.sql)), false);
});
