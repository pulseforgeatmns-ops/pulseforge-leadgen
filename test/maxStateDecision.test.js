const assert = require('node:assert/strict');
const test = require('node:test');
const { loadMaxOrchestrationConfig } = require('../config/maxOrchestration');
const { determineStateDecision } = require('../utils/maxStateDecision');

const config = loadMaxOrchestrationConfig({ env: {} });
const now = new Date('2026-07-15T12:00:00.000Z');
const score = (value, direct = null) => ({ score: value, components: [], direct_state_event: direct });

test('promotes cold to heating, heating to warm, and warm to hot', () => {
  assert.equal(determineStateDecision({ prospect: { lifecycle_state: 'cold' }, scoreResult: score(40), config, now }).recommended_state, 'heating');
  assert.equal(determineStateDecision({ prospect: { lifecycle_state: 'heating' }, scoreResult: score(60), config, now }).recommended_state, 'warm');
  assert.equal(determineStateDecision({ prospect: { lifecycle_state: 'warm' }, scoreResult: score(80), config, now }).recommended_state, 'hot');
});

test('direct reply, unsubscribe, invalid bounce, and operator events bypass thresholds', () => {
  assert.equal(determineStateDecision({ prospect: { lifecycle_state: 'cold' }, scoreResult: score(0, 'email_positive_reply'), config, now }).recommended_state, 'engaged');
  const unsubscribe = determineStateDecision({ prospect: { lifecycle_state: 'warm' }, scoreResult: score(90, 'email_unsubscribed'), config, now });
  assert.equal(unsubscribe.recommended_state, 'disqualified');
  assert.equal(unsubscribe.next_best_action, 'suppress_outreach');
  assert.deepEqual(unsubscribe.actions, ['stop_automated_sequences']);
  assert.equal(determineStateDecision({ prospect: { lifecycle_state: 'hot' }, scoreResult: score(90, 'email_hard_bounced_confirmed_invalid'), config, now }).recommended_state, 'null');
  assert.equal(determineStateDecision({ prospect: { lifecycle_state: 'cold' }, scoreResult: score(0, 'operator_marked_hot'), config, now }).recommended_state, 'hot');
});

test('hysteresis prevents rapid downgrade and permits stabilized downgrade', () => {
  const fresh = determineStateDecision({ prospect: { lifecycle_state: 'warm' }, scoreResult: score(45), config, now });
  assert.equal(fresh.recommended_state, 'warm');
  assert.ok(fresh.reason_codes.includes('DOWNGRADE_STABILIZING'));
  const stable = determineStateDecision({
    prospect: { lifecycle_state: 'warm', downgrade_candidate_since: '2026-07-12T11:00:00.000Z' },
    scoreResult: score(45), config, now,
  });
  assert.equal(stable.recommended_state, 'heating');
});

test('terminal states require an explicit operator restore', () => {
  assert.equal(determineStateDecision({ prospect: { lifecycle_state: 'disqualified' }, scoreResult: score(100), config, now }).recommended_state, 'disqualified');
  assert.equal(determineStateDecision({ prospect: { lifecycle_state: 'null' }, scoreResult: score(0, 'operator_marked_warm'), config, now }).recommended_state, 'warm');
});

test('warm recommendation describes actions but does not execute them', () => {
  const decision = determineStateDecision({
    prospect: { lifecycle_state: 'heating', email: null, phone: '555' }, scoreResult: score(65), config, now,
  });
  assert.equal(decision.next_best_action, 'prioritized_enrichment');
  assert.deepEqual(decision.actions, ['pause_cold_sequence', 'retry_enrichment']);
});

test('out-of-office reply does not recommend engaged', () => {
  const decision = determineStateDecision({
    prospect: { lifecycle_state: 'heating' },
    scoreResult: score(45, null),
    signals: [{ event_type: 'email_out_of_office' }],
    config,
    now,
  });
  assert.equal(decision.recommended_state, 'heating');
});
