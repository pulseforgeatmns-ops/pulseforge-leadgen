const assert = require('node:assert/strict');
const test = require('node:test');
const { loadMaxOrchestrationConfig } = require('../config/maxOrchestration');
const { calculateWarmthScore } = require('../utils/maxWarmthScoring');

const config = loadMaxOrchestrationConfig({ env: {} });
const now = new Date('2026-07-15T12:00:00.000Z');
const signal = (event_type, hoursAgo, metadata = {}) => ({
  event_type,
  event_timestamp: new Date(now.getTime() - hoursAgo * 3600000).toISOString(),
  metadata,
});

test('scores ICP ranges without overlap', () => {
  assert.equal(calculateWarmthScore({ prospect: { icp_score: 85 }, signals: [], config, now }).score, 30);
  assert.equal(calculateWarmthScore({ prospect: { icp_score: 70 }, signals: [], config, now }).score, 20);
  assert.equal(calculateWarmthScore({ prospect: { icp_score: 55 }, signals: [], config, now }).score, 10);
});

test('uses highest human-open tier and proxy opens score zero', () => {
  const result = calculateWarmthScore({
    prospect: {},
    signals: [signal('email_human_opened', 1), signal('email_human_opened', 2), signal('email_human_opened', 3), signal('email_proxy_opened', 1)],
    config,
    now,
  });
  assert.equal(result.components.find(c => c.code === 'THREE_OR_MORE_HUMAN_OPENS').points, 12);
  assert.equal(result.components.filter(c => c.code.includes('HUMAN_OPEN')).length, 1);
  assert.equal(result.score, 22);
});

test('applies highest ICP delta and recency tiers deterministically', () => {
  const inputs = {
    prospect: { icp_score: 70, vertical_tier: 'A', decision_maker: true, email: 'a@example.com', email_verified: true, phone: '555' },
    signals: [signal('icp_score_changed', 4, { old_score: 20, new_score: 65 }), signal('email_clicked', 2, { verified: true })],
    config,
    now,
  };
  const first = calculateWarmthScore(inputs);
  const second = calculateWarmthScore(inputs);
  assert.deepEqual(first, second);
  assert.ok(first.components.some(c => c.code === 'ICP_INCREASE_40'));
  assert.ok(!first.components.some(c => c.code === 'ICP_INCREASE_15'));
  assert.ok(first.components.some(c => c.code === 'SIGNAL_WITHIN_24H'));
  assert.equal(first.score, 90);
});

test('time decay, unverified email, enrichment failure, and soft bounce are applied', () => {
  const result = calculateWarmthScore({
    prospect: { icp_score: 50, email: 'a@example.com', email_verified: false },
    signals: [
      signal('email_human_opened', 24 * 8),
      signal('enrichment_failed', 2), signal('enrichment_failed', 3),
      signal('email_soft_bounced', 4),
    ],
    config,
    now,
  });
  assert.ok(!result.components.some(c => c.code.includes('HUMAN_OPEN')));
  assert.equal(result.score, 0);
  assert.ok(result.components.some(c => c.code === 'REPEATED_ENRICHMENT_FAILURE'));
  assert.ok(result.components.some(c => c.code === 'SOFT_BOUNCE'));
});

test('direct state events are exposed alongside their ordinary recency component', () => {
  const result = calculateWarmthScore({ prospect: {}, signals: [signal('email_unsubscribed', 1)], config, now });
  assert.equal(result.direct_state_event, 'email_unsubscribed');
  assert.equal(result.score, 10);
});

test('a newer explicit operator restore can supersede a terminal direct event', () => {
  const result = calculateWarmthScore({
    prospect: {},
    signals: [signal('email_unsubscribed', 48), signal('operator_marked_warm', 1)],
    config,
    now,
  });
  assert.equal(result.direct_state_event, 'operator_marked_warm');
});

test('unknown opens contribute zero warmth just like proxy opens', () => {
  const result = calculateWarmthScore({
    prospect: {},
    signals: [signal('email_unknown_opened', 1), signal('email_proxy_opened', 1)],
    config,
    now,
  });
  assert.equal(result.score, 0);
  assert.equal(result.last_meaningful_signal_at, null);
});
