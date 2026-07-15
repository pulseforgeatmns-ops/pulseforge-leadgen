const assert = require('node:assert/strict');
const test = require('node:test');
const { formatShadowDigest, stageConversion } = require('../utils/maxOrchestrationAnalytics');

test('digest formatter labels recommendations as shadow and separates open sources', () => {
  const text = formatShadowDigest({
    lifecycle_recommendations: { 'Heating → Warm': 3, 'Any → Engaged': 1 },
    warm_queue: { total_warm: 8, new_warm: 3, without_verified_email: 2, with_phone: 5, blocked: 2, missing_sequence_config: 4 },
    activity: { decisions: 10, actions: 7, duplicates: 2, failures: 1, decay_evaluations: 4, manual_overrides: 1, requiring_review: 2 },
    open_breakdown: { human: 6, proxy: 9, unknown: 3 },
    funnel: [{ label: 'Delivered → Human opened', available: true, entering: 30, advancing: 12, conversion_rate: 40 }],
  });
  assert.match(text, /SHADOW ONLY/);
  assert.match(text, /were not executed/);
  assert.match(text, /6 human; 9 proxy; 3 unknown/);
  assert.match(text, /Delivered → Human opened: 12\/30 \(40%\)/);
});

test('funnel comparisons are unavailable below the minimum sample', () => {
  assert.deepEqual(stageConversion(null, 2, 20), { available: false, reason: 'canonical source unavailable' });
  assert.deepEqual(stageConversion(10, 4, 20), { available: false, reason: 'sample below 20', entering: 10, advancing: 4 });
  assert.equal(stageConversion(25, 5, 20).conversion_rate, 20);
});
