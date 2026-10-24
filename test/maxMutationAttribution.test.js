'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { classifyOperationalMutations, diffOperationalSnapshots } = require('../utils/maxMutationAttribution');

const base = { entity: 'prospect', entity_id: 'p1', client_id: 1, field: 'status', before: 'cold', after: 'dead' };

test('exact original-handler correlation classifies a legitimate concurrent mutation', () => {
  const mutation = { ...base, correlation_id: 'brevo-event-1' };
  const report = classifyOperationalMutations([mutation], {
    originalHandlerEvidence: [{
      correlation_id: 'brevo-event-1', handler_status: 'success', entity: 'prospect',
      entity_id: 'p1', client_id: 1, allowed_fields: ['status', 'do_not_contact'], expected_after: 'dead',
    }],
  });
  assert.deepEqual(report.counts, { global: 1, max_attributable: 0, expected_original_handler: 1, unattributed: 0 });
  assert.equal(report.stop_required, false);
});

test('Max transaction ownership makes an operational mutation a stop condition', () => {
  const report = classifyOperationalMutations([{ ...base, transaction_owner: 'max' }]);
  assert.equal(report.counts.max_attributable, 1);
  assert.equal(report.stop_required, true);
});

test('an operational mutation without exact attribution fails closed', () => {
  const report = classifyOperationalMutations([{ ...base, correlation_id: 'unknown' }]);
  assert.equal(report.counts.unattributed, 1);
  assert.equal(report.stop_required, true);
});

test('no mutation passes with zero attribution counts', () => {
  const mutations = diffOperationalSnapshots({ status: 'cold' }, { status: 'cold' }, { entityId: 'p1' });
  const report = classifyOperationalMutations(mutations);
  assert.deepEqual(report.counts, { global: 0, max_attributable: 0, expected_original_handler: 0, unattributed: 0 });
  assert.equal(report.stop_required, false);
});

test('close concurrent original and Max events remain separately attributable by exact evidence', () => {
  const mutations = [
    { ...base, correlation_id: 'brevo-event-1' },
    { ...base, field: 'cal_queue', before: 0, after: 1, correlation_id: 'max-decision-1', max_decision_id: 'd1' },
  ];
  const report = classifyOperationalMutations(mutations, {
    maxDecisionIds: ['d1'],
    originalHandlerEvidence: [{
      correlation_id: 'brevo-event-1', handler_status: 'success', entity: 'prospect',
      entity_id: 'p1', client_id: 1, allowed_fields: ['status'], expected_after: 'dead',
    }],
  });
  assert.equal(report.counts.expected_original_handler, 1);
  assert.equal(report.counts.max_attributable, 1);
  assert.equal(report.stop_required, true);
});
