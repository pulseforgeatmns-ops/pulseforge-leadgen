'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  evaluateResumption,
  parseArgs,
  REMAINING_REFILL_IDS,
} = require('../scripts/verifyGovernedOutboundResumption');

test('production inspect requires confirmation and optional --out', () => {
  assert.deepEqual(parseArgs(['--confirm-production']), {
    confirmProduction: true, out: null, help: false,
  });
  assert.equal(parseArgs(['--confirm-production', '--out', 'artifacts/out.json']).out, 'artifacts/out.json');
});

const remaining = REMAINING_REFILL_IDS.map((id, index) => ({
  id,
  status: 'pending',
  reason: null,
  attempted_at: null,
  refill: 'true',
  email: `pending${index}@example.com`,
}));

function snapshot(overrides = {}) {
  return {
    reconcileAt: '2026-09-29T18:18:27.536Z',
    forceSend: false,
    dispatchUnavailableNow: false,
    emmettCapacity: 16,
    program: {
      mode: 'active',
      last_error: 'spacing',
      last_tick_at: '2026-09-29T18:25:41.440Z',
    },
    item: {
      id: 'daily_b6405a6360afa44baaf23c2a_3',
      status: 'failed',
      reason: 'brevo_http_400',
      attempted_at: '2026-09-29T18:20:40.640Z',
      refill: 'true',
    },
    envelopeItems: remaining,
    events: [
      {
        created_at: '2026-09-29T18:18:27.536Z',
        event_type: 'send_reconciled',
        reason: 'reconciled_not_sent',
        provider_outcome: 'PROVIDER_CONFIRMED_NOT_SENT',
      },
      {
        created_at: '2026-09-29T18:20:40.809Z',
        event_type: 'send_failed',
        reason: 'brevo_http_400',
      },
      {
        created_at: '2026-09-29T18:20:40.938Z',
        event_type: 'tick_blocked',
        reason: 'provider_rejected',
      },
    ],
    executions: [{
      prospect_id: '03c2e326-39c5-4e29-abda-cdb25f077e0b',
      execution_identity: '91cd2a73cab1c6c82d44ed44470b1e3b5df5721c18e5b2d811b0f404a3b85a6b',
    }],
    uncertainItems: [],
    ...overrides,
  };
}

test('PASS when the post-reconciliation tick used the corrected persist path and then spaced', () => {
  const result = evaluateResumption(snapshot());
  assert.equal(result.verdict, 'PASS');
  assert.deepEqual(result.failed, []);
  assert.equal(result.programOperational, true);
});

test('FAIL if uncertain_send_requires_reconciliation returns', () => {
  const result = evaluateResumption(snapshot({
    program: { mode: 'active', last_error: 'uncertain_send_requires_reconciliation' },
    uncertainItems: [{ id: 'daily_b6405a6360afa44baaf23c2a_4', status: 'uncertain', reason: 'abandoned_attempt' }],
  }));
  assert.equal(result.verdict, 'FAIL');
  assert.ok(result.failed.includes('notBlockedByUncertainReconciliation'));
});

test('FAIL on a post-reconciliation 23502 or abandoned_attempt', () => {
  const with23502 = evaluateResumption(snapshot({
    events: [
      { created_at: '2026-09-29T18:18:27.536Z', event_type: 'send_reconciled', reason: 'reconciled_not_sent' },
      { created_at: '2026-09-29T18:21:00.000Z', event_type: 'tick_blocked', reason: '23502', sqlstate: '23502' },
    ],
  }));
  assert.equal(with23502.verdict, 'FAIL');
  assert.ok(with23502.failed.includes('noSqlstate23502'));

  const abandoned = evaluateResumption(snapshot({
    item: {
      id: 'daily_b6405a6360afa44baaf23c2a_3',
      status: 'uncertain',
      reason: 'abandoned_attempt',
      attempted_at: '2026-09-29T18:20:40.640Z',
    },
    events: [
      { created_at: '2026-09-29T18:18:27.536Z', event_type: 'send_reconciled', reason: 'reconciled_not_sent' },
      { created_at: '2026-09-29T18:21:00.000Z', event_type: 'send_uncertain', reason: 'abandoned_attempt' },
    ],
  }));
  assert.equal(abandoned.verdict, 'FAIL');
  assert.ok(abandoned.failed.includes('noNewAbandonedOrUncertainSend'));
});
