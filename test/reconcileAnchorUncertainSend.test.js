'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { parseArgs, outcomeFromEvidence, INCIDENT, EVIDENCE } = require('../scripts/reconcileAnchorUncertainSend');

test('incident identifiers match the 12:14:39 ET Sacramento PMG attempt', () => {
  assert.equal(INCIDENT.itemId, 'daily_b6405a6360afa44baaf23c2a_3');
  assert.equal(INCIDENT.email, 'hello@sacramentopmg.com');
  assert.equal(INCIDENT.attemptedAt, '2026-09-29T16:14:39.271Z');
  assert.match(EVIDENCE, /23502/);
  assert.match(EVIDENCE, /execution_identity/);
});

test('inspect is the default; apply requires an explicit operator', () => {
  assert.deepEqual(parseArgs(['--confirm-production']), {
    confirmProduction: true, apply: false, operator: null,
  });
  assert.equal(parseArgs(['--confirm-production', '--apply', '--operator', 'jake']).apply, true);
  assert.equal(parseArgs(['--confirm-production', '--apply', '--operator', 'jake']).operator, 'jake');
});

test('provider outcome stays fail-closed unless Brevo and execution evidence are empty', () => {
  const item = { provider_message_id: null };
  assert.equal(outcomeFromEvidence({ item, executions: [], brevo: { skipped: false, events: [] } }), 'PROVIDER_CONFIRMED_NOT_SENT');
  assert.equal(outcomeFromEvidence({ item, executions: [], brevo: { skipped: true, events: [] } }), 'PROVIDER_STATE_UNKNOWN');
  assert.equal(outcomeFromEvidence({
    item: { provider_message_id: '<id>' }, executions: [], brevo: { skipped: false, events: [] },
  }), 'PROVIDER_CONFIRMED_SENT');
});
