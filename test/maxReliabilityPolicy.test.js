'use strict';

/**
 * MAX-P0-003 review — database-free policy pins:
 *   - which /execute requests must carry an Idempotency-Key (R3);
 *   - the permanent receipt is an allowlisted summary (retention);
 *   - SMTP failures are definitive only with proof of non-delivery (R2),
 *     using the error shapes nodemailer 8.x really produces.
 */

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { requiresIdempotencyKey, executeReceipt, executeCanonicalResponse } = require('../routes/acquisitionMissions');
const { smtpFailureIsDefinitive } = require('../services/tenantMailbox');

test('Idempotency-Key is required whenever a request can reach external effects or spend', () => {
  const required = [
    {}, { intent: '' }, { intent: '   ' }, // effective intent comes from the pending decision
    { intent: 'EXECUTE_OUTBOUND' }, { intent: ' EXECUTE_OUTBOUND ' }, { intent: 'execute_outbound' },
    { intent: 'APPROVE_DISCOVERY' }, { intent: 'APPROVE_EXECUTION' },
    { intent: 'AUTONOMOUS_PROGRESSION' }, { intent: 'MISSION_CONTINUATION' }, { intent: 'CONTINUE_INVESTIGATION' },
    { intent: 'APPROVE_PLAN', executionMode: 'autonomous' }, // APPROVE_PLAN can chain into discovery
  ];
  for (const command of required) assert.equal(requiresIdempotencyKey(command), true, JSON.stringify(command));
  for (const command of [{ intent: 'APPROVE_PLAN' }, { intent: 'GENERATE_VARIANTS' }]) {
    assert.equal(requiresIdempotencyKey(command), false, JSON.stringify(command));
  }
});

test('the permanent execute receipt keeps no free text, payload or snapshot', () => {
  const receipt = executeReceipt({
    spec: 'SPEC-171',
    request: { id: 'cer_1', intent: 'APPROVE_PLAN', source: 'approval_button', missionId: 'm1',
      question: 'call Dana on 555-0100', payload: { note: 'private' } },
    action: 'plan_approved', specialist: 'operator', runtimeOwner: 'amo',
    execution: { state: 'committed_internal', evidence: { raw: 'x' } },
    executionResult: { transactionId: 'tme_1', rollbackReason: 'free text', blockReason: 'free text', errorClass: null },
    snapshot: { mission: { title: 'secret' } },
  });
  const text = JSON.stringify(receipt);
  for (const leaked of ['555-0100', 'private', 'free text', 'secret', '"raw"']) assert.equal(text.includes(leaked), false, leaked);
  assert.deepEqual(receipt.request, { id: 'cer_1', intent: 'APPROVE_PLAN', source: 'approval_button', missionId: 'm1' });
  assert.equal(receipt.execution.state, 'committed_internal');
  assert.equal(receipt.executionResult.transactionId, 'tme_1');
});

test('SMTP failures are definitive only with proof of non-delivery (real nodemailer 8.x shapes)', () => {
  const e = (message, props) => Object.assign(new Error(message), props);
  // nodemailer tags these with command 'CONN' at ANY stage, including after DATA: unknown.
  for (const err of [
    e('Connection closed unexpectedly', { code: 'ECONNECTION', command: 'CONN' }),
    e('read ECONNRESET', { code: 'ESOCKET', command: 'CONN', syscall: 'read' }),
    e('Timeout', { code: 'ETIMEDOUT', command: 'CONN' }),
    e('Greeting never received', { code: 'ETIMEDOUT', command: 'CONN' }),
    e('write EPIPE', { code: 'ESOCKET', command: 'CONN', syscall: 'write' }),
    e('anything else', {}),
  ]) assert.equal(smtpFailureIsDefinitive(err), false, err.message);
  for (const err of [
    e('550 mailbox unavailable', { code: 'EENVELOPE', responseCode: 550 }),
    e('452 try later', { code: 'EMESSAGE', responseCode: 452 }),
    e('Invalid login', { code: 'EAUTH', responseCode: 535 }),
    e('getaddrinfo ENOTFOUND smtp.example.test', { code: 'EDNS', command: 'CONN' }),
    e('connect ECONNREFUSED 192.0.2.1:465', { code: 'ESOCKET', command: 'CONN', syscall: 'connect' }),
    e('connect ENETUNREACH 192.0.2.1:465', { code: 'ESOCKET', command: 'CONN' }),
  ]) assert.equal(smtpFailureIsDefinitive(err), true, err.message);
});

test('the receipt is composed from the real execute response shape (executeCanonicalResponse → executeReceipt)', () => {
  const routed = {
    request: { id: 'cer_9', intent: 'APPROVE_DISCOVERY', source: 'approval_button', missionId: 'm9', question: 'free text 555-0199' },
    action: 'discovery_approved', specialist: 'scout', runtimeOwner: 'amo', snapshot: { mission: { title: 'x' } },
    execution: { state: 'committed_internal' },
    executionResult: { alreadyExecuted: false, rolledBack: false, executionOutcome: 'committed', transactionId: 'tme_9',
      error: { message: 'boom', tmeClass: 'specialist' } },
  };
  const live = executeCanonicalResponse(routed);
  const receipt = executeReceipt(live);
  assert.equal(receipt.request.intent, 'APPROVE_DISCOVERY');
  assert.equal(receipt.executionResult.transactionId, 'tme_9');
  assert.equal(receipt.executionResult.errorClass, 'specialist');
  assert.equal(receipt.execution.state, 'committed_internal');
  assert.equal(JSON.stringify(receipt).includes('555-0199'), false);
  assert.equal(JSON.stringify(receipt).includes('boom'), false);
});
