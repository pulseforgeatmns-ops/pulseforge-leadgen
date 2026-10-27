'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const { buildDecisionReceipt } = require('../packages/max/decisionExecution/receipt');
const { evaluateOperationalDecision, MemoryDecisionStore } = require('../packages/max/decisionExecution');
const { MemoryStateStore } = require('../packages/max/stateIngestion');

function receipt(options = {}) {
  const { action, execution, verification, noIntent, noId } = { action: 'ASK_AO_STATUS', execution: 'EXECUTED', verification: 'VERIFIED', ...options };
  const intentExecution = Object.hasOwn(options, 'intentExecution') ? options.intentExecution : execution;
  const intentVerification = Object.hasOwn(options, 'intentVerification') ? options.intentVerification : verification;
  return buildDecisionReceipt({
    decision: { selected_action: { action_type: action }, execution_status: execution, verification_status: verification, rationale: 'Fixture', reevaluate_after: null },
    intent: noIntent ? null : { execution_status: intentExecution, verification_status: intentVerification, output_payload: { task_id: noId ? null : 'task-fixture', prompt: 'Check fixture' } },
    snapshot: { account: { company_name: 'Fixture', owner_name: 'Tony' } },
  });
}
function notWaiting(result) {
  assert.notEqual(result.status, 'Waiting on AO');
  assert.notEqual(result.status, 'EXECUTED');
  assert.equal(result.waiting_on, null);
  assert.equal(result.prompt, null);
  assert.notEqual(result.next_evaluation, 'after AO response');
}
for (const action of ['ASK_AO_STATUS', 'CREATE_AO_TASK']) {
  for (const execution of ['ACTION_VERIFICATION_FAILED', 'EXECUTION_FAILURE', 'EXECUTION_BLOCKED', 'EXECUTING', 'PENDING', 'AUTHORIZED', 'SUPERSEDED', 'ACTION_ALREADY_ACTIVE', undefined]) {
    test(`${action}: ${execution} cannot become waiting from a returned ID`, () => {
      // Intent deliberately disagrees: decision's non-success state wins.
      const result = receipt({ action, execution, verification: 'VERIFICATION_FAILED', intentExecution: 'EXECUTED', intentVerification: 'VERIFIED' });
      notWaiting(result);
    });
  }
  for (const verification of ['VERIFICATION_FAILED', 'PENDING', null, undefined, 'NOT_APPLICABLE']) {
    test(`${action}: executed with ${verification} verification is not success`, () => notWaiting(receipt({ action, verification })));
  }
  test(`${action}: conflicting failed/pending intent cannot override the decision`, () => {
    notWaiting(receipt({ action, intentExecution: 'ACTION_VERIFICATION_FAILED', intentVerification: 'VERIFICATION_FAILED' }));
    notWaiting(receipt({ action, intentVerification: 'PENDING' }));
    notWaiting(receipt({ action, intentExecution: undefined }));
  });
  test(`${action}: verified result alone may report waiting, missing receipt does not invent it`, () => {
    const good = receipt({ action });
    assert.equal(good.status, 'Waiting on AO'); assert.equal(good.waiting_on, 'Tony');
    assert.equal(good.next_evaluation, 'after AO response'); assert.equal(good.prompt, 'Check fixture');
    for (const options of [{ noIntent: true }, { noId: true }]) {
      const stored = receipt({ action, ...options });
      assert.equal(stored.status, 'EXECUTED'); assert.equal(stored.waiting_on, null);
      assert.equal(stored.prompt, null); assert.equal(stored.next_evaluation, null);
    }
  });
}
test('non-task execution does not hide unknown verification; no-action and blocked states are unchanged', () => {
  assert.equal(receipt({ action: 'RESOLVE_EXPECTATION', verification: 'PENDING' }).status, 'PENDING');
  assert.equal(receipt({ action: 'RESOLVE_EXPECTATION', verification: 'VERIFICATION_FAILED' }).status, 'ACTION_VERIFICATION_FAILED');
  assert.equal(receipt({ action: 'NO_ACTION', execution: 'NO_ACTION_RECORDED', verification: 'NOT_APPLICABLE' }).status, 'NO_ACTION_RECORDED');
  assert.equal(receipt({ action: 'ESCALATE_OPERATOR', execution: 'EXECUTION_BLOCKED', verification: 'NOT_APPLICABLE' }).status, 'EXECUTION_BLOCKED');
});
function fixture(failed = false) {
  const expectation = { id: 'exp-1', client_id: 1, prospect_id: 'p-1', ao_id: 10, expectation_type: 'inbound_call', status: 'OVERDUE', expected_window: { ends_at: '2026-09-01T00:00:00.000Z' }, source_evidence: { account_name: 'Fixture', ao_name: 'Tony' } };
  const stateStore = new MemoryStateStore({ clientId: 1, users: [{ id: 10, name: 'Tony' }], companies: [{ id: 'co-1', name: 'Fixture', client_id: 1 }], prospects: [{ id: 'p-1', client_id: 1, company_id: 'co-1', company_name: 'Fixture', assigned_ao_id: 10, status: 'warm', relationship_active: true, suppress_cold_outreach: true, acquisition_metadata: { maxStateIngestion: { relationship_active: true, suppress_cold_outreach: true } }, activities: [] }], expectations: [expectation] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1, simulateVerificationFailure: failed });
  const input = { clientId: 1, trigger: { type: 'EXPECTATION_OVERDUE', payload: {} }, stateStore, decisionStore, expectation, now: new Date('2026-10-05T00:00:00.000Z') };
  return { input, decisionStore };
}
test('real pipeline failure and duplicate replay remain failed without repeating the task', async () => {
  const { input, decisionStore } = fixture(true);
  const result = await evaluateOperationalDecision(input);
  assert.equal(result.decision.execution_status, 'ACTION_VERIFICATION_FAILED'); notWaiting(result.receipt);
  assert.equal(result.telemetry.actions_verified, 0); assert.equal(result.telemetry.verification_failures, 1);
  const count = decisionStore.aoTasks.length;
  decisionStore.simulateVerificationFailure = false;
  const replay = await evaluateOperationalDecision(input);
  assert.equal(replay.duplicate, true); notWaiting(replay.receipt);
  assert.equal(decisionStore.aoTasks.length, count);
});
test('verified pipeline result and read-only replay retain known success without another task', async () => {
  const { input, decisionStore } = fixture();
  const result = await evaluateOperationalDecision(input);
  assert.equal(result.receipt.status, 'Waiting on AO'); assert.equal(result.decision.verification_status, 'VERIFIED');
  const count = decisionStore.aoTasks.length;
  const replay = await evaluateOperationalDecision(input);
  assert.equal(replay.duplicate, true); assert.equal(replay.receipt.status, 'EXECUTED');
  assert.equal(replay.receipt.waiting_on, null); assert.equal(decisionStore.aoTasks.length, count);
});

test('actual evaluate and scan API routes preserve failure receipts and existing response shape', async () => {
  const { input } = fixture(true); const failed = await evaluateOperationalDecision(input);
  const routes = new Map(); const router = { post: (p, ...h) => routes.set(p, h.at(-1)), get: () => {} };
  const module = { exports: {} };
  const deps = { express: { Router: () => router }, '../middleware/auth': { requireAuth: () => {}, requireRole: () => () => {} }, '../db': {}, '../utils/maxOperationalAuthorization': { resolveMaxOperationalScope: async () => ({ clientId: 1 }) }, '../services/maxDecisionExecutionService': { evaluateDecision: async () => failed, runExpectationDecisionScan: async () => [failed] } };
  vm.compileFunction(fs.readFileSync(path.join(__dirname, '../routes/maxDecisionExecution.js'), 'utf8'), ['module', 'require', 'console'], { filename: 'routes/maxDecisionExecution.js' })(module, name => { assert.ok(Object.hasOwn(deps, name), name); return deps[name]; }, console);
  for (const route of ['/api/v1/max/decisions/evaluate', '/api/v1/max/decisions/scan-expectations']) {
    const res = { json(body) { this.body = body; return this; }, status(code) { this.code = code; return this; } };
    await routes.get(route)({ body: {}, query: {}, session: { active_client_id: 1 } }, res);
    assert.equal(res.body.ok, true); // Request handled, NOT proof of business success.
    const item = res.body.decisions?.[0] || res.body;
    assert.equal(item.execution_status, 'ACTION_VERIFICATION_FAILED'); notWaiting(item.receipt);
  }
});

test('pending receipt becomes waiting only after successful readback, without executing again', async () => {
  const { verifyActionIntent } = require('../packages/max/decisionExecution/verify');
  const decision = { selected_action: { action_type: 'CREATE_AO_TASK' }, execution_status: 'EXECUTED', verification_status: null, rationale: 'Fixture' };
  const intent = { action_type: 'CREATE_AO_TASK', execution_status: 'EXECUTED', verification_status: null, output_payload: { task_id: 'task-1', prompt: 'Check fixture' } };
  const snapshot = { account: { id: 'p-1', owner_id: 10, owner_name: 'Tony' } };
  notWaiting(buildDecisionReceipt({ decision, intent, snapshot }));
  let reads = 0;
  const verified = await verifyActionIntent({ decisionStore: { findAoTask: async id => { reads++; assert.equal(id, 'task-1'); return { id, owner_id: 10, prospect_id: 'p-1', status: 'open' }; } }, stateStore: {}, intent, snapshot });
  const result = buildDecisionReceipt({ decision: { ...decision, verification_status: verified.verification_status }, intent: verified, snapshot });
  assert.equal(reads, 1); assert.equal(result.status, 'Waiting on AO');
});

for (const action of ['RESOLVE_EXPECTATION', 'DELEGATE_AGENT', 'SUPERSEDE_PRIOR']) {
  test(`${action}: generic receipt verification cannot contradict conservative execution status`, () => {
    for (const options of [
      { intentVerification: 'VERIFICATION_FAILED' },
      { intentExecution: 'ACTION_VERIFICATION_FAILED', intentVerification: 'VERIFIED' },
      { intentExecution: 'EXECUTION_FAILURE', intentVerification: 'VERIFIED' },
      { execution: 'EXECUTION_FAILURE', verification: 'VERIFIED' },
    ]) {
      const result = receipt({ action, ...options });
      assert.notEqual(result.status, 'EXECUTED'); assert.equal(result.verification, 'VERIFICATION_FAILED');
    }
    for (const options of [{ verification: null }, { intentVerification: undefined }, { intentExecution: undefined }]) {
      const result = receipt({ action, ...options });
      assert.equal(result.status, 'PENDING'); assert.equal(result.verification, 'PENDING');
    }
    for (const execution of ['PENDING', 'EXECUTING', 'EXECUTION_BLOCKED', 'SUPERSEDED']) {
      const result = receipt({ action, execution, verification: 'VERIFIED' });
      assert.equal(result.status, execution); assert.equal(result.verification, 'PENDING');
    }
    assert.equal(receipt({ action }).verification, 'VERIFIED');
    assert.equal(receipt({ action, noIntent: true }).verification, 'VERIFIED');
  });
}

test('known failed readback dominates a pending or missing intent status', () => {
  for (const action of ['ASK_AO_STATUS', 'CREATE_AO_TASK', 'RESOLVE_EXPECTATION']) {
    for (const intentExecution of ['PENDING', 'EXECUTING', undefined]) {
      const result = receipt({ action, verification: 'VERIFICATION_FAILED', intentExecution, intentVerification: 'PENDING' });
      assert.equal(result.status, 'ACTION_VERIFICATION_FAILED');
      if (action === 'RESOLVE_EXPECTATION') assert.equal(result.verification, 'VERIFICATION_FAILED');
      else notWaiting(result);
    }
  }
});
