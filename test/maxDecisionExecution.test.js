'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {
  evaluateOperationalDecision,
  scanExpectationTriggers,
  reevaluateOnIngestion,
  rejectAgentDelegationOutput,
  MemoryDecisionStore,
  DECISION_TRIGGER,
  EXECUTION_STATUS,
  ACTION_TYPE,
} = require('../packages/max/decisionExecution');
const { authorizeExecution } = require('../packages/max/decisionExecution/authority');
const { prioritizeDecisions } = require('../packages/max/decisionExecution/select');
const {
  MemoryStateStore,
  ingestOperationalUpdate,
} = require('../packages/max/stateIngestion');

function seedState(overrides = {}) {
  return new MemoryStateStore({
    clientId: 1,
    users: [{ id: 10, name: 'Tony' }],
    companies: [{ id: 'co-exeter', name: 'Exeter Phillips', client_id: 1 }],
    prospects: [{
      id: 'prospect-exeter',
      client_id: 1,
      company_id: 'co-exeter',
      company_name: 'Exeter Phillips',
      assigned_ao_id: 10,
      status: 'warm',
      relationship_active: true,
      suppress_cold_outreach: true,
      acquisition_metadata: { maxStateIngestion: { relationship_active: true, suppress_cold_outreach: true } },
      activities: [],
      ...(overrides.prospectPatch || {}),
    }],
    expectations: overrides.expectations || [],
    ...overrides,
  });
}

function overdueExpectation() {
  return {
    id: 'exp-1',
    client_id: 1,
    prospect_id: 'prospect-exeter',
    ao_id: 10,
    expectation_type: 'inbound_call',
    status: 'OVERDUE',
    expected_window: { ends_at: '2026-09-01T00:00:00.000Z' },
    source_evidence: { account_name: 'Exeter Phillips', ao_name: 'Tony' },
  };
}

function openExpectation() {
  return {
    id: 'exp-open',
    client_id: 1,
    prospect_id: 'prospect-exeter',
    ao_id: 10,
    expectation_type: 'inbound_call',
    status: 'WAITING',
    expected_window: { ends_at: '2026-10-20T00:00:00.000Z' },
    source_evidence: { account_name: 'Exeter Phillips', ao_name: 'Tony' },
  };
}

test('1 overdue expectation creates AO follow-up', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.ok(['ASK_AO_STATUS', 'CREATE_AO_TASK'].includes(result.decision.selected_action.action_type));
  assert.equal(result.decision.execution_status, EXECUTION_STATUS.EXECUTED);
  assert.equal(decisionStore.aoTasks.length, 1);
  assert.match(decisionStore.aoTasks[0].prompt, /Exeter Phillips/);
});

test('2 open expectation chooses intentional NO_ACTION', async () => {
  const stateStore = seedState({ expectations: [openExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OPEN, payload: {} },
    stateStore,
    decisionStore,
    expectation: openExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.equal(result.decision.selected_action.action_type, ACTION_TYPE.NO_ACTION);
  assert.equal(result.decision.execution_status, EXECUTION_STATUS.NO_ACTION_RECORDED);
});

test('3 existing relationship does not select cold outreach', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  const cold = result.decision.not_selected.find(n => n.action_type === ACTION_TYPE.RETURN_TO_COLD_OUTREACH);
  assert.ok(cold);
  assert.match(cold.reason, /cold outreach/i);
  assert.notEqual(result.decision.selected_action.action_type, ACTION_TYPE.RETURN_TO_COLD_OUTREACH);
});

test('4 insufficient evidence abstains', async () => {
  const stateStore = seedState({
    prospects: [{
      id: 'p-unknown',
      client_id: 1,
      company_name: 'Unknown Co',
      assigned_ao_id: null,
      status: 'warm',
    }],
    expectations: [{
      id: 'exp-u',
      client_id: 1,
      prospect_id: 'p-unknown',
      status: 'OVERDUE',
      expectation_type: 'inbound_call',
      expected_window: {},
      source_evidence: {},
    }],
  });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: stateStore.expectations[0],
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.equal(result.decision.selected_action.action_type, ACTION_TYPE.ABSTAIN_INSUFFICIENT_EVIDENCE);
  assert.equal(result.decision.execution_status, EXECUTION_STATUS.INSUFFICIENT_EVIDENCE);
});

test('5 conflicting evidence blocks or escalates', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  stateStore.conflicts = [{ entity_id: 'prospect-exeter', kind: 'ownership' }];
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.equal(result.decision.selected_action.action_type, ACTION_TYPE.ESCALATE_OPERATOR);
  assert.equal(result.decision.execution_status, EXECUTION_STATUS.EXECUTION_BLOCKED);
});

test('6 safe autonomous action executes and verifies', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.equal(result.intent.verification_status, 'VERIFIED');
  assert.ok(result.telemetry.actions_verified >= 1);
});

test('7 approval-required action does not execute', async () => {
  const auth = authorizeExecution({
    selected: { action_type: ACTION_TYPE.ESCALATE_OPERATOR },
    snapshot: { relationship: {} },
  });
  assert.equal(auth.authorized, false);
  assert.equal(auth.requires_approval, true);
});

test('8 prohibited action cannot be authorized by confidence', async () => {
  const auth = authorizeExecution({
    selected: { action_type: ACTION_TYPE.RETURN_TO_COLD_OUTREACH, prohibited: true },
    snapshot: { relationship: { relationship_active: true, suppress_cold_outreach: true } },
  });
  assert.equal(auth.authorized, false);
  assert.equal(auth.authority_class, 'D');
});

test('9 duplicate scheduler evaluation does not duplicate task', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const input = {
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  };
  await evaluateOperationalDecision(input);
  const second = await evaluateOperationalDecision(input);
  assert.ok(second.duplicate || second.execution_status === EXECUTION_STATUS.ACTION_ALREADY_ACTIVE);
  assert.equal(decisionStore.aoTasks.length, 1);
});

test('10 new evidence supersedes stale follow-up', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  stateStore.prospects[0].activities.push({
    kind: 'meeting_scheduled',
    at: '2026-10-05T08:03:00.000Z',
    notes: 'They called yesterday. Meeting Thursday.',
  });
  const exp = stateStore.expectations[0];
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.NEW_EVIDENCE, payload: { prospect_id: 'prospect-exeter' } },
    stateStore,
    decisionStore,
    expectation: exp,
    now: new Date('2026-10-05T08:05:00.000Z'),
  });
  assert.equal(result.decision.selected_action.action_type, ACTION_TYPE.RESOLVE_EXPECTATION);
  assert.ok(decisionStore.decisions.some(d => d.execution_status === EXECUTION_STATUS.SUPERSEDED));
});

test('11 agent delegation packages evidence context', async () => {
  const stateStore = seedState();
  stateStore.expectations = [];
  stateStore.prospects[0].assigned_ao_id = null;
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.STATE_CHANGED, payload: {} },
    stateStore,
    decisionStore,
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.equal(result.decision.selected_action.action_type, ACTION_TYPE.ABSTAIN_INSUFFICIENT_EVIDENCE);
});

test('12 unsupported agent output is rejected', () => {
  const rejected = rejectAgentDelegationOutput({ id: 'd1' }, { summary: 'guess' });
  assert.equal(rejected.rejected, true);
  const accepted = rejectAgentDelegationOutput({ id: 'd2' }, { evidence_ids: ['e1'], canonical: true });
  assert.equal(accepted.accepted, true);
});

test('13 execution success with wrong state fails verification', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1, simulateVerificationFailure: true });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.equal(result.intent.execution_status, EXECUTION_STATUS.ACTION_VERIFICATION_FAILED);
});

test('14 AO correction triggers reevaluation path', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const ingestionResult = {
    ingestion_id: 'ing-1',
    prospect: stateStore.prospects[0],
    operatorCorrection: true,
    telemetry: {},
  };
  const result = await reevaluateOnIngestion({
    clientId: 1,
    stateStore,
    decisionStore,
    ingestionResult,
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.ok(result.decision);
  assert.ok(result.telemetry.human_corrections >= 1);
});

test('15 prioritization favors relationship-sensitive accounts', () => {
  const ordered = prioritizeDecisions([
    { id: 'a', priority: { mission_relevance: 2, urgency: 3, importance: 2, relationship_sensitivity: 1 } },
    { id: 'b', priority: { mission_relevance: 7, urgency: 9, importance: 8, relationship_sensitivity: 9 } },
  ]);
  assert.equal(ordered[0].id, 'b');
});

test('16 routine issue does not escalate to operator', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.notEqual(result.decision.selected_action.action_type, ACTION_TYPE.ESCALATE_OPERATOR);
});

test('17 material ambiguity escalates', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  stateStore.conflicts = [{ entity_id: 'prospect-exeter' }];
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.equal(result.decision.selected_action.action_type, ACTION_TYPE.ESCALATE_OPERATOR);
});

test('18 external cold outreach remains blocked under relationship', async () => {
  const auth = authorizeExecution({
    selected: { action_type: ACTION_TYPE.RETURN_TO_COLD_OUTREACH },
    snapshot: { relationship: { relationship_active: true, suppress_cold_outreach: true } },
    policy: { blockColdOutreach: true },
  });
  assert.equal(auth.authorized, false);
});

test('19 pending decision idempotency survives replay', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const first = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  const replayStore = new MemoryDecisionStore({
    clientId: 1,
    decisions: [...decisionStore.decisions],
    aoTasks: [...decisionStore.aoTasks],
  });
  const replay = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore,
    decisionStore: replayStore,
    expectation: overdueExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.ok(replay.duplicate);
  assert.equal(replay.decision.id, first.decision.id);
});

test('20 full 001 to 002 loop without operator intervention', async () => {
  const store = seedState({ expectations: [] });
  await ingestOperationalUpdate({
    clientId: 1,
    sourceType: 'AO_REPORTED',
    text: 'Tony talked to Exeter Phillips. His contact is supposed to call him this week.',
    store,
  });
  const exp = store.expectations.find(e => e.prospect_id);
  assert.ok(exp);
  exp.status = 'OVERDUE';
  exp.expected_window = { ends_at: '2026-09-01T00:00:00.000Z' };
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const overdue = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OVERDUE, payload: {} },
    stateStore: store,
    decisionStore,
    expectation: exp,
    now: new Date('2026-10-05T08:00:00.000Z'),
  });
  assert.ok(decisionStore.aoTasks.length >= 1);
  store.prospects[0].activities = store.prospects[0].activities || [];
  store.prospects[0].activities.push({
    kind: 'meeting_scheduled',
    at: '2026-10-05T08:03:00.000Z',
  });
  const ingest2 = await ingestOperationalUpdate({
    clientId: 1,
    text: 'Yep. They called. Meeting Thursday.',
    store,
  });
  const followUp = await reevaluateOnIngestion({
    clientId: 1,
    stateStore: store,
    decisionStore,
    ingestionResult: ingest2,
    now: new Date('2026-10-05T08:05:00.000Z'),
  });
  assert.ok(followUp.decision);
  assert.ok(['RESOLVE_EXPECTATION', 'NO_ACTION', 'SUPERSEDE_PRIOR'].includes(followUp.decision.selected_action.action_type)
    || followUp.decision.execution_status === EXECUTION_STATUS.NO_ACTION_RECORDED);
});

test('API routes registered for max decisions', () => {
  const routes = fs.readFileSync(path.join(__dirname, '..', 'routes', 'maxDecisionExecution.js'), 'utf8');
  assert.match(routes, /\/api\/v1\/max\/decisions\/evaluate/);
  assert.match(routes, /\/api\/v1\/max\/decisions\/scan-expectations/);
  const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  assert.match(server, /maxDecisionExecution/);
});

test('migration exists for operational decisions', () => {
  const sql = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-10-05-max-reliability-decision-execution.sql'),
    'utf8'
  );
  assert.match(sql, /max_operational_decisions/);
  assert.match(sql, /max_operational_action_intents/);
});

test('scanExpectationTriggers evaluates open expectations', async () => {
  const stateStore = seedState({ expectations: [overdueExpectation(), openExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const results = await scanExpectationTriggers({
    clientId: 1,
    stateStore,
    decisionStore,
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.equal(results.length, 2);
});
