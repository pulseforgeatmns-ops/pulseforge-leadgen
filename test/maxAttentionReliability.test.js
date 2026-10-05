'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {
  evaluateOperationalDecision,
  reevaluateOnIngestion,
  MemoryDecisionStore,
  DECISION_TRIGGER,
  ACTION_TYPE,
  EXECUTION_STATUS,
} = require('../packages/max/decisionExecution');
const {
  MemoryStateStore,
  ingestOperationalUpdate,
} = require('../packages/max/stateIngestion');
const {
  MemoryAttentionStore,
  runAttentionCycle,
  syncAttentionFromDecision,
  wakeAttentionForIngestion,
  ATTENTION_STATUS,
  REVIEW_TRIGGER,
  AUDIENCE_TIER,
} = require('../packages/max/attention');

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
      acquisition_metadata: { maxStateIngestion: { relationship_active: true } },
      activities: [],
      ...(overrides.prospectPatch || {}),
    }],
    expectations: overrides.expectations || [],
    ...overrides,
  });
}

function openExpectation() {
  return {
    id: 'exp-open',
    client_id: 1,
    prospect_id: 'prospect-exeter',
    ao_id: 10,
    expectation_type: 'inbound_call',
    status: 'WAITING',
    expected_window: { ends_at: '2026-10-10T00:00:00.000Z' },
    source_evidence: { account_name: 'Exeter Phillips', ao_name: 'Tony' },
  };
}

test('1 open expectation decision creates WAITING attention with TIME review', async () => {
  const stateStore = seedState({ expectations: [openExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const attentionStore = new MemoryAttentionStore({ clientId: 1 });
  const result = await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OPEN, payload: {} },
    stateStore,
    decisionStore,
    attentionStore,
    expectation: openExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.equal(result.decision.selected_action.action_type, ACTION_TYPE.NO_ACTION);
  assert.equal(attentionStore.items.length, 1);
  const item = attentionStore.items[0];
  assert.equal(item.status, ATTENTION_STATUS.WAITING);
  assert.equal(item.review_trigger, REVIEW_TRIGGER.TIME);
  assert.equal(item.audience_tier, AUDIENCE_TIER.WAITING_SILENT);
  assert.ok(item.next_review_at);
});

test('2 repeated evaluations dedupe to one unresolved attention item', async () => {
  const stateStore = seedState({ expectations: [openExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const attentionStore = new MemoryAttentionStore({ clientId: 1 });
  const exp = openExpectation();
  const opts = {
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OPEN, payload: {} },
    stateStore,
    decisionStore,
    attentionStore,
    expectation: exp,
    now: new Date('2026-10-05T00:00:00.000Z'),
  };
  await evaluateOperationalDecision(opts);
  await evaluateOperationalDecision(opts);
  const unresolved = attentionStore.items.filter(i => i.status !== ATTENTION_STATUS.RESOLVED);
  assert.equal(unresolved.length, 1);
});

test('3 scheduler scan alone does not resolve WAITING attention', async () => {
  const stateStore = seedState({ expectations: [openExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const attentionStore = new MemoryAttentionStore({ clientId: 1 });
  const exp = openExpectation();
  await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OPEN, payload: {} },
    stateStore,
    decisionStore,
    attentionStore,
    expectation: exp,
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  const item = attentionStore.items[0];
  item.next_review_at = '2026-10-04T00:00:00.000Z';
  await runAttentionCycle({
    clientId: 1,
    stateStore,
    decisionStore,
    attentionStore,
    now: new Date('2026-10-05T12:00:00.000Z'),
  });
  const after = attentionStore.items.find(i => i.id === item.id);
  assert.notEqual(after.status, ATTENTION_STATUS.RESOLVED);
});

test('4 ingestion wakes attention before re-evaluation', async () => {
  const store = seedState({ expectations: [openExpectation()] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const attentionStore = new MemoryAttentionStore({ clientId: 1 });
  await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OPEN, payload: {} },
    stateStore: store,
    decisionStore,
    attentionStore,
    expectation: openExpectation(),
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  const ingest = await ingestOperationalUpdate({
    clientId: 1,
    text: 'Yep. They called. Meeting Thursday.',
    store,
  });
  await wakeAttentionForIngestion({
    attentionStore,
    clientId: 1,
    ingestionResult: ingest,
    stateStore: store,
    now: new Date('2026-10-05T08:05:00.000Z'),
  });
  const item = attentionStore.items[0];
  assert.equal(item.review_trigger, REVIEW_TRIGGER.EVIDENCE);
  assert.ok(
    item.supporting_evidence.some(e => e.kind === 'ingestion' && e.ingestion_id === ingest.ingestion_id),
    'ingestion should wake attention with durable evidence'
  );
});

test('5 restart recovery: overdue attention runs 002 exactly once via claim', async () => {
  const exp = openExpectation();
  exp.status = 'OVERDUE';
  exp.expected_window = { ends_at: '2026-10-01T00:00:00.000Z' };
  const stateStore = seedState({ expectations: [exp] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const attentionStore = new MemoryAttentionStore({ clientId: 1 });
  await syncAttentionFromDecision({
    attentionStore,
    decision: {
      id: 'dec-prior',
      client_id: 1,
      subject_type: 'expectation',
      subject_id: exp.id,
      selected_action: { action_type: ACTION_TYPE.NO_ACTION },
      supporting_evidence: [],
      priority: {},
      reevaluate_after: '2026-10-01T00:00:00.000Z',
      owner: 'Tony',
      execution_status: EXECUTION_STATUS.NO_ACTION_RECORDED,
      canonical_state_snapshot: { account: { name: 'Exeter Phillips' } },
    },
    now: new Date('2026-10-01T12:00:00.000Z'),
  });
  const item = attentionStore.items[0];
  item.next_review_at = '2026-10-01T00:00:00.000Z';

  const cycle1 = await runAttentionCycle({
    clientId: 1,
    stateStore,
    decisionStore,
    attentionStore,
    now: new Date('2026-10-05T08:00:00.000Z'),
  });
  assert.equal(cycle1.claimed, 1);
  assert.equal(cycle1.evaluated, 1);
  assert.ok(decisionStore.aoTasks.length >= 1 || decisionStore.decisions.length >= 1);

  const cycle2 = await runAttentionCycle({
    clientId: 1,
    stateStore,
    decisionStore,
    attentionStore,
    now: new Date('2026-10-05T08:05:00.000Z'),
  });
  assert.equal(cycle2.claimed, 0);
});

test('6 full 001→002→003 chain without operator intervention', async () => {
  const store = seedState({ expectations: [] });
  const decisionStore = new MemoryDecisionStore({ clientId: 1 });
  const attentionStore = new MemoryAttentionStore({ clientId: 1 });

  await ingestOperationalUpdate({
    clientId: 1,
    sourceType: 'AO_REPORTED',
    text: 'Tony talked to Exeter Phillips. His contact is supposed to call him this week.',
    store,
  });
  const exp = store.expectations.find(e => e.prospect_id);
  assert.ok(exp);

  await evaluateOperationalDecision({
    clientId: 1,
    trigger: { type: DECISION_TRIGGER.EXPECTATION_OPEN, payload: {} },
    stateStore: store,
    decisionStore,
    attentionStore,
    expectation: exp,
    now: new Date('2026-10-05T00:00:00.000Z'),
  });
  assert.equal(attentionStore.items.length, 1);
  assert.ok(
    [ATTENTION_STATUS.WAITING, ATTENTION_STATUS.ACTIVE].includes(attentionStore.items[0].status),
    'unresolved attention recorded after open-window NO_ACTION'
  );

  exp.status = 'OVERDUE';
  exp.expected_window = { ends_at: '2026-10-03T00:00:00.000Z' };
  attentionStore.items[0].next_review_at = '2026-10-03T00:00:00.000Z';
  attentionStore.items[0].status = ATTENTION_STATUS.OVERDUE;

  await runAttentionCycle({
    clientId: 1,
    stateStore: store,
    decisionStore,
    attentionStore,
    now: new Date('2026-10-05T08:00:00.000Z'),
  });
  assert.ok(decisionStore.aoTasks.length >= 1);

  store.prospects[0].activities = [{ kind: 'meeting_scheduled', at: '2026-10-05T08:03:00.000Z' }];
  const ingest2 = await ingestOperationalUpdate({
    clientId: 1,
    text: 'Yep. They called. Meeting Tuesday.',
    store,
  });
  await reevaluateOnIngestion({
    clientId: 1,
    stateStore: store,
    decisionStore,
    attentionStore,
    ingestionResult: ingest2,
    now: new Date('2026-10-05T08:05:00.000Z'),
  });

  const resolved = attentionStore.items.some(i => i.status === ATTENTION_STATUS.RESOLVED);
  assert.ok(resolved || attentionStore.items.some(i => i.resolution_evidence?.length));
});

test('7 routes and migration registered', () => {
  const routes = fs.readFileSync(path.join(__dirname, '..', 'routes', 'maxAttention.js'), 'utf8');
  assert.match(routes, /\/api\/v1\/max\/attention\/cycle/);
  const cron = fs.readFileSync(path.join(__dirname, '..', 'routes', 'cron.js'), 'utf8');
  assert.match(cron, /max-attention-cycle/);
  const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  assert.match(server, /maxAttention/);
  const sql = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-10-05-max-reliability-attention.sql'),
    'utf8'
  );
  assert.match(sql, /max_attention_items/);
});
