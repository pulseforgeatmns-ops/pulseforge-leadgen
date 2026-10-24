'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const {
  MemoryAttentionStore, wakeAttentionForSubject, wakeAttentionForIngestion,
  runAttentionCycle, ATTENTION_STATUS, REVIEW_TRIGGER, AUDIENCE_TIER,
} = require('../packages/max/attention');
const { evaluateAttentionItem } = require('../packages/max/attention/scheduler');
const { MANUAL_REVIEW_NAMESPACE, MANUAL_REVIEW_PREFIX, isManualReviewOnlyAttention } = require('../packages/max/attention/manualReviewOnly');
const { PostgresAttentionStore } = require('../packages/max/attention/store/postgresStore');
const { MemoryStateStore } = require('../packages/max/stateIngestion');
const { MemoryDecisionStore } = require('../packages/max/decisionExecution');
const { SCOPE, applyAoHelpReview, previewAoHelpReview, reconcileAoHelpReview } = require('../services/maxAoHelpReviewService');
const { parseArgs } = require('../scripts/reviewMaxAoHelpEvent');

// All dependencies are in-memory stores or injected query recorders. This suite
// must not load the shared application pool, initialize schema, or call providers.
const now = new Date('2026-10-20T12:00:00.000Z');
const clientId = 10;
const prospectId = 'fixture-prospect';
const markers = [
  { name: 'reserved fingerprint alone', patch: { dedup_fingerprint: `${MANUAL_REVIEW_PREFIX}10:fixture` } },
  { name: 'priority flag alone', patch: { priority: { manual_review_only: true } } },
  { name: 'false reserved flag', patch: { priority: { manual_review_only: false } } },
  { name: 'string reserved flag', patch: { priority: { manual_review_only: 'true' } } },
  { name: 'null reserved flag', patch: { priority: { manual_review_only: null } } },
  { name: 'malformed reserved namespace', patch: { dedup_fingerprint: 'ao-help-review:unexpected-version' } },
  { name: 'array reserved flag', patch: { priority: ['manual_review_only'] } },
  { name: 'both markers', patch: { dedup_fingerprint: `${MANUAL_REVIEW_PREFIX}10:fixture-both`, priority: { manual_review_only: true } } },
];

function item(id, patch = {}) {
  return {
    id, client_id: clientId, subject_type: 'prospect', subject_id: prospectId,
    status: ATTENTION_STATUS.WAITING, review_trigger: REVIEW_TRIGGER.MANUAL,
    audience_tier: AUDIENCE_TIER.OPERATOR_VISIBLE, dedup_fingerprint: `ordinary:${id}`,
    priority: {}, next_review_at: '2026-10-19T12:00:00.000Z',
    supporting_evidence: [{ kind: 'existing', id: 'source-1' }],
    last_reviewed_at: '2026-10-19T11:00:00.000Z',
    claim_token: null, claimed_at: null, execution_failures: 0,
    ...patch,
  };
}

function attentionStore() {
  return new MemoryAttentionStore({ clientId, clock: () => now.getTime() });
}

function assertNoDecisionEffects(store) {
  for (const key of ['decisions', 'actionIntents', 'aoTasks', 'delegations', 'escalations']) {
    assert.deepEqual(store[key], [], `${key} must remain empty`);
  }
}

test('reserved review namespace or marker presence fails closed, not ordinary visibility/manual status', () => {
  assert.equal(MANUAL_REVIEW_PREFIX, 'ao-help-review:v1:');
  for (const { patch } of markers) assert.equal(isManualReviewOnlyAttention(item('review', patch)), true);
  for (const value of [
    null, undefined, {}, item('ordinary'),
    item('embedded', { dedup_fingerprint: 'other:ao-help-review:v1:10:fixture' }),
  ]) assert.equal(isManualReviewOnlyAttention(value), false);
});

test('subject wake leaves every review-only row unchanged while ordinary MANUAL/operator attention wakes', async () => {
  const store = attentionStore();
  for (const [index, { patch }] of markers.entries()) await store.createAttention(item(`review-${index}`, patch));
  await store.createAttention(item('ordinary'));
  await store.createAttention(item('other-client', { client_id: 11 }));
  await store.createAttention(item('other-prospect', { subject_id: 'other-prospect' }));
  const before = structuredClone(store.items);
  const evidence = [{ kind: 'new-evidence', id: 'source-2' }];
  const woken = await wakeAttentionForSubject({
    attentionStore: store, clientId, subjectType: 'prospect', subjectId: prospectId, evidence, now,
  });
  assert.deepEqual(woken.map(row => row.id), ['ordinary']);
  for (const previous of before.filter(row => row.id !== 'ordinary')) {
    assert.deepEqual(await store.findById(previous.id), previous);
  }
  const ordinary = await store.findById('ordinary');
  assert.equal(ordinary.status, ATTENTION_STATUS.ACTIVE);
  assert.equal(ordinary.review_trigger, REVIEW_TRIGGER.EVIDENCE);
  assert.equal(ordinary.next_review_at, now.toISOString());
  assert.equal(ordinary.last_reviewed_at, now.toISOString());
  assert.deepEqual(ordinary.supporting_evidence, [...before.find(row => row.id === 'ordinary').supporting_evidence, ...evidence]);
});

test('ingestion wake preserves marked prospect and expectation rows, and wakes only ordinary related rows', async () => {
  const store = attentionStore();
  const expectations = [{ id: 'expectation-1', client_id: clientId, prospect_id: prospectId }];
  for (const [index, { patch }] of markers.entries()) {
    await store.createAttention(item(`prospect-review-${index}`, patch));
    await store.createAttention(item(`expectation-review-${index}`, {
      ...patch, subject_type: 'expectation', subject_id: 'expectation-1',
    }));
  }
  await store.createAttention(item('ordinary-prospect'));
  await store.createAttention(item('ordinary-expectation', { subject_type: 'expectation', subject_id: 'expectation-1' }));
  await store.createAttention(item('unrelated-expectation', { subject_type: 'expectation', subject_id: 'expectation-other' }));
  await store.createAttention(item('other-client', { client_id: 11 }));
  const before = structuredClone(store.items);
  const woken = await wakeAttentionForIngestion({
    attentionStore: store, clientId, now, expectations,
    ingestionResult: { ingestion_id: 'fixture-ingestion', prospect: { id: prospectId } },
  });
  assert.deepEqual(woken.map(row => row.id), ['ordinary-prospect', 'ordinary-expectation']);
  for (const previous of before.filter(row => !row.id.startsWith('ordinary-'))) {
    assert.deepEqual(await store.findById(previous.id), previous);
  }
  for (const row of woken) {
    assert.equal(row.status, ATTENTION_STATUS.ACTIVE);
    assert.equal(row.review_trigger, REVIEW_TRIGGER.EVIDENCE);
    assert.equal(row.next_review_at, now.toISOString());
    assert.deepEqual(row.supporting_evidence.at(-1), {
      kind: 'ingestion', ingestion_id: 'fixture-ingestion', at: now.toISOString(),
    });
  }
});

test('due/claim filtering skips corrupted review rows before the limit without starving ordinary attention', async () => {
  const store = attentionStore();
  for (const [index, { patch }] of markers.entries()) {
    await store.createAttention(item(`corrupted-review-${index}`, {
      ...patch, status: ATTENTION_STATUS.ACTIVE, review_trigger: REVIEW_TRIGGER.EVIDENCE,
    }));
  }
  await store.createAttention(item('ordinary'));
  const before = structuredClone(store.items);
  assert.deepEqual((await store.listDueForReview({ clientId, now, limit: 1 })).map(row => row.id), ['ordinary']);
  const claimed = await store.claimDueItems({ clientId, now, limit: 1, claimToken: 'fixture-owner' });
  assert.deepEqual(claimed.map(row => row.id), ['ordinary']);
  assert.equal(claimed[0].claim_token, 'fixture-owner');
  assert.equal(claimed[0].status, ATTENTION_STATUS.OVERDUE);
  for (const previous of before.filter(row => row.id !== 'ordinary')) {
    assert.deepEqual(await store.findById(previous.id), previous);
  }
  assert.deepEqual(await store.claimDueItems({ clientId, now, limit: 20, claimToken: 'another-owner' }), []);
});

for (const { name, patch } of markers) {
  test(`ownership-locked evaluation skips ${name} added after claim without downstream effects`, async () => {
    const store = attentionStore();
    await store.createAttention(item('changed-after-claim'));
    const [claimed] = await store.claimDueItems({ clientId, now, limit: 1, claimToken: 'fixture-owner' });
    assert.equal(isManualReviewOnlyAttention(claimed), false);
    await store.updateAttention(claimed.id, { ...patch, review_trigger: REVIEW_TRIGGER.EVIDENCE });
    const before = structuredClone(await store.findById(claimed.id));
    const stateStore = new MemoryStateStore({ clientId });
    const decisionStore = new MemoryDecisionStore({ clientId });
    let downstreamCalls = 0;
    const unexpected = async () => { downstreamCalls += 1; throw new Error('Review-only evaluation must not execute'); };
    for (const key of ['readProspect', 'readProspectActivities', 'readExpectation', 'applyMutation', 'persistIngestion']) {
      stateStore[key] = unexpected;
    }
    for (const key of ['persistDecision', 'persistActionIntent', 'createAoTask', 'createDelegation', 'createEscalation']) {
      decisionStore[key] = unexpected;
    }
    const result = await evaluateAttentionItem({
      item: claimed, claimToken: 'fixture-owner', clientId, now,
      attentionStore: store, stateStore, decisionStore,
    });
    assert.deepEqual(result, { skipped: true });
    assert.equal(downstreamCalls, 0, 'the fence must stop before state reads, decisions, tasks or provider-capable delegation');
    assertNoDecisionEffects(decisionStore);
    assert.deepEqual(stateStore.mutations, []);
    assert.deepEqual(await store.findById(claimed.id), { ...before, claim_token: null, claimed_at: null });
  });
}

test('ordinary OPERATOR_VISIBLE/MANUAL attention is still evaluated and can create its existing AO task', async () => {
  const store = attentionStore();
  const stateStore = new MemoryStateStore({
    clientId, users: [{ id: 24, name: 'Fixture AO' }],
    prospects: [{ id: prospectId, client_id: clientId, assigned_ao_id: 24, relationship_active: true, activities: [] }],
    expectations: [{ id: 'fixture-expectation', client_id: clientId, prospect_id: prospectId, ao_id: 24,
      status: 'OVERDUE', expectation_type: 'inbound_call', expected_window: { ends_at: '2026-10-01T00:00:00Z' } }],
  });
  const decisionStore = new MemoryDecisionStore({ clientId });
  await store.createAttention(item('ordinary', {
    subject_type: 'expectation', subject_id: 'fixture-expectation', priority: { urgency: 1 },
  }));
  const result = await runAttentionCycle({ clientId, now, attentionStore: store, stateStore, decisionStore });
  assert.equal(result.claimed, 1);
  assert.equal(result.evaluated, 1);
  assert.equal(result.failed, 0);
  assert.equal(decisionStore.decisions.length, 1);
  assert.equal(decisionStore.aoTasks.length, 1);
  assert.equal(decisionStore.aoTasks[0].prospect_id, prospectId);
  assert.equal(decisionStore.aoTasks[0].owner_id, 24);
});

test('PostgreSQL claim query applies both reserved markers inside its locking transaction', async () => {
  const calls = [];
  const connection = {
    async query(sql, args) { calls.push({ sql, args }); return { rows: [] }; },
    release() { calls.push({ sql: 'RELEASE' }); },
  };
  const store = new PostgresAttentionStore({ connect: async () => connection }, { clientId });
  assert.deepEqual(await store.claimDueItems({ clientId, now, limit: 1, claimToken: 'fixture-owner' }), []);
  const selection = calls.find(({ sql }) => /SELECT id FROM max_attention_items/.test(sql));
  assert.ok(selection);
  assert.match(selection.sql, /dedup_fingerprint NOT LIKE \$6/);
  assert.match(selection.sql, /NOT \(priority \? 'manual_review_only'\)/);
  assert.match(selection.sql, /FOR UPDATE SKIP LOCKED/);
  assert.equal(selection.args[5], `${MANUAL_REVIEW_NAMESPACE}%`);
  assert.equal(selection.args[0], clientId);
  assert.equal(selection.args[3], 1);
  assert.doesNotMatch(selection.sql, /audience_tier|review_trigger/);
  assert.deepEqual(calls.map(({ sql }) => sql.trim().split(/\s+/)[0]), ['BEGIN', 'SELECT', 'COMMIT', 'RELEASE']);
});

function operatorServiceWith(db) {
  const filename = path.join(__dirname, '../services/maxAttentionService.js');
  const module = { exports: {} };
  const dependencies = {
    '../db': db,
    '../packages/max/attention': require('../packages/max/attention'),
    '../packages/max/stateIngestion': require('../packages/max/stateIngestion'),
    '../packages/max/decisionExecution': require('../packages/max/decisionExecution'),
  };
  vm.compileFunction(fs.readFileSync(filename, 'utf8'), ['module', 'require'], { filename })(module, name => {
    assert.ok(Object.hasOwn(dependencies, name), `Unexpected live dependency: ${name}`);
    return dependencies[name];
  });
  return module.exports;
}

test('operator listing exposes review-only attention using SELECT only, without schema initialization', async () => {
  const queries = [];
  const review = item('review', { ...markers.find(marker => marker.name === 'both markers').patch, next_review_at: null });
  const db = { query: async (sql, args) => {
    queries.push({ sql, args });
    assert.match(sql, /^SELECT \* FROM max_attention_items WHERE client_id = \$1 AND status = ANY/);
    return { rows: [review, item('ordinary'), item('silent', { audience_tier: AUDIENCE_TIER.WAITING_SILENT })] };
  } };
  const listed = await operatorServiceWith(db).listOperatorAttention(clientId, { db, limit: 10 });
  assert.deepEqual(listed.map(row => row.id), ['review', 'ordinary']);
  assert.equal(listed[0].priority.manual_review_only, true);
  assert.equal(queries.length, 1);
  assert.equal(queries[0].args[0], clientId);
});

test('operator listing fails closed on missing schema and never attempts to create it', async () => {
  const queries = [];
  const failure = Object.assign(new Error('fixture missing relation'), { code: '42P01' });
  const db = { query: async sql => { queries.push(sql); throw failure; } };
  await assert.rejects(operatorServiceWith(db).listOperatorAttention(clientId, { db }), error => error === failure);
  assert.equal(queries.length, 1);
  assert.match(queries[0], /^SELECT /);
});

const scopeArgs = [
  `--client-id=${SCOPE.clientId}`, `--prospect-id=${SCOPE.prospectId}`,
  `--ao-id=${SCOPE.aoId}`, `--activity-id=${SCOPE.activityId}`,
];

test('CLI defaults to read-only preview and requires every exact approved event identifier', () => {
  assert.equal(parseArgs(scopeArgs).mode, 'preview');
  assert.equal(parseArgs([...scopeArgs, '--reconcile']).mode, 'reconcile');
  assert.throws(() => parseArgs([]), /Explicit approved event scope is required/);
  for (const [index, arg] of scopeArgs.entries()) {
    assert.throws(() => parseArgs(scopeArgs.filter((_, i) => i !== index)), /Explicit approved event scope is required/);
    assert.throws(() => parseArgs(scopeArgs.map((value, i) => i === index ? `${arg}-wrong` : value)),
      /Explicit approved event scope is required/);
  }
  assert.throws(() => parseArgs([...scopeArgs, scopeArgs[0]]), /Unknown or duplicate argument/);
  assert.throws(() => parseArgs([...scopeArgs, '--all-clients=true']), /Unknown or duplicate argument/);
  assert.throws(() => parseArgs([...scopeArgs, '--apply', '--reconcile']), /Choose only one mode/);
});

test('CLI apply requires exact review-only confirmation and read-only modes reject apply arguments', () => {
  assert.throws(() => parseArgs([...scopeArgs, '--apply']), /Explicit review-only confirmation is required/);
  assert.throws(() => parseArgs([...scopeArgs, '--apply', '--confirm=yes']), /Explicit review-only confirmation is required/);
  const confirmation = `--confirm=operator-review-only:${SCOPE.activityId}`;
  const version = 'a'.repeat(64);
  const parsed = parseArgs([...scopeArgs, '--apply', confirmation,
    `--expected-source-version=${version}`, `--preview-observed-at=${now.toISOString()}`]);
  assert.equal(parsed.mode, 'apply');
  assert.equal(parsed.expectedSourceVersion, version);
  assert.equal(parsed.previewObservedAt, now.toISOString());
  for (const mode of [[], ['--reconcile']]) {
    for (const arg of [confirmation, `--expected-source-version=${version}`, `--preview-observed-at=${now.toISOString()}`]) {
      assert.throws(() => parseArgs([...scopeArgs, ...mode, arg]), /Apply-only arguments are not allowed in read-only mode/);
    }
  }
});

test('service rejects wrong event scope and invalid preview inputs before opening any connection', async () => {
  let connections = 0;
  const pool = { connect: async () => { connections += 1; throw new Error('Unexpected database connection'); } };
  for (const action of [previewAoHelpReview, applyAoHelpReview, reconcileAoHelpReview]) {
    for (const key of ['clientId', 'prospectId', 'aoId', 'activityId']) {
      await assert.rejects(action({ pool, ...SCOPE, [key]: 'wrong' }), { code: 'AO_HELP_REVIEW_SCOPE_MISMATCH' });
    }
  }
  for (const input of [
    {},
    { expectedSourceVersion: 'a'.repeat(64) },
    { expectedSourceVersion: 'short', previewObservedAt: now.toISOString() },
    { expectedSourceVersion: 'g'.repeat(64), previewObservedAt: now.toISOString() },
    { expectedSourceVersion: 'a'.repeat(64), previewObservedAt: 'not-a-date' },
  ]) {
    await assert.rejects(applyAoHelpReview({ pool, ...SCOPE, ...input }), { code: 'AO_HELP_REVIEW_PREVIEW_REQUIRED' });
  }
  assert.equal(connections, 0);
});
