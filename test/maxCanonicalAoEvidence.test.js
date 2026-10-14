'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { MemoryStateStore } = require('../packages/max/stateIngestion');
const { MemoryDecisionStore, evaluateOperationalDecision, reevaluateOnIngestion } = require('../packages/max/decisionExecution');
const { MemoryAttentionStore } = require('../packages/max/attention');
const { buildUnderstanding } = require('../packages/max/decisionExecution/understand');
const { normalizeAoActivity } = require('../packages/max/decisionExecution/activityEvidence');
const { wakeAttentionForIngestion } = require('../packages/max/attention/wake');

const now = new Date('2026-10-14T00:00:00Z');
function context() {
  return { clientId: 1, now,
    stateStore: new MemoryStateStore({ clientId: 1,
      prospects: [{ id: 'p1', client_id: 1, assigned_ao_id: 10, activities: [] }],
      expectations: [{ id: 'e1', client_id: 1, prospect_id: 'p1', ao_id: 10,
        expectation_type: 'inbound_call', status: 'OVERDUE',
        expected_window: { ends_at: '2026-10-01T00:00:00Z' } }] }),
    decisionStore: new MemoryDecisionStore(), attentionStore: new MemoryAttentionStore({ clientId: 1 }) };
}
function evaluate(c, expectation = c.stateStore.expectations[0]) {
  return evaluateOperationalDecision({ ...c, expectation,
    trigger: { type: 'NEW_EVIDENCE', payload: {} } });
}
function resolving(c) {
  c.stateStore.prospects[0].activities.push({ kind: 'call_completed', at: '2026-10-13T00:00:00Z' });
}

test('AO translator promotes only structured positive outcomes with full provenance', () => {
  const row = { id: 'a1', tenant_id: 1, prospect_id: 'p1', ao_id: 10,
    activity_type: 'outcome_logged', outcome: 'spoke_decision_maker', metadata: {}, created_at: now };
  const normalized = normalizeAoActivity(row);
  assert.equal(normalized.kind, 'call_completed');
  assert.equal(normalized.id, row.id);
  assert.equal(normalized.at, now);
  assert.equal(normalized.source, 'ao_prospect_activity');
  for (const patch of [
    { activity_type: 'note', notes: 'They called and booked a meeting' },
    { outcome: 'called_no_answer' }, { outcome: 'left_voicemail' },
    { outcome: 'spoke_gatekeeper' }, { outcome: 'interested' },
    { outcome: 'needs_jake' }, { outcome: 'unknown' },
    { metadata: { internal_outcome: 'called_no_answer' } },
  ]) assert.equal(normalizeAoActivity({ ...row, ...patch }).kind, null);
});

test('missing memory history is unknown; an explicit scoped empty array is known zero', async () => {
  const c = context();
  const input = { ...c, expectation: c.stateStore.expectations[0], trigger: { type: 'NEW_EVIDENCE' } };
  assert.equal((await buildUnderstanding(input)).snapshot.subsequent_activity_count, 0);
  delete c.stateStore.prospects[0].activities;
  const unknown = await buildUnderstanding(input);
  assert.equal(unknown.snapshot.activity_read_status, 'unavailable');
  assert.equal(unknown.snapshot.subsequent_activity_count, null);
  assert.equal(unknown.snapshot.epistemic.subsequent_activity, 'UNKNOWN');
  const result = await evaluate(c);
  assert.equal(result.decision.execution_status, 'INSUFFICIENT_EVIDENCE');
  assert.equal(result.intent, null);
});

test('a supplied stale expectation cannot replace canonical status or window', async () => {
  const c = context();
  const supplied = structuredClone(c.stateStore.expectations[0]);
  c.stateStore.expectations[0].status = 'WAITING';
  c.stateStore.expectations[0].expected_window.ends_at = '2026-10-20T00:00:00Z';
  resolving(c);
  const result = await evaluate(c, supplied);
  assert.equal(result.snapshot.expectation.status, 'WAITING');
  assert.equal(result.decision.selected_action.action_type, 'NO_ACTION');
  assert.equal(c.decisionStore.aoTasks.length, 0);
  assert.equal(c.stateStore.expectations[0].status, 'WAITING');
});

test('canonical resolved/missing or changed-AO expectations cannot execute stale followups', async () => {
  for (const change of ['resolved', 'missing', 'owner']) {
    const c = context();
    const supplied = structuredClone(c.stateStore.expectations[0]);
    if (change === 'resolved') c.stateStore.expectations[0].status = 'RESOLVED';
    if (change === 'missing') c.stateStore.expectations = [];
    if (change === 'owner') c.stateStore.prospects[0].assigned_ao_id = 11;
    resolving(c);
    const result = await evaluate(c, supplied);
    assert.ok(['NO_ACTION', 'ABSTAIN_INSUFFICIENT_EVIDENCE'].includes(result.decision.selected_action.action_type));
    assert.equal(c.decisionStore.aoTasks.length, 0);
    assert.equal(result.intent, null);
  }
});

test('ingestion with multiple possible subjects cannot arbitrarily wake attention', async () => {
  const c = context();
  c.stateStore.prospects.push({ id: 'p2', client_id: 1, assigned_ao_id: 10, activities: [] });
  c.stateStore.expectations.push({ ...c.stateStore.expectations[0], id: 'e2', prospect_id: 'p2' });
  await c.attentionStore.createAttention({ id: 'attention-1', client_id: 1,
    subject_type: 'expectation', subject_id: 'e1', status: 'WAITING', dedup_fingerprint: 'f1' });
  const result = await reevaluateOnIngestion({ ...c, ingestionResult: { ingestion_id: 'i1' } });
  assert.equal(result.reason, 'ambiguous_ingestion_subject');
  assert.equal(c.attentionStore.items[0].status, 'WAITING');
  assert.deepEqual(await wakeAttentionForIngestion({ ...c, ingestionResult: { ingestion_id: 'i1' } }), []);
});

test('ingestion read failure on a later subject has no wake or action on an earlier one', async () => {
  const c = context();
  const read = c.stateStore.readProspect.bind(c.stateStore);
  c.stateStore.readProspect = async id => { if (id === 'p2') throw Error('unavailable'); return read(id); };
  await c.attentionStore.createAttention({ id: 'attention-1', client_id: 1,
    subject_type: 'expectation', subject_id: 'e1', status: 'WAITING', dedup_fingerprint: 'f1' });
  const result = await reevaluateOnIngestion({ ...c, ingestionResult: {
    ingestion_id: 'i1', prospects: [{ id: 'p1' }, { id: 'p2' }] } });
  assert.equal(result.reason, 'canonical_context_unavailable');
  assert.equal(c.attentionStore.items[0].status, 'WAITING');
  assert.equal(c.decisionStore.decisions.length, 0);
});

test('partial resolution supersession failure is visible; retry repairs only verified bookkeeping', async () => {
  const c = context();
  const prior = await evaluate(c);
  resolving(c);
  const original = c.decisionStore.supersedeActiveDecisions.bind(c.decisionStore);
  c.decisionStore.supersedeActiveDecisions = async () => { throw Error('supersession write failed'); };
  const ingest = { ...c, ingestionResult: { ingestion_id: 'i1', prospect: { id: 'p1' } } };
  await assert.rejects(reevaluateOnIngestion(ingest), /supersession write failed/);
  assert.equal(c.stateStore.expectations[0].status, 'RESOLVED');
  assert.equal(c.decisionStore.aoTasks[0].status, 'open');
  c.stateStore.updateExpectation = async () => assert.fail('retry cannot repeat resolution');
  c.decisionStore.supersedeActiveDecisions = original;
  const replay = await reevaluateOnIngestion(ingest);
  assert.equal(replay.duplicate, true);
  assert.equal(replay.decision.verification_status, 'VERIFIED');
  assert.equal(c.decisionStore.aoTasks[0].status, 'superseded');
  assert.equal(c.decisionStore.decisions.find(d => d.id === prior.decision.id).execution_status, 'SUPERSEDED');
  assert.equal(c.decisionStore.actionIntents.length, 2);
});


test('resolution replay takes precedence over an older no-action receipt for the same expectation', async () => {
  const c = context();
  const exp = c.stateStore.expectations[0];
  exp.status = 'WAITING';
  exp.expected_window.ends_at = '2026-10-20T00:00:00Z';
  const earlier = await evaluate(c);
  assert.equal(earlier.decision.selected_action.action_type, 'NO_ACTION');
  exp.status = 'OVERDUE';
  exp.expected_window.ends_at = '2026-10-01T00:00:00Z';
  resolving(c);
  const resolved = await evaluate(c);
  assert.equal(resolved.decision.verification_status, 'VERIFIED');
  c.stateStore.updateExpectation = async () => assert.fail('replay must not resolve again');
  const replay = await evaluate(c);
  assert.equal(replay.duplicate, true);
  assert.equal(replay.decision.id, resolved.decision.id);
  assert.equal(replay.decision.selected_action.action_type, 'RESOLVE_EXPECTATION');
  assert.equal(replay.decision.verification_status, 'VERIFIED');
});


test('scope-less ingestion retries the original single subject after partial supersession failure', async () => {
  const c = context();
  const prior = await evaluate(c);
  resolving(c);
  const supersede = c.decisionStore.supersedeActiveDecisions.bind(c.decisionStore);
  c.decisionStore.supersedeActiveDecisions = async () => { throw Error('scope-less supersession failure'); };
  const input = { ...c, ingestionResult: { ingestion_id: 'single-subject-retry' } };
  await assert.rejects(reevaluateOnIngestion(input), /scope-less supersession failure/);
  assert.equal(c.stateStore.expectations[0].status, 'RESOLVED');
  assert.equal(c.decisionStore.aoTasks[0].status, 'open');
  // A new unrelated open expectation cannot redirect the already scoped retry.
  c.stateStore.prospects.push({ id: 'p2', client_id: 1, assigned_ao_id: 10, activities: [] });
  c.stateStore.expectations.push({ ...c.stateStore.expectations[0], id: 'e2', prospect_id: 'p2', status: 'OVERDUE' });
  c.stateStore.updateExpectation = async () => assert.fail('retry cannot repeat resolution');
  c.decisionStore.supersedeActiveDecisions = supersede;
  const result = await reevaluateOnIngestion(input);
  assert.equal(result.duplicate, true);
  assert.equal(result.decision.subject_id, 'e1');
  assert.equal(result.decision.verification_status, 'VERIFIED');
  assert.equal(c.decisionStore.decisions.find(d => d.id === prior.decision.id).execution_status, 'SUPERSEDED');
  assert.equal(c.decisionStore.aoTasks[0].status, 'superseded');
  assert.equal(c.stateStore.expectations[1].status, 'OVERDUE');
  assert.equal(c.decisionStore.actionIntents.length, 2);
});
