'use strict';
// P0 (production 2026-11-10 17:04, tenant 10): with an execution approval
// pending, "brief me on my prospects" was scored AMBIGUOUS by
// PendingDecisionResolver, the pending-decision owner kept the turn (SPEC-202)
// and Max re-showed the approval card ("I didn't catch a clear yes or no…")
// instead of the prospects. An explicit operational read with no decision
// signal now reaches the read handler; the decision stays pending, unchanged.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const A = require('../AmoOperatorApproval');
const { createHermeticWorkspaceEngine, unavailableOperatingEvidence } = require('./workspaceTestHarness');
const { createTestAmoRuntime, liveScoutStub } = require('./amoTestRuntime');
const { narrativeFixture } = require('./fixtures/narrativeInvestigationProspects');
const { resolvePendingOperatorDecision } = require('../PendingDecisionResolver');
const { operationalReadDuringPendingDecision } = require('../WorkspaceOwnershipResolver');

const TENANT = '10';
const PENDING = { kind: 'execution_approval', prompt: 'Authorize external execution of prepared outreach?' };
const READS = ['brief me on my prospects', 'Brief me on my prospects.', 'show me my prospects'];

async function readyMission(tenantId = TENANT) {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId, objective: 'Acquire recurring commercial cleaning clients in Greater Manchester.', targetSegment: 'Facility buyers' });
  const g = () => engine.get(mission.id, tenantId);
  await A.advancePlanAfterApproval({ engine, mission, tenantId, question: 'Approved.' });
  await A.advanceDiscoveryAfterApproval({ engine, mission, tenantId, question: 'Approved. Begin Discovery.', runScout: liveScoutStub });
  await A.advancePrioritizationAfterApproval({ engine, mission: g(), tenantId, question: 'Approved prioritization.' });
  await A.advanceMaxPrioritization({ engine, mission: g(), tenantId, allowFixtureFallback: true });
  await A.advanceAcquisitionApproach({ engine, mission: g(), tenantId, allowFixtureFallback: true });
  await A.advancePaigeVariants({ engine, mission: g(), tenantId, allowFixtureFallback: true, question: 'Generate variants.' });
  await A.advanceEmmettCapacity({ engine, mission: g(), tenantId, allowFixtureFallback: true, question: 'Plan capacity.' });
  assert.equal(g().pendingOperatorDecision.kind, 'execution_approval');
  return { engine, mission, get: g };
}

async function workspace(engine, mission, { tenantId = TENANT, loads = [] } = {}) {
  const rows = narrativeFixture(Number(tenantId));
  const ws = createHermeticWorkspaceEngine({ disableLlm: true, missionsEnabled: true, resolverEnabled: true,
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    operatingEvidenceOpts: { ...unavailableOperatingEvidence(), loadProspects: async input => {
      loads.push(input.clientId);
      return { available: true, status: 'found', counts: { total: rows.length }, records: { status: 'found', complete: true, rows } };
    } } });
  const opened = await ws.open({ tenantId, page: 'command-deck' });
  const session = ws._sessions.get(opened.sessionId);
  session.context.missionId = mission.id;
  session.context.acquisitionMissionId = mission.id;
  return { ws, ask: question => ws.ask({ sessionId: opened.sessionId, question }) };
}

test('resolver-level: only messages with no decision signal yield to an operational read', () => {
  const bypass = q => Boolean(operationalReadDuringPendingDecision(q,
    { pendingDecisionResolution: resolvePendingOperatorDecision(q, { id: 'm', pendingOperatorDecision: PENDING }) }, {}));
  for (const q of READS) assert.equal(bypass(q), true, q);
  for (const q of [
    // explicit approval and rejection
    'Approved.', 'Yes', 'Approve and send.', "No, don't send this.", 'Reject it.',
    // genuine ambiguity / deferral
    'maybe', 'hmm', 'Hold on.', 'Not yet.', 'Brief me on my prospects; hold off on sending.',
    // mixed: a read plus a decision or an affirmative execution instruction
    'Yes, brief me on my prospects.', 'Approve it and brief me on my prospects.',
    'Brief me on my prospects, then go ahead.', 'Brief me on my prospects and send the approved emails.',
    'Brief me on my prospects. Cancel the mission.',
  ]) assert.equal(bypass(q), false, q);
  // Never without a pending, unresolved, ambiguous resolution.
  assert.equal(operationalReadDuringPendingDecision('brief me on my prospects', null, {}), null);
  assert.equal(operationalReadDuringPendingDecision('brief me on my prospects',
    { pendingDecisionResolution: { pending: true, resolved: true, outcome: 'ambiguous' } }, {}), null);
  assert.equal(operationalReadDuringPendingDecision('brief me on my prospects',
    { pendingDecisionResolution: { pending: true, resolved: false, outcome: 'ambiguous', requestedActionMismatch: {} } }, {}), null);
});

test('explicit operational reads retrieve prospects while an execution approval is pending; the decision is unchanged', async () => {
  const { engine, mission, get } = await readyMission();
  const before = JSON.stringify(get());
  for (const q of READS) {
    const loads = [];
    const { ask } = await workspace(engine, mission, { loads });
    const r = await ask(q);
    assert.equal(r.workspaceOwnership.owner, 'knowledge_retrieval', q);
    assert.equal(r.workspaceOwnership.reason, 'operational_read', q);
    assert.equal(r.workspaceOwnership.pendingDecisionPreserved, true, q);
    assert.equal(r.executionDomain, 'workspace', q);
    assert.notEqual(r.presentation, 'mission_communication', q);
    assert.doesNotMatch(r.prose, /didn't catch a clear yes or no|Execution Ready|Authorize external execution/i, q);
    // A briefing reports the inventory; a "show" request lists the records.
    const listed = (r.prose.match(/\[prospect:10:/g) || []).length;
    assert.ok(/6 existing prospects/.test(r.prose) || listed === 6, `${q}: prospects retrieved`);
    assert.doesNotMatch(r.prose, /prospect:(?!10:)\d+:/, `${q}: no other tenant's records`);
    assert.match(r.prose, /No action was executed|No (?:writes, )?emails, calls, assignments/, q);
    assert.deepEqual(loads, [10], `${q}: prospects read for the session tenant only`);
    assert.equal(r.structured.metadata.executed ?? false, false, q);
    // The pending decision is unresolved and byte-identical.
    assert.equal(JSON.stringify(get()), before, q);
    assert.equal(get().pendingOperatorDecision.kind, 'execution_approval');
  }
});

test('approval, rejection, genuine ambiguity and mixed execution requests keep approval handling', async () => {
  for (const [q, expect] of [
    ['Approved.', r => assert.equal(r.workspaceOwnership.owner, 'active_mission')],
    ["No, don't send this.", r => assert.equal(r.workspaceOwnership.owner, 'active_mission')],
    ['maybe', r => assert.equal(r.workspaceOwnership.reason, 'pending_decision_turn_ownership')],
    ['Hold on.', r => assert.equal(r.workspaceOwnership.reason, 'pending_decision_turn_ownership')],
    ['Yes, brief me on my prospects.', r => assert.equal(r.workspaceOwnership.owner, 'active_mission')],
    ['Approve it and brief me on my prospects.', r => assert.equal(r.workspaceOwnership.owner, 'active_mission')],
    ['Brief me on my prospects and send the approved emails.', r => {
      assert.equal(r.workspaceOwnership.owner, 'active_mission');
      assert.match(r.prose, /Nothing was approved or sent/);
    }],
  ]) {
    const { engine, mission, get } = await readyMission();
    const { ask } = await workspace(engine, mission);
    const r = await ask(q);
    assert.notEqual(r.workspaceOwnership.reason, 'operational_read', q);
    assert.notEqual(r.workspaceOwnership.pendingDecisionPreserved, true, q);
    expect(r);
    if (q !== 'Approved.') assert.equal(get().pendingOperatorDecision?.kind, 'execution_approval', `${q}: decision still pending`);
  }
});

test('another tenant\'s pending decision never redirects a read, and the read stays tenant-scoped', async () => {
  const { engine, mission, get } = await readyMission('10');
  const before = JSON.stringify(get());
  const loads = [];
  const rows = narrativeFixture(21);
  const ws = createHermeticWorkspaceEngine({ disableLlm: true, missionsEnabled: true, resolverEnabled: true,
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    operatingEvidenceOpts: { ...unavailableOperatingEvidence(), loadProspects: async input => {
      loads.push(input.clientId);
      return { available: true, status: 'found', counts: { total: rows.length }, records: { status: 'found', complete: true, rows } };
    } } });
  const { sessionId } = await ws.open({ tenantId: '21', page: 'command-deck' });
  const r = await ws.ask({ sessionId, question: 'brief me on my prospects' });
  assert.equal(r.workspaceOwnership.owner, 'knowledge_retrieval');
  assert.deepEqual(loads, [21]);
  assert.equal(JSON.stringify(get()), before);
});
