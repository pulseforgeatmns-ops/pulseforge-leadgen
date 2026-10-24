'use strict';

// Synthetic regression coverage only. No provider, database, or durable dispatch.
require('../../../test/helpers/denyWebsiteEvidenceNetwork.cjs');

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const amo = require('../index');
const {
  hasDiscoveryArtifact,
  hasPendingPrioritizationApproval,
  hasPendingDiscoveryInvestigation,
  presentableOperatorDecision,
  assertMissionStateConsistent,
} = require('../PendingOperatorDecision');
const {
  evaluatePrioritizationReadiness,
  buildPostDiscoveryPendingDecision,
} = require('../DecisionReadiness');
const { findLatestDiscoveryContribution } = require('../DiscoveryPresentation');
const { findLatestScoutDiscovery } = require('../../max/workspace/MaxPrioritizationExecutor');
const { inspectQuestion } = require('../Inspection');
const { buildMissionInspectionSnapshot } = require('../MissionInspectionSnapshot');
const { extractMissionFacts, composeConversationalResponse } = require('../../max/workspace/ConversationLayer');
const {
  advancePlanAfterApproval,
  advancePrioritizationAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');

const MISSION_ID = 'mission_readiness_truth';
const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';
const PRIORITIZATION = amo.OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL;
const INVESTIGATION = amo.OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION;

function readyPayload(overrides = {}) {
  return {
    summary: 'One qualified prospect with attributable evidence.',
    discoveryStatus: 'complete',
    qualifiedCount: 1,
    confidence: 0.8,
    rankedProspects: [{ id: 'candidate_current', name: 'Current prospect', readinessState: 'unknown' }],
    evidence: [{ label: 'Company website', source: 'website' }],
    ...overrides,
  };
}

function weakPayload() {
  return {
    summary: 'No prospects.',
    discoveryStatus: 'incomplete',
    rankedProspects: [],
    coverage: { cities: { planned: 4, searched: 1 } },
  };
}

function scoutRow(id, payload, overrides = {}) {
  return {
    id,
    missionId: MISSION_ID,
    specialist: 'scout',
    kind: 'discovery',
    at: '2026-10-24T00:00:00.000Z',
    payload,
    ...overrides,
  };
}

function snapshot(contributions = [], missionOverrides = {}) {
  return {
    mission: {
      id: MISSION_ID,
      tenantId: '10',
      stage: amo.STAGES.DISCOVER,
      structuredMissionApproved: true,
      pendingOperatorDecision: { stage: amo.STAGES.DISCOVER, kind: PRIORITIZATION, prompt: 'Approve prioritization?' },
      ...missionOverrides,
    },
    contributions,
  };
}

function assertNotConsumable(state) {
  const before = structuredClone(state);
  assert.equal(hasPendingPrioritizationApproval(state), false);
  const presented = presentableOperatorDecision(state);
  assert.equal(presented.consumable, false);
  assert.match(presented.prompt, /Prioritization unavailable/);
  assert.deepEqual(presented.choices, []);
  assert.equal(hasPendingDiscoveryInvestigation(state), false, 'a display projection is not an investigation grant');
  assert.doesNotThrow(() => assertMissionStateConsistent(state));
  assert.deepEqual(state, before, 'readiness guards must not rewrite stored decisions or contributions');
}

async function seedEngine(rows, missionOverrides = {}) {
  const engine = amo.createAcquisitionMissionEngine();
  const initial = engine.create({ tenantId: '10', objective: OBJECTIVE, targetSegment: 'Law Firms' });
  await advancePlanAfterApproval({ engine, mission: initial, tenantId: '10', question: 'Approved.' });
  const mission = {
    ...engine.get(initial.id, '10'),
    pendingOperatorDecision: { stage: amo.STAGES.DISCOVER, kind: PRIORITIZATION, prompt: 'Approve prioritization?' },
    ...missionOverrides,
  };
  const saved = engine.store.snapshot();
  engine.store.restore({
    ...saved,
    missions: [[mission.id, mission]],
    contributions: [...saved.contributions, ...rows.map(row => ({ ...row, missionId: mission.id }))],
  });
  return { engine, mission };
}

describe('Post-discovery decision readiness truth', () => {
  const insufficient = [
    ['empty evidence', {}],
    ['provider blocked', readyPayload({ blocked: true })],
    ['incomplete coverage', readyPayload({ discoveryStatus: 'incomplete' })],
    ['no ranked prospects', readyPayload({ rankedProspects: [] })],
    ['missing summary', readyPayload({ summary: '' })],
    ['missing provenance', readyPayload({ evidence: [] })],
    ['fixture-only provenance', readyPayload({ evidence: ['fixture'] })],
    ['ready prospects without buying signals', readyPayload({ rankedProspects: [{ name: 'Ready prospect', readinessState: 'ready' }] })],
    ['not-ready prospects', readyPayload({ rankedProspects: [{ name: 'Not ready', readinessState: 'not_ready' }] })],
  ];

  for (const [label, payload] of insufficient) {
    it(`does not advertise stale prioritization approval for ${label}`, () => {
      assert.equal(evaluatePrioritizationReadiness(payload).sufficient, false);
      const pending = buildPostDiscoveryPendingDecision(payload);
      assert.equal(pending.kind, INVESTIGATION);
      assertNotConsumable(snapshot([scoutRow('scout_weak', payload)]));

      const investigation = snapshot([scoutRow('scout_weak', payload)], { pendingOperatorDecision: pending });
      assert.equal(hasPendingDiscoveryInvestigation(investigation), true);
      assert.equal(presentableOperatorDecision(investigation).prompt, 'Continue investigation?');
      assert.doesNotThrow(() => assertMissionStateConsistent(investigation));
    });
  }

  it('requires a current mission-bound discovery artifact', () => {
    for (const rows of [[], [scoutRow('foreign_scout', readyPayload(), { missionId: 'other_tenant_mission' })]]) {
      const state = snapshot(rows);
      if (!rows.length) assert.equal(hasDiscoveryArtifact(state), false);
      assertNotConsumable(state);
    }
  });

  for (const invalidation of [
    { superseded: true },
    { payload: { superseded: true } },
    { rolledBack: true },
    { commitStatus: 'rolled_back' },
    { payload: { rolledBack: true } },
    { payload: { commitStatus: 'rolled_back' } },
  ]) {
    it(`does not revive invalidated discovery: ${JSON.stringify(invalidation)}`, () => {
      const row = scoutRow('invalidated', readyPayload(invalidation));
      const state = snapshot([row]);
      assertNotConsumable(state);
    });
  }

  it('uses newest active evidence regardless of input order', () => {
    const old = scoutRow('old', weakPayload(), { at: '2026-10-23T00:00:00Z' });
    const current = scoutRow('current', readyPayload());
    const invalid = scoutRow('invalid_newest', readyPayload({ superseded: true }), { at: '2026-10-25T00:00:00Z' });
    const foreign = scoutRow('foreign', weakPayload(), { missionId: 'other_tenant_mission', at: '2026-10-26T00:00:00Z' });
    for (const rows of [[current, old, invalid, foreign], [foreign, invalid, old, current]]) {
      const state = snapshot(rows);
      assert.equal(findLatestDiscoveryContribution(rows, state.mission).id, 'current');
      assert.equal(findLatestScoutDiscovery(rows, state.mission).id, 'current');
      assert.equal(hasPendingPrioritizationApproval(state), true);
      assert.equal(presentableOperatorDecision(state).kind, PRIORITIZATION);
      assert.doesNotThrow(() => assertMissionStateConsistent(state));
    }
  });

  it('does not rescue insufficient current evidence with an older sufficient artifact', () => {
    const old = scoutRow('old_ready', readyPayload(), { at: '2026-10-23T00:00:00Z' });
    const current = scoutRow('current_weak', weakPayload());
    for (const rows of [[current, old], [old, current]]) assertNotConsumable(snapshot(rows));
  });

  it('uses the existing deterministic tie-breaker and active revision pointer', () => {
    const old = scoutRow('scout_a', readyPayload());
    const current = scoutRow('scout_z', weakPayload());
    assertNotConsumable(snapshot([current, old]));
    const state = snapshot([current, old], { revisionState: { scoutContributionId: old.id } });
    assert.equal(findLatestScoutDiscovery(state.contributions, state.mission).id, old.id);
    assert.equal(hasPendingPrioritizationApproval(state), true);
    assert.doesNotThrow(() => assertMissionStateConsistent(state));
    old.payload.superseded = true;
    assertNotConsumable(state);
  });

  it('preserves ADR-101 unknown readiness and does not authorize execution or invent a pending decision', () => {
    for (const readinessState of ['unknown', undefined]) {
      const payload = readyPayload({ rankedProspects: [{ name: 'Qualified prospect', readinessState }] });
      assert.equal(evaluatePrioritizationReadiness(payload).sufficient, true);
      assert.equal(buildPostDiscoveryPendingDecision(payload).kind, PRIORITIZATION);
      const state = snapshot([scoutRow('current', payload)]);
      assert.equal(presentableOperatorDecision(state).consumable, true);
      assert.equal(amo.hasPendingExecutionApproval(state), false);
      state.mission.pendingOperatorDecision = null;
      assert.equal(presentableOperatorDecision(state), null);
      assert.equal(hasPendingPrioritizationApproval(state), false);
    }
  });

  it('preserves sufficient known readiness with typed buying signals and provenance', () => {
    const payload = readyPayload({
      rankedProspects: [{ name: 'Ready prospect', readinessState: 'ready' }],
      buyingSignals: [{ type: 'hiring', label: 'Hiring office manager', source: 'job_board' }],
    });
    assert.equal(evaluatePrioritizationReadiness(payload).sufficient, true);
    assert.equal(hasPendingPrioritizationApproval(snapshot([scoutRow('current', payload)])), true);
  });

  it('inspection stays available and approval rejects stale evidence without writes or dispatch', async () => {
    const { engine, mission } = await seedEngine([scoutRow('current_weak', weakPayload())]);
    const before = engine.store.snapshot();
    const calls = [];
    for (const name of ['putMission', 'addContribution', 'updateContribution', 'addEvent', 'addExecutionRecord', 'restore']) {
      engine.store[name] = () => { calls.push(name); throw new Error(`Unexpected write: ${name}`); };
    }
    const dispatch = async () => { calls.push('dispatch'); throw new Error('Unexpected dispatch'); };
    const inspected = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(inspected.executableDecision.consumable, false);
    assert.equal(inspected.mission.pendingOperatorDecision.kind, PRIORITIZATION);
    assert.ok(inspected.blocker.reason);
    const status = inspectQuestion('Why did Scout stop?', inspected, { silent: true });
    assert.match(status.prose, /Prioritization unavailable/);
    assert.doesNotMatch(status.prose, /Approve prioritization\?/);
    assert.match(extractMissionFacts(inspected).pending.prompt, /Prioritization unavailable/);
    const inspection = buildMissionInspectionSnapshot(inspected);
    assert.equal(inspection.waiting_on, 'evidence');
    assert.equal(inspection.pending_decision.consumable, false);
    assert.equal(inspection.pending_decision.type, null);
    assert.doesNotMatch(JSON.stringify(inspection), /Approve prioritization\?/);
    await assert.rejects(() => advancePrioritizationAfterApproval({
      engine, mission, tenantId: '10', question: 'Approved.', runMax: dispatch,
      pool: { query: dispatch, connect: dispatch }, persist: dispatch,
    }), { code: 'tme_evidence_insufficient' });
    assert.deepEqual(calls, []);
    assert.deepEqual(engine.store.snapshot(), before);
  });

  it('missing or invalidated evidence remains inspectable without changing stored decisions', async () => {
    for (const rows of [[], [scoutRow('invalidated', readyPayload({ superseded: true }))]]) {
      const { engine, mission } = await seedEngine(rows);
      const before = engine.store.snapshot();
      const inspected = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(inspected.discoveryArtifact, null);
      assert.equal(inspected.executableDecision.consumable, false);
      assert.equal(inspected.progression.stage, 'discovery_review');
      assert.equal(inspected.workspaceContext.workspaceMode, 'discovery_review');
      assert.equal(inspected.workspaceContext.executionState.discoveryRunning, false);
      assert.ok(inspected.workspaceContext.visibleComponents.includes('operatorDecision'));
      assert.match(inspected.progression.pause.requiredDecision, /Prioritization unavailable/);
      assert.doesNotMatch(JSON.stringify([inspected.progression.pause, inspected.blocker, inspected.health]), /Waiting for Scout/);
      await assert.rejects(() => advancePrioritizationAfterApproval({ engine, mission, tenantId: '10' }), { code: 'tme_discovery_missing' });
      assert.deepEqual(engine.store.snapshot(), before);
    }
  });

  it('preserves the existing explicit investigation gate when historical evidence is invalidated', async () => {
    const pending = buildPostDiscoveryPendingDecision(weakPayload());
    const { engine, mission } = await seedEngine([scoutRow('invalidated', readyPayload({ superseded: true }))], {
      pendingOperatorDecision: pending,
    });
    const before = engine.store.snapshot();
    const inspected = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(inspected.discoveryArtifact, null);
    assert.equal(inspected.executableDecision.kind, INVESTIGATION);
    assert.equal(hasPendingDiscoveryInvestigation(inspected), true);
    assert.equal(hasPendingPrioritizationApproval(inspected), false);
    assert.deepEqual(engine.store.snapshot(), before);
  });

  it('preserves structural wrong-stage, unapproved and cancelled rejection', () => {
    for (const changes of [{ stage: amo.STAGES.UNDERSTAND }, { structuredMissionApproved: false }, { planCancelled: true }]) {
      const state = snapshot([scoutRow('ready', readyPayload())], changes);
      assert.equal(hasPendingPrioritizationApproval(state), false);
      assert.throws(() => assertMissionStateConsistent(state), { code: 'MISSION_STATE_INCONSISTENT' });
    }
  });

  it('null optional evidence or signals cannot cause a read outage or grant missing provenance', async () => {
    for (const payload of [
      readyPayload({ buyingSignals: [null] }),
      readyPayload({ buyingSignals: [{ type: 'hiring', label: 'Hiring manager' }], evidence: [null, { source: 'website' }] }),
    ]) {
      assert.equal(evaluatePrioritizationReadiness(payload).sufficient, true);
      const { engine, mission } = await seedEngine([scoutRow('ready', payload)]);
      const before = engine.store.snapshot();
      assert.equal(engine.inspect(mission.id, { tenantId: '10' }).executableDecision.consumable, true);
      assert.deepEqual(engine.store.snapshot(), before);
    }
    const missing = readyPayload({ buyingSignals: [null], evidence: [null] });
    assert.equal(evaluatePrioritizationReadiness(missing).sufficient, false);
    assertNotConsumable(snapshot([scoutRow('missing', missing)]));
  });

  it('dashboard renders unavailable evidence without an approve or continue button', () => {
    const source = fs.readFileSync(path.join(__dirname, '../../../public/acquisition-missions.html'), 'utf8');
    const panel = { innerHTML: '' };
    let dispatches = 0;
    const context = vm.createContext({
      workspaceMode: 'discovery_review', isComponentVisible: () => true,
      $: id => id === 'operatorPanel' ? panel : null,
      submitExecution: () => { dispatches += 1; },
    });
    for (const name of ['intentFromPending', 'approvalButtonLabel', 'renderOperatorDecision']) {
      const start = source.indexOf(`  function ${name}(`);
      const end = source.indexOf('\n  function ', start + 1);
      assert.ok(start >= 0 && end > start);
      vm.runInContext(source.slice(start, end), context);
    }
    const decision = presentableOperatorDecision(snapshot([scoutRow('weak', weakPayload())]));
    context.renderOperatorDecision(decision, null, null);
    assert.match(panel.innerHTML, /Prioritization unavailable/);
    assert.doesNotMatch(panel.innerHTML, /<button|Approve prioritization|Continue investigation/);
    assert.equal(context.intentFromPending(decision), null);
    assert.equal(context.intentFromPending({ kind: PRIORITIZATION, consumable: true }), 'APPROVE_PRIORITIZATION');
    assert.equal(context.intentFromPending({ kind: INVESTIGATION, consumable: true }), 'CONTINUE_INVESTIGATION');
    assert.equal(dispatches, 0);
  });

  it('rendered conversation reports unavailable evidence instead of inviting approval or investigation', () => {
    for (const payload of [weakPayload(), readyPayload({ companies: [{ name: 'Current prospect' }], evidence: [] })]) {
      const state = snapshot([scoutRow('current', payload)]);
      state.executableDecision = presentableOperatorDecision(state);
      state.workspace = { scout: { state: 'complete' } };
      for (const intent of ['inspect', 'explain', 'strategy', 'brainstorm', 'challenge']) {
        for (const inspectionSnapshot of [null, buildMissionInspectionSnapshot(state)]) {
          const response = composeConversationalResponse({
            question: 'What should we do next?', snapshot: state, inspectionSnapshot,
            conversationIntent: { intent }, explicitReasoning: true,
          });
          assert.match(response.prose, /Prioritization unavailable/, intent);
          assert.doesNotMatch(response.prose, /approve prioritization|continue investigation|waiting on you|waiting on discovery_investigation|lower.*evidence (bar|threshold)/i, intent);
        }
      }
      assert.equal(state.mission.pendingOperatorDecision.kind, PRIORITIZATION);
      assert.equal(hasPendingDiscoveryInvestigation(state), false);
    }
  });

  it('inspection and attempted approval remain tenant-scoped without writes or dispatch', async () => {
    const { engine, mission } = await seedEngine([scoutRow('current', readyPayload())]);
    const before = engine.store.snapshot();
    let dispatched = false;
    assert.throws(() => engine.inspect(mission.id, { tenantId: '20' }), { code: 'amo_tenant_mismatch' });
    await assert.rejects(() => advancePrioritizationAfterApproval({
      engine, mission, tenantId: '20', question: 'Approved.',
      runMax: async () => { dispatched = true; },
    }), { code: 'amo_tenant_mismatch' });
    assert.equal(dispatched, false);
    assert.deepEqual(engine.store.snapshot(), before);
  });

  for (const usePointer of [false, true]) {
    it(`inspection, approval validation and Max consume the same ${usePointer ? 'revision-bound' : 'latest'} artifact`, async () => {
      const current = scoutRow('current_ready', readyPayload());
      const other = scoutRow('other_weak', weakPayload(), { at: usePointer ? '2026-10-25T00:00:00Z' : '2026-10-23T00:00:00Z' });
      const { engine, mission } = await seedEngine([current, other], usePointer ? { revisionState: { scoutContributionId: current.id } } : {});
      const before = engine.store.snapshot();
      const inspected = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(inspected.discoveryArtifact.summary, current.payload.summary);
      assert.equal(inspected.executableDecision.kind, PRIORITIZATION);
      assert.deepEqual(engine.store.snapshot(), before, 'inspection must be read-only');
      const result = await advancePrioritizationAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
      assert.equal(result.snapshot.mission.stage, amo.STAGES.UNDERSTAND);
      assert.equal(result.prioritization.payload.priorities[0].name, 'Current prospect');
      assert.equal(result.snapshot.contributions.filter(row => row.payload.action === 'prioritization_approved').length, 1);
      assert.equal(amo.hasPendingExecutionApproval(result.snapshot), false);
    });
  }
});
