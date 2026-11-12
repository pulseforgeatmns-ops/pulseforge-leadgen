'use strict';

/**
 * P0 Max reliability — S2 conversational orchestration.
 *
 * A review-only mission must refuse every prohibited command with the
 * restriction named, whatever decision is pending, without dispatching
 * anything. Region clarification must accept punctuated answers but never a
 * question. Status must not claim discovery before discovery is authorized,
 * and Scout is not a source until Scout has contributed.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const amo = require('../../../acquisition-mission');
const {
  listExecutionRouterAudit,
  clearExecutionRouterAudit,
} = require('../../../acquisition-mission/ExecutionRouter');
const {
  commandedIntents,
  maybeHandleProhibitedCommandTurn,
} = require('../ProhibitedCommandGuard');
const {
  missionStatusLabel,
  reconcileMissionStatus,
} = require('../../../acquisition-mission/MissionStatus');
const { normalizePlanClarificationAnswer } = require('../../../acquisition-mission/MissionPlanner');
const {
  classifyPendingDecisionCaptureIntent,
  CAPTURE_INTENTS,
} = require('../pendingDecisionCaptureGuard');

const TENANT = '25';
const REVIEW_ONLY =
  'Set up a review-only contact intake mission for an existing customer. ' +
  'No outreach, calls, content, or campaign activation.';
const PROHIBITED_CLASSES = ['content', 'outreach', 'outreach_authorization', 'paid_media', 'provider_research'];

function harness() {
  const runtime = createTestAmoRuntime();
  const engine = runtime.engine();
  const ws = createHermeticWorkspaceEngine({
    acquisitionMissionRuntime: runtime,
    clientIntelligenceService: { getApprovedClientBlueprint: async () => null },
  });
  let sessionId = null;
  const ask = async (question) => {
    const result = await ws.ask({
      sessionId,
      question,
      context: sessionId ? undefined : { tenantId: TENANT, page: 'command-deck' },
    });
    sessionId = result.sessionId;
    return result;
  };
  const mission = () => engine.list(TENANT)[0] || null;
  return { runtime, engine, ask, mission };
}

function missionFingerprint(engine, mission) {
  const row = engine.get(mission.id, TENANT);
  return {
    stage: row.stage,
    version: row.version,
    pending: row.pendingOperatorDecision && row.pendingOperatorDecision.kind,
    constraints: JSON.stringify(row.authorizationConstraints),
    contributions: engine.store.listContributions(mission.id).length,
  };
}

function dispatchedIntents() {
  return listExecutionRouterAudit()
    .filter((row) => row.outcome !== 'blocked_by_constraint')
    .map((row) => row.intent);
}

function assertConstraintRefusal(result, actionClassText) {
  assert.match(result.prose, new RegExp(`^Not executed: this mission's authorization forbids ${actionClassText} \\(review-only mission\\)\\.`));
  assert.equal(result.structured.metadata.blocker, amo.AUTHORIZATION_CONSTRAINT_BLOCKED);
  assert.equal(result.structured.metadata.executionState, 'blocked');
  assert.doesNotMatch(result.prose, /active mission unhandled|didn't catch a clear yes or no/i);
}

let fetchCalls;
let realFetch;
beforeEach(() => {
  clearExecutionRouterAudit();
  fetchCalls = 0;
  realFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    throw new Error('network is not allowed in this test');
  };
});
afterEach(() => {
  globalThis.fetch = realFetch;
});

describe('P0 S2 — prohibited commands are refused with the restriction named', () => {
  it('creates one review-only mission with the documented prohibitions and no Scout source', async () => {
    const h = harness();
    const created = await h.ask(REVIEW_ONLY);
    const mission = h.mission();
    assert.ok(mission, 'mission created');
    assert.equal(h.engine.list(TENANT).length, 1);
    assert.equal(mission.authorizationConstraints.reviewOnly, true);
    assert.deepEqual(mission.authorizationConstraints.prohibited, PROHIBITED_CLASSES);
    // Review-only planning defers Scout targeting, so the plan is ready at once.
    assert.equal(mission.pendingOperatorDecision.kind, 'plan_approval');
    assert.equal(mission.status, 'Plan ready for approval', 'no "Discovering" before discovery is authorized');
    assert.doesNotMatch(created.prose, /Status\s*\|*\s*Discovering/);
    assert.doesNotMatch(created.prose, /Sources[^]*\bscout\b/i, 'Scout is not a source before it contributes');
    assert.deepEqual(h.engine.store.listContributions(mission.id), []);
    assert.deepEqual(dispatchedIntents(), []);
  });

  for (const [command, actionClassText] of [
    ['Approve discovery.', 'provider research'],
    ['Run Scout discovery now.', 'provider research'],
    ['Send the emails.', 'outreach'],
    ['Call the prospects.', 'outreach'],
    ['Approve the emails.', 'outreach authorization'],
    ['Draft the outreach content.', 'content'],
    ['Launch paid ads.', 'paid media'],
  ]) {
    it(`review-only, while plan approval is pending: "${command}"`, async () => {
      const h = harness();
      await h.ask(REVIEW_ONLY);
      const mission = h.mission();
      const before = missionFingerprint(h.engine, mission);
      clearExecutionRouterAudit();

      const result = await h.ask(command);

      assertConstraintRefusal(result, actionClassText);
      assert.equal(result.structured.metadata.prohibitedCommandGuard, true);
      assert.equal(result.structured.metadata.pendingDecisionKind, 'plan_approval');
      assert.deepEqual(missionFingerprint(h.engine, mission), before, 'mission unchanged; plan not approved as a side effect');
      assert.deepEqual(dispatchedIntents(), [], 'nothing dispatched');
      assert.ok(listExecutionRouterAudit().every((row) => row.outcome === 'blocked_by_constraint'));
      assert.equal(fetchCalls, 0, 'no provider or network call');
    });
  }

  // Research allowed, outreach/content prohibited: Scout planning questions
  // are still asked, and prohibited commands are refused while one is pending.
  const RESTRICTED_RESEARCH_ALLOWED =
    'Set up a contact intake mission for an existing customer. No outreach, calls, content, or campaign activation.';
  for (const [command, actionClassText] of [
    ['Send the emails.', 'outreach'],
    ['Call the prospects.', 'outreach'],
    ['Approve the emails.', 'outreach authorization'],
    ['Draft the outreach content.', 'content'],
  ]) {
    it(`while a plan clarification is pending: "${command}"`, async () => {
      const h = harness();
      await h.ask(RESTRICTED_RESEARCH_ALLOWED);
      const mission = h.mission();
      assert.equal(mission.pendingOperatorDecision.kind, 'plan_clarification');
      const before = missionFingerprint(h.engine, mission);
      clearExecutionRouterAudit();

      const result = await h.ask(command);

      assert.match(result.prose, new RegExp(`^Not executed: this mission's authorization forbids ${actionClassText}\\.`));
      assert.equal(result.structured.metadata.blocker, amo.AUTHORIZATION_CONSTRAINT_BLOCKED);
      assert.equal(result.structured.metadata.pendingDecisionKind, 'plan_clarification');
      assert.deepEqual(missionFingerprint(h.engine, mission), before);
      assert.deepEqual(dispatchedIntents(), []);
      assert.equal(fetchCalls, 0);
    });
  }

  it('a punctuated region answer resumes planning on a research-allowed mission', async () => {
    const h = harness();
    await h.ask(RESTRICTED_RESEARCH_ALLOWED);
    assert.equal(h.mission().pendingOperatorDecision.field, 'geography.region');
    const region = await h.ask('Nashua NH.');
    assert.notEqual(region.workspaceOwnership.owner, 'reasoning', 'captured, not a generic briefing');
    assert.equal(h.mission().pendingOperatorDecision.field, 'market.segment');
    assert.equal(h.mission().status, 'Clarification required');
  });

  it('walks the documented S2 flow: plan approval, then refusals at discovery approval', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    assert.equal(h.mission().pendingOperatorDecision.kind, 'plan_approval');

    // Plan approval is planning, not a prohibited class: it keeps its normal path.
    const approved = await h.ask('Approve the plan.');
    assert.equal(approved.structured.metadata.blocker, undefined);
    assert.equal(h.mission().pendingOperatorDecision.kind, 'discovery_approval');
    assert.equal(h.mission().status, 'Plan approved (review only)');

    const mission = h.mission();
    const before = missionFingerprint(h.engine, mission);
    clearExecutionRouterAudit();

    // Pending decision's own action: refused by the router (same constraint).
    const discovery = await h.ask('Approve discovery.');
    assertConstraintRefusal(discovery, 'provider research');

    // A different action while discovery approval is pending: refused, not
    // read as an unclear yes/no.
    const send = await h.ask('Send the emails.');
    assertConstraintRefusal(send, 'outreach');
    assert.equal(send.structured.metadata.prohibitedCommandGuard, true);
    assert.equal(send.structured.metadata.pendingDecisionKind, 'discovery_approval');

    assert.deepEqual(missionFingerprint(h.engine, mission), before, 'no approval consumed, no stage change');
    assert.deepEqual(dispatchedIntents(), []);
    assert.deepEqual(
      listExecutionRouterAudit().map((row) => row.intent).sort(),
      ['APPROVE_DISCOVERY', 'EXECUTE_OUTBOUND']
    );
    assert.equal(h.engine.store.listContributions(mission.id).some((row) => row.specialist === 'scout'), false);
    assert.equal(fetchCalls, 0);
  });

  it('REST intents for every prohibited class are blocked before dispatch', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    const mission = h.mission();
    const svc = require('../../../../services/acquisitionMission');
    let specialistCalls = 0;
    const spy = async () => { specialistCalls += 1; return {}; };
    for (const intent of ['APPROVE_DISCOVERY', 'START_DISCOVERY', 'CONTINUE_INVESTIGATION', 'GENERATE_VARIANTS',
      'ASSESS_PAID_ACQUISITION', 'APPROVE_EXECUTION', 'EXECUTE_OUTBOUND']) {
      await assert.rejects(
        svc.executeCanonical({
          tenantId: TENANT, missionId: mission.id, intent, role: 'operator', operatorId: 'op',
          canonicalSender: { tenantId: TENANT, senderEmail: 'ops@example.invalid', senderName: 'Ops', sendingDomain: 'example.invalid' },
          runScout: spy, sendEmail: spy,
        }, { acquisitionMissionRuntime: h.runtime, persist: false }),
        (err) => err.code === amo.AUTHORIZATION_CONSTRAINT_BLOCKED,
        intent
      );
    }
    assert.equal(specialistCalls, 0);
    assert.equal(fetchCalls, 0);
  });
});

describe('P0 S2 — guard boundaries (precedence, polarity, scope, tenant)', () => {
  it('reads commands only from affirmative, non-question text', () => {
    assert.deepEqual(commandedIntents('Do not send the emails.'), []);
    assert.deepEqual(commandedIntents("Don't run discovery yet."), []);
    assert.deepEqual(commandedIntents('Can you send the emails?'), []);
    assert.deepEqual(commandedIntents('Why can we not send emails'), []);
    assert.deepEqual(commandedIntents('Show me the outreach plan.'), [], 'naming an object is not a command');
    assert.deepEqual(commandedIntents('Approve the plan.'), [], 'planning is not a prohibited class');
    assert.deepEqual(commandedIntents('Send the emails.').map((r) => r.intent), ['EXECUTE_OUTBOUND']);
    assert.deepEqual(
      commandedIntents('Draft the emails.').map((r) => r.intent),
      ['GENERATE_VARIANTS', 'EXECUTE_OUTBOUND'],
      'drafting is explained as content first'
    );
  });

  it('a negated command is not refused as a command', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    const result = await h.ask('Do not send the emails.');
    assert.notEqual(result.structured.metadata.prohibitedCommandGuard, true);
  });

  it('leaves missions without that prohibition alone', async () => {
    const h = harness();
    await h.ask('Set up a contact intake mission for an existing customer. No content.');
    const mission = h.mission();
    assert.deepEqual(mission.authorizationConstraints.prohibited, ['content']);
    const turn = await maybeHandleProhibitedCommandTurn({
      question: 'Send the emails.',
      context: { tenantId: TENANT },
      acquisitionMissionRuntime: h.runtime,
    });
    assert.equal(turn, null, 'outreach is not prohibited on this mission');
    const content = await maybeHandleProhibitedCommandTurn({
      question: 'Draft the content.',
      context: { tenantId: TENANT },
      acquisitionMissionRuntime: h.runtime,
    });
    assert.equal(content.structured.metadata.blocker, amo.AUTHORIZATION_CONSTRAINT_BLOCKED);
  });

  it('never resolves another tenant\'s mission', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    const turn = await maybeHandleProhibitedCommandTurn({
      question: 'Send the emails.',
      context: { tenantId: '26' },
      acquisitionMissionRuntime: h.runtime,
    });
    assert.equal(turn, null);
  });

  it('a command naming the pending decision\'s own action keeps the router path', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: TENANT,
      clientId: Number(TENANT),
      objective: 'Find property managers in Nashua NH',
      createdBy: 'test',
      authorizationConstraints: amo.extractAuthorizationConstraints('Review-only.'),
    });
    engine.store.putMission({
      ...engine.get(mission.id, TENANT),
      pendingOperatorDecision: { stage: 'discover', kind: 'discovery_approval', prompt: 'Approve discovery?', responseType: 'yes_no' },
      missionPlanDraft: null,
      structuredMission: { immutable: true },
    });
    const turn = await maybeHandleProhibitedCommandTurn({
      question: 'Approve discovery.',
      context: { tenantId: TENANT },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    });
    assert.equal(turn, null, 'discovery approval stays with the router, which enforces the same restriction');
  });
});

describe('P0 S2 — region clarification punctuation', () => {
  const pending = {
    stage: 'discover',
    kind: 'plan_clarification',
    prompt: 'Which region should this mission cover?',
    question: 'Which region should this mission cover?',
    choices: [],
    field: 'geography.region',
    responseType: 'text',
  };
  const classify = (message) => classifyPendingDecisionCaptureIntent({ message, pendingDecision: pending });

  for (const answer of ['Nashua NH', 'Nashua NH.', 'Nashua NH!', 'Nashua NH...', 'Manchester NH.', 'Nashville TN.']) {
    it(`accepts "${answer}"`, () => {
      assert.equal(classify(answer), CAPTURE_INTENTS.DECISION_RESPONSE);
    });
  }

  for (const answer of ['Nashua NH?', 'Nashua NH.?', 'Is it Nashua NH.', 'Is it Nashua NH', 'Can you do Nashua NH.',
    'Maybe Nashua NH.', 'Not Nashua NH.', 'What about Nashua NH.', 'Status.']) {
    it(`does not capture "${answer}" as an answer`, () => {
      assert.notEqual(classify(answer), CAPTURE_INTENTS.DECISION_RESPONSE);
    });
  }

  it('stores the region without the sentence period, and leaves other fields untouched', () => {
    assert.equal(normalizePlanClarificationAnswer('Nashua NH.', pending), 'Nashua NH');
    assert.equal(normalizePlanClarificationAnswer('Nashua NH?', pending), 'Nashua NH?');
    assert.equal(
      normalizePlanClarificationAnswer('Property managers.', { field: 'market.segment' }),
      'Property managers.'
    );
  });
});

describe('P0 S2 — mission status before discovery', () => {
  it('names the pending pre-discovery decision instead of "Discovering"', () => {
    assert.equal(missionStatusLabel('discover', { kind: 'plan_clarification' }), 'Clarification required');
    assert.equal(missionStatusLabel('discover', { kind: 'plan_edit' }), 'Plan edit in progress');
    assert.equal(missionStatusLabel('discover', { kind: 'plan_approval' }), 'Plan ready for approval');
    assert.equal(missionStatusLabel('discover', { kind: 'discovery_approval' }), 'Awaiting discovery approval');
  });

  it('reports "Discovering" once discovery is authorized, and other stages unchanged', () => {
    assert.equal(missionStatusLabel('discover', null), 'Discovering');
    assert.equal(missionStatusLabel('discover', { kind: 'discovery_investigation' }), 'Discovering');
    assert.equal(missionStatusLabel('discover', { kind: 'prioritization_approval' }), 'Discovering');
    assert.equal(missionStatusLabel('understand', null), 'Understanding');
    assert.equal(missionStatusLabel('ready', { kind: 'execution_approval' }), 'Ready');
  });

  it('never rewrites cancelled, custom or non-discover statuses', () => {
    assert.equal(reconcileMissionStatus({ stage: 'discover', status: 'Cancelled', planCancelled: true }), 'Cancelled');
    assert.equal(reconcileMissionStatus({ stage: 'discover', status: 'Paused by operator', pendingOperatorDecision: { kind: 'plan_approval' } }), 'Paused by operator');
    assert.equal(reconcileMissionStatus({ stage: 'prepare', status: 'Preparing' }), 'Preparing');
    assert.equal(
      reconcileMissionStatus({ stage: 'discover', status: 'Discovering', pendingOperatorDecision: { kind: 'discovery_approval' } }),
      'Awaiting discovery approval',
      'existing missions stored as "Discovering" are corrected when next written'
    );
  });

  it('cites Scout as a source once a Scout contribution exists', () => {
    const { applyAmoPresentationContract } = require('../AcquisitionOwnership');
    const mission = { id: 'm1', title: 'Mission', objective: 'Find property managers' };
    const without = applyAmoPresentationContract({}, { mission, snapshot: { contributions: [] }, created: true });
    assert.deepEqual(without.sources, ['acquisition_mission']);
    const withScout = applyAmoPresentationContract({}, {
      mission,
      snapshot: { contributions: [{ specialist: 'scout', kind: 'discovery', payload: {} }] },
      created: false,
    });
    assert.deepEqual(withScout.sources, ['acquisition_mission', 'scout']);
  });
});
