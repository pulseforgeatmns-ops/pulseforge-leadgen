'use strict';

/**
 * P0 execution-intent routing.
 *
 * Explicit execution requests ("go ahead and launch the campaign", "let's run
 * it") used to match the execution-policy session directive and be consumed by
 * the session-configuration bypass. They must instead reach the execution-intent
 * resolver, where:
 *   1. an explicit, specified request routes into the governed mission system;
 *   2. a contextual approval resolves only against the matching pending decision,
 *      under the existing approval and authorization gates;
 *   3. a real session-setting change updates the setting and says nothing ran.
 *
 * Deterministic: no network, no database, no provider. Scout is a stub.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const approval = require('../AmoOperatorApproval');
const { createHermeticWorkspaceEngine: createWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { isSessionConfigurationMessage, classifyMessageType } = require('../MessageTypeClassifier');
const { MESSAGE_TYPES } = require('../MessageType');
const { resolveOperatorObjective } = require('../OperatorObjectiveResolutionEngine');
const { PRIMARY_OBJECTIVES } = require('../PrimaryObjective');
const { mismatchedActionTargets } = require('../PendingDecisionResolver');
const { getSessionState, EXECUTION_POLICIES } = require('../SessionState');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

const EXPLICIT_EXECUTION_REQUESTS = [
  "Let's run it",
  'Go ahead and run Scout',
  "Let's proceed",
  'Go ahead and approve',
  "Let's launch",
  'Go ahead and launch the campaign',
  'Go ahead and send the emails',
  "Let's execute",
  "Let's run discovery",
  'Time to execute',
];

const REAL_SETTINGS = [
  "For the rest of this conversation: Don't execute anything.",
  'For the rest of this conversation, go ahead and execute without asking.',
  'Be concise.',
  'Enable execution',
  'Stop theorizing',
];

const STAGES = { PLAN: 'plan_approval', DISCOVERY: 'discovery_approval' };

async function setup({ stage = STAGES.PLAN, constraint = null, scout = 'ok', tenantId = '10' } = {}) {
  const engine = amo.createAcquisitionMissionEngine();
  let mission = engine.create({
    tenantId,
    objective: OBJECTIVE,
    targetSegment: 'Law Firms',
    authorizationConstraints: constraint ? amo.extractAuthorizationConstraints(constraint) : null,
  });
  if (stage === STAGES.DISCOVERY) {
    await approval.advancePlanAfterApproval({ engine, mission, tenantId, question: 'Approved.' });
    mission = engine.get(mission.id, tenantId);
  }
  const scoutCalls = [];
  const ws = createWorkspaceEngine({
    missionsEnabled: true,
    resolverEnabled: true,
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    scoutAcquisitionOpts: {
      runScout: async (input) => {
        scoutCalls.push(input);
        if (scout === 'throws') throw new Error('provider unavailable');
        return { status: 'completed', payload: {} };
      },
    },
  });
  const open = (t = tenantId, bind = true) => {
    const opened = ws.open({ tenantId: t, page: 'command-deck' });
    if (bind) {
      const session = ws._sessions.get(opened.sessionId);
      session.context.missionId = mission.id;
      session.context.acquisitionMissionId = mission.id;
    }
    return opened.sessionId;
  };
  const snapshot = () => {
    const m = engine.get(mission.id, tenantId);
    return JSON.stringify({ stage: m.stage, pending: m.pendingOperatorDecision && m.pendingOperatorDecision.kind });
  };
  return { engine, mission, ws, open, scoutCalls, snapshot, tenantId };
}

function routed() {
  return amo.listExecutionRouterAudit().map((a) => ({ intent: a.intent, outcome: a.outcome }));
}

describe('P0 execution-intent routing — classification', () => {
  it('explicit execution intent is never classified as a session setting', () => {
    for (const q of EXPLICIT_EXECUTION_REQUESTS) {
      assert.equal(isSessionConfigurationMessage(q), false, q);
      assert.notEqual(classifyMessageType(q).type, MESSAGE_TYPES.SESSION_CONFIGURATION, q);
      for (const hasActiveMission of [false, true]) {
        const r = resolveOperatorObjective({ question: q, hasActiveMission });
        assert.notEqual(r.primaryObjective, PRIMARY_OBJECTIVES.WORKSPACE_OPERATION, `${q} (mission=${hasActiveMission})`);
      }
    }
  });

  it('with an active mission, explicit execution intent binds to mission execution', () => {
    for (const q of EXPLICIT_EXECUTION_REQUESTS) {
      const r = resolveOperatorObjective({ question: q, hasActiveMission: true });
      assert.equal(r.primaryObjective, PRIMARY_OBJECTIVES.MISSION_EXECUTION, q);
    }
  });

  it('real session settings stay session settings', () => {
    for (const q of REAL_SETTINGS) {
      assert.equal(isSessionConfigurationMessage(q), true, q);
    }
  });

  it('negated and interrogative execution wording is not an execution command', () => {
    for (const q of ["Don't launch the campaign yet.", 'Should we launch the campaign?', "Can we go ahead and send the emails?"]) {
      const r = resolveOperatorObjective({ question: q, hasActiveMission: false });
      assert.notEqual(r.primaryObjective, PRIMARY_OBJECTIVES.MISSION_EXECUTION, q);
    }
  });
});

describe('P0 execution-intent routing — no mission: explicit intent is reported, never consumed', () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  for (const question of EXPLICIT_EXECUTION_REQUESTS) {
    it(`"${question}" → not a session setting, nothing routed, explicit outcome`, async () => {
      const ws = createWorkspaceEngine();
      const opened = ws.open({ tenantId: '10' });
      const result = await ws.ask({ sessionId: opened.sessionId, question });
      assert.notEqual(result.resolution && result.resolution.action, 'session_configured');
      assert.notEqual(String(result.prose).trim(), 'Acknowledged.');
      assert.match(
        result.prose,
        /Not executed: there is no mission|Not executed: this request was routed|will not launch or send anything/,
      );
      assert.deepEqual(routed(), []);
    });
  }
});

describe('P0 execution-intent routing — governed mission execution', () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  it('authorized explicit request reaches the correct execution route (APPROVE_DISCOVERY → Scout)', async () => {
    const t = await setup({ stage: STAGES.DISCOVERY });
    const result = await t.ws.ask({ sessionId: t.open(), question: 'Go ahead and run Scout' });
    assert.equal(result.resolution.action, 'discovery_approved');
    assert.deepEqual(routed().map((r) => r.intent), ['APPROVE_DISCOVERY']);
    assert.equal(t.scoutCalls.length, 1);
    // Dispatched is not completed: a stub result has no live discovery evidence.
    assert.equal(result.structured.metadata.executionVerified, false);
    assert.notEqual(result.structured.metadata.executionState, 'completed_verified');
  });

  it('contextual "Let\'s run it" resolves against the pending discovery decision only', async () => {
    const t = await setup({ stage: STAGES.DISCOVERY });
    const result = await t.ws.ask({ sessionId: t.open(), question: "Let's run it" });
    assert.equal(result.resolution.action, 'discovery_approved');
    assert.deepEqual(routed().map((r) => r.intent), ['APPROVE_DISCOVERY']);
  });

  it('contextual "Let\'s run it" with a plan decision pending asks for the decision instead', async () => {
    const t = await setup({ stage: STAGES.PLAN });
    const before = t.snapshot();
    const result = await t.ws.ask({ sessionId: t.open(), question: "Let's run it" });
    assert.equal(result.resolution.action, 'clarify');
    assert.match(result.prose, /Approve mission plan\?/);
    assert.deepEqual(routed(), []);
    assert.equal(t.snapshot(), before);
  });

  it('unauthorized: a review-only mission refuses the request and Scout never runs', async () => {
    const t = await setup({ stage: STAGES.DISCOVERY, constraint: 'Review only.' });
    const before = t.snapshot();
    const result = await t.ws.ask({ sessionId: t.open(), question: 'Go ahead and run Scout' });
    assert.equal(t.scoutCalls.length, 0);
    assert.deepEqual(routed(), [{ intent: 'APPROVE_DISCOVERY', outcome: 'blocked_by_constraint' }]);
    assert.match(result.prose, /^Not executed:/);
    assert.equal(result.structured.metadata.executionState, 'blocked');
    assert.equal(t.snapshot(), before, 'approval not consumed');
  });

  for (const [stage, question, mismatch] of [
    [STAGES.PLAN, 'Go ahead and send the emails', 'outbound'],
    [STAGES.PLAN, 'Go ahead and launch the campaign', 'outbound'],
    [STAGES.DISCOVERY, 'Go ahead and send the emails', 'outbound'],
    [STAGES.DISCOVERY, 'Go ahead and launch the campaign', 'outbound'],
    [STAGES.DISCOVERY, "Let's approve the plan", 'plan'],
  ]) {
    it(`ambiguous approval "${question}" cannot approve an unrelated ${stage} decision`, async () => {
      const t = await setup({ stage });
      const before = t.snapshot();
      const result = await t.ws.ask({ sessionId: t.open(), question });
      assert.equal(result.resolution.action, 'clarify');
      assert.match(result.prose, new RegExp(`^Not executed: you asked for ${mismatch}`));
      assert.match(result.prose, /Nothing was approved or dispatched/);
      assert.deepEqual(routed(), []);
      assert.equal(t.scoutCalls.length, 0);
      assert.equal(t.snapshot(), before);
    });
  }

  it('"run Scout" with a plan decision pending approves only the plan; Scout waits for discovery approval', async () => {
    const t = await setup({ stage: STAGES.PLAN });
    const result = await t.ws.ask({ sessionId: t.open(), question: 'Go ahead and run Scout' });
    assert.equal(result.resolution.action, 'plan_approved');
    assert.deepEqual(routed(), [{ intent: 'APPROVE_PLAN', outcome: 'dispatched' }]);
    assert.equal(t.scoutCalls.length, 0, 'Scout needs its own discovery approval');
    assert.equal(JSON.parse(t.snapshot()).pending, STAGES.DISCOVERY);
  });

  it('matching named approvals still resolve their own decision', () => {
    assert.equal(mismatchedActionTargets('plan_approval', 'Approve the plan'), null);
    assert.equal(mismatchedActionTargets('discovery_approval', 'Approve discovery, no outreach yet'), null);
    assert.equal(mismatchedActionTargets('execution_approval', 'Go ahead and send the emails'), null);
    assert.equal(mismatchedActionTargets('discovery_approval', 'Go ahead'), null, 'generic approval has no named target');
    assert.deepEqual(mismatchedActionTargets('plan_approval', 'Go ahead and send the emails'), ['outbound']);
  });
});

describe('P0 execution-intent routing — outcome states are distinguished', () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  it('requested but not authorized: clarify, no execution state claimed', async () => {
    const t = await setup({ stage: STAGES.PLAN });
    const result = await t.ws.ask({ sessionId: t.open(), question: 'Go ahead and send the emails' });
    assert.equal(result.resolution.action, 'clarify');
    assert.notEqual(result.structured.metadata.executionState, 'completed_verified');
    assert.notEqual(result.structured.metadata.executionState, 'committed_internal');
  });

  it('authorized internal step: committed_internal with internal evidence', async () => {
    const t = await setup({ stage: STAGES.PLAN });
    const result = await t.ws.ask({ sessionId: t.open(), question: "Let's approve the plan" });
    assert.equal(result.resolution.action, 'plan_approved');
    assert.equal(result.structured.metadata.executionState, 'committed_internal');
    assert.deepEqual(routed(), [{ intent: 'APPROVE_PLAN', outcome: 'dispatched' }]);
  });

  it('dispatched without verifiable evidence: blocked, never completed', async () => {
    const t = await setup({ stage: STAGES.DISCOVERY });
    const result = await t.ws.ask({ sessionId: t.open(), question: 'Go ahead and run Scout' });
    assert.deepEqual(routed(), [{ intent: 'APPROVE_DISCOVERY', outcome: 'dispatched' }]);
    assert.equal(result.structured.metadata.executionState, 'blocked');
    assert.match(result.prose, /did not complete/);
  });

  it('specialist failure: failed and rolled back, approval still pending', async () => {
    const t = await setup({ stage: STAGES.DISCOVERY, scout: 'throws' });
    const before = t.snapshot();
    const result = await t.ws.ask({ sessionId: t.open(), question: 'Go ahead and run Scout' });
    assert.deepEqual(routed(), [{ intent: 'APPROVE_DISCOVERY', outcome: 'rolled_back' }]);
    assert.equal(result.structured.metadata.executionState, 'failed');
    assert.equal(result.structured.metadata.executionVerified, false);
    assert.equal(t.snapshot(), before);
  });
});

describe('P0 execution-intent routing — tenant isolation', () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  it('a session for another tenant cannot execute against this tenant\'s mission', async () => {
    const t = await setup({ stage: STAGES.DISCOVERY, tenantId: '10' });
    const before = t.snapshot();
    const foreign = t.open('2');
    await assert.rejects(
      t.ws.ask({ sessionId: foreign, question: 'Go ahead and run Scout' }),
      /does not belong to this tenant/,
    );
    assert.equal(t.scoutCalls.length, 0);
    assert.deepEqual(routed(), []);
    assert.equal(t.snapshot(), before);
  });

  it('a real setting in one session does not leak into another tenant\'s session', async () => {
    const ws = createWorkspaceEngine();
    const a = ws.open({ tenantId: '10' });
    const b = ws.open({ tenantId: '2' });
    const ack = await ws.ask({ sessionId: a.sessionId, question: "For the rest of this conversation: Don't execute anything." });
    assert.match(ack.prose, /^Acknowledged\. Session settings updated\. No operational work was executed\./);
    const stateB = getSessionState(ws._sessions.get(b.sessionId));
    assert.notEqual(stateB && stateB.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
  });
});
