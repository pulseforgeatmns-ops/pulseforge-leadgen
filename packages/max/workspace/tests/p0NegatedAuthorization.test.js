'use strict';

/**
 * P0 — negated authorization never dispatches (authorization polarity).
 *
 * Found by the semantic generalization audit: PendingDecisionResolver matched
 * approval vocabulary on raw text, so
 *   - "Don't proceed." / "Do not go ahead." resolved to AFFIRM at every
 *     pending-decision kind (after the outreach package was shown, "Don't
 *     proceed." dispatched APPROVE_EXECUTION and consumed the approval);
 *   - "Don't approve this." resolved to AFFIRM at plan / discovery /
 *     prioritization / investigation gates (dispatching APPROVE_PLAN …) and to
 *     REJECT → CANCEL_PLAN at the execution gate;
 *   - "Don't cancel the mission." resolved to cancel.
 *
 * Contract:
 *   - a prohibition of approve / authorize / proceed / go ahead / send /
 *     execute / launch / continue … is a HELD decline: outcome REJECT,
 *     action 'decline', nothing approved, sent, or cancelled;
 *   - an affirmative authorization contradicted by a prohibition of the same
 *     decision is AMBIGUOUS (never dispatched);
 *   - an approval whose prohibitions name a different, later action
 *     ("Approved. Do not run Scout discovery yet." on a plan) holds until the restriction can be durably enforced;
 *   - explicit approvals and explicit cancellations keep their meaning.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

// Isolate the Scout provider boundary (as audit008 does) BEFORE the workspace
// modules load, so the discovery-gate "Approved." control runs the real
// approval transaction without reaching the tenant database or network.
const A = require('../AmoOperatorApproval');
const { liveScoutStub: scoutProviderStub } = require('./amoTestRuntime');
const originalAdvanceDiscovery = A.advanceDiscoveryAfterApproval;
A.advanceDiscoveryAfterApproval = (input = {}) =>
  originalAdvanceDiscovery({ ...input, runScout: input.runScout || scoutProviderStub });

const amo = require('../../../acquisition-mission');
const { OPERATOR_DECISION_KINDS: K } = require('../../../acquisition-mission/types');
const { EXECUTION_INTENTS } = require('../../../acquisition-mission/ExecutionRequest');
const {
  resolvePendingOperatorDecision,
  authorizationPolarityVerdict,
  RESOLUTION_OUTCOMES: O,
} = require('../PendingDecisionResolver');
const { classifyAuthorizationPolarity, prohibitsAuthorization } = require('../NegatedDirective');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const { maybeHandleAcquisitionMissionExecution } = require('../AcquisitionMissionExecution');
const { createTestAmoRuntime, liveScoutStub } = require('./amoTestRuntime');

const TENANT = '10';
const ALL_KINDS = [
  K.PLAN_APPROVAL,
  K.PLAN_CLARIFICATION,
  K.DISCOVERY_APPROVAL,
  K.DISCOVERY_INVESTIGATION,
  K.PRIORITIZATION_APPROVAL,
  K.EXECUTION_APPROVAL,
];

const QUALIFIED = [
  'Maybe approve the plan? Not sure yet.',
  'Approve it, but hold the emails until Monday.',
  'Perhaps go ahead with this.',
  'I guess this is approved.',
  'Yes, provided the compliance review succeeds.',
  'Authorize this only after the customer confirms.',
  'Approved. Wait for my next message before sending.',
  'Go ahead unless finance objects.',
  'Approved. Do not run Scout discovery yet.',
];

const PROHIBITIONS = [
  "Don't proceed.",
  "Don't approve this.",
  'Do not approve this.',
  'Do not go ahead.',
  'No, do not approve.',
  "No, don't send this.",
  'Do not send it.',
  'Do not authorize this.',
  "Don't execute this.",
  'Never launch this.',
  "Don't continue.",
  'Do not move forward with this.',
  // Inline negation the clause splitter does not cover.
  'I would not approve this.',
  'We should not send this.',
  "We can't proceed with this.",
];

function pendingMission(kind) {
  const pending = { kind, prompt: 'Decide.' };
  if (kind === K.PLAN_CLARIFICATION) {
    pending.field = 'geography.region';
    pending.responseType = 'structured_clarification';
  }
  return { id: 'm_p0', stage: 'x', pendingOperatorDecision: pending };
}

describe('P0 authorization polarity — resolver matrix (every pending-decision kind)', () => {
  for (const kind of ALL_KINDS) {
    for (const q of QUALIFIED) {
      it(`${kind}: qualified decision holds without authority`, () => {
        const r = resolvePendingOperatorDecision(q, pendingMission(kind));
        assert.equal(r.resolved, false, q);
        assert.equal(r.executionIntent || null, null, q);
        assert.equal(r.resolvedFromPendingDecision, false, q);
      });
    }
    for (const q of PROHIBITIONS) {
      it(`${kind}: "${q}" is a held decline — never approve, never cancel`, () => {
        const r = resolvePendingOperatorDecision(q, pendingMission(kind));
        assert.equal(r.resolved, false);
        assert.equal(r.resolvedFromPendingDecision, false);
        assert.equal(r.outcome, O.REJECT);
        assert.equal(r.action, 'decline');
        assert.equal(r.held, true);
        assert.equal(r.executionIntent, null);
        assert.equal(r.executionAction, null);
      });
    }

    it(`${kind}: "Don't cancel the mission." does not cancel`, () => {
      const r = resolvePendingOperatorDecision("Don't cancel the mission.", pendingMission(kind));
      assert.equal(r.resolved, false);
      assert.notEqual(r.action, 'cancel');
      assert.notEqual(r.executionIntent, EXECUTION_INTENTS.CANCEL_PLAN);
    });

    it(`${kind}: an explicit cancellation still cancels`, () => {
      for (const q of ['Cancel the mission.', "Don't send it. Cancel the mission."]) {
        const r = resolvePendingOperatorDecision(q, pendingMission(kind));
        assert.equal(r.resolved, true, q);
        assert.equal(r.outcome, O.REJECT, q);
        assert.equal(r.action, 'cancel', q);
        assert.equal(r.executionIntent, EXECUTION_INTENTS.CANCEL_PLAN, q);
      }
    });

    it(`${kind}: questions about approval are questions, not decisions`, () => {
      for (const q of ['Why should we approve this?', 'What happens if we approve this?', 'What happens if we do not approve this?']) {
        const r = resolvePendingOperatorDecision(q, pendingMission(kind));
        assert.equal(r.resolved, false, q);
        assert.notEqual(r.outcome, O.AFFIRM, q);
        assert.equal(r.executionIntent || null, null, q);
      }
    });
  }

  for (const kind of [K.PLAN_APPROVAL, K.DISCOVERY_APPROVAL, K.DISCOVERY_INVESTIGATION, K.PRIORITIZATION_APPROVAL, K.EXECUTION_APPROVAL]) {
    it(`${kind}: explicit approvals still approve`, () => {
      for (const q of ['Approved.', 'Go ahead.', 'Yes.']) {
        const r = resolvePendingOperatorDecision(q, pendingMission(kind));
        assert.equal(r.resolved, true, q);
        assert.equal(r.outcome, O.AFFIRM, q);
        assert.ok(r.executionIntent, q);
      }
    });
  }

  it('"Approved, but don\'t send it." never cancels the mission at any gate', () => {
    for (const kind of ALL_KINDS) {
      const r = resolvePendingOperatorDecision("Approved, but don't send it.", pendingMission(kind));
      assert.notEqual(r.action, 'cancel', kind);
      assert.notEqual(r.executionIntent, EXECUTION_INTENTS.CANCEL_PLAN, kind);
    }
    // Future-stage restrictions cannot be dropped at internal gates.
    assert.equal(resolvePendingOperatorDecision("Approved, but don't send it.", pendingMission(K.PLAN_APPROVAL)).resolved, false);
    // Execution gate: the same sentence contradicts itself and is held.
    assert.equal(resolvePendingOperatorDecision("Approved, but don't send it.", pendingMission(K.EXECUTION_APPROVAL)).outcome, O.AMBIGUOUS);
  });

  it('execution gate: an authorization contradicted by a no-send prohibition is ambiguous', () => {
    for (const q of ['Approve it but do not send anything.', "Go ahead, but don't send it."]) {
      const r = resolvePendingOperatorDecision(q, pendingMission(K.EXECUTION_APPROVAL));
      assert.equal(r.resolved, false, q);
      assert.equal(r.outcome, O.AMBIGUOUS, q);
      assert.equal(r.mixedPolarity, true, q);
      assert.equal(r.executionIntent, null, q);
    }
  });

  it('an untargeted prohibition contradicting an approval is ambiguous at every gate', () => {
    for (const kind of ALL_KINDS) {
      const r = resolvePendingOperatorDecision('Approved, but do not proceed.', pendingMission(kind));
      assert.equal(r.resolved, false, kind);
      assert.notEqual(r.outcome, O.AFFIRM, kind);
    }
  });

  it('plan gate: approval plus a prohibition of a later action holds rather than losing the future restriction', () => {
    assert.equal(authorizationPolarityVerdict('Approved. Do not run Scout discovery yet.', K.PLAN_APPROVAL).verdict, 'mixed');
    const r = resolvePendingOperatorDecision('Approved. Do not run Scout discovery yet.', pendingMission(K.PLAN_APPROVAL));
    assert.equal(r.outcome, O.AMBIGUOUS);
    assert.equal(r.resolved, false);
  });

  it('execution gate: a no-send reply with an explicit revision request still revises', () => {
    const r = resolvePendingOperatorDecision("Don't send this. Rewrite the messaging first.", pendingMission(K.EXECUTION_APPROVAL));
    assert.equal(r.outcome, O.REQUEST_REVISION);
    assert.equal(r.executionIntent, EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH);
  });

  it('polarity classifier separates negated from affirmative vocabulary', () => {
    assert.deepEqual(
      (({ negatedAuthorization, affirmativeAuthorization, affirmativeCancel }) => ({ negatedAuthorization, affirmativeAuthorization, affirmativeCancel }))(
        classifyAuthorizationPolarity("Don't proceed.")
      ),
      { negatedAuthorization: true, affirmativeAuthorization: false, affirmativeCancel: false }
    );
    assert.equal(prohibitsAuthorization('Approved.'), false);
    assert.equal(prohibitsAuthorization('Cancel it.'), false);
    assert.equal(prohibitsAuthorization("Don't cancel the mission."), true);
    assert.equal(prohibitsAuthorization("Don't send it. Cancel the mission."), false);
  });
});

// ---------------------------------------------------------------------------
// End to end — real WorkspaceEngine.ask() at each reachable gate
// ---------------------------------------------------------------------------

async function missionAt(kind) {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: TENANT, objective: 'Acquire recurring commercial cleaning customers for Anchor Cleaning in Greater Manchester.', targetSegment: 'Facility buyers' });
  const g = () => engine.get(mission.id, TENANT);
  if (kind !== K.PLAN_APPROVAL) await A.advancePlanAfterApproval({ engine, mission, tenantId: TENANT, question: 'Approved.' });
  if (kind === K.PRIORITIZATION_APPROVAL || kind === K.EXECUTION_APPROVAL) {
    await A.advanceDiscoveryAfterApproval({ engine, mission: g(), tenantId: TENANT, question: 'Approved. Begin Discovery.', runScout: liveScoutStub });
  }
  if (kind === K.EXECUTION_APPROVAL) {
    await A.advancePrioritizationAfterApproval({ engine, mission: g(), tenantId: TENANT, question: 'Approved prioritization.' });
    await A.advanceMaxPrioritization({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true });
    await A.advanceAcquisitionApproach({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true });
    await A.advancePaigeVariants({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true, question: 'Generate variants.' });
    await A.advanceEmmettCapacity({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true, question: 'Plan capacity.' });
  }
  assert.equal(g().pendingOperatorDecision.kind, kind, 'fixture reached the gate');
  const ws = createHermeticWorkspaceEngine({ disableLlm: true, missionsEnabled: true, resolverEnabled: true, acquisitionMissionRuntime: createTestAmoRuntime({ engine }) });
  const opened = await ws.open({ tenantId: TENANT, page: 'command-deck' });
  const session = ws._sessions.get(opened.sessionId);
  session.context.missionId = mission.id;
  session.context.acquisitionMissionId = mission.id;
  // The execution gate requires the package to have been shown first; show it
  // so the presentation guard is NOT what stops the dispatch under test.
  if (kind === K.EXECUTION_APPROVAL) await ws.ask({ sessionId: opened.sessionId, question: 'Show me the prepared outreach.' });
  return {
    engine,
    ask: (question) => ws.ask({ sessionId: opened.sessionId, question }),
    fingerprint: () => JSON.stringify({ m: g(), e: engine.store.listEvents(mission.id), c: engine.store.listContributions(mission.id) }),
    get: g,
  };
}

const E2E_GATES = [K.PLAN_APPROVAL, K.DISCOVERY_APPROVAL, K.PRIORITIZATION_APPROVAL, K.EXECUTION_APPROVAL];
const E2E_PROHIBITIONS = ["Don't proceed.", "Don't approve this.", 'Do not go ahead.', "No, don't send this.", 'Do not authorize this.', "Don't cancel the mission."];

describe('P0 authorization polarity — end to end', { concurrency: false }, () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  it('exact reproduction: after the package is shown, "Don\'t proceed." does not dispatch APPROVE_EXECUTION', async () => {
    const c = await missionAt(K.EXECUTION_APPROVAL);
    amo.clearExecutionRouterAudit();
    const before = c.fingerprint();
    const r = await c.ask("Don't proceed.");
    assert.deepEqual(amo.listExecutionRouterAudit(), []);
    assert.equal(c.fingerprint(), before);
    assert.equal(c.get().pendingOperatorDecision.kind, K.EXECUTION_APPROVAL);
    assert.match(r.prose, /not approved/i);
    assert.match(r.prose, /Nothing was approved, sent, or cancelled/);
  });

  for (const kind of E2E_GATES) {
    for (const q of [...E2E_PROHIBITIONS, ...QUALIFIED]) {
      it(`${kind}: "${q}" dispatches nothing and leaves the mission unchanged`, async () => {
        const c = await missionAt(kind);
        amo.clearExecutionRouterAudit();
        const before = c.fingerprint();
        const r = await c.ask(q);
        assert.deepEqual(amo.listExecutionRouterAudit(), [], 'no execution-router dispatch');
        assert.equal(c.fingerprint(), before, 'mission, events and contributions unchanged');
        assert.equal(c.get().pendingOperatorDecision.kind, kind, 'decision still pending');
        assert.equal(Boolean(c.get().planCancelled), false, 'mission not cancelled');
        assert.ok(String(r.prose || '').trim(), 'the operator gets an answer');
      });
    }

    it(`${kind}: "Approved." still dispatches the approval for this gate (control)`, async () => {
      const c = await missionAt(kind);
      amo.clearExecutionRouterAudit();
      await c.ask('Approved.');
      const intents = amo.listExecutionRouterAudit().map((a) => a.intent);
      const expected = {
        [K.PLAN_APPROVAL]: 'APPROVE_PLAN',
        [K.DISCOVERY_APPROVAL]: 'APPROVE_DISCOVERY',
        [K.PRIORITIZATION_APPROVAL]: 'APPROVE_PRIORITIZATION',
        [K.EXECUTION_APPROVAL]: 'APPROVE_EXECUTION',
      }[kind];
      assert.deepEqual(intents, [expected]);
    });
  }

  // Chat routing of a bare "Cancel the mission." to the AMO handler is
  // unchanged by this fix (it is not routed there on main either); the
  // execution handler is where an explicit cancellation must keep dispatching.
  for (const kind of E2E_GATES) {
    it(`${kind}: bare cancellation reaches the governed router`, async () => {
      const c = await missionAt(kind);
      amo.clearExecutionRouterAudit();
      await c.ask('Cancel the mission.');
      assert.deepEqual(amo.listExecutionRouterAudit().map(a => a.intent), ['CANCEL_PLAN']);
      assert.equal(c.get().planCancelled, true);
    });
  }

  it('plan gate: an explicit cancellation still dispatches CANCEL_PLAN from the execution handler', async () => {
    for (const question of ['Cancel it.', "Don't send it. Cancel it."]) {
      const engine = amo.createAcquisitionMissionEngine();
      const mission = engine.create({ tenantId: TENANT, objective: 'Acquire recurring commercial cleaning customers for Anchor Cleaning in Greater Manchester.', targetSegment: 'Facility buyers' });
      const snapshot = engine.inspect(mission.id, { tenantId: TENANT });
      const context = { tenantId: TENANT, missionId: mission.id };
      const operatorIntent = await analyzeOperatorIntent({ question, mission, snapshot, context, resolveMission: false });
      amo.clearExecutionRouterAudit();
      await maybeHandleAcquisitionMissionExecution({ question, operatorIntent, context, acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true });
      assert.deepEqual(amo.listExecutionRouterAudit().map((a) => a.intent), ['CANCEL_PLAN'], question);
    }
  });

  it('plan gate: negated prohibitions dispatch nothing from the execution handler either', async () => {
    for (const question of ["Don't proceed.", "Don't approve this.", "Don't cancel the mission."]) {
      const engine = amo.createAcquisitionMissionEngine();
      const mission = engine.create({ tenantId: TENANT, objective: 'Acquire recurring commercial cleaning customers for Anchor Cleaning in Greater Manchester.', targetSegment: 'Facility buyers' });
      const before = engine.inspect(mission.id, { tenantId: TENANT });
      const context = { tenantId: TENANT, missionId: mission.id };
      amo.clearExecutionRouterAudit();
      // With and without a resolved operator intent (defense in depth).
      const operatorIntent = await analyzeOperatorIntent({ question, mission, snapshot: before, context, resolveMission: false });
      await maybeHandleAcquisitionMissionExecution({ question, operatorIntent, context, acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true });
      await maybeHandleAcquisitionMissionExecution({ question, context, acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true });
      assert.deepEqual(amo.listExecutionRouterAudit(), [], question);
      assert.deepEqual(engine.inspect(mission.id, { tenantId: TENANT }), before, question);
    }
  });

  it('execution gate: an approval contradicted by a no-send prohibition dispatches nothing', async () => {
    const c = await missionAt(K.EXECUTION_APPROVAL);
    amo.clearExecutionRouterAudit();
    const before = c.fingerprint();
    const r = await c.ask('Approve it but do not send anything.');
    assert.deepEqual(amo.listExecutionRouterAudit(), []);
    assert.equal(c.fingerprint(), before);
    assert.match(r.prose, /Nothing was approved|nothing was approved/);
  });
});
