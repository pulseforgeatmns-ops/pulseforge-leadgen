'use strict';

/**
 * P0 R-1 — a retracted, withdrawn, self-contradicted or ambiguous decision is
 * never resolved (fail closed), at every pending-decision gate.
 *
 * Found by the P0-MAX-REASONING-001 integration review: a retraction idiom
 * that carried no negation or uncertainty marker was ignored, so the preceding
 * clause resolved —
 *   - "Go ahead. Scratch that." / "Forget it." / "Strike that." dispatched the
 *     gate's approval (APPROVE_EXECUTION after the package was shown);
 *   - "Approve it. Cancel that." dispatched CANCEL_PLAN.
 *
 * Contract:
 *   - an affirmative decision followed (or preceded) by a retraction of what
 *     was just said is AMBIGUOUS: nothing approved, sent or cancelled, the
 *     decision stays pending, and the operator is asked to answer explicitly;
 *   - an approval and a cancellation in separate sentences of one message
 *     contradict each other and are AMBIGUOUS;
 *   - explicit cancellations ("Cancel it.", "Yes, cancel it.", "Go ahead and
 *     cancel it.", "Don't send it. Cancel the mission.") and explicit approvals
 *     keep their meaning;
 *   - a retraction on the next turn never approves the next gate or cancels.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const A = require('../AmoOperatorApproval');
const { liveScoutStub } = require('./amoTestRuntime');
const originalAdvanceDiscovery = A.advanceDiscoveryAfterApproval;
A.advanceDiscoveryAfterApproval = (input = {}) =>
  originalAdvanceDiscovery({ ...input, runScout: input.runScout || liveScoutStub });

const amo = require('../../../acquisition-mission');
const { OPERATOR_DECISION_KINDS: K } = require('../../../acquisition-mission/types');
const { resolvePendingOperatorDecision, RESOLUTION_OUTCOMES: O } = require('../PendingDecisionResolver');
const { detectExecutionAction } = require('../AcquisitionMissionExecution');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const TENANT = '10';
const ALL_KINDS = [
  K.PLAN_APPROVAL,
  K.PLAN_CLARIFICATION,
  K.DISCOVERY_APPROVAL,
  K.DISCOVERY_INVESTIGATION,
  K.PRIORITIZATION_APPROVAL,
  K.EXECUTION_APPROVAL,
];
const GATES = [K.PLAN_APPROVAL, K.DISCOVERY_APPROVAL, K.PRIORITIZATION_APPROVAL, K.EXECUTION_APPROVAL];

const SAME_TURN_RETRACTIONS = [
  'Go ahead. Scratch that.',
  'Go ahead. Forget it.',
  'Approved. Strike that.',
  'Approve it. Cancel that.',
  'Yes, go ahead. Never mind.',
  'Approved. Actually, wait — hold on.',
  "Approve it. On second thought, don't.",
  'Approved — no, wait.',
  'Yes. Disregard that.',
  'Go ahead. Undo that.',
  'Approved. I take it back.',
  'I withdraw my approval.',
  'Revoke my approval.',
];

const CONTRADICTED_BY_CANCEL = [
  'Approved. Cancel the mission.',
  'Go ahead. Actually, cancel it.',
  'Cancel it. Approve the plan.',
  'Proceed. Abort the mission.',
];

const AMBIGUOUS = ['Hmm.', 'ok?', 'Yes and no.', 'I think so, maybe.', 'Not sure.'];

const EXPLICIT_CANCELS = [
  'Cancel it.',
  'Cancel the mission.',
  'Yes, cancel it.',
  'Yes. Cancel the mission.',
  'Go ahead and cancel it.',
  "Don't send it. Cancel the mission.",
];

function pendingMission(kind) {
  const pending = { kind, prompt: 'Decide.' };
  if (kind === K.PLAN_CLARIFICATION) {
    pending.field = 'geography.region';
    pending.responseType = 'structured_clarification';
  }
  return { id: 'm_r1', stage: 'x', pendingOperatorDecision: pending };
}

/** detectExecutionAction on a bare fixture: no action, or a fail-closed state check. */
function detectedAction(q, mission) {
  try {
    return detectExecutionAction(q, { mission });
  } catch (e) {
    assert.equal(e.code, 'MISSION_STATE_INCONSISTENT', String(e && e.message));
    return null;
  }
}

describe('R-1 resolver matrix — every pending-decision kind', () => {
  for (const kind of ALL_KINDS) {
    for (const q of [...SAME_TURN_RETRACTIONS, ...CONTRADICTED_BY_CANCEL]) {
      it(`${kind}: "${q}" is held as ambiguous`, () => {
        const r = resolvePendingOperatorDecision(q, pendingMission(kind));
        assert.equal(r.resolved, false, q);
        assert.notEqual(r.resolvedFromPendingDecision, true, q);
        assert.equal(r.executionIntent || null, null, q);
        assert.notEqual(r.action, 'cancel', q);
        const action = detectedAction(q, pendingMission(kind));
        assert.ok(action === null || action === 'pending_operator_decision', `${q} → ${action}`);
      });
    }
    // A structured plan clarification captures a free-text field answer
    // (CLARIFY_PLAN supplies an input; plan approval is still required), so
    // "Not sure." is captured as the answer there. That is input quality, not
    // authority, and is tracked separately from R-1.
    for (const q of kind === K.PLAN_CLARIFICATION ? [] : AMBIGUOUS) {
      it(`${kind}: "${q}" resolves no authority`, () => {
        const r = resolvePendingOperatorDecision(q, pendingMission(kind));
        assert.notEqual(r.resolvedFromPendingDecision, true, q);
        assert.equal(r.executionIntent || null, null, q);
      });
    }
  }

  for (const kind of GATES) {
    for (const q of ['Go ahead. Scratch that.', 'Approve it. Cancel that.', 'Approved. Cancel the mission.']) {
      it(`${kind}: "${q}" carries the retraction marker`, () => {
        const r = resolvePendingOperatorDecision(q, pendingMission(kind));
        assert.equal(r.outcome, O.AMBIGUOUS);
        assert.equal(r.held, true);
        assert.equal(r.retractedAuthorization, true);
      });
    }
    for (const q of EXPLICIT_CANCELS) {
      it(`${kind}: control — "${q}" is still an explicit cancellation`, () => {
        const r = resolvePendingOperatorDecision(q, pendingMission(kind));
        assert.equal(r.outcome, O.REJECT, q);
        assert.equal(r.action, 'cancel', q);
        assert.equal(r.resolved, true, q);
        assert.equal(r.executionIntent, 'CANCEL_PLAN', q);
      });
    }
    for (const q of ['Approved.', 'Go ahead.', 'Yes.']) {
      it(`${kind}: control — "${q}" still approves`, () => {
        const r = resolvePendingOperatorDecision(q, pendingMission(kind));
        assert.equal(r.outcome, O.AFFIRM, q);
        assert.equal(r.resolved, true, q);
      });
    }
  }
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
  // Show the package so the presentation guard is not what stops a dispatch.
  if (kind === K.EXECUTION_APPROVAL) await ws.ask({ sessionId: opened.sessionId, question: 'Show me the prepared outreach.' });
  return {
    ask: (question) => ws.ask({ sessionId: opened.sessionId, question }),
    fingerprint: () => JSON.stringify({ m: g(), e: engine.store.listEvents(mission.id), c: engine.store.listContributions(mission.id) }),
    get: g,
  };
}

const intents = () => amo.listExecutionRouterAudit().map((a) => a.intent);

describe('R-1 end to end — every reachable gate', { concurrency: false }, () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  for (const kind of GATES) {
    for (const q of [...SAME_TURN_RETRACTIONS, ...CONTRADICTED_BY_CANCEL, ...AMBIGUOUS]) {
      it(`${kind}: "${q}" dispatches nothing and leaves the decision pending`, async () => {
        const c = await missionAt(kind);
        amo.clearExecutionRouterAudit();
        const before = c.fingerprint();
        const r = await c.ask(q);
        assert.deepEqual(intents(), [], 'no execution-router dispatch');
        assert.equal(c.fingerprint(), before, 'mission, events and contributions unchanged');
        assert.equal(c.get().pendingOperatorDecision.kind, kind, 'decision still pending');
        assert.equal(Boolean(c.get().planCancelled), false, 'mission not cancelled');
        assert.ok(String(r.prose || '').trim(), 'the operator gets an answer');
      });
    }

    it(`${kind}: the operator is told nothing happened and how to answer`, async () => {
      const c = await missionAt(kind);
      const r = await c.ask('Go ahead. Scratch that.');
      assert.match(String(r.prose), /Not executed/);
      assert.match(String(r.prose), /Nothing was approved, sent, or cancelled/);
    });

    it(`${kind}: control — "Approved." still dispatches this gate's approval`, async () => {
      const c = await missionAt(kind);
      amo.clearExecutionRouterAudit();
      await c.ask('Approved.');
      assert.deepEqual(intents(), [{
        [K.PLAN_APPROVAL]: 'APPROVE_PLAN',
        [K.DISCOVERY_APPROVAL]: 'APPROVE_DISCOVERY',
        [K.PRIORITIZATION_APPROVAL]: 'APPROVE_PRIORITIZATION',
        [K.EXECUTION_APPROVAL]: 'APPROVE_EXECUTION',
      }[kind]]);
    });

    it(`${kind}: control — "Yes, cancel it." still cancels through the governed router`, async () => {
      const c = await missionAt(kind);
      amo.clearExecutionRouterAudit();
      await c.ask('Yes, cancel it.');
      assert.deepEqual(intents(), ['CANCEL_PLAN']);
      assert.equal(c.get().planCancelled, true);
    });
  }

  for (const q of ['Wait, I take that back.', 'Scratch that.', 'Never mind.', 'I withdraw my approval.']) {
    it(`withdrawn on the next turn: "${q}" neither approves the next gate nor cancels`, async () => {
      const c = await missionAt(K.PLAN_APPROVAL);
      await c.ask('Approved.');
      assert.deepEqual(intents(), ['APPROVE_PLAN'], 'control: the plan approval dispatched');
      const next = c.get().pendingOperatorDecision.kind;
      amo.clearExecutionRouterAudit();
      const before = c.fingerprint();
      await c.ask(q);
      assert.deepEqual(intents(), [], 'a retraction is never the next approval or a cancellation');
      assert.equal(c.fingerprint(), before);
      assert.equal(c.get().pendingOperatorDecision.kind, next);
      assert.equal(Boolean(c.get().planCancelled), false);
    });
  }
});
