'use strict';

/**
 * P0-MAX-REASONING-001 integration readiness — authorization characterization.
 *
 * #1093 (prohibitions), #1096 (qualified/hedged approvals, governed
 * cancellation) and the execution-approval binding (reviewed package revision)
 * already have suites. This file covers the remaining classes the integration
 * review was asked to test explicitly, through the real WorkspaceEngine.ask():
 *
 *   - ambiguous replies at a pending gate;
 *   - approvals withdrawn in the same turn, and on the next turn;
 *   - stale approvals: an execution approval whose package presentation was
 *     lost in a restart (session recovery with the ConversationLedger on);
 *   - pending decisions across session recovery: the decision stays where the
 *     mission store puts it and recovered memory never authorizes anything.
 *
 * It changes no authorization behaviour. A case that does not hold is recorded
 * with `todo` naming the defect so it stays visible without failing the lane.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');

const A = require('../AmoOperatorApproval');
const amo = require('../../../acquisition-mission');
const { OPERATOR_DECISION_KINDS: K } = require('../../../acquisition-mission/types');
const { resolvePendingOperatorDecision } = require('../PendingDecisionResolver');
const { detectExecutionAction } = require('../AcquisitionMissionExecution');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime, liveScoutStub } = require('./amoTestRuntime');
const { ConversationContinuity } = require('../../context/ConversationContinuity');
const { ConversationLedgerRecorder } = require('../../context/ConversationLedgerRecorder');
const { validate, TTL_MS } = require('../../context/ConversationContextStore');

const TENANT = '10';
const LEDGER_ON = { MAX_CONVERSATION_CONTEXT: 'true', MAX_CONVERSATION_LEDGER: 'true' };
const GATES = [K.PLAN_APPROVAL, K.DISCOVERY_APPROVAL, K.PRIORITIZATION_APPROVAL, K.EXECUTION_APPROVAL];
const APPROVE_INTENT = {
  [K.PLAN_APPROVAL]: 'APPROVE_PLAN',
  [K.DISCOVERY_APPROVAL]: 'APPROVE_DISCOVERY',
  [K.PRIORITIZATION_APPROVAL]: 'APPROVE_PRIORITIZATION',
  [K.EXECUTION_APPROVAL]: 'APPROVE_EXECUTION',
};

const AMBIGUOUS = [
  'Hmm.',
  'ok?',
  'Yes and no.',
  'I think so, maybe.',
  'Not sure.',
];

// Defect R-1 (found by this suite, not fixed here — authorization behaviour
// changes need their own reviewed fix): a retraction idiom that carries no
// negation or uncertainty marker is ignored, so the preceding approval
// resolves. At every gate "Go ahead. Scratch that." dispatches the approval
// (APPROVE_EXECUTION after the package is shown, consuming it), and
// "Approve it. Cancel that." dispatches CANCEL_PLAN (the anaphoric "that"
// meant the approval, not the mission). Held today: "Never mind", "On second
// thought, don't", "no, wait", "Disregard that".
const RETRACTION_DEFECT = 'R-1: unmarked retraction idiom resolves the preceding approval (or cancels)';
const KNOWN_R1 = new Set([
  'Go ahead. Scratch that.',
  'Go ahead. Forget it.',
  'Approved. Strike that.',
  'Approve it. Cancel that.',
]);
const todoFor = (q) => (KNOWN_R1.has(q) ? { todo: RETRACTION_DEFECT } : {});

const WITHDRAWN_SAME_TURN = [
  'Approved. Actually, wait — hold on.',
  'Yes, go ahead. Never mind.',
  "Approve it. On second thought, don't.",
  'Go ahead. Scratch that.',
  'Approved — no, wait.',
  'Go ahead. Forget it.',
  'Yes. Disregard that.',
  'Approved. Strike that.',
  'Approve it. Cancel that.',
];

/** In-memory twin of ConversationContextStore (same validation and TTL). */
class MemoryContextStore {
  constructor() { this.rows = new Map(); }
  key(t, s) { return JSON.stringify([t, s]); }
  async load(tenantId, sessionId) {
    const row = this.rows.get(this.key(tenantId, sessionId));
    if (!row || row.expiresAt <= Date.now()) return null;
    return { revision: row.revision, context: validate(JSON.parse(row.json), { tenantId, sessionId }) };
  }
  async save(tenantId, sessionId, context, expectedRevision = null) {
    const projected = validate(context, { tenantId, sessionId });
    const row = this.rows.get(this.key(tenantId, sessionId));
    if (row && row.expiresAt > Date.now() && row.revision !== expectedRevision) return { saved: false, conflict: true };
    const revision = randomUUID();
    this.rows.set(this.key(tenantId, sessionId), { revision, json: JSON.stringify(projected), expiresAt: Date.now() + TTL_MS });
    return { saved: true, revision };
  }
}

function workspaceFor(engine, { store = null } = {}) {
  const opts = {
    disableLlm: true, missionsEnabled: true, resolverEnabled: true,
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
  };
  if (store) {
    opts.conversationContinuity = new ConversationContinuity({ env: LEDGER_ON, store });
    opts.conversationLedger = new ConversationLedgerRecorder({ env: LEDGER_ON });
    opts.conversationReferenceLoader = async ({ tenantId, missionId }) => {
      const m = engine.get(missionId, tenantId);
      return m ? { found: true, mission: m } : { found: false, mission: null };
    };
  }
  return createHermeticWorkspaceEngine(opts);
}

async function missionAt(kind, { store = null } = {}) {
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
  const ws = workspaceFor(engine, { store });
  const opened = await ws.open({ tenantId: TENANT, page: 'command-deck' });
  const session = ws._sessions.get(opened.sessionId);
  session.context.missionId = mission.id;
  session.context.acquisitionMissionId = mission.id;
  if (kind === K.EXECUTION_APPROVAL) await ws.ask({ sessionId: opened.sessionId, question: 'Show me the prepared outreach.' });
  return {
    engine, ws, store, sessionId: opened.sessionId, missionId: mission.id,
    ask: (question) => ws.ask({ sessionId: opened.sessionId, question }),
    fingerprint: () => JSON.stringify({ m: g(), e: engine.store.listEvents(mission.id), c: engine.store.listContributions(mission.id) }),
    get: g,
  };
}

const intents = () => amo.listExecutionRouterAudit().map((a) => a.intent);

describe('authorization readiness — ambiguous and withdrawn approvals (resolver, every gate)', () => {
  for (const kind of GATES) {
    const mission = { id: 'm_r', stage: 'x', pendingOperatorDecision: { kind, prompt: 'Decide.' } };
    for (const q of [...AMBIGUOUS, ...WITHDRAWN_SAME_TURN]) {
      it(`${kind}: "${q}" resolves no authority`, todoFor(q), () => {
        const r = resolvePendingOperatorDecision(q, mission);
        assert.notEqual(r.resolvedFromPendingDecision, true, q);
        assert.equal(r.executionIntent || null, null, q);
        // 'pending_operator_decision' routes the turn to the clarifying
        // pending-decision answer; it is not an action.
        // The bare fixture is not an executable mission past the plan gate, so
        // the consumability check fails closed (MISSION_STATE_INCONSISTENT);
        // that is also no authority. The end-to-end block uses real missions.
        let action = null;
        try { action = detectExecutionAction(q, { mission }); } catch (e) {
          assert.equal(e.code, 'MISSION_STATE_INCONSISTENT', String(e && e.message));
        }
        assert.ok(action === null || action === 'pending_operator_decision', `${q} → ${action}`);
      });
    }
  }
});

describe('authorization readiness — end to end', { concurrency: false }, () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  for (const kind of GATES) {
    for (const q of [...AMBIGUOUS, ...WITHDRAWN_SAME_TURN]) {
      it(`${kind}: "${q}" dispatches nothing and leaves the decision pending`, todoFor(q), async () => {
        const c = await missionAt(kind);
        amo.clearExecutionRouterAudit();
        const before = c.fingerprint();
        const r = await c.ask(q);
        assert.deepEqual(intents(), [], 'no execution-router dispatch');
        assert.equal(c.fingerprint(), before, 'mission, events and contributions unchanged');
        assert.equal(c.get().pendingOperatorDecision.kind, kind);
        assert.ok(String(r.prose || '').trim());
      });
    }
  }

  it('withdrawn on the next turn: "Wait, I take that back." neither approves the next gate nor cancels', async () => {
    const c = await missionAt(K.PLAN_APPROVAL);
    await c.ask('Approved.');
    assert.deepEqual(intents(), ['APPROVE_PLAN'], 'control: the plan approval dispatched');
    const next = c.get().pendingOperatorDecision.kind;
    amo.clearExecutionRouterAudit();
    const before = c.fingerprint();
    await c.ask('Wait, I take that back.');
    assert.deepEqual(intents(), [], 'a retraction is never the next approval or a cancellation');
    assert.equal(c.fingerprint(), before);
    assert.equal(c.get().pendingOperatorDecision.kind, next);
    assert.equal(Boolean(c.get().planCancelled), false);
  });

  it('characterization (product decision, not a defect): a repeated bare "Approved." approves the next gate', async () => {
    // Each gate is approved by the same word and there is no message
    // idempotency key, so a duplicated "Approved." advances two gates. Recorded
    // so the behaviour is visible to rollout review; changing it is a product
    // decision (e.g. bind a chat approval to the gate card it answers).
    const c = await missionAt(K.PLAN_APPROVAL);
    await c.ask('Approved.');
    await c.ask('Approved.');
    assert.deepEqual(intents(), ['APPROVE_PLAN', 'APPROVE_DISCOVERY']);
  });
});

describe('authorization readiness — pending decisions across session recovery', { concurrency: false }, () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  it('stale execution approval: after a restart the package presentation is not restored, so "Approved." does not execute', async () => {
    const store = new MemoryContextStore();
    const c = await missionAt(K.EXECUTION_APPROVAL, { store });
    await c.ws._conversationContinuity.drain();
    const second = workspaceFor(c.engine, { store });
    const recovered = await second.recoverConversationSession({ tenantId: TENANT, sessionId: c.sessionId });
    assert.equal(recovered.recovered, true);
    const s = second._sessions.get(c.sessionId);
    assert.equal(s.context.presentedExecutionDecision, undefined, 'the reviewed-package binding is not memory');
    amo.clearExecutionRouterAudit();
    const before = c.fingerprint();
    const r = await second.ask({ sessionId: c.sessionId, question: 'Approved.' });
    // Chat still submits the approval, without a reviewed revision; the TME
    // stage is the defence: it rolls back as unreviewed and commits nothing.
    const audit = amo.listExecutionRouterAudit();
    assert.ok(audit.every((a) => a.outcome !== 'committed'), 'nothing committed');
    for (const a of audit.filter((x) => x.intent === 'APPROVE_EXECUTION')) {
      assert.equal(a.outcome, 'rolled_back');
      assert.equal(a.request.payload.expectedPreparedArtifactRevision, null, 'no reviewed revision survives a restart');
    }
    assert.equal(c.fingerprint(), before, 'mission, events and contributions unchanged');
    assert.equal(c.get().pendingOperatorDecision.kind, K.EXECUTION_APPROVAL, 'decision still pending');
    assert.match(String(r.prose), /Not executed/);
    assert.match(String(r.prose), /Nothing was approved or sent/);
  });

  it('after recovery the pending decision is the mission store\'s, and remembered approvals grant nothing', async () => {
    const store = new MemoryContextStore();
    const c = await missionAt(K.PLAN_APPROVAL, { store });
    await c.ask('Approved.');
    const pendingNow = c.get().pendingOperatorDecision.kind;
    await c.ws._conversationContinuity.drain();
    const second = workspaceFor(c.engine, { store });
    await second.recoverConversationSession({ tenantId: TENANT, sessionId: c.sessionId });
    const view = second.getConversationLedger({ tenantId: TENANT, sessionId: c.sessionId }).view;
    assert.ok(view.remembered.decisions.length > 0);
    assert.ok(view.remembered.decisions.every((d) => d.authority === 'none'));
    assert.deepEqual(view.authority.consumableApprovals, []);
    amo.clearExecutionRouterAudit();
    const before = c.fingerprint();
    for (const q of ['Same as before.', 'Do what I approved earlier.', 'Continue with what we decided.']) {
      await second.ask({ sessionId: c.sessionId, question: q });
    }
    assert.deepEqual(intents(), [], 'a reference to an earlier approval is never a new approval');
    assert.equal(c.fingerprint(), before);
    assert.equal(c.get().pendingOperatorDecision.kind, pendingNow);
  });
});
