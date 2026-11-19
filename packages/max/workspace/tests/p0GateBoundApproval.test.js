'use strict';

/**
 * P0 — gate-bound chat approvals (MAX_GATE_BOUND_APPROVAL, default-off).
 *
 * An approval applies only to the decision package this conversation last
 * presented. Verified at all four reachable gates through the real
 * WorkspaceEngine.ask(): default-off parity, drift (the mission moved on
 * without this conversation), a revised package, a conversation that never saw
 * the decision, cancellation unaffected, the execution gate's own binding
 * intact, and the documented double-send limit.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');

const A = require('../AmoOperatorApproval');
const { liveScoutStub } = require('./amoTestRuntime');
const originalAdvanceDiscovery = A.advanceDiscoveryAfterApproval;
A.advanceDiscoveryAfterApproval = (input = {}) =>
  originalAdvanceDiscovery({ ...input, runScout: input.runScout || liveScoutStub });

const amo = require('../../../acquisition-mission');
const { OPERATOR_DECISION_KINDS: K } = require('../../../acquisition-mission/types');
const G = require('../GateBoundApproval');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const TENANT = '10';
const GATES = [K.PLAN_APPROVAL, K.DISCOVERY_APPROVAL, K.PRIORITIZATION_APPROVAL, K.EXECUTION_APPROVAL];
const APPROVE = {
  [K.PLAN_APPROVAL]: 'APPROVE_PLAN',
  [K.DISCOVERY_APPROVAL]: 'APPROVE_DISCOVERY',
  [K.PRIORITIZATION_APPROVAL]: 'APPROVE_PRIORITIZATION',
  [K.EXECUTION_APPROVAL]: 'APPROVE_EXECUTION',
};
const NEXT = {
  [K.PLAN_APPROVAL]: K.DISCOVERY_APPROVAL,
};

async function advanceTo(engine, mission, kind) {
  const g = () => engine.get(mission.id, TENANT);
  if (kind !== K.PLAN_APPROVAL) await A.advancePlanAfterApproval({ engine, mission: g(), tenantId: TENANT, question: 'Approved.' });
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
}

async function fixture(kind = K.PLAN_APPROVAL) {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: TENANT, objective: 'Acquire recurring commercial cleaning customers for Anchor Cleaning in Greater Manchester.', targetSegment: 'Facility buyers' });
  await advanceTo(engine, mission, kind);
  const ws = createHermeticWorkspaceEngine({ disableLlm: true, missionsEnabled: true, resolverEnabled: true, acquisitionMissionRuntime: createTestAmoRuntime({ engine }) });
  const openSession = async () => {
    const opened = await ws.open({ tenantId: TENANT, page: 'command-deck' });
    const s = ws._sessions.get(opened.sessionId);
    s.context.missionId = mission.id;
    s.context.acquisitionMissionId = mission.id;
    return {
      id: opened.sessionId,
      session: s,
      ask: (question) => ws.ask({ sessionId: opened.sessionId, question }),
    };
  };
  const g = () => engine.get(mission.id, TENANT);
  return {
    engine, mission, g, openSession,
    fingerprint: () => JSON.stringify({ m: g(), e: engine.store.listEvents(mission.id), c: engine.store.listContributions(mission.id) }),
  };
}

/** A turn that presents the pending decision (package for the execution gate). */
async function present(c, kind) {
  const r = await c.ask(kind === K.EXECUTION_APPROVAL ? 'Show me the prepared outreach.' : 'What decision is pending on this mission?');
  return r;
}

const intents = () => amo.listExecutionRouterAudit().map((a) => a.intent);

let savedFlag;
function flagOn() { process.env[G.FLAG] = 'true'; }
beforeEach(() => { savedFlag = process.env[G.FLAG]; delete process.env[G.FLAG]; amo.clearExecutionRouterAudit(); });
afterEach(() => { if (savedFlag === undefined) delete process.env[G.FLAG]; else process.env[G.FLAG] = savedFlag; });

describe('decision identity', () => {
  it('is stable across re-reads, distinct per gate, and changes when the package changes', async () => {
    const f = await fixture(K.PLAN_APPROVAL);
    const k1 = G.pendingDecisionKey(f.g());
    assert.match(k1, /^plan_approval:[0-9a-f]{32}$/);
    assert.equal(G.pendingDecisionKey(f.g()), k1);
    const revised = { ...f.g(), pendingOperatorDecision: { ...f.g().pendingOperatorDecision, missionUnderstanding: 'Revised plan.' } };
    assert.notEqual(G.pendingDecisionKey(revised), k1);
    await A.advancePlanAfterApproval({ engine: f.engine, mission: f.g(), tenantId: TENANT, question: 'Approved.' });
    assert.match(G.pendingDecisionKey(f.g()), /^discovery_approval:/);
    assert.equal(G.pendingDecisionKey({ id: 'x' }), null);
  });

  it('is off unless the flag is exactly true', () => {
    assert.equal(G.isGateBoundApprovalEnabled({}), false);
    assert.equal(G.isGateBoundApprovalEnabled({ [G.FLAG]: '1' }), false);
    assert.equal(G.isGateBoundApprovalEnabled({ [G.FLAG]: 'true' }), true);
  });
});

describe('default-off parity', { concurrency: false }, () => {
  it('without the flag, an approval in a conversation that never saw the decision still dispatches (today\'s contract)', async () => {
    const f = await fixture(K.PLAN_APPROVAL);
    const c = await f.openSession();
    await c.ask('Approved.');
    assert.deepEqual(intents(), ['APPROVE_PLAN']);
  });

  it('without the flag, a mission that moved on is approved at its new gate', async () => {
    const f = await fixture(K.PLAN_APPROVAL);
    const c = await f.openSession();
    await present(c, K.PLAN_APPROVAL);
    await A.advancePlanAfterApproval({ engine: f.engine, mission: f.g(), tenantId: TENANT, question: 'Approved.' });
    amo.clearExecutionRouterAudit();
    await c.ask('Approved.');
    assert.deepEqual(intents(), ['APPROVE_DISCOVERY']);
  });
});

describe('gate-bound approvals — every reachable gate', { concurrency: false }, () => {
  for (const kind of GATES) {
    it(`${kind}: a presented decision is approved as before`, async () => {
      flagOn();
      const f = await fixture(kind);
      const c = await f.openSession();
      await present(c, kind);
      assert.equal(c.session.context.presentedDecision.kind, kind);
      amo.clearExecutionRouterAudit();
      await c.ask('Approved.');
      assert.deepEqual(intents(), [APPROVE[kind]]);
    });

    if (kind === K.EXECUTION_APPROVAL) {
      it(`${kind}: not gate-bound twice — the reviewed-package binding refuses an unshown package`, async () => {
        flagOn();
        const f = await fixture(kind);
        const c = await f.openSession();
        const before = f.fingerprint();
        const r = await c.ask('Approved.');
        assert.ok(amo.listExecutionRouterAudit().every((a) => a.outcome !== 'committed'), 'nothing committed');
        assert.equal(f.fingerprint(), before);
        assert.match(String(r.prose), /The prepared outreach has not been shown in this conversation yet/);
        assert.ok(!G.GATE_APPROVAL_INTENTS.has('APPROVE_EXECUTION'));
      });
      continue;
    }

    it(`${kind}: never presented in this conversation → not executed, current decision shown, next approval answers it`, async () => {
      flagOn();
      const f = await fixture(kind);
      const c = await f.openSession();
      const before = f.fingerprint();
      const r = await c.ask('Approved.');
      assert.deepEqual(intents(), [], 'nothing dispatched');
      assert.equal(f.fingerprint(), before, 'mission, events and contributions unchanged');
      assert.match(String(r.prose), /Not executed: the decision you are approving has not been shown/);
      assert.match(String(r.prose), /Nothing was approved, sent, or cancelled/);
      assert.match(String(r.prose), /Currently pending:/);
      assert.equal(c.session.context.presentedDecision.kind, kind, 'the refusal presented the current decision');
      await c.ask('Approved.');
      assert.deepEqual(intents(), [APPROVE[kind]]);
    });

    it(`${kind}: the decision changed after it was presented → not executed`, async () => {
      flagOn();
      const f = await fixture(kind);
      const c = await f.openSession();
      await present(c, kind);
      // Another surface revises the decision package (same gate, new content).
      const m = f.g();
      f.engine.store.putMission({ ...m, pendingOperatorDecision: { ...m.pendingOperatorDecision, prompt: `${m.pendingOperatorDecision.prompt} (revised)` } });
      assert.notEqual(G.pendingDecisionKey(f.g()), c.session.context.presentedDecision.key, 'fixture changed the package');
      amo.clearExecutionRouterAudit();
      const before = f.fingerprint();
      const r = await c.ask('Approved.');
      assert.deepEqual(intents(), []);
      assert.equal(f.fingerprint(), before);
      assert.match(String(r.prose), /changed after it was shown/);
    });
  }

  it('drift: the mission moved to the next gate elsewhere → this conversation\'s approval does not apply to it', async () => {
    flagOn();
    const f = await fixture(K.PLAN_APPROVAL);
    const c = await f.openSession();
    await present(c, K.PLAN_APPROVAL);
    await A.advancePlanAfterApproval({ engine: f.engine, mission: f.g(), tenantId: TENANT, question: 'Approved.' });
    assert.equal(f.g().pendingOperatorDecision.kind, NEXT[K.PLAN_APPROVAL]);
    amo.clearExecutionRouterAudit();
    const before = f.fingerprint();
    const r = await c.ask('Approved.');
    assert.deepEqual(intents(), [], 'the plan-card approval is not a discovery approval');
    assert.equal(f.fingerprint(), before);
    assert.match(String(r.prose), /changed after it was shown/);
    await c.ask('Approved.');
    assert.deepEqual(intents(), ['APPROVE_DISCOVERY'], 'after seeing the discovery decision, an explicit approval answers it');
  });

  it('another conversation\'s presentation does not count', async () => {
    flagOn();
    const f = await fixture(K.PLAN_APPROVAL);
    const seen = await f.openSession();
    await present(seen, K.PLAN_APPROVAL);
    const other = await f.openSession();
    await other.ask('Approved.');
    assert.deepEqual(intents(), []);
  });

  it('cancellation is not an approval and is not bound', async () => {
    flagOn();
    const f = await fixture(K.PLAN_APPROVAL);
    const c = await f.openSession();
    await c.ask('Cancel it.');
    assert.deepEqual(intents(), ['CANCEL_PLAN']);
    assert.equal(f.g().planCancelled, true);
  });

  it('known limit (documented): a duplicated "Approved." still approves the next gate, because the first answer presents it', async () => {
    flagOn();
    const f = await fixture(K.PLAN_APPROVAL);
    const c = await f.openSession();
    await present(c, K.PLAN_APPROVAL);
    amo.clearExecutionRouterAudit();
    await c.ask('Approved.');
    await c.ask('Approved.');
    assert.deepEqual(intents(), ['APPROVE_PLAN', 'APPROVE_DISCOVERY']);
  });
});
