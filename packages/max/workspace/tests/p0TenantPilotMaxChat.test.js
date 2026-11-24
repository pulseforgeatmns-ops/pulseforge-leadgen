'use strict';
/**
 * P0-SCOUT-LEVI-001 — the Max chat surface cannot bypass the tenant-21
 * prospecting-only pilot policy (utils/tenantPilotPolicy.js).
 *
 * Drives the real WorkspaceEngine.ask() with an AMO mission pending plan or
 * discovery approval. For tenant 21, approval and execution phrasing never
 * reaches Scout (stub counts calls), the mission does not advance, and the
 * router audit records the policy refusal. Controls: tenants 10 and 1 keep the
 * existing behaviour. Deterministic: no network, no database, no provider.
 */
const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const approval = require('../AmoOperatorApproval');
const { createHermeticWorkspaceEngine: createWorkspaceEngine, unavailableOperatingEvidence } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const OBJECTIVE = 'Find active real-estate operators with current rehab projects in Chicago.';
const EXECUTION_PHRASES = [
  'Approved. Begin discovery.',
  'Go ahead and run Scout',
  "Let's run it",
  "Let's proceed",
  'Go ahead and approve',
  'Go ahead and send the emails',
  "Let's launch",
  "Let's execute",
];

async function setup({ tenantId, stage = 'discovery' }) {
  const engine = amo.createAcquisitionMissionEngine();
  let mission = engine.create({ tenantId, objective: OBJECTIVE, targetSegment: 'Real-estate operators' });
  if (stage === 'discovery') {
    // Fixture only: the plan stage is advanced directly (no specialist, no provider).
    await approval.advancePlanAfterApproval({ engine, mission, tenantId, question: 'Approved.' });
    mission = engine.get(mission.id, tenantId);
  }
  const scoutCalls = [];
  const ws = createWorkspaceEngine({
    missionsEnabled: true,
    resolverEnabled: true,
    // Read paths stay hermetic (no database in this deterministic test).
    operatingEvidenceOpts: unavailableOperatingEvidence(),
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    scoutAcquisitionOpts: {
      runScout: async (input) => { scoutCalls.push(input); return { status: 'completed', payload: {} }; },
    },
  });
  const open = () => {
    const opened = ws.open({ tenantId, page: 'command-deck' });
    const session = ws._sessions.get(opened.sessionId);
    session.context.missionId = mission.id;
    session.context.acquisitionMissionId = mission.id;
    return opened.sessionId;
  };
  const state = () => {
    const m = engine.get(mission.id, tenantId);
    return JSON.stringify({ stage: m.stage, pending: m.pendingOperatorDecision && m.pendingOperatorDecision.kind });
  };
  return { ws, open, scoutCalls, state };
}

const audit = () => amo.listExecutionRouterAudit().map(a => ({ intent: a.intent, outcome: a.outcome, owner: a.runtimeOwner }));

describe('tenant-21 pilot: Max chat cannot dispatch Scout or outbound', () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  for (const question of EXECUTION_PHRASES) {
    it(`discovery pending — "${question}" reaches no specialist and leaves the mission unchanged`, async () => {
      const t = await setup({ tenantId: '21' });
      const before = t.state();
      const result = await t.ws.ask({ sessionId: t.open(), question });
      assert.ok(result, 'Max answers instead of throwing');
      assert.equal(t.scoutCalls.length, 0, 'Scout was never called');
      assert.equal(t.state(), before, 'mission did not advance');
      const meta = (result.structured && result.structured.metadata) || {};
      assert.notEqual(meta.executionVerified, true);
      assert.notEqual(meta.executionState, 'completed_verified');
      assert.notEqual(meta.executionState, 'committed_internal');
      const answer = String((result.structured && result.structured.answer) || result.prose || result.answer || '');
      if (meta.blocker === 'tenant_pilot_policy_denied') assert.match(answer, /prospecting-only pilot/);
      for (const entry of audit()) {
        assert.ok(entry.outcome !== 'dispatched' && entry.outcome !== 'executed', JSON.stringify(entry));
      }
    });
  }

  it('plan pending — "Approved." is refused by the policy (no advance to discovery)', async () => {
    const t = await setup({ tenantId: '21', stage: 'plan' });
    const before = t.state();
    await t.ws.ask({ sessionId: t.open(), question: 'Approved.' });
    assert.equal(t.state(), before);
    assert.equal(t.scoutCalls.length, 0);
    const refused = audit().filter(a => a.outcome === 'tenant_pilot_policy_denied');
    assert.ok(refused.length >= 1, `router audit shows the policy refusal: ${JSON.stringify(audit())}`);
  });

  it('a read question still gets an answer and routes nothing', async () => {
    const t = await setup({ tenantId: '21' });
    const result = await t.ws.ask({ sessionId: t.open(), question: 'Brief me on my prospects.' });
    assert.ok(result);
    assert.equal(t.scoutCalls.length, 0);
    assert.equal(audit().filter(a => a.outcome !== 'tenant_pilot_policy_denied').length, 0);
  });
});

describe('controls: other tenants keep existing behaviour', () => {
  beforeEach(() => amo.clearExecutionRouterAudit());
  for (const tenantId of ['10', '1']) {
    it(`tenant ${tenantId}: "Go ahead and run Scout" still dispatches discovery`, async () => {
      const t = await setup({ tenantId });
      const result = await t.ws.ask({ sessionId: t.open(), question: 'Go ahead and run Scout' });
      assert.equal(result.resolution && result.resolution.action, 'discovery_approved');
      assert.equal(t.scoutCalls.length, 1);
      assert.equal(audit().some(a => a.outcome === 'tenant_pilot_policy_denied'), false);
    });
  }
});
