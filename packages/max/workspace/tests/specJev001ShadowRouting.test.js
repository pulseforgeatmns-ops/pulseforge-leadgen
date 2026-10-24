'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { DecisionService } = require('../../../decision-service/DecisionService');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const ENV = { DECISION_SHADOW_ENABLED: 'true', DECISION_PROVIDER: 'jev', JEV_TIMEOUT_MS: '20' };
const suggestion = {
  intent: 'general_chat', confidence: 0.99, mission_bound_probability: 0,
  approval_probability: 0, inspection_probability: 0,
  requires_human_clarification: false, risk_if_misrouted: 'low', recommended_route: 'conversation',
};
function setup(env, evaluate) {
  const rows = [], states = [];
  const service = new DecisionService({ env, audit: row => rows.push(row), provider: {
    name: 'jev', model: 'jev-test', evaluate: evaluate || (async state => {
      states.push(state);
      return { decision: { ...suggestion }, model: 'jev-test' };
    }),
  } });
  const runtime = createTestAmoRuntime();
  const mission = runtime.engine().create({
    id: 'mission-jev-test', tenantId: '10',
    objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
    targetSegment: 'Law Firms',
  });
  const workspace = createWorkspaceEngine({
    decisionService: service, disableLlm: true, acquisitionMissionRuntime: runtime,
    missionsEnabled: true, resolverEnabled: true,
  });
  const opened = workspace.open({ tenantId: '10', missionId: mission.id, acquisitionMissionId: mission.id });
  return { workspace, runtime, mission, sessionId: opened.sessionId, service, rows, states };
}

function routingView(result) {
  return {
    route: result.route, owner: result.workspaceOwnership?.owner,
    action: result.resolution?.action, reason: result.resolution?.reason,
    pipeline: result.routingTrace?.pipeline, objective: result.routingTrace?.primaryObjective,
    answer: result.prose,
  };
}

test('real workspace routes and mission effects agree with unwrapped production when disabled and enabled', async () => {
  // Plan approval is internal only; the default session policy stops before
  // discovery. All runtimes are in-memory and LLM presentation is disabled.
  for (const question of ['Who are you?', 'What operating mode are you currently using?',
    "Don't execute anything.", 'asdf', 'What are the risks?', 'Approved.']) {
    const production = setup({});
    const baseline = await production.workspace._askProduction({ sessionId: production.sessionId, question });
    for (const env of [{}, ENV]) {
      const current = setup(env);
      const before = current.runtime.engine().inspect(current.mission.id, { tenantId: '10' }).mission;
      const result = await current.workspace.ask({ sessionId: current.sessionId, question });
      assert.deepEqual(routingView(result), routingView(baseline), question);
      const after = current.runtime.engine().inspect(current.mission.id, { tenantId: '10' }).mission;
      const expected = production.runtime.engine().inspect(production.mission.id, { tenantId: '10' }).mission;
      assert.equal(after.version, expected.version, `mission version: ${question}`);
      assert.equal(after.stage, expected.stage, `mission stage: ${question}`);
      assert.deepEqual(after.pendingOperatorDecision, expected.pendingOperatorDecision, `pending decision: ${question}`);
      await current.service.drain();
      assert.equal(current.rows.length, env.DECISION_SHADOW_ENABLED ? 1 : 0, question);
      if (current.rows.length) {
        assert.equal(current.rows[0].recommended_route, 'conversation');
        assert.equal(current.rows[0].current_route.owner, result.workspaceOwnership?.owner || null);
        if (question === 'Approved.') {
          assert.equal(current.states[0].context.mission.pending_decision.kind, before.pendingOperatorDecision.kind);
          assert.equal(current.rows[0].current_route.route, 'approval');
        }
      }
    }
  }
});

test('workspace returns exact production result before an unresolved provider settles', async () => {
  let started = false;
  const current = setup(ENV, async () => { started = true; return new Promise(() => {}); });
  const sentinel = { route: 'mission', answer: 'original result' };
  current.workspace._askProduction = async () => sentinel;
  assert.strictEqual(await current.workspace.ask({ question: 'continue' }), sentinel);
  assert.equal(started, false, 'provider is deferred beyond response path');
  await current.service.drain();
  assert.equal(current.rows[0].errors[0].code, 'timeout');
});

test('provider throws and invalid schema never change production output', async () => {
  for (const evaluate of [() => { throw Error('provider failure'); }, async () => ({ decision: { confidence: 'bad' } })]) {
    const current = setup(ENV, evaluate);
    const result = await current.workspace.ask({ sessionId: current.sessionId, question: 'What operating mode are you currently using?' });
    assert.equal(result.workspaceOwnership.owner, 'session_state_manager');
    await current.service.drain();
    assert.equal(current.rows.length, 1);
    assert.equal(current.rows[0].status, 'error');
    assert.equal(current.rows[0].current_route.route, 'inspection');
  }
});

test('production errors retain exact identity and still receive shadow audit', async () => {
  const current = setup(ENV);
  const original = new Error('production error');
  current.workspace._askProduction = async () => { throw original; };
  await assert.rejects(current.workspace.ask({ question: 'continue' }), error => error === original);
  await current.service.drain();
  assert.equal(current.rows[0].current_route.failed, true);
  assert.equal(current.rows[0].comparison, 'unavailable');
});

test('observer initialization/completion bugs cannot change routing', async () => {
  for (const service of [{ begin() { throw Error('begin failed'); } }, { begin() { return { complete() { throw Error('finish failed'); } }; } }]) {
    const workspace = createWorkspaceEngine({ decisionService: service, disableLlm: true });
    const result = { route: 'mission' };
    workspace._askProduction = async () => result;
    assert.strictEqual(await workspace.ask({ question: 'continue' }), result);
  }
});

test('compound turns get one audit; internal MIEP steps get none', async () => {
  const current = setup(ENV);
  await current.workspace.ask({ sessionId: current.sessionId, question: 'Use concise responses and explain the current session settings.' });
  await current.service.drain();
  assert.equal(current.rows.length, 1);
  await current.workspace.ask({ sessionId: current.sessionId, question: 'Who are you?', _miepInternal: true });
  await current.service.drain();
  assert.equal(current.rows.length, 1);
});
