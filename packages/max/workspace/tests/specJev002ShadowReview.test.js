'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { DecisionService } = require('../../../decision-service/DecisionService');
const { createShadowEventSink } = require('../../../decision-service/ShadowEventSink');
const { observeOperatorHttp } = require('../../../decision-service/httpObserver');
const { buildShadowReview } = require('../../../decision-service/shadowReview');
const fixture = require('../../../../test/fixtures/decisionShadowEvent.json');
const ENV = { DECISION_SHADOW_ENABLED: 'true', DECISION_PROVIDER: 'jev', JEV_TIMEOUT_MS: '10' };
const decision = Object.fromEntries(['intent', 'confidence', 'mission_bound_probability', 'approval_probability',
  'inspection_probability', 'requires_human_clarification', 'risk_if_misrouted', 'recommended_route'].map(key => [key, fixture[key]]));
const input = { question: 'What is the current status and confidence of the Anchor STR mission?', context: { tenantId: '10' } };
const result = { route: 'intelligence', workspaceOwnership: { owner: 'reasoning' },
  routingTrace: { pipeline: 'ClientIntelligence' }, answer: 'unchanged production answer' };
function setup(options = {}) {
  const logs = [], warnings = [];
  const service = new DecisionService({ env: ENV,
    provider: { name: 'jev', evaluate: async () => ({ decision, model: 'jev-test' }) },
    audit: row => logs.push(row), warningAudit: warning => warnings.push(warning), ...options });
  const workspace = createWorkspaceEngine({ decisionService: service, disableLlm: true });
  workspace._askProduction = async () => result;
  return { service, workspace, logs, warnings };
}

test('throwing and rejecting persistence cannot change workspace result or original production error', async () => {
  for (const write of [() => { throw Error('storage failed'); }, async () => { throw Error('storage failed'); }]) {
    const { service, workspace, logs } = setup({ persistence: { write } });
    assert.strictEqual(await workspace.ask(input), result);
    await service.drain();
    assert.equal(logs[0].comparison, 'mismatch');
    const original = Error('original production error');
    workspace._askProduction = async () => { throw original; };
    await assert.rejects(workspace.ask(input), error => error === original);
    await service.drain();
    assert.equal(logs[1].current_route.failed, true);
  }
});

test('stalled database is off the workspace and HTTP response paths; it cannot consume route state', async () => {
  let started = false, release;
  const sink = createShadowEventSink({ env: { ...ENV, DATABASE_URL: 'test' }, createPool: () => ({
    on() {}, query: () => { started = true; return new Promise(resolve => { release = resolve; }); },
  }), warn() {} });
  const { workspace, service } = setup({ persistence: sink });
  assert.strictEqual(await workspace.ask(input), result);
  assert.equal(started, false);
  await new Promise(resolve => setImmediate(resolve));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(started, true);
  const json = { original: true };
  const res = { statusCode: 200, json(body) { assert.strictEqual(body, json); return this; } };
  // A second request can return while the first write remains stalled.
  observeOperatorHttp({ body: { message: 'Status?' } }, res, { service });
  assert.strictEqual(res.json(json), res);
  release();
  // Subsequent writes settle normally so shutdown can drain.
  sink.write = () => {};
  await service.drain();
});

test('disabled shadow writes nothing even with a supplied persistence implementation', async () => {
  const { workspace, service, logs } = setup({ env: { ...ENV, DECISION_SHADOW_ENABLED: 'false' },
    persistence: { write() { assert.fail('disabled write'); } } });
  assert.strictEqual(await workspace.ask(input), result);
  await service.drain();
  assert.equal(logs.length, 0);
});

test('provider errors, malformed decisions and timeouts persist as review errors without failing requests', async () => {
  for (const evaluate of [
    () => { throw Object.assign(Error('private provider body'), { code: 'http_error', http_status: 401 }); },
    async () => ({ decision: { confidence: 'bad' } }),
    () => new Promise(() => {}),
  ]) {
    const rows = [];
    const { workspace, service } = setup({ provider: { name: 'jev', evaluate },
      persistence: { write: row => rows.push(structuredClone(row)) } });
    assert.strictEqual(await workspace.ask(input), result);
    await service.drain();
    const report = buildShadowReview(rows);
    assert.equal(report.summary.errors, 1);
    assert.equal(report.summary.mismatches, 0);
    assert.equal(rows[0].status, 'error');
    assert.equal(rows[0].current_route.route, 'conversation');
    assert.equal(rows[0].comparison, 'unavailable');
    assert.doesNotMatch(JSON.stringify(rows), /private provider body|unchanged production answer/);
  }
});

test('stdout failure does not prevent durable sink dispatch', async () => {
  const rows = [];
  const { workspace, service } = setup({ audit: () => { throw Error('stdout failed'); },
    persistence: { write: row => rows.push(row) } });
  assert.strictEqual(await workspace.ask(input), result);
  await service.drain();
  assert.equal(rows.length, 1);
  assert.equal(buildShadowReview(rows).summary.likely_mission_inspections, 1);
});

test('SPEC-JEV-003 warning annotates the known mismatch without changing the workspace response', async () => {
  const { workspace, service, logs, warnings } = setup();
  assert.strictEqual(await workspace.ask(input), result);
  await service.drain();
  assert.equal(logs.length, 1);
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0].event, 'DECISION_SHADOW_ROUTING_WARNING');
  assert.equal(warnings[0].spec, 'SPEC-JEV-003');
  assert.equal(warnings[0].reason, 'likely_mission_inspection');
  assert.equal(warnings[0].action, 'review_current_route_without_changing_routing');
  assert.equal(warnings[0].decision_id, logs[0].decision_id);
  assert.equal(warnings[0].recommended_route, 'inspection');
});

test('SPEC-JEV-003 warning failures cannot affect routing or persistence', async () => {
  const rows = [];
  const { workspace, service, logs } = setup({
    warningAudit: () => { throw Error('warning failed'); },
    persistence: { write: row => rows.push(row) },
  });
  assert.strictEqual(await workspace.ask(input), result);
  await service.drain();
  assert.equal(logs.length, 1);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].comparison, 'mismatch');
});
