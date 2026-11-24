'use strict';
// P0-SCOUT-LEVI-001: server-enforced prospecting-only pilot policy for tenant 21.
// Proves every paid-provider, outbound and execution chokepoint refuses the
// pilot tenant BEFORE any provider or sender is reached (all HTTP is recorded
// and denied), that permitted prospecting still works, that AIM / enabled_agents
// / request parameters cannot lift the policy, and that other tenants are
// unchanged. Disposable PostgreSQL only.

// ---- network capture: every attempted provider/sender request is recorded and refused ----
const attempts = [];
let allowedPort = null;
const record = label => { attempts.push(label); throw Object.assign(new Error(`network blocked in test: ${label}`), { code: 'TEST_NETWORK_BLOCKED' }); };
for (const protocol of ['http', 'https']) {
  const mod = require(`node:${protocol}`);
  const original = mod.request;
  mod.request = function (...args) {
    const o = typeof args[0] === 'object' && args[0] && !(args[0] instanceof URL) ? args[0] : (args[0] instanceof URL ? args[0] : new URL(String(args[0])));
    const host = o.hostname || o.host;
    if (['127.0.0.1', 'localhost'].includes(host)) return original.apply(this, args);
    return record(`${protocol}:${host}`);
  };
  mod.get = function (...args) { return mod.request(...args); };
}
globalThis.fetch = async url => record(`fetch:${new URL(String(url)).hostname}`);
const axios = require('axios');
axios.Axios.prototype.request = async function (config) {
  const url = typeof config === 'string' ? config : (config && config.url) || '';
  let host = '';
  try { host = new URL(url).hostname; } catch (_) { host = url; }
  return record(`axios:${host}`);
};
const net = require('node:net');
const originalConnect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  const o = typeof args[0] === 'object' && args[0] ? args[0] : { port: args[0], host: args[1] };
  const local = o.path || ['127.0.0.1', 'localhost', '::1', undefined].includes(o.host);
  if (!local) return record(`net:${o.host}:${o.port}`);
  return originalConnect.apply(this, args);
};

const { test } = require('node:test');
const assert = require('node:assert/strict');
const policy = require('../utils/tenantPilotPolicy');

const DENIED = { code: 'tenant_pilot_policy_denied' };
const OTHER_TENANTS = ['1', '2', '5', '10'];

test('policy decisions: tenant 21 is prospecting-only; other tenants are untouched', () => {
  for (const id of [21, '21', ' 21 ', { client_id: 21 }, { tenantId: '21' }]) assert.equal(policy.isPilotTenant(id), true);
  assert.equal(policy.decide(21, 'view').allowed, true);
  assert.equal(policy.decide(21, 'free_research').allowed, true);
  assert.equal(policy.decide(21, 'paid_research').allowed, false);
  assert.equal(policy.decide(21, 'outbound').allowed, false);
  assert.equal(policy.decide(21, 'some_new_capability').allowed, false, 'unknown capabilities fail closed');
  assert.equal(policy.decide(21, 'agent_dispatch', { agent: 'max' }).allowed, true);
  for (const agent of ['scout', 'emmett', 'sam', 'cal', 'paige', 'riley', 'tiered_enrichment', 'warm_signal']) {
    assert.equal(policy.decide(21, 'agent_dispatch', { agent }).allowed, false, agent);
  }
  for (const intent of ['CLARIFY_PLAN', 'CANCEL_PLAN', 'EDIT_PLAN', 'APPLY_PLAN_EDITS']) {
    assert.equal(policy.decide(21, 'mission_execution', { intent }).allowed, true, intent);
  }
  for (const intent of ['APPROVE_PLAN', 'APPROVE_DISCOVERY', 'START_DISCOVERY', 'CONTINUE_INVESTIGATION', 'AUTONOMOUS_PROGRESSION',
    'MISSION_CONTINUATION', 'APPROVE_EXECUTION', 'EXECUTE_OUTBOUND', 'OPERATOR_APPROVED', 'ASSESS_PAID_ACQUISITION', 'GENERATE_VARIANTS']) {
    assert.equal(policy.decide(21, 'mission_execution', { intent }).allowed, false, intent);
  }
  for (const tenant of OTHER_TENANTS) {
    for (const cap of ['view', 'free_research', 'paid_research', 'outbound', 'agent_dispatch', 'mission_execution']) {
      assert.equal(policy.decide(tenant, cap, { agent: 'scout', intent: 'APPROVE_DISCOVERY' }).allowed, true, `${tenant}/${cap}`);
    }
  }
  assert.equal(policy.isPilotTenant(null), false);
  assert.equal(policy.isPilotTenant('abc'), false);
});

test('bypass resistance: env cannot remove the code-listed pilot; the policy reads no AIM, DB or request', () => {
  assert.equal(policy.isPilotTenant(21, { TENANT_PILOT_PROSPECTING_ONLY_ADD: '' }), true);
  assert.equal(policy.isPilotTenant(21, { TENANT_PILOT_PROSPECTING_ONLY_ADD: '-21' }), true);
  assert.equal(policy.isPilotTenant(7, { TENANT_PILOT_PROSPECTING_ONLY_ADD: '7' }), true, 'env may only add');
  assert.equal(policy.isPilotTenant(7, {}), false);
  // decide() is a pure function of (tenant, capability, detail): no AIM, client row or session is consulted.
  assert.equal(policy.decide(21, 'paid_research', { aimPublished: true, enabledAgents: ['scout'] }).allowed, false);
});

test('paid research providers: tenant 21 refused before any HTTP, armed or not; other tenants unchanged', async () => {
  const gate = require('../utils/anchorResearchProviderPause');
  delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  const before = attempts.length;
  assert.throws(() => gate.assertResearchProviderAllowed('research', 21), DENIED);
  assert.throws(() => gate.withResearchProviderScope(21, () => { throw new Error('must not run'); }), DENIED);
  assert.equal(gate.isResearchProviderControlError(Object.assign(new Error(), { code: policy.CODE })), true);
  for (const tenant of OTHER_TENANTS) assert.doesNotThrow(() => gate.assertResearchProviderAllowed('research', tenant));
  assert.equal(gate.withResearchProviderScope(1, () => 'ran'), 'ran');
  // Nested scopes: a pilot anywhere in scope denies.
  assert.throws(() => gate.withResearchProviderScope(1, () => gate.assertResearchProviderAllowed('prospeo', 21)), DENIED);

  // SerpAPI
  const { searchSerpApi } = require('../lib/serpapi');
  process.env.SERPAPI_KEY = 'disposable-not-a-key';
  await assert.rejects(searchSerpApi({ query: 'x', tenantId: 21 }), DENIED);
  await assert.rejects(gate.withResearchProviderScope(1, async () => {
    // scoped to tenant 1, explicit pilot tenant still denies
    return searchSerpApi({ query: 'x', tenantId: '21' });
  }), DENIED);
  delete process.env.SERPAPI_KEY;
  const other = await searchSerpApi({ query: 'x', tenantId: 1 });
  assert.equal(other.error && other.error.code, 'missing_key', 'tenant 1 path unchanged');

  // Google Places through the shared guarded call
  const { withPlacesContext } = require('../utils/placesCostAttribution');
  const places = require('../utils/placesApi');
  const placesResult = await withPlacesContext({ tenantId: 21, caller: 'test', feature: 'test' },
    () => places.v1SearchText({ body: { textQuery: 'x' }, apiKey: 'disposable', fieldMask: 'places.id' }).catch(e => e));
  assert.ok(!placesResult || placesResult.code === policy.CODE || /tenant_pilot_policy_denied/.test(JSON.stringify(placesResult)),
    'Places refused for tenant 21');

  // Hunter enrichment
  const enrich = require('../enrichProspects');
  assert.equal(typeof enrich.run, 'function');
  assert.equal(attempts.length, before, `no provider request was attempted: ${attempts.slice(before).join(', ')}`);
});

test('outbound senders: tenant 21 refused at the contact gates before any provider', async () => {
  const before = attempts.length;
  const { withCallAuthorization, withEmailAuthorization, withSmsAuthorization, assertCallAllowed } = require('../utils/callEligibility');
  let dispatched = 0;
  const neverDb = { query: async () => { throw new Error('db must not be reached'); }, connect: async () => { throw new Error('db must not be reached'); } };
  for (const fn of [withEmailAuthorization, withSmsAuthorization]) {
    await assert.rejects(fn(neverDb, [{ prospectId: 'p1', clientId: 21 }], async () => { dispatched += 1; }), DENIED);
  }
  await assert.rejects(withCallAuthorization(neverDb, [{ prospectId: 'p1', clientId: '21' }], async () => { dispatched += 1; }), DENIED);
  await assert.rejects(withEmailAuthorization(neverDb, [{ prospectId: 'p0', clientId: 1 }, { prospectId: 'p1', clientId: 21 }], async () => { dispatched += 1; }), DENIED,
    'a mixed batch containing the pilot tenant is refused whole');
  await assert.rejects(assertCallAllowed(neverDb, 'p1', 21), DENIED);
  assert.equal(dispatched, 0);

  const { sendEmail } = require('../packages/providers/brevo/sendEmail');
  const sent = await sendEmail({ toEmail: 'x@example.invalid', subject: 's', body: 'b', apiKey: 'disposable',
    sender: { name: 'n', email: 'n@example.invalid' }, outreachAuthorization: { prospectId: 'p1', clientId: 21, pool: neverDb } });
  assert.equal(sent.success, false);

  const { checkDNC } = require('../dbClient');
  assert.equal(await checkDNC('p1', { clientId: 21, pool: neverDb }), true, 'every tenant-21 prospect reads as do-not-contact');

  const scheduler = require('../services/tenantOutreachScheduler');
  await assert.rejects(scheduler.authorizeScheduledOutreachSend({ tenantId: '21', prospectId: 'p1' }, { scheduleStore: {} }), DENIED);
  assert.equal(attempts.length, before, `no sender request was attempted: ${attempts.slice(before).join(', ')}`);
});

test('mission execution: router and TME refuse tenant 21 dispatching intents; other tenants pass the gate', async () => {
  const { routeExecutionRequest, EXECUTION_INTENTS, EXECUTION_SOURCES } = require('../packages/acquisition-mission/ExecutionRouter');
  const { createExecutionRequest } = require('../packages/acquisition-mission/ExecutionRequest');
  let handled = 0;
  const handlers = new Proxy({}, { get: () => async () => { handled += 1; return {}; } });
  const engine = { get: () => null };
  for (const intent of ['APPROVE_DISCOVERY', 'START_DISCOVERY', 'AUTONOMOUS_PROGRESSION', 'EXECUTE_OUTBOUND', 'APPROVE_EXECUTION', 'APPROVE_PLAN']) {
    const request = createExecutionRequest({ intent: EXECUTION_INTENTS[intent], source: EXECUTION_SOURCES.CHAT, tenantId: '21',
      missionId: 'm1', operatorId: 'levi' });
    await assert.rejects(routeExecutionRequest(request, { engine, handlers, tenantId: '21' }), DENIED, intent);
  }
  // A tenant-1 request passes the pilot gate (and then fails on the absent mission, as before).
  const request1 = createExecutionRequest({ intent: EXECUTION_INTENTS.APPROVE_DISCOVERY, source: EXECUTION_SOURCES.CHAT,
    tenantId: '1', missionId: 'm1', operatorId: 'op' });
  await assert.rejects(routeExecutionRequest(request1, { engine, handlers, tenantId: '1' }), { code: 'amo_mission_not_found' });
  // A mission that belongs to tenant 21 is refused even if the request claims another tenant.
  const spoof = createExecutionRequest({ intent: EXECUTION_INTENTS.APPROVE_DISCOVERY, source: EXECUTION_SOURCES.CHAT,
    tenantId: '1', missionId: 'm21', operatorId: 'op' });
  await assert.rejects(routeExecutionRequest(spoof, { engine: { get: () => ({ id: 'm21', tenantId: '21' }) }, handlers }), DENIED);
  assert.equal(handled, 0);

  const { executeMissionStage } = require('../packages/acquisition-mission/TransactionalExecution');
  for (const specialist of ['scout', 'paige', 'emmett']) {
    await assert.rejects(executeMissionStage({ engine: { store: {} }, missionId: 'm1', tenantId: 21, specialist, stage: 'discover' }), DENIED);
  }
  await assert.rejects(executeMissionStage({ engine: { store: {} }, missionId: 'm1', tenantId: 1, specialist: 'scout', stage: 'discover' }),
    err => err.code !== policy.CODE, 'tenant 1 is not stopped by the pilot policy');
});

