'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { DecisionService, readConfig, selectProvider } = require('../packages/decision-service/DecisionService');
const { parseDecision, DECISION_SCHEMA } = require('../packages/decision-service/schema');
const { JevProvider, QUESTIONS, ENDPOINT, parseJevResponse } = require('../packages/decision-service/providers/JevProvider');
const { NoopProvider } = require('../packages/decision-service/providers/NoopProvider');
const { snapshotInput } = require('../packages/decision-service/privacy');
const { observedRoute } = require('../packages/decision-service/observedRoute');
const { observeLegacyChat, observeOperatorHttp } = require('../packages/decision-service/httpObserver');

const ENV = { DECISION_SHADOW_ENABLED: 'true', DECISION_PROVIDER: 'jev', JEV_ENABLED: 'true', JEV_API_KEY: 'test-key' };
const decision = () => ({
  intent: 'mission_instruction', confidence: 0.9, mission_bound_probability: 0.8,
  approval_probability: 0, inspection_probability: 0.1,
  requires_human_clarification: false, risk_if_misrouted: 'high', recommended_route: 'mission',
});
function responseBody() {
  const d = decision();
  const answers = Object.fromEntries(Object.entries(QUESTIONS).map(([key, question]) => {
    if (question.type === 'noul') return [key, { type: 'noul', noul: key === 'requires_human_clarification' ? 0.1 : d[key] }];
    const winner = d[key];
    return [key, { type: 'choice', choice: winner, confidence: 0.9,
      probabilities: Object.fromEntries(Object.keys(question.criteria).map(option => [option, option === winner ? 1 : 0])) }];
  }));
  return { model: 'jev-1.13.0', answers, usage: { input_tokens: 123, output_tokens: 40 } };
}
function fakeProvider(evaluate = async () => ({ decision: decision(), model: 'jev-test' })) {
  return { name: 'jev', model: 'jev-test', evaluate };
}
function fixture(options = {}) {
  const rows = [];
  const service = new DecisionService({ env: ENV, provider: fakeProvider(), audit: row => rows.push(row), ...options });
  return { service, rows };
}
const input = { question: 'Continue the mission', sessionId: 'session-a', context: { tenantId: '10', missionId: 'mission-a' } };

test('schema requires all fields, rejects malformed types, non-finite probabilities and extras', () => {
  assert.deepEqual(parseDecision(decision()), decision());
  for (const key of DECISION_SCHEMA.required) {
    const missing = decision(); delete missing[key];
    assert.throws(() => parseDecision(missing), { code: 'invalid_response' });
  }
  for (const value of [-0.1, 1.1, Infinity, NaN, '0.9', null, true]) {
    assert.throws(() => parseDecision({ ...decision(), confidence: value }), { code: 'invalid_response' });
  }
  for (const extra of [{ intent: 'invented' }, { recommended_route: 'send_email' }, { requires_human_clarification: 'false' }, { extra: 'secret' }]) {
    assert.throws(() => parseDecision({ ...decision(), ...extra }), { code: 'invalid_response' });
  }
  for (const malformed of [null, [], 'json', 1]) assert.throws(() => parseDecision(malformed));
  assert.equal(parseDecision({ ...decision(), confidence: 0 }).confidence, 0);
  assert.equal(parseDecision({ ...decision(), confidence: 1 }).confidence, 1);
});

test('env defaults and all Jev gates fail to noop without network', async () => {
  assert.equal(readConfig({}).enabled, false);
  for (const env of [{}, { ...ENV, DECISION_SHADOW_ENABLED: 'false' }, { ...ENV, JEV_ENABLED: 'false' },
    { ...ENV, JEV_API_KEY: '' }, { ...ENV, DECISION_PROVIDER: 'other' }, { ...ENV, JEV_MODEL: 'invalid model' }]) {
    assert.ok(selectProvider(readConfig(env)) instanceof NoopProvider);
  }
  assert.ok(selectProvider(readConfig(ENV)) instanceof JevProvider);
  assert.equal(readConfig({ JEV_TIMEOUT_MS: '-1', DECISION_SHADOW_MAX_PENDING: '0' }).timeoutMs, 1500);
  const { service, rows } = fixture({ env: {}, provider: fakeProvider(() => assert.fail('disabled provider called')) });
  assert.equal(service.begin(input), null);
  await service.drain();
  assert.equal(rows.length, 0);
});

test('enabled noop logs missing configuration without fabricating a recommendation', async () => {
  const { service, rows } = fixture({ env: { ...ENV, JEV_API_KEY: '' }, provider: undefined });
  service.begin(input).complete({ route: 'mission' });
  await service.drain();
  assert.equal(rows[0].status, 'fallback');
  assert.equal(rows[0].fallback_reason, 'missing_api_key');
  assert.equal(rows[0].recommended_route, null);
  assert.equal(rows[0].comparison, 'unavailable');
});

test('shadow logs comparisons once per message with correlation and all decision fields', async () => {
  const { service, rows } = fixture();
  const a = service.begin(input);
  a.complete({ route: 'mission' }); a.complete({ route: 'identity' });
  service.begin({ ...input, question: 'Who are you?' }).complete({ route: 'identity' });
  service.begin(input).complete({ metadata: { miep: true }, route: 'mission' });
  await service.drain();
  assert.equal(rows.length, 3);
  assert.equal(new Set(rows.map(row => row.decision_id)).size, 3);
  assert.deepEqual(rows.map(row => row.comparison), ['match', 'mismatch', 'unavailable']);
  for (const row of rows) {
    for (const key of DECISION_SCHEMA.required) assert.deepEqual(row[key], decision()[key]);
    assert.equal(row.mode, 'shadow');
    assert.equal(row.session_id, 'session-a');
    assert.equal(row.tenant_id, '10');
    assert.ok(row.latency_ms >= 0);
    assert.equal(row.raw_redacted_response, null);
  }
  assert.equal(service.begin({ ...input, _miepInternal: true }), null);
});

test('provider sees a copied redacted snapshot, not mutable routing/session data', async () => {
  let state;
  const { service } = fixture({ provider: fakeProvider(async value => { state = value; value.context.page = 'mutated'; return { decision: decision() }; }) });
  const session = { context: { page: 'workspace' }, messages: [{ role: 'max', text: 'Approve discovery?' }] };
  const shadow = service.begin({ question: 'yes' }, session);
  const mission = { id: 'mission-a', stage: 'discover', pendingOperatorDecision: { kind: 'discovery_approval', prompt: 'Approve?' } };
  shadow.captureMission(mission);
  mission.pendingOperatorDecision = null;
  session.messages[0].text = 'Later turn';
  shadow.complete({ route: 'mission' });
  await service.drain();
  assert.equal(state.context.mission.pending_decision.kind, 'discovery_approval');
  assert.equal(state.recent_messages[0].text, 'Approve discovery?');
  assert.equal(session.context.page, 'workspace');
  assert.equal(state.current_route, undefined, 'avoid feeding production recommendation to evaluator');
  const privateState = snapshotInput({
    question: 'api_key=super-secret token:private-token Bearer hidden-password sk-hidden-value me@example.com https://host/?token=secret',
    context: { apiKey: 'do-not-copy', customers: ['do-not-copy'] },
  });
  assert.doesNotMatch(JSON.stringify(privateState), /super-secret|private-token|hidden-password|sk-hidden-value|me@example.com|token=secret|do-not-copy/);
});

test('Jev sends documented typed questions and validates a redacted response', async () => {
  const raw = responseBody();
  raw.secret = 'never-log'; raw.answers.intent.echo = 'never-log'; raw.usage.secret = 'never-log';
  let request;
  const rows = [];
  const service = new DecisionService({ env: { ...ENV, DECISION_SHADOW_LOG_RAW: 'true' }, audit: row => rows.push(row),
    fetchImpl: async (url, options) => { request = { url, options }; return new Response(JSON.stringify(raw)); } });
  service.begin(input).complete({ route: 'mission' });
  await service.drain();
  assert.equal(request.url, ENDPOINT);
  assert.equal(request.options.headers.Authorization, 'Bearer test-key');
  assert.equal(request.options.redirect, 'error');
  const body = JSON.parse(request.options.body);
  assert.equal(body.model, 'jev-latest');
  assert.equal(Object.keys(body.questions).length, 7);
  assert.equal(body.questions.recommended_route.type, 'choice');
  assert.equal(body.questions.approval_probability.type, 'noul');
  assert.equal(rows[0].model, 'jev-1.13.0');
  assert.equal(rows[0].raw_redacted_response.answers.intent.choice, 'mission_instruction');
  assert.doesNotMatch(JSON.stringify(rows), /never-log|test-key|Continue the mission/);
});

test('Jev rejects missing/wrong answer types, invalid choices and malformed distributions', () => {
  assert.equal(parseJevResponse(responseBody()).decision.confidence, 0.9);
  const mutations = [
    raw => { delete raw.answers.intent; }, raw => { raw.model = 'other'; },
    raw => { raw.answers.approval_probability.noul = '1'; },
    raw => { raw.answers.intent.type = 'score'; }, raw => { raw.answers.intent.confidence = null; },
    raw => { raw.answers.intent.choice = 'hijack'; }, raw => { raw.answers.intent.probabilities = {}; },
    raw => { raw.answers.intent.probabilities.approval = 0.5; },
    raw => { raw.answers.intent.probabilities.mission_instruction = 0; raw.answers.intent.probabilities.approval = 1; },
    raw => { raw.answers.requires_human_clarification.noul = 2; },
  ];
  for (const mutate of mutations) { const raw = responseBody(); mutate(raw); assert.throws(() => parseJevResponse(raw), { code: 'invalid_response' }); }
});

test('network, HTTP, JSON, schema and oversized failures produce safe error audits', async () => {
  for (const [code, fetchImpl] of [
    ['provider_error', async () => { throw new Error('Authorization: secret-private'); }],
    ['http_error', async () => new Response('private error body', { status: 429 })],
    ['invalid_response', async () => new Response('private malformed json')],
    ['invalid_response', async () => new Response(JSON.stringify({ secret: 'private' }))],
    ['invalid_response', async () => new Response('x'.repeat(65537))],
  ]) {
    const { service, rows } = fixture({ provider: undefined, fetchImpl });
    service.begin(input).complete({ route: 'mission' });
    await service.drain();
    assert.equal(rows[0].status, 'error');
    assert.equal(rows[0].errors[0].code, code);
    assert.equal(rows[0].fallback_provider, 'noop');
    assert.equal(rows[0].current_route.route, 'mission');
    assert.doesNotMatch(JSON.stringify(rows), /secret-private|private/);
  }
});

test('uncooperative provider times out, aborts and cannot produce duplicate late logs', async () => {
  let signal, resolve;
  const { service, rows } = fixture({ env: { ...ENV, JEV_TIMEOUT_MS: '10' },
    provider: fakeProvider((_state, options) => { signal = options.signal; return new Promise(done => { resolve = done; }); }) });
  service.begin(input).complete({ route: 'mission' });
  await service.drain();
  assert.equal(rows[0].errors[0].code, 'timeout');
  assert.equal(signal.aborted, true);
  resolve({ decision: decision() });
  await new Promise(done => setImmediate(done));
  assert.equal(rows.length, 1);
});

test('capacity is bounded and saturation still produces a per-message audit', async () => {
  const { service, rows } = fixture({ env: { ...ENV, DECISION_SHADOW_MAX_PENDING: '1', JEV_TIMEOUT_MS: '10' },
    provider: fakeProvider(() => new Promise(() => {})) });
  for (let i = 0; i < 3; i++) service.begin(input).complete({ route: 'mission' });
  await service.drain();
  assert.equal(rows.length, 3);
  assert.equal(rows.filter(row => row.fallback_reason === 'capacity_limit').length, 2);
  assert.equal(service._pending.size, 0);
});

test('audit failures are isolated, including asynchronous rejection', async () => {
  for (const audit of [() => { throw Error('sink failed'); }, async () => { throw Error('sink failed'); }]) {
    const { service } = fixture({ audit });
    assert.doesNotThrow(() => service.begin(input).complete({ route: 'mission' }));
    await service.drain();
  }
});

test('observed route recognizes approval/inspection and avoids invented comparisons', () => {
  assert.equal(observedRoute({ resolution: { action: 'approve_discovery' }, route: 'intelligence' }).route, 'approval');
  assert.equal(observedRoute({ routingTrace: { primaryObjective: 'session_inspection' }, route: 'intelligence' }).route, 'inspection');
  assert.equal(observedRoute({ metadata: { miep: true }, route: 'mission' }).route, null);
  assert.equal(observedRoute(null, true).route, null);
});

test('legacy HTTP observer preserves body, status, this and return value; logs errors too', async () => {
  const { service, rows } = fixture();
  for (const status of [200, 500]) {
    const body = status === 200 ? { route: 'mission', answer: 'original' } : { error: 'private' };
    const res = { statusCode: status, json(value) { assert.strictEqual(value, body); assert.equal(this.statusCode, status); return this; } };
    observeLegacyChat({ body: { question: 'Continue' } }, res, service, 10);
    assert.strictEqual(res.json(body), res);
  }
  await service.drain();
  assert.equal(rows.length, 2);
  assert.equal(rows[0].source, 'legacy_chat');
  assert.equal(rows[0].tenant_id, '10');
  assert.equal(rows[1].current_route.failed, true);
  assert.doesNotMatch(JSON.stringify(rows), /private/);
});

test('auxiliary operator message endpoints log fixed handlers or unavailable comparisons without changing response', async () => {
  const { service, rows } = fixture();
  for (const [source, routeHint, body, expected] of [
    ['amo_ask', 'inspection', { answer: 'Mission progress' }, 'inspection'],
    ['amo_execute', 'mission', { action: 'plan_approved' }, 'approval'],
    ['ao_briefing', 'ao_briefing', { answer: 'AO briefing' }, 'intelligence'],
    ['ao_ask', null, { answer: 'AO response' }, null],
    ['ao_respond', null, { answer: 'AO session response' }, null],
  ]) {
    const original = structuredClone(body);
    const res = { statusCode: 200, json(value) { assert.strictEqual(value, body); return value; } };
    observeOperatorHttp({ body: { message: 'Continue' } }, res, { service, source, clientId: 10, sessionId: 'session-a', missionId: 'mission-a', routeHint });
    assert.strictEqual(res.json(body), body);
    await service.drain();
    assert.deepEqual(body, original);
    assert.equal(rows.at(-1).current_route.route, expected);
    assert.equal(rows.at(-1).source, source);
  }
  const res = { json() {} };
  const originalJson = res.json;
  observeOperatorHttp({ body: { question: 'hello' } }, res, { service: fixture({ env: {} }).service });
  assert.strictEqual(res.json, originalJson, 'disabled observer does not wrap HTTP responses');
});
