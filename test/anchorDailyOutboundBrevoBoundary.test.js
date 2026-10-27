'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const guard = require('../utils/callEligibility');
const boundary = require('../services/governedOutboundProviderBoundary');

function load(file, dependencies, env = {}) {
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '..', file), 'utf8'), {
    module, exports: module.exports, process: { env },
    require(id) {
      if (!Object.hasOwn(dependencies, id)) throw new Error('Unstubbed dependency: ' + id);
      return dependencies[id];
    },
  }, { filename: file });
  return module.exports;
}
const reviewed = () => ({ do_not_contact: false, is_synthetic: false, ao_outreach_review_required: false, ao_call_suppressed: false });
function fixture({ row = reviewed(), sqlError = null, commitError = null, postError = null, missingCallColumn = false, featureEnabled = true, clientId = 10 } = {}) {
  const queries = [], requests = [], releases = [];
  const client = {
    async query(config) {
      const sql = typeof config === 'string' ? config : config.text;
      queries.push({ sql, values: config.values, timeout: config.query_timeout });
      if (/to_regclass/.test(sql)) return { rows: [{ control_table: null }] };
      if (/FROM prospects/.test(sql)) {
        if (sqlError) throw sqlError;
        if (missingCallColumn && /ao_call_suppressed/.test(sql)) throw Object.assign(new Error('column "ao_call_suppressed" does not exist'), { code: '42703' });
        return { rows: row == null ? [] : [{...row,id:config.values[0],client_id:config.values[1]}] };
      }
      if (sql === 'COMMIT' && commitError) throw commitError;
      return { rows: [] };
    },
    release(discard) { releases.push(discard); },
  };
  const pool = { connect: async () => client };
  const send = load('packages/providers/brevo/sendEmail.js', {
    axios: { async post(...args) { requests.push(args); if (postError) throw postError; return { data: { messageId: 'fixture-receipt' } }; } },
    '../../../utils/callEligibility': guard,
  }, featureEnabled ? { ANCHOR_BREVO_BOUNDARY_DIAGNOSTICS_ENABLED: 'true' } : {}).sendEmail;
  const tracker = boundary.createProviderBoundaryTracker();
  const input = { toEmail: 'fixture@example.test', subject: 'Approved fixture', body: 'Synthetic',
    apiKey: 'fixture-only', sender: { name: 'Fixture', email: 'sender@example.test' }, requireExplicitSender: true,
    outreachAuthorization: { pool, prospectId: 'contact-fixture', clientId }, providerBoundary: tracker };
  return { pool, send, tracker, input, queries, requests, releases };
}
function wrappedSender(send, pool, { featureEnabled = true, clientId = 10 } = {}) {
  const source = fs.readFileSync(path.join(__dirname, '../services/governedOutboundAdapters.js'), 'utf8');
  const module = { exports: {} };
  vm.runInNewContext(source, { module, exports: module.exports, process: { env: featureEnabled ? { ANCHOR_BREVO_BOUNDARY_DIAGNOSTICS_ENABLED: 'true' } : {} }, require(id) {
    if (id === '../packages/providers/brevo/sendEmail') return { sendEmail: send };
    return {};
  } });
  return module.exports.adapters(pool, { governedContext: {
    tenantId: String(clientId), clientId, transport: { usesBrevoTransport: true },
  } });
}

test('all email/SMS/call paths retain their existing mandatory full-schema query', async () => {
  for (const authorize of [guard.withEmailAuthorization, guard.withSmsAuthorization, guard.withCallAuthorization]) {
    const f = fixture({ missingCallColumn: true });
    let calls = 0;
    await assert.rejects(authorize(f.pool, [{ prospectId: 'contact-fixture', clientId: 10 }], async () => { calls++; }), { code: '42703' });
    assert.equal(calls, 0);
    const select = f.queries.find(q => /FROM prospects/.test(q.sql));
    assert.match(select.sql, /ao_call_suppressed/);
    assert.match(select.sql, /ao_outreach_review_required/);
    assert.deepEqual(select.values, ['contact-fixture', 10]);
    assert.ok(f.queries.every(q => q.timeout === 6000));
  }
});

test('disabled Anchor and every non-Anchor wrapper preserve conservative marking', async () => {
  for (const options of [{ featureEnabled: false }, { clientId: 13 }, { clientId: 1 }, { clientId: 17 }]) {
    const f = fixture({ missingCallColumn: true, ...options });
    const send = wrappedSender(f.send, f.pool, options).send;
    boundary.markLeafProviderSend(send, f.tracker);
    assert.equal(f.tracker.crossed, true);
    const result = await send(f.input);
    assert.equal(result.success, false);
    assert.equal(result.providerErrorCode, '42703');
    assert.equal(f.requests.length, 0);
  }
});

test('call-only suppression remains enforced by calls and does not suppress reviewed email/SMS', async () => {
  const row = { ...reviewed(), ao_call_suppressed: true };
  for (const authorize of [guard.withEmailAuthorization, guard.withSmsAuthorization]) {
    const f = fixture({ row }); let calls = 0;
    await authorize(f.pool, [{ prospectId: 'contact-fixture', clientId: 10 }], async () => { calls++; });
    assert.equal(calls, 1);
  }
  const f = fixture({ row }); let calls = 0;
  await assert.rejects(guard.withCallAuthorization(f.pool, [{ prospectId: 'contact-fixture', clientId: 10 }], async () => { calls++; }), { code: 'CALL_SUPPRESSED' });
  assert.equal(calls, 0);
  assert.match(f.queries.find(q => /FROM prospects/.test(q.sql)).sql, /ao_call_suppressed/);
});

test('feature-enabled Anchor Brevo wrappers do not pre-mark the provider boundary', async () => {
  for (const kind of ['sendFor', 'send']) {
    const f = fixture({ sqlError: Object.assign(new Error('column "ao_call_suppressed" does not exist'), { code: '42703' }) });
    const adapters = wrappedSender(f.send, f.pool);
    const send = kind === 'send' ? adapters.send : adapters.sendFor({}, { item: { prospect_id: 'contact-fixture' } });
    boundary.markLeafProviderSend(send, f.tracker);
    assert.equal(f.tracker.crossed, false, kind);
    const result = await send(f.input);
    assert.equal(result.success, false);
    assert.equal(result.providerErrorCode, '42703');
    assert.equal(result.providerBoundaryCrossed, false);
    assert.equal(result.failureStage, 'authorization');
    assert.equal(f.tracker.crossed, false);
    assert.equal(f.requests.length, 0);
    assert.equal(f.queries.at(-1).sql, 'ROLLBACK');
  }
});

test('only an explicitly instrumented Brevo declaration defers marking; legacy custom leaves stay conservative', () => {
  for (const marker of [undefined, false, 1, 'true']) {
    const send = async () => ({ success: false });
    send.isInstrumentedBrevoTransport = marker;
    const tracker = boundary.createProviderBoundaryTracker();
    boundary.markLeafProviderSend(send, tracker);
    assert.equal(tracker.crossed, true);
  }
  assert.equal(boundary.isPreProviderOutboundFailure({ code: '42703' }), false, 'never broadly classify stored SQLSTATE as unsent');
});

test('DNC, synthetic and absent CRM records never reach instrumented Brevo HTTP', async () => {
  for (const row of [null, { ...reviewed(), do_not_contact: true }, { ...reviewed(), is_synthetic: true }]) {
    const f = fixture({ row });
    boundary.markLeafProviderSend(wrappedSender(f.send, f.pool).send, f.tracker);
    const result = await f.send(f.input);
    assert.equal(result.success, false);
    assert.equal(f.requests.length, 0);
    assert.equal(f.tracker.crossed, false);
  }
});

test('successful HTTP crosses immediately before the request and only then commits', async () => {
  const f = fixture();
  boundary.markLeafProviderSend(wrappedSender(f.send, f.pool).send, f.tracker);
  assert.equal(f.tracker.crossed, false);
  const result = await f.send(f.input);
  assert.equal(result.success, true);
  assert.equal(result.providerMessageId, 'fixture-receipt');
  assert.equal(f.tracker.crossed, true);
  assert.equal(f.requests.length, 1);
  assert.equal(f.queries.at(-1).sql, 'COMMIT');
});

test('HTTP failures and post-HTTP COMMIT failures retain crossed/uncertain semantics', async () => {
  for (const failure of [
    { postError: Object.assign(new Error('timeout'), { code: 'ECONNABORTED' }), stage: 'provider_request' },
    { commitError: Object.assign(new Error('response lost'), { code: '08006' }), stage: 'authorization_commit' },
  ]) {
    const f = fixture(failure);
    boundary.markLeafProviderSend(wrappedSender(f.send, f.pool).send, f.tracker);
    const result = await f.send(f.input);
    assert.equal(result.success, false);
    assert.equal(result.providerBoundaryCrossed, true);
    assert.equal(result.failureStage, failure.stage);
    assert.equal(f.tracker.crossed, true);
    assert.equal(f.requests.length, 1);
    assert.equal(boundary.isPreProviderOutboundFailure({ code: result.providerErrorCode }, f.tracker), false);
  }
});

