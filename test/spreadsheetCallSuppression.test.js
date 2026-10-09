'use strict';

// No live database, environment loading, provider or network access. Production
// call modules execute with explicit fail-on-use stubs at every I/O boundary.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const guard = require('../utils/callEligibility');
const { canonicalOutboundEmailIneligibilityReason } = require('../utils/canonicalEmailEligibility');
const { routeProspect } = require('../services/aoProspectRoutingService');

function loadIsolated(file, dependencies) {
  const module = { exports: {} };
  const localRequire = id => {
    if (!(id in dependencies)) throw new Error(`Unstubbed dependency: ${id}`);
    return dependencies[id];
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '..', file), 'utf8'), {
    module, exports: module.exports, require: localRequire,
    process: { env: {} }, console, Buffer, URL, setTimeout,
  }, { filename: file });
  return module.exports;
}

function calModules(row, queryLog = [], providerCalls = []) {
  const pool = { query: async (sql, params) => { if (typeof sql === 'object') { params = sql.values; sql = sql.text; } queryLog.push({ sql, params }); return { rows: row ? [row] : [] }; }, release() {} };
  pool.connect = async () => pool;
  const common = {
    dotenv: { config() {} },
    axios: { post: async (...args) => { providerCalls.push(args); return { data: { call_id: 'stub' } }; } },
    './db': pool, './dbClient': {}, './utils/callEligibility': guard,
    './utils/callDispositions': { notSyntheticSql: async () => 'COALESCE(p.is_synthetic, false) = false' },
    './utils/clientContext': { getRuntimeClientId: () => 10 }, './utils/icpScoring': {},
  };
  return {
    single: loadIsolated('calAgent.js', common),
    batch: loadIsolated('calBatchAgent.js', common),
  };
}

for (const restriction of ['ao_call_suppressed', 'ao_outreach_review_required', 'do_not_contact', 'is_synthetic']) {
  test(`${restriction} blocks both automated provider boundaries`, async () => {
    const queryLog = []; const providerCalls = [];
    const { single, batch } = calModules({ [restriction]: true }, queryLog, providerCalls);
    await assert.rejects(single.initiateCall({ id: 'specific-prospect', client_id: 10, phone: '6035551212' }, 'Example'), { code: 'CALL_SUPPRESSED' });
    await assert.rejects(batch.createBlandBatch([{ metadata: { prospect_id: 'specific-prospect', client_id: 10 }, phone_number: '+16035551212' }]), { code: 'CALL_SUPPRESSED' });
    assert.equal(providerCalls.length, 0);
    assert.ok(queryLog.filter(q => /FROM prospects/.test(q.sql)).every(q => /id = \$1 AND client_id = \$2/.test(q.sql)));
    assert.ok(queryLog.filter(q => /FROM prospects/.test(q.sql)).every(q => q.params[0] === 'specific-prospect' && q.params[1] === 10));
  });
}

test('missing record, invalid scope, or CRM read failure prevents dispatch', async () => {
  const providerCalls = [];
  const { single } = calModules(null, [], providerCalls);
  await assert.rejects(single.initiateCall({ id: 'wrong-tenant', client_id: 10 }), { code: 'CALL_SUPPRESSED' });
  await assert.rejects(single.initiateCall({ id: 'missing-scope' }), { code: 'CALL_SUPPRESSED' });
  await assert.rejects(guard.assertCallAllowed({ query: async () => { throw new Error('offline'); } }, 'id', 10), /offline/);
  assert.equal(providerCalls.length, 0);
});

test('fresh unblocked prospect can reach the stub provider and batch formatting rejects suppressed snapshots', async () => {
  const calls = [];
  const { single, batch } = calModules({ do_not_contact: false, is_synthetic: false, ao_call_suppressed: false }, [], calls);
  await single.initiateCall({ id: 'allowed', client_id: 10, phone: '6035551212' }, 'Example');
  assert.equal(calls.length, 1);
  const formatted = batch.buildCallData([{ id: 'blocked', phone: '6035551212', ao_call_suppressed: true }, { id: 'allowed', client_id: 10, phone: '6035551212' }]);
  assert.equal(formatted.formattedProspects.length, 1);
  assert.equal(formatted.skipped[0].reason, 'call_suppressed');
});

test('call-only suppression does not alter canonical email eligibility or global DNC', () => {
  const row = { email: 'person@example-business.com', email_verified: true, email_status: 'verified', do_not_contact: false };
  const before = canonicalOutboundEmailIneligibilityReason(row);
  assert.equal(before, null);
  assert.equal(canonicalOutboundEmailIneligibilityReason({ ...row, ao_call_suppressed: true }), before);
  assert.equal(row.do_not_contact, false);
  assert.equal(canonicalOutboundEmailIneligibilityReason({ ...row, do_not_contact: true }), 'do_not_contact');
});

test('canonical callback scheduling rejects a suppressed prospect before writing', async () => {
  const queries = [];
  const client = { query: async sql => { queries.push(sql); return { rows: /SELECT \*/.test(sql) ? [{ id: 'id', ao_call_suppressed: true }] : [] }; }, release() {} };
  const pool = { connect: async () => client };
  const lifecycle = loadIsolated('services/lifecycleService.js', {
    '../db': pool, '../utils/callEligibility': guard, '../utils/lifecycleSchema': { ensureLifecycleSchema: async () => {} },
  });
  await assert.rejects(lifecycle.scheduleProspectCallback({ pool, prospectId: 'id', clientId: 10, callbackAt: new Date(), source: 'test' }), { code: 'CALL_SUPPRESSED' });
  assert.equal(queries.filter(sql => /^\s*(UPDATE|INSERT)/.test(sql)).length, 0);
  assert.ok(queries.includes('ROLLBACK'));
});

test('call preparation refuses suppressed workspace instead of returning a dialable number', async () => {
  const prep = loadIsolated('services/callPreparation.js', {
    '../utils/setterPlaybooks': {}, '../utils/callEligibility': guard,
    './prospectWorkspace': { getProspectWorkspace: async () => ({ prospect: { callProhibited: true } }) },
  });
  await assert.rejects(prep.getCallPreparation({ clientId: 10, prospectId: 'id' }), { code: 'CALL_SUPPRESSED' });
});

test('AO routing does not recommend calls or globally suppress the prospect', () => {
  const routing = routeProspect({ prospect: { id: 'id', phone: '6035551212', ao_call_suppressed: true, email: 'pat@example-business.com', icp_score: 90, vertical: 'property_manager', service_area_match: 'Manchester' }, company: { name: 'Example', location: 'Manchester, NH' }, availableAos: [] });
  assert.match(routing.recommended_first_action, /Calls suppressed/);
  assert.notEqual(routing.recommended_motion, 'SUPPRESS');
});

test('call queue selectors exclude the prospect-scoped suppression flag', async () => {
  const queries = [];
  const { batch } = calModules(null, queries);
  await batch.getBatchCandidates();
  assert.equal((queries[0].sql.match(/COALESCE\(p\.ao_call_suppressed, false\) = false/g) || []).length, 2);
  const setter = fs.readFileSync(path.join(__dirname, '..', 'routes/setter.js'), 'utf8');
  assert.match(setter, /WHERE p.source = 'scout'\s+AND COALESCE\(p.ao_call_suppressed, false\) = false/);
});

test('provider handoff holds sorted unique suppression locks until it settles', async () => {
  const events = [];
  const client = {
    query: async (config, params) => {
      const sql = config.text || config; params = config.values || params;
      events.push({ sql, params, queryTimeout: config.query_timeout });
      return { rows: [{ ao_call_suppressed: false }] };
    },
    release: discard => events.push({ release: discard }),
  };
  await guard.withCallAuthorization({ connect: async () => client }, [
    { prospectId: 'z', clientId: 10 }, { prospectId: 'a', clientId: 10 }, { prospectId: 'a', clientId: 10 },
  ], async () => { events.push({ provider: true }); });
  assert.deepEqual(events.filter(x => /pg_advisory_xact_lock/.test(x.sql)).map(x => x.params[0]), ['max:call:10:a', 'max:call:10:z']);
  const provider = events.findIndex(x => x.provider);
  assert.ok(events.findIndex(x => x.sql === 'COMMIT') > provider);
  assert.ok(events.filter(x => x.sql).every(x => x.queryTimeout === 6000));
  assert.ok(events.some(x => /set_config\('lock_timeout', '5s', true\)/.test(x.sql)));
  assert.ok(events.some(x => /set_config\('statement_timeout', '5s', true\)/.test(x.sql)));
  assert.ok(events.some(x => /set_config\('idle_in_transaction_session_timeout', '0', true\)/.test(x.sql)));
  assert.equal(events.at(-1).release, false);
});

test('failed provider handoff and rollback never leak a reusable locked session', async () => {
  let released;
  const client = {
    query: async config => {
      if (config.text === 'ROLLBACK') throw new Error('connection lost');
      return { rows: [{}] };
    },
    release: discard => { released = discard; },
  };
  await assert.rejects(guard.withCallAuthorization({ connect: async () => client }, [{ prospectId: 'id', clientId: 10 }], async () => { throw new Error('provider timeout'); }), /provider timeout/);
  assert.equal(released, true);
});

test('legacy AO phone conversion holds unresolved identity and suppressed linked identity before writing', async () => {
  for (const crmId of [null, 'linked']) {
    const writes = [];
    const row = { id: 'task', client_id: 10, crm_prospect_id: crmId, contact_phone: '6035551212', next_action: 'revisit' };
    const pool = { query: async sql => {
      if (/FROM ao_follow_up_tasks/.test(sql)) return { rows: [row] };
      if (/FROM prospects/.test(sql)) return { rows: [{ ao_call_suppressed: true }] };
      writes.push(sql); throw new Error('Unexpected write');
    }, connect: async () => { throw new Error('Unexpected transaction'); } };
    const service = loadIsolated('services/aoFieldService.js', {
      axios: {}, '../db': pool, '../utils/callEligibility': guard,
      '../utils/aoMessageTemplates': {}, '../utils/aoQueueFormat': {}, '../utils/aoAssignment': {},
    });
    const task = await service.getTaskForFollowUp('task', 7);
    assert.equal(task.call_prohibited, true);
    assert.equal(task.contact_phone, null);
    await assert.rejects(service.convertToPhoneFollowUp('task', 7), { code: 'CALL_SUPPRESSED' });
    assert.equal(writes.length, 0);
  }
});

test('lock timeout rolls back, discards the session, and never reads CRM or dispatches', async () => {
  const events = []; let providerCalls = 0; let released;
  const client = {
    query: async config => {
      events.push(config.text);
      assert.equal(config.query_timeout, 6000);
      if (/pg_advisory_xact_lock/.test(config.text)) throw Object.assign(new Error('canceling statement due to lock timeout'), { code: '55P03' });
      return { rows: [] };
    },
    release: discard => { released = discard; },
  };
  await assert.rejects(guard.withCallAuthorization({ connect: async () => client }, [{ prospectId: 'id', clientId: 10 }], async () => { providerCalls++; }), { code: '55P03' });
  assert.equal(providerCalls, 0);
  assert.equal(events.some(sql => /FROM prospects/.test(sql)), false);
  assert.equal(events.at(-1), 'ROLLBACK');
  assert.equal(released, true);
});

test('outreach review hold independently prevents email eligibility', () => {
  const row = { email: 'pat@example-business.com', email_status: 'verified', email_verified: true, do_not_contact: false, ao_outreach_review_required: true };
  assert.equal(canonicalOutboundEmailIneligibilityReason(row), 'outreach_review_required');
  assert.equal(row.do_not_contact, false);
  assert.equal(canonicalOutboundEmailIneligibilityReason({ ...row, ao_outreach_review_required: false }), null);
});

test('email provider shares the admission lock but respects call-only scope', async () => {
  for (const held of [false, true]) {
    const queries = []; const sent = []; let crossed = false;
    const connection = { query: async config => {
      queries.push(config.text);
      return { rows: [{ ao_call_suppressed: true, do_not_contact: false, ao_outreach_review_required: held }] };
    }, release() {} };
    const send = loadIsolated('packages/providers/brevo/sendEmail.js', {
      axios: { post: async (...args) => { sent.push(args); return { data: { messageId: 'stub' } }; } },
      '../../../utils/callEligibility': guard,
    }).sendEmail;
    const result = await send({ toEmail: 'pat@example-business.com', subject: 'Approved message', body: 'Reviewed copy', apiKey: 'fake-key',
      outreachAuthorization: { pool: { connect: async () => connection }, prospectId: 'id', clientId: 10 },
      providerBoundary: { markCrossed() { crossed = true; } },
    });
    assert.equal(result.success, !held);
    assert.equal(sent.length, held ? 0 : 1);
    assert.equal(crossed, !held);
    assert.ok(queries.some(sql => /pg_advisory_xact_lock/.test(sql)));
    assert.equal(queries.at(-1), held ? 'ROLLBACK' : 'COMMIT');
    if (held) assert.equal(result.providerErrorCode, 'outreach_review_required');
  }
});

test('mailbox verification exemption requires server-owned purpose and exact non-CRM identity', () => {
  const { isNonCrmMailboxVerification } = require('../services/tenantMailbox');
  const input = { prospectId: 'safe-test', missionId: 'mailbox-verification', outreachAssetId: 'mailbox-test-send', metadata: { purpose: 'mailbox_verification', operatorCommand: 'acquisition:mailbox:test-send' } };
  assert.equal(isNonCrmMailboxVerification(input), false);
  assert.equal(isNonCrmMailboxVerification(input, { purpose: 'mailbox_verification' }), true);
  assert.equal(isNonCrmMailboxVerification({ ...input, prospectId: 'real-crm-id' }, { purpose: 'mailbox_verification' }), false);
  assert.equal(isNonCrmMailboxVerification({ ...input, missionId: 'real-mission' }, { purpose: 'mailbox_verification' }), false);
});
