'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const hold = require('../utils/anchorOutreachReview');
const { governedContactReason, governedContactDataReason } = require('../utils/governedContactEligibility');
const { routeProspect } = require('../services/aoProspectRoutingService');

const prospectId = '10000000-0000-0000-0000-000000000010';
const clear = { id: prospectId, client_id: 10, ao_outreach_review_required: false,
  do_not_contact: false, is_synthetic: false, ao_call_suppressed: false };

function guardModule() {
  const mod = { exports: {} };
  const code = fs.readFileSync(path.join(__dirname, '../utils/callEligibility.js'), 'utf8');
  vm.runInNewContext(code, { module: mod, exports: mod.exports, require(id) {
    if (id === './anchorOutreachReview') return hold;
    if (id === '../services/anchorEmailRecovery') return {
      loadRecoveryState: async () => ({ mode: 'legacy' }),
      assertRecoveryLeaf: async () => ({ legacy: true }),
    };
    throw new Error(`Unexpected import in offline guard test: ${id}`);
  } });
  return mod.exports;
}

function guardPool(row, readError = null) {
  return { async connect() { return { release() {}, async query(query) {
    const sql = typeof query === 'string' ? query : query.text;
    if (sql.startsWith('SELECT do_not_contact') || sql.startsWith('SELECT id, client_id')) {
      if (readError) throw readError;
      return { rows: row ? [row] : [] };
    }
    return { rows: [] };
  } }; } };
}

for (const value of [true, null, undefined, 'false', 0]) {
  test(`Anchor review state ${String(value)} blocks each legacy provider boundary`, async () => {
    const guard = guardModule();
    for (const method of ['withEmailAuthorization', 'withSmsAuthorization', 'withCallAuthorization']) {
      let handoffs = 0;
      const pool = guardPool({ ...clear, ao_outreach_review_required: value });
      await assert.rejects(guard[method](pool, [{ prospectId, clientId: 10 }], async () => { handoffs++; }));
      assert.equal(handoffs, 0);
    }
  });
}

test('literal false permits one mocked handoff and preserves non-Anchor null semantics', async () => {
  const guard = guardModule();
  for (const tenant of [1, 10, 13, 17]) for (const method of ['withEmailAuthorization', 'withSmsAuthorization', 'withCallAuthorization']) {
    let handoffs = 0;
    const row = { ...clear, client_id: tenant, ao_outreach_review_required: tenant === 10 ? false : null };
    await guard[method](guardPool(row), [{ prospectId, clientId: tenant }], async () => { handoffs++; });
    assert.equal(handoffs, 1, `${tenant} ${method}`);
  }
});

test('missing row and read failures never reach a provider', async () => {
  for (const [row, error] of [[null, null], [clear, new Error('offline read failure')]]) {
    let handoffs = 0;
    await assert.rejects(guardModule().withEmailAuthorization(guardPool(row, error),
      [{ prospectId, clientId: 10 }], async () => { handoffs++; }));
    assert.equal(handoffs, 0);
  }
});

test('persisted contact eligibility requires review while prospective address validation grants no send permission', () => {
  const row = { email: 'owner@localbusiness.example', email_verified: true, email_status: 'valid',
    domain: 'localbusiness.example', enrichment_provenance: { email: { source: 'website_email' } }, client_id: 10 };
  for (const value of [true, null, undefined]) {
    assert.equal(governedContactReason({ ...row, ao_outreach_review_required: value }), 'outreach_review_required');
  }
  assert.equal(governedContactDataReason(row), null);
  assert.equal(governedContactReason(row), 'outreach_review_required');
  assert.equal(governedContactReason({ ...row, ao_outreach_review_required: false }), null);
});

test('held/unknown Anchor prospects get no AO recommendation or active first action', () => {
  for (const value of [true, null, undefined]) {
    const routing = routeProspect({
      prospect: { ...clear, ao_outreach_review_required: value, email: 'office@example.com',
        phone: '6035550100', vertical: 'property_manager', icp_score: 90, service_area_match: 'Manchester, NH' },
      company: { name: 'Offline Example Property', location: 'Manchester, NH' },
      availableAos: [{ id: 7, name: 'Offline AO', active: true }],
    });
    assert.equal(routing.recommended_ao_id, null);
    assert.equal(routing.recommended_motion, 'NURTURE');
    assert.match(routing.recommended_first_action, /Separate outreach review/);
    assert.notEqual(routing.recommended_motion, 'SUPPRESS', 'review holds are not opt-outs');
  }
});

test('fresh review reads reject missing/wrong tenant records and read failures', async () => {
  for (const rows of [[], [{ ...clear, id: 'different' }], [{ ...clear, client_id: 1 }], [{ ...clear, ao_outreach_review_required: true }]]) {
    await assert.rejects(hold.assertAnchorOutreachReviewCleared({ query: async () => ({ rows }) }, prospectId, 10), { code: 'outreach_review_required' });
  }
  await assert.rejects(hold.assertAnchorOutreachReviewCleared({ query: async () => { throw new Error('offline error'); } }, prospectId, 10));
  let reads = 0;
  await hold.assertAnchorOutreachReviewCleared({ query: async () => { reads++; return { rows: [clear] }; } }, prospectId, 10);
  assert.equal(reads, 1);
  await hold.assertAnchorOutreachReviewCleared({ query: async () => { throw new Error('non-Anchor read forbidden'); } }, prospectId, 13);
});

test('SQL alias interpolation accepts identifiers only', () => {
  assert.throws(() => hold.anchorOutreachReviewSql('p); DELETE FROM prospects; --'));
});

test('stale warm events and task creation re-read the hold before any external request', async () => {
  let reads = [], externalRequests = 0;
  let liveRow = { ...clear, ao_outreach_review_required: true };
  const dbPath = require.resolve('../db');
  const axiosPath = require.resolve('axios');
  const warmPath = require.resolve('../warmRoutingAgent');
  const previous = new Map([dbPath, axiosPath, warmPath].map(p => [p, require.cache[p]]));
  const tokenBefore = process.env.TODOIST_API_TOKEN;
  process.env.TODOIST_API_TOKEN = 'offline-fixture-only';
  const offlineDb = { async query(sql) {
    reads.push(sql);
    if (/SELECT id, client_id/.test(sql)) return { rows: liveRow ? [liveRow] : [] };
    if (/^(BEGIN|COMMIT|ROLLBACK|SET LOCAL)/.test(sql)) return { rows: [] };
    throw new Error('Held warm path attempted a write or unexpected query');
  }, async connect() { return { query: this.query, release() {} }; } };
  require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: offlineDb };
  require.cache[axiosPath] = { id: axiosPath, filename: axiosPath, loaded: true, exports: { async post() {
    externalRequests++; return { data: { id: 'offline-task-id' } };
  } } };
  delete require.cache[warmPath];
  try {
    const warm = require('../warmRoutingAgent');
    const staleProspect = { ...clear, company_name: 'Offline Example' };
    for (const value of [true, null, undefined]) {
      liveRow = { ...clear, ao_outreach_review_required: value };
      reads = [];
      const event = await warm.processProspectEvents(staleProspect, []);
      assert.equal(event.reason, 'outreach_review_required');
      assert.equal(event.consumed, 0);
      assert.equal(reads.length, 1);
      await assert.rejects(warm.createTodoistTask({ prospect: staleProspect, reason: 'REPLY_RECEIVED', due: 'today' }), { code: 'outreach_review_required' });
      assert.equal(externalRequests, 0);
    }
    liveRow = { ...clear };
    assert.equal(await warm.createTodoistTask({ prospect: staleProspect, reason: 'REPLY_RECEIVED', due: 'today' }), 'offline-task-id');
    assert.equal(externalRequests, 1, 'completed review permits exactly one mocked task request');
  } finally {
    if (tokenBefore === undefined) delete process.env.TODOIST_API_TOKEN;
    else process.env.TODOIST_API_TOKEN = tokenBefore;
    for (const [p, v] of previous) { delete require.cache[p]; if (v) require.cache[p] = v; }
  }
});

test('direct AO task creation cannot turn a stale reviewed snapshot into a held task', async () => {
  const { createTaskFromRouting } = require('../services/aoProspectTaskService');
  for (const value of [true, null, undefined]) {
    let writes = 0;
    const db = { async query(sql) {
      if (/SELECT id, client_id/.test(sql)) return { rows: [{ ...clear, ao_outreach_review_required: value }] };
      writes++; throw new Error('Held account reached a task mutation');
    } };
    await assert.rejects(createTaskFromRouting({ clientId: 10, prospectId,
      routing: { recommended_ao_id: 7, recommended_motion: 'AO_LED' }, db }), { code: 'outreach_review_required' });
    assert.equal(writes, 0);
  }
});
