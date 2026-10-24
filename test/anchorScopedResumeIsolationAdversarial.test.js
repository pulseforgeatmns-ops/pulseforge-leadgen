'use strict';

// Independent SPEC-262 isolation regressions. All databases/transports here are
// synthetic. In particular, no recovery table is present in non-Anchor fixtures.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const legacySelect = `SELECT do_not_contact, is_synthetic, ao_call_suppressed, ao_outreach_review_required
     FROM prospects WHERE id = $1 AND client_id = $2`;
const settings = `SELECT set_config('lock_timeout', '5s', true),
      set_config('statement_timeout', '5s', true),
      set_config('idle_in_transaction_session_timeout', '0', true)`;
const lockSql = 'SELECT pg_advisory_xact_lock(hashtextextended($1, 0))';

function guardWithForbiddenRecovery() {
  const module = { exports: {} };
  const forbidden = new Proxy({}, { get(_target, name) {
    return () => { throw new Error(`forbidden recovery dependency: ${String(name)}`); };
  } });
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../utils/callEligibility.js'), 'utf8'), {
    module, exports: module.exports, require(id) {
      if (/anchor.*(?:Recovery|Quarantine|Resume)/i.test(id)) return forbidden;
      throw new Error(`unexpected guard import: ${id}`);
    },
  }, { filename: 'isolated-callEligibility.js' });
  return module.exports;
}

function legacyPool({ error, row = { do_not_contact: false, is_synthetic: false,
  ao_call_suppressed: false, ao_outreach_review_required: false } } = {}) {
  const trace = [];
  const client = { async query(q, v) {
    const text = typeof q === 'string' ? q : q.text;
    const values = typeof q === 'string' ? v : q.values;
    trace.push({ text, values: values || [] });
    assert.doesNotMatch(text, /anchor_email|to_regclass|recovery|quarantine/i);
    if (text === legacySelect) {
      if (error) throw error;
      return { rows: row ? [row] : [] };
    }
    assert.ok(['BEGIN READ ONLY', settings, lockSql, 'COMMIT', 'ROLLBACK'].includes(text), text);
    return { rows: [] };
  }, release(discard) { trace.push({ release: Boolean(discard) }); } };
  return { trace, async connect() { return client; } };
}

function expectedTrace(tenant, { failure = false } = {}) {
  return [
    { text: 'BEGIN READ ONLY', values: [] },
    { text: settings, values: [] },
    { text: lockSql, values: [`max:call:${tenant}:synthetic-contact`] },
    { text: legacySelect, values: ['synthetic-contact', tenant] },
    { text: failure ? 'ROLLBACK' : 'COMMIT', values: [] },
    { release: failure },
  ];
}

for (const tenant of [1, 13, 17]) {
  test(`isolation: tenant ${tenant} email retains the exact original SQL/locks without recovery tables`, async () => {
    const guard = guardWithForbiddenRecovery();
    const pool = legacyPool(); let sends = 0;
    const result = await guard.withEmailAuthorization(pool,
      [{ prospectId: 'synthetic-contact', clientId: tenant }], async () => { sends++; return 'synthetic-receipt'; });
    assert.equal(result, 'synthetic-receipt'); assert.equal(sends, 1);
    assert.deepEqual(JSON.parse(JSON.stringify(pool.trace)), expectedTrace(tenant));
  });

  test(`isolation: tenant ${tenant} retains missing-call-column failure even after review schema exists`, async () => {
    const guard = guardWithForbiddenRecovery();
    const pool = legacyPool({ error: Object.assign(new Error('column ao_call_suppressed does not exist'), { code: '42703' }) });
    let sends = 0;
    await assert.rejects(guard.withEmailAuthorization(pool,
      [{ prospectId: 'synthetic-contact', clientId: tenant }], async () => { sends++; }), { code: '42703' });
    assert.equal(sends, 0);
    assert.deepEqual(JSON.parse(JSON.stringify(pool.trace)), expectedTrace(tenant, { failure: true }));
  });
}

test('isolation: calls and SMS retain their original guards, including tenant 10', async () => {
  const guard = guardWithForbiddenRecovery();
  for (const tenant of [1, 10, 13, 17]) for (const method of ['withCallAuthorization', 'withSmsAuthorization']) {
    const pool = legacyPool(); let sends = 0;
    await guard[method](pool, [{ prospectId: 'synthetic-contact', clientId: tenant }], async () => { sends++; });
    assert.equal(sends, 1, `${tenant} ${method}`);
    assert.deepEqual(JSON.parse(JSON.stringify(pool.trace)), expectedTrace(tenant));
    const absent = legacyPool({ error: Object.assign(new Error('column ao_call_suppressed does not exist'), { code: '42703' }) });
    await assert.rejects(guard[method](absent, [{ prospectId: 'synthetic-contact', clientId: tenant }], async () => { sends++; }), { code: '42703' });
    assert.equal(sends, 1);
    assert.deepEqual(JSON.parse(JSON.stringify(absent.trace)), expectedTrace(tenant, { failure: true }));
  }
});

test('isolation: existing non-Anchor email versus call-only suppression semantics remain distinct', async () => {
  const guard = guardWithForbiddenRecovery();
  for (const tenant of [1, 13, 17]) {
    const row = { do_not_contact: false, is_synthetic: false, ao_call_suppressed: true, ao_outreach_review_required: false };
    let sends = 0;
    await guard.withEmailAuthorization(legacyPool({ row }), [{ prospectId: 'synthetic-contact', clientId: tenant }], async () => { sends++; });
    await guard.withSmsAuthorization(legacyPool({ row }), [{ prospectId: 'synthetic-contact', clientId: tenant }], async () => { sends++; });
    await assert.rejects(guard.withCallAuthorization(legacyPool({ row }), [{ prospectId: 'synthetic-contact', clientId: tenant }], async () => { sends++; }), { code: 'CALL_SUPPRESSED' });
    assert.equal(sends, 2);
    for (const key of ['do_not_contact', 'is_synthetic', 'ao_outreach_review_required']) {
      const held = { ...row, [key]: true };
      await assert.rejects(guard.withEmailAuthorization(legacyPool({ row: held }), [{ prospectId: 'synthetic-contact', clientId: tenant }], async () => { sends++; }), { code: 'outreach_review_required' });
    }
    assert.equal(sends, 2);
  }
});

test('isolation: non-Anchor uncertainty and candidate checks never inspect recovery state', async () => {
  const integration = require('../services/anchorEmailRecoveryIntegration');
  const db = { query: async () => { throw new Error('forbidden recovery database read'); } };
  for (const tenantId of ['1', '13', '17']) {
    assert.equal(await integration.uncertaintyState(db, tenantId, { programId: 'synthetic-program' }), null);
    await integration.assertNotQuarantinedWhenInstalled(db, { tenantId, prospectId: 'synthetic-contact', companyId: 'synthetic-company', email: 'synthetic@example.test' });
  }
});

test('isolation: non-Anchor counts and expiry retain original SQL with no recovery schema or locks', async () => {
  const { GovernedOutboundStore } = require('../services/governedOutboundStore');
  for (const tenantId of ['1', '13', '17']) {
    const calls = []; const totals = { today: 2, total: 3, uncertain: 1, last_attempt: null };
    const db = { async query(q, values) {
      const text = typeof q === 'string' ? q : q.text;
      calls.push({ text, values: values || q.values || [] });
      assert.doesNotMatch(text, /anchor_email|to_regclass|recovery|quarantine|pg_advisory/i);
      return { rows: /SELECT\s+count\(\*\)/.test(text) ? [totals] : [] };
    }, release() {} };
    const store = new GovernedOutboundStore({ query: db.query.bind(db), connect: async () => db }, tenantId);
    assert.deepEqual(await store.counts({ id: 'synthetic-program' }, '2026-10-19'), totals);
    assert.equal(calls.length, 1);
    assert.deepEqual(calls[0].values, ['synthetic-program', '2026-10-19', tenantId]);
    calls.length = 0;
    await store.expire('2026-10-19');
    assert.equal(calls.length, 5);
    assert.equal(calls[0].text, 'BEGIN');
    assert.equal(calls[3].text, "UPDATE acquisition_outbound_items SET status='uncertain',reason='abandoned_attempt' WHERE tenant_id=$1 AND status='attempted' AND attempted_at<now()-interval '5 minutes' RETURNING *");
    assert.equal(calls[4].text, 'COMMIT');
  }
});

test('isolation: non-Anchor Max research continues its original injected path without recovery reads', async () => {
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  const calls = [], events = [];
  const pool = { query: async () => { throw new Error('unexpected database query; no recovery tables installed'); } };
  const program = { id: 'synthetic-babrun-program', tenant_id: '13', mode: 'active', policy_hash: 'synthetic-policy',
    source_mission_id: 'synthetic-source', policy: { dailyCap: 5 } };
  const inventory = n => ({ clean: Array.from({ length: n }, (_, i) => ({ prospectId: `synthetic-${i}` })), excluded: [], scope: {} });
  const result = await runMaxOutboundControlLoop({ pool, tenantId: '13', program,
    source: { id: program.source_mission_id, payload: { structuredMission: { market: { segment: 'restaurant' }, geography: { region: 'Synthetic Region' } } } },
    now: new Date('2026-10-19T15:00:00Z'), cycleId: 'synthetic-isolation-cycle', timestamps: {}, funnel: {},
    store: { tenantId: '13', clientId: 13, event: async (type, key, payload) => events.push({ type, key, payload }) },
    governedAdapters: {}, logger: { warn() {} },
    infrastructure: { cap: 5, snapshot: { sentToday: 1 }, assessed: { governor: { outcome: 'proceed' }, health: { score: 74 } },
      operating: { recommendedSafeDailyCapacity: 5, authorizationLimitedCapacity: 5, scheduleLimitedCapacity: 5,
        dispatchCapacityNow: 5, planningDailyCapacity: 5, dispatchableDailyCapacity: 5, effectiveDailyCapacity: 5, governor: 'proceed' } },
    inventory: inventory(2), inventoryAfter: inventory(15),
    scoutRamp: async ({ plan }) => { calls.push(plan.deficit); return { promoted: 13, enrichmentPromoted: 13, discoveredQueued: 13 }; },
    runPreparationRefill: async () => { throw new Error('unexpected extra preparation'); },
  });
  assert.deepEqual(calls, [13]);
  assert.equal(result.plan.cleanInventory, 15);
  assert.equal(result.plan.state, 'healthy');
  assert.equal(events.length, 1);
  assert.equal(events[0].type, 'max_outbound_control');
  assert.equal(events[0].payload.scoutInvoked, true);
});

test('isolation: non-Anchor direct preparation refill preserves the original callback and result', async () => {
  const { applyPreparationRefill } = require('../services/maxOutboundControlLoop');
  for (const tenantId of ['1', '13', '17']) {
    let calls = 0;
    const result = await applyPreparationRefill({
      pool: { query: async () => { throw new Error('forbidden recovery query during non-Anchor refill'); } },
      tenantId, preparation: { prepareRequested: true, pendingPrepared: 0 },
      runPreparationRefill: async () => { calls++; return { preparedAdded: 2, pendingPrepared: 2 }; },
    });
    assert.equal(calls, 1); assert.equal(result.preparedAdded, 2); assert.equal(result.pendingPrepared, 2);
  }
});

test('isolation: conflicting Anchor context or mixed tenant identities cannot select a legacy bypass', async () => {
  const guard = guardWithForbiddenRecovery();
  for (const identities of [
    [{ prospectId: 'synthetic-contact', clientId: 13 }],
    [{ prospectId: 'synthetic-contact', clientId: 10 }, { prospectId: 'synthetic-other', clientId: 13 }],
  ]) {
    const pool = legacyPool(); let sends = 0;
    await assert.rejects(guard.withEmailAuthorization(pool, identities, async () => { sends++; },
      { context: { tenantId: '10', programId: 'synthetic-anchor-program' } }), { code: 'anchor_recovery_scope_invalid' });
    assert.equal(sends, 0);
    assert.deepEqual(pool.trace, [{ release: true }]);
  }
});

test('adversarial: scoped grant is explicit, detached, batch-bound and cannot broaden tenant/channel/review policy', () => {
  const resume = require('../services/anchorScopedResume');
  const { scopedResumeFixture, clone } = require('./helpers/anchorScopedResumeFixture');
  const f = scopedResumeFixture(), options = { now: f.now };
  const valid = resume.validateScopedResumeManifest(f.manifest, options);
  assert.equal(Object.isFrozen(valid.admissions[0]), true);
  assert.equal(Object.isFrozen(valid.scopes.email), true);
  f.manifest.admissions[0].recipientEmail = 'changed@different.test';
  assert.notEqual(valid.admissions[0].recipientEmail, f.manifest.admissions[0].recipientEmail);
  for (const key of Object.keys(valid)) {
    const missing = clone(valid); delete missing[key];
    assert.throws(() => resume.validateScopedResumeManifest(missing, options), key);
  }
  for (const mutate of [
    m => { m.tenantId = '13'; }, m => { m.otherPrograms = 'all'; }, m => { m.calls = 'allow'; },
    m => { m.sms = 'allow'; }, m => { m.manualCrmEmail = 'allow'; }, m => { m.admissionPolicy = 'mission_bound_admission'; },
    m => { m.reviewSchemaPolicy = 'default_false_backfill'; }, m => { m.retryAllowed = true; },
    m => { m.admissions.push(clone(m.admissions[0])); }, m => { m.admissions[0].recipientEmail += ',second@example.test'; },
    m => { m.scopes.email.dailyCap = '2'; }, m => { m.scopes.email.totalCap = 0; },
    m => { m.scopes.email.transport = 'smtp'; }, m => { m.scopes.email.maxSequenceStep = 2; },
    m => { m.scopes.preparation = { enabled: true }; }, m => { m.expiresAt = '2026-10-19T15:30:00Z'; },
  ]) { const bad = clone(valid); mutate(bad); assert.throws(() => resume.validateScopedResumeManifest(bad, options)); }
});

test('adversarial: the email repair cannot accept enabled paid research even with a plausible budget hash', () => {
  const resume = require('../services/anchorScopedResume');
  const { scopedResumeFixture } = require('./helpers/anchorScopedResumeFixture');
  const f = scopedResumeFixture();
  f.manifest.scopes.research = { enabled: true, approvalReference: 'synthetic-separate-research',
    dailyCap: 1, totalCap: 1, operations: ['research_existing_source'], providerBudgetHash: 'a'.repeat(64) };
  assert.throws(() => resume.validateScopedResumeManifest(f.manifest, { now: f.now }));
});

test('adversarial: relinking/import/provenance and email changes invalidate admission; true is never released', () => {
  const resume = require('../services/anchorScopedResume');
  const { scopedResumeFixture, clone } = require('./helpers/anchorScopedResumeFixture');
  const f = scopedResumeFixture(), company = f.companies[0], admission = f.manifest.admissions[0];
  const original = resume.identityFingerprint(f.prospect, company);
  assert.equal(original, admission.identityFingerprint);
  for (const mutate of [
    p => { p.email = 'replacement@synthetic-recovery.test'; },
    p => { p.source = 'imported'; },
    p => { p.source_ref = 'synthetic-new-import'; },
    p => { p.import_metadata = { source: 'synthetic-sheet' }; },
    p => { p.enrichment_provenance = { mission_bound_admission: { missionId: 'new-mission' } }; },
    p => { p.acquisition_metadata = { relinked: true }; },
  ]) { const p = clone(f.prospect); mutate(p); assert.notEqual(resume.identityFingerprint(p, company), original); }
  for (const flag of [true, null, undefined, 'false', 0]) {
    const p = { ...f.prospect, ao_outreach_review_required: flag };
    assert.equal(resume.identityFingerprint(p, company), original, 'separate review CAS does not rewrite identity fingerprint');
    assert.throws(() => resume.assertIdentityReview({ prospect: p, companyNameKey: admission.companyNameKey,
      companyDomain: admission.companyDomain, identityFingerprint: original }, admission));
  }
});

test('adversarial: actual payload binding rejects unchecked recipients, headers, sender and HTML substitutions', () => {
  const resume = require('../services/anchorScopedResume');
  const { scopedResumeFixture, clone } = require('./helpers/anchorScopedResumeFixture');
  const f = scopedResumeFixture();
  assert.deepEqual(resume.assertActualPayload(f.input.command), [f.item.email]);
  for (const mutate of [
    c => { c.actualPayload.to.push({ email: 'hidden@example.test', name: '' }); },
    c => { c.actualPayload.cc = [{ email: 'hidden@example.test' }]; },
    c => { c.actualPayload.bcc = [{ email: 'hidden@example.test' }]; },
    c => { c.actualPayload.to[0].email = 'held@example.test'; },
    c => { c.actualPayload.sender.email = 'changed@example.test'; },
    c => { c.actualPayload.subject += ' changed'; },
    c => { c.actualPayload.textContent += ' changed'; },
    c => { c.actualPayload.htmlContent += '<p>Unapproved content</p>'; },
    c => { c.actualPayload.headers['X-Recipient'] = 'hidden@example.test'; },
    c => { c.actualPayload.headers['Idempotency-Key'] += '-replay'; },
  ]) { const command = clone(f.input.command); mutate(command); assert.throws(() => resume.assertActualPayload(command)); }
  const smtp = { transport: 'smtp', actualPayload: { from: 'sender@example.test', replyTo: 'sender@example.test',
    to: ['synthetic@example.test'], subject: 'Synthetic', text: 'Local fixture', headers: { 'Message-ID': 'synthetic-message' } } };
  assert.deepEqual(resume.assertActualPayload(smtp), ['synthetic@example.test']);
  for (const key of ['cc', 'bcc', 'envelope']) {
    const command = clone(smtp); command.actualPayload[key] = ['hidden@example.test'];
    assert.throws(() => resume.assertActualPayload(command));
  }
});

test('adversarial: Brevo dispatch receives the same frozen actual object that authorization inspected', async () => {
  const module = { exports: {} }; let inspected = null, sent = null;
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../packages/providers/brevo/sendEmail.js'), 'utf8'), {
    module, exports: module.exports, process: { env: { BREVO_API_KEY: 'synthetic-local-key' } },
    require(id) {
      if (id === 'axios') return { post: async (_url, payload) => { sent = payload; return { data: { messageId: 'synthetic-receipt' }, headers: {} }; } };
      if (id === '../../../utils/callEligibility') return { withEmailAuthorization: async (_db, _ids, dispatch, options) => {
        inspected = options.command.actualPayload;
        assert.ok(inspected); assert.equal(Object.isFrozen(inspected), true);
        assert.equal(Object.isFrozen(inspected.to[0]), true); assert.equal(Object.isFrozen(inspected.sender), true);
        assert.throws(() => { inspected.to[0].email = 'held@example.test'; }, TypeError);
        return dispatch();
      } };
      if (/anchorScopedResume/.test(id)) return require('../services/anchorScopedResume');
      throw new Error(`unexpected Brevo import: ${id}`);
    },
  });
  const result = await module.exports.sendEmail({ toEmail: 'synthetic@example.test', subject: 'Synthetic subject', body: 'Synthetic body',
    sender: { email: 'sender@example.test', name: 'Synthetic Sender' }, idempotencyKey: 'synthetic-once',
    outreachAuthorization: { prospectId: 'synthetic-contact', clientId: 10, pool: {} } });
  assert.equal(result.success, true, result.error);
  assert.equal(sent, inspected);
});
