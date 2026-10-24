'use strict';

// Independent SPEC-261 adversarial tests. Synthetic data and pure helpers only;
// no provider module, production pool, credentials, or live database is used.
const test = require('node:test');
const assert = require('node:assert/strict');
const quarantine = require('../services/anchorEmailQuarantine');
const recovery = require('../services/anchorEmailRecovery');

const clone = value => JSON.parse(JSON.stringify(value));

function decision() {
  return {
    tenantId: '10', itemId: 'local-old-item', envelopeId: 'local-old-envelope',
    programId: 'local-old-program', missionId: 'local-old-mission',
    canonicalId: 'local-old-canonical', attemptToken: '2026-10-16 17:08:04.859123+00',
    candidateId: '30000000-0000-4000-8000-000000000001', expectedRevision: 'local-old-revision',
    companyNameKey: 'heldbusiness', companyDomain: 'held-business.test',
    recipientEmail: 'held@example.test',
    prospectId: '10000000-0000-4000-8000-000000000001',
    companyId: '20000000-0000-4000-8000-000000000001',
    expectedStatus: 'uncertain', expectedReason: 'provider_or_persistence_error',
    approvalRef: 'local-synthetic-approval-only',
  };
}

function unresolved(d = decision()) {
  return {
    tenant_id: d.tenantId, item_id: d.itemId, envelope_id: d.envelopeId,
    program_id: d.programId, mission_id: d.missionId, canonical_id: d.canonicalId,
    candidate_id: d.candidateId, expected_revision: d.expectedRevision,
    company_name_key: d.companyNameKey, company_domain: d.companyDomain,
    attempt_token: d.attemptToken, recipient_email: d.recipientEmail,
    prospect_id: d.prospectId, company_id: d.companyId,
    status: d.expectedStatus, reason: d.expectedReason,
  };
}

function quarantineRow(d = decision()) {
  const hash = quarantine.decisionHash(d);
  const row = unresolved(d);
  delete row.status;
  delete row.reason;
  return {
    ...row, id: `quarantine_${hash}`, expected_status: d.expectedStatus,
    expected_reason: d.expectedReason, approval_ref: d.approvalRef,
    decision_hash: hash, created_at: '2026-10-19T16:00:00.000Z',
  };
}

function ownAttempt(row) {
  return {
    tenantId: row.tenant_id, programId: row.program_id,
    envelopeId: row.envelope_id, itemId: row.item_id, attemptToken: row.attempt_token,
  };
}

test('adversarial: quarantine decisions require explicit exact scope and preserve microseconds', () => {
  const base = decision();
  assert.equal(quarantine.normalizeDecision(base).attemptToken, base.attemptToken);
  assert.match(quarantine.decisionHash(base), /^[a-f0-9]{64}$/);
  for (const key of Object.keys(base)) {
    const missing = clone(base);
    delete missing[key];
    assert.throws(() => quarantine.normalizeDecision(missing), `missing ${key}`);
  }
  for (const change of [
    { tenantId: '13' }, { tenantId: 10 }, { expectedStatus: 'sent' },
    { attemptToken: '2026-10-16T17:08:04.859Z' },
    { attemptToken: '2026-10-16 17:08:04.8591234+00' },
    { retryAllowed: true }, { providerOutcome: 'PROVIDER_CONFIRMED_NOT_SENT' },
    { recipientEmail: 'held@example.test,second@example.test' },
  ]) {
    assert.throws(() => quarantine.normalizeDecision({ ...base, ...change }), JSON.stringify(change));
  }
  const later = { ...base, attemptToken: '2026-10-16 17:08:04.859124+00' };
  assert.notEqual(quarantine.decisionHash(base), quarantine.decisionHash(later));
});

test('adversarial: one exact quarantine excludes only its own unchanged unresolved attempt', () => {
  const row = unresolved();
  const approved = quarantineRow();
  const before = JSON.stringify({ row, approved });
  const result = quarantine.classifyBlockingRows([row], [approved]);
  assert.equal(result.rawCount, 1);
  assert.equal(result.quarantinedCount, 1);
  assert.equal(result.ownAttemptCount, 0);
  assert.equal(result.blockingCount, 0);
  assert.equal(JSON.stringify({ row, approved }), before, 'classification must not mutate evidence');
});

test('adversarial: every stale, incomplete, or substituted quarantine binding remains a blocker', () => {
  const row = unresolved();
  const approved = quarantineRow();
  const changes = [
    { tenant_id: '13' }, { item_id: 'different-item' },
    { envelope_id: 'different-envelope' }, { program_id: 'different-program' },
    { mission_id: 'different-mission' }, { canonical_id: 'different-canonical' },
    { candidate_id: 'different-candidate' }, { expected_revision: 'different-revision' },
    { company_name_key: 'differentbusiness' }, { company_domain: 'different.test' },
    { prospect_id: 'different-contact' }, { company_id: 'different-company' },
    { recipient_email: 'different@example.test' },
    { attempt_token: '2026-10-16 17:08:04.859124+00' },
    { attempt_token: null }, { canonical_id: null }, { company_id: null },
    { status: 'attempted' }, { reason: 'changed-reason' },
  ];
  for (const change of changes) {
    const result = quarantine.classifyBlockingRows([{ ...row, ...change }], [approved]);
    assert.equal(result.rawCount, 1, JSON.stringify(change));
    assert.equal(result.blockingCount, 1, JSON.stringify(change));
    assert.equal(result.quarantinedCount, 0, JSON.stringify(change));
  }
  for (const change of [
    { decision_hash: '0'.repeat(64) }, { approval_ref: '' },
    { expected_status: 'attempted' }, { attempt_token: null },
  ]) {
    const result = quarantine.classifyBlockingRows([row], [{ ...approved, ...change }]);
    assert.equal(result.blockingCount, 1, JSON.stringify(change));
  }
});

test('adversarial: missing attempt timestamps and a second unknown cannot vanish from blocking counts', () => {
  const old = unresolved();
  const other = { ...old, item_id: 'second-item', envelope_id: 'second-envelope',
    program_id: 'second-program', canonical_id: 'second-canonical', attempt_token: null };
  const result = quarantine.classifyBlockingRows([old, other], [quarantineRow()]);
  assert.equal(result.rawCount, 2);
  assert.equal(result.quarantinedCount, 1);
  assert.equal(result.blockingCount, 1);
  assert.equal(result.blockingRows[0].item_id, 'second-item');
});

test('adversarial: scheduler excludes only its exact non-quarantined attempted item', () => {
  const old = unresolved();
  const own = {
    ...old, item_id: 'fresh-item', envelope_id: 'fresh-envelope',
    program_id: 'fresh-program', canonical_id: 'fresh-canonical',
    recipient_email: 'fresh@example.test',
    prospect_id: '10000000-0000-4000-8000-000000000002',
    company_id: '20000000-0000-4000-8000-000000000002',
    company_name_key: 'freshbusiness', company_domain: 'fresh-business.test',
    attempt_token: '2026-10-19 16:00:00.123456+00', status: 'attempted', reason: null,
  };
  const exact = quarantine.classifyBlockingRows([old, own], [quarantineRow()], { ownAttempt: ownAttempt(own) });
  assert.equal(exact.rawCount, 2);
  assert.equal(exact.quarantinedCount, 1);
  assert.equal(exact.ownAttemptCount, 1);
  assert.equal(exact.blockingCount, 0);
  for (const change of [
    { tenantId: '13' }, { itemId: 'different' }, { programId: 'different' },
    { envelopeId: 'different' }, { attemptToken: '2026-10-19 16:00:00.123457+00' },
    { attemptToken: null },
  ]) {
    const result = quarantine.classifyBlockingRows([old, own], [quarantineRow()], {
      ownAttempt: { ...ownAttempt(own), ...change },
    });
    assert.equal(result.ownAttemptCount, 0, JSON.stringify(change));
    assert.equal(result.blockingCount, 1, JSON.stringify(change));
  }
  const noGenericAllowance = quarantine.classifyBlockingRows([own], []);
  assert.equal(noGenericAllowance.blockingCount, 1);
});

test('adversarial: quarantined or uncertain items cannot use scheduler own-item exclusion', () => {
  const old = unresolved();
  const attemptedDecision = { ...decision(), expectedStatus: 'attempted' };
  const attempted = unresolved(attemptedDecision);
  const quarantinedOwn = quarantine.classifyBlockingRows([attempted], [quarantineRow(attemptedDecision)], { ownAttempt: ownAttempt(attempted) });
  assert.equal(quarantinedOwn.ownAttemptCount, 0);
  assert.equal(quarantinedOwn.blockingCount, 1);
  const uncertainOwn = quarantine.classifyBlockingRows([old], [], { ownAttempt: ownAttempt(old) });
  assert.equal(uncertainOwn.ownAttemptCount, 0);
  assert.equal(uncertainOwn.blockingCount, 1);
});

const NOW = new Date('2026-10-19T16:00:00.000Z');
function manifest() {
  return {
    version: 1, id: 'local-manifest', approvalReference: 'local-approval-only',
    tenantId: '10', channel: 'email', provider: 'brevo', transport: 'governed_brevo',
    programId: 'local-program', policyHash: 'a'.repeat(64), senderEmail: 'sender@example.test',
    inboxIntegrationId: 'local-reply-inbox', validFrom: '2026-10-19T15:30:00.000Z',
    expiresAt: '2026-10-19T16:30:00.000Z', maxFreshSends: 1,
    quarantineDecisionHash: 'b'.repeat(64),
    admissions: [{
      itemId: 'local-fresh-item', envelopeId: 'local-fresh-envelope',
      prospectId: '10000000-0000-4000-8000-000000000002',
      companyId: '20000000-0000-4000-8000-000000000002',
      candidateId: '30000000-0000-4000-8000-000000000002',
      recipientEmail: 'fresh@example.test', revision: 'local-fresh-revision',
      envelopeApprovalId: 'local-envelope-approval', manifestHash: 'c'.repeat(64),
      subject: 'Local synthetic approved subject', body: 'Local synthetic body.\nNo external send.',
    }],
  };
}

test('adversarial: valid Brevo manifest needs no invented SMTP identity and is detached/frozen', () => {
  const raw = manifest();
  const validated = recovery.validateManifest(raw, { now: NOW });
  assert.deepEqual(validated, raw);
  assert.equal(Object.hasOwn(validated, 'sendingIdentityId'), false);
  assert.equal(Object.isFrozen(validated), true);
  assert.equal(Object.isFrozen(validated.admissions), true);
  assert.equal(Object.isFrozen(validated.admissions[0]), true);
  raw.admissions[0].recipientEmail = 'changed@example.test';
  assert.equal(validated.admissions[0].recipientEmail, 'fresh@example.test');
});

test('adversarial: manifests reject missing choices, broadened scope, extra recipients and stale bounds', () => {
  const base = manifest();
  for (const key of Object.keys(base)) {
    const missing = clone(base); delete missing[key];
    assert.throws(() => recovery.validateManifest(missing, { now: NOW }), `missing ${key}`);
  }
  for (const change of [
    { version: '1' }, { tenantId: 10 }, { tenantId: '13' }, { channel: 'sms' },
    { provider: 'smtp' }, { transport: 'manual_brevo' }, { maxFreshSends: 2 },
    { maxFreshSends: 0 }, { maxFreshSends: '1' }, { approvalReference: '' },
    { sendingIdentityId: 'invented-smtp' }, { reviewRequired: false },
    { senderEmail: 'Sender@example.test' }, { senderEmail: 'sender@example.test,extra@example.test' },
    { policyHash: 'A'.repeat(64) }, { quarantineDecisionHash: '' },
    { admissions: [] }, { admissions: [...base.admissions, ...base.admissions] },
    { expiresAt: NOW.toISOString() }, { validFrom: '2026-10-19T16:00:01.000Z' },
    { validFrom: '2026-10-18T15:30:00.000Z' }, { expiresAt: '2026-10-19 16:30:00' },
  ]) {
    assert.throws(() => recovery.validateManifest({ ...base, ...change }, { now: NOW }), JSON.stringify(change));
  }
  for (const change of [
    { prospectId: '' }, { companyId: null }, { candidateId: '' }, { envelopeApprovalId: '' },
    { recipientEmail: 'fresh@example.test,extra@example.test' }, { recipientEmail: 'fresh@example.test\r\nBcc: x@y.test' },
    { subject: 'Injected\r\nheader' }, { body: '' }, { body: 'bad\u0000body' },
    { cc: ['hidden@example.test'] }, { firstTouch: false },
  ]) {
    assert.throws(() => recovery.validateManifest({ ...base, admissions: [{ ...base.admissions[0], ...change }] }, { now: NOW }), JSON.stringify(change));
  }
});

function stateDb({ present = true, rows, catalog, error } = {}) {
  const calls = [];
  const db = { async query(input) {
    const sql = typeof input === 'string' ? input : input.text;
    calls.push(sql);
    assert.match(sql, /^SELECT\b/i, 'state inspection cannot write or run DDL');
    assert.doesNotMatch(sql, /pg_advisory|FOR\s+(?:UPDATE|SHARE)|nextval/i, 'state inspection cannot take mutation locks');
    if (error) throw error;
    if (/to_regclass/.test(sql)) return catalog || { rows: [{ control_table: present ? 'anchor_email_recovery_control' : null }] };
    if (/FROM anchor_email_recovery_control/.test(sql)) return { rows: rows === undefined
      ? [{ singleton: true, maintenance_armed: true, manifest: manifest() }] : rows };
    throw new Error(`Unexpected state SQL: ${sql}`);
  } };
  return { db, calls };
}

test('adversarial: state inspection is read-only and only missing/unarmed schema preserves legacy', async () => {
  const absent = stateDb({ present: false });
  assert.equal((await recovery.loadRecoveryState(absent.db, { env: {}, now: NOW })).mode, 'legacy');
  const armedAbsent = stateDb({ present: false });
  assert.equal((await recovery.loadRecoveryState(armedAbsent.db, { env: { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' }, now: NOW })).mode, 'maintenance');
  assert.equal(absent.calls.length, 1);
  assert.equal(armedAbsent.calls.length, 1);
  const active = stateDb();
  assert.equal((await recovery.loadRecoveryState(active.db, { env: { ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: 'local-manifest' }, now: NOW })).mode, 'active');
  assert.equal(active.calls.length, 2);
});

test('adversarial: durable maintenance survives env removal, invalid controls and unavailable reads', async () => {
  const validControl = { singleton: true, maintenance_armed: true, manifest: manifest() };
  const scenarios = [
    { rows: [validControl], env: {} },
    { rows: [validControl], env: { ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: 'wrong-manifest' } },
    { rows: [] }, { rows: [validControl, validControl] },
    { rows: [{ ...validControl, maintenance_armed: false }] },
    { rows: [{ ...validControl, maintenance_armed: 'true' }] },
    { rows: [{ ...validControl, singleton: false }] },
    { rows: [{ ...validControl, manifest: null }] },
    { rows: [{ ...validControl, manifest: { ...manifest(), maxFreshSends: 2 } }] },
    { catalog: { rows: [] } }, { catalog: { rows: [{}] } },
    { error: Object.assign(new Error('unavailable'), { code: '08006' }) },
    { error: Object.assign(new Error('permission'), { code: '42501' }) },
  ];
  for (const scenario of scenarios) {
    const f = stateDb(scenario);
    const state = await recovery.loadRecoveryState(f.db, {
      env: scenario.env || { ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: 'local-manifest' }, now: NOW,
    });
    assert.equal(state.mode, 'maintenance', JSON.stringify(scenario));
    assert.equal(Object.hasOwn(state, 'manifest'), false);
  }
});

test('adversarial: quarantine inspection is SELECT-only and binds candidate separately from CRM contact', async () => {
  const d = decision();
  const item = { id: d.itemId, tenant_id: d.tenantId, envelope_id: d.envelopeId,
    prospect_id: d.prospectId, company_id: d.companyId, candidate_id: d.candidateId,
    email: d.recipientEmail, attempt_token: d.attemptToken, status: d.expectedStatus,
    reason: d.expectedReason, provider_message_id: null };
  const envelope = { id: d.envelopeId, tenant_id: '10', program_id: d.programId,
    mission_id: d.missionId, revision: d.expectedRevision };
  const canonical = { id: d.canonicalId, tenant_id: '10', mission_id: d.missionId,
    prospect_id: d.candidateId, prepared_artifact_revision: d.expectedRevision,
    payload: { email: d.recipientEmail }, status: 'attempted', provider_message_id: null, sent_at: null };
  const calls = [];
  const db = { async query(input) {
    const sql = typeof input === 'string' ? input : input.text; calls.push(sql);
    assert.match(sql, /^SELECT\b/i);
    assert.doesNotMatch(sql, /pg_advisory|FOR\s+(?:UPDATE|SHARE)|nextval/i);
    if (/FROM acquisition_outbound_items/.test(sql)) return { rows: [clone(item)] };
    if (/FROM acquisition_outbound_envelopes/.test(sql)) return { rows: [clone(envelope)] };
    if (/FROM acquisition_mission_outbound_executions/.test(sql)) return { rows: [clone(canonical)] };
    if (/to_regclass/.test(sql)) return { rows: [{ present: false }] };
    throw new Error(`Unexpected inspection SQL: ${sql}`);
  } };
  const inspected = await quarantine.inspectQuarantine(db, d);
  assert.equal(inspected.exactAttemptMatches, true);
  assert.equal(inspected.outcome, 'UNKNOWN');
  assert.equal(inspected.retryAllowed, false);
  assert.equal(inspected.applied, false);
  assert.equal(inspected.quarantinePresent, false);
  assert.equal(calls.length, 4);
  canonical.prospect_id = d.prospectId;
  assert.equal((await quarantine.inspectQuarantine(db, d)).exactAttemptMatches, false, 'CRM contact cannot substitute for canonical candidate');
  canonical.prospect_id = d.candidateId;
  canonical.prepared_artifact_revision = 'changed-revision';
  assert.equal((await quarantine.inspectQuarantine(db, d)).exactAttemptMatches, false);
});

test('adversarial: company quarantine catches duplicate aliases and rejects drift or public identities', async () => {
  const d = decision(), q = quarantineRow(d);
  const original = { id: d.companyId, name: 'Held Business LLC', domain: d.companyDomain, website: `https://${d.companyDomain}` };
  const candidate = { id: '20000000-0000-4000-8000-000000000002', name: 'Fresh Business LLC',
    domain: 'fresh-business.test', website: 'https://fresh-business.test' };
  const identity = { tenantId: '10', prospectId: '10000000-0000-4000-8000-000000000002',
    companyId: candidate.id, email: 'fresh@fresh-business.test', itemId: 'fresh-item' };
  async function attempt(changeCandidate = {}, changeOriginal = {}, changeIdentity = {}) {
    const db = { async query(sql) {
      if (/FROM anchor_email_quarantines/.test(sql)) return { rows: [q] };
      if (/FROM companies c/.test(sql)) return { rows: [{ ...original, ...changeOriginal }, { ...candidate, ...changeCandidate }] };
      throw new Error(`Unexpected alias SQL: ${sql}`);
    } };
    return quarantine.assertNotQuarantined(db, { ...identity, ...changeIdentity });
  }
  assert.equal(await attempt(), true);
  for (const change of [
    { name: 'Held Business, Inc.' },
    { domain: d.companyDomain, website: `https://www.${d.companyDomain}` },
    { domain: 'gmail.com', website: 'https://gmail.com' },
    { domain: 'fresh-business.test', website: 'https://contradictory.test' },
    { name: null }, { domain: null, website: null },
  ]) await assert.rejects(attempt(change), JSON.stringify(change));
  await assert.rejects(attempt({}, { domain: 'changed-held.test', website: 'https://changed-held.test' }));
  await assert.rejects(attempt({}, {}, { email: d.recipientEmail }));
  await assert.rejects(attempt({}, {}, { prospectId: d.prospectId }));
  await assert.rejects(attempt({}, {}, { companyId: d.companyId }));
});

test('adversarial: imports and default/help preflight cannot load a pool or provider', () => {
  const { execFileSync } = require('node:child_process');
  const path = require('node:path');
  const script = `
    const assert = require('node:assert/strict');
    const Module = require('node:module');
    const original = Module._load;
    Module._load = function(name, ...args) {
      if (['pg','axios','nodemailer','dotenv'].includes(name) || /(?:^|\\/)db(?:\\.js)?$/.test(name)) {
        throw new Error('Forbidden import: ' + name);
      }
      return original.call(this, name, ...args);
    };
    require('./services/anchorEmailRecovery');
    require('./services/anchorEmailQuarantine');
    require('./services/anchorEmailRecoveryIntegration');
    require('./services/anchorEmailRecoveryPreview');
    require('./services/anchorEmailRecoveryPrepareExisting');
    require('./services/anchorEmailRecoveryApproval');
    const preflight = require('./scripts/anchorEmailRecoveryPreflight');
    (async () => {
      assert.equal((await preflight.main([])).exitCode, 2);
      assert.equal((await preflight.main(['--help'])).exitCode, 0);
      assert.equal((await preflight.main(['--apply'])).exitCode, 2);
      assert.equal((await preflight.main(['--test-database'], { ANCHOR_RECOVERY_TEST_DATABASE_URL:'postgresql://production.invalid:5432/db' })).exitCode, 2);
      const report = await preflight.inspectPreflight({query: async sql => {
        assert.match(sql, /^SELECT/); assert.doesNotMatch(sql, /pg_advisory|FOR UPDATE|FOR SHARE/);
        assert.match(sql, /information_schema.columns|pg_catalog\.pg_trigger|pg_catalog\.pg_constraint/);
        return {rows:[]};
      }});
      assert.equal(report.activationAllowed, false); assert.equal(report.status, 'blocked');
    })().catch(error => { console.error(error); process.exitCode=1; });
  `;
  execFileSync(process.execPath, ['-e', script], {
    cwd: path.join(__dirname, '..'), env: { PATH: process.env.PATH }, timeout: 10000,
    stdio: 'pipe',
  });
});

function leafFixture() { return require('./helpers/anchorRecoveryManifestFixture').recoveryFixture(); }
function leaf(f) { return recovery.assertRecoveryLeaf(f.db, f.input, { now: f.now, env: f.env }); }

test('adversarial: exact governed Brevo leaf binds real payload and retains database token precision', async () => {
  const f = leafFixture();
  const result = await leaf(f);
  assert.equal(result.legacy, false);
  assert.equal(result.attemptToken, '2026-10-19 16:00:00.123456+00');
  assert.equal(result.item.id, f.item.id);
  assert.equal(result.prospect.id, f.prospect.id);
  const crmQuery = f.calls.find(call => /FROM prospects p/.test(call.sql));
  assert.match(crmQuery.sql, /p\.ao_outreach_review_required/);
  assert.doesNotMatch(crmQuery.sql, /ao_call_suppressed/);
  assert.equal(f.calls.some(call => /tenant_sending_identities/.test(call.sql)), false, 'Brevo does not invent SMTP identity');
  assert.ok(f.calls.every(call => /^SELECT\b/i.test(call.sql)), 'leaf validation does not consume or mutate in its rollback-prone transaction');
});

test('adversarial: good metadata cannot authorize different recipient, sender, copy, hidden recipients or transport', async () => {
  const cases = [
    ['actual recipient', f => { f.input.command.toEmail = f.oldDecision.recipientEmail; }],
    ['recipient list', f => { f.input.command.toEmail += ',other@example.test'; }],
    ['subject', f => { f.input.command.subject += ' altered'; }],
    ['body', f => { f.input.command.body += ' altered'; }],
    ['sender email', f => { f.input.command.sender.email = 'other@example.test'; }],
    ['sender name', f => { f.input.command.sender.name = 'Different Sender'; }],
    ['cc', f => { f.input.command.cc = ['hidden@example.test']; }],
    ['bcc', f => { f.input.command.bcc = ['hidden@example.test']; }],
    ['to', f => { f.input.command.to = ['hidden@example.test']; }],
    ['toEmails', f => { f.input.command.toEmails = ['hidden@example.test']; }],
    ['recipients', f => { f.input.command.recipients = ['hidden@example.test']; }],
    ['SMS', f => { f.input.channel = 'sms'; }],
    ['call', f => { f.input.channel = 'call'; }],
    ['other tenant', f => { f.input.identities[0].clientId = 13; }],
    ['identity substitution', f => { f.input.identities[0].prospectId = f.oldDecision.prospectId; }],
    ['manual context', f => { delete f.input.context; }],
    ['SMTP provider', f => { f.input.context.provider = 'smtp'; }],
    ['manual route', f => { f.input.context.transport = 'manual_brevo'; }],
    ['second CRM identity', f => { f.input.identities.push({ prospectId: 'extra', clientId: 10 }); }],
    ['idempotency key', f => { f.input.command.idempotencyKey = 'different'; }],
  ];
  for (const [label, change] of cases) {
    const f = leafFixture(); change(f);
    await assert.rejects(leaf(f), label);
  }
});

test('adversarial: imports/relinked provenance cannot clear mandatory review or other safety holds', async () => {
  for (const value of [true, null, undefined, 'false', 0]) {
    const f = leafFixture();
    f.prospect.ao_outreach_review_required = value;
    f.prospect.source = 'mission_bound_admission';
    f.prospect.enrichment_provenance.mission_bound_admission = { mission_id: f.envelope.mission_id, source: 'mission_bound_discovery' };
    await assert.rejects(leaf(f), { code: 'outreach_review_required' });
  }
  const cases = [
    f => { f.prospect.do_not_contact = true; },
    f => { f.prospect.is_synthetic = true; },
    f => { f.prospect.email_verified = false; },
    f => { f.prospect.email = 'different@example.test'; },
    f => { f.prospect.company_id = f.oldDecision.companyId; },
    f => { f.program.mode = 'paused'; },
    f => { f.approval.payload.payload.invalidated = true; },
    f => { f.client.sender_name = 'Changed Sender'; },
    f => { f.integration.status = 'inactive'; },
    f => { f.env = {}; },
    f => { f.fail = /FROM prospects p/; },
  ];
  for (const change of cases) { const f = leafFixture(); change(f); await assert.rejects(leaf(f)); }
});

test('adversarial: replay, microsecond mismatch, second unknown and missing attempt all stop the leaf', async () => {
  const cases = [
    f => { f.reservations[0].dispatch_started_at = f.now.toISOString(); },
    f => { f.reservations[0].attempt_token = '2026-10-19 16:00:00.123457+00'; },
    f => { f.reservations[0].attempt_token = '2026-10-19 16:00:00.123+00'; },
    f => { f.item.attempt_token = null; },
    f => { f.item.status = 'uncertain'; },
    f => { f.reservations = []; },
    f => { f.reservations.push(clone(f.reservations[0])); },
    f => { f.unresolved.push({ ...f.unresolved[1], item_id: 'second-unknown', canonical_id: 'second-canonical', attempt_token: null }); },
    f => { f.quarantine = null; },
    f => { f.quarantine.attempt_token = '2026-10-16 17:08:04.859124+00'; },
  ];
  for (const change of cases) { const f = leafFixture(); change(f); await assert.rejects(leaf(f)); }
});

test('adversarial: dispatch reservation uses exact independent autocommit CAS and cannot replay a lost response', async () => {
  const { consumeRecoveryDispatch } = require('../services/anchorEmailRecoveryIntegration');
  const authorization = { legacy: false, manifest: { id: 'manifest' }, item: { id: 'item' },
    attemptToken: '2026-10-19 16:00:00.123456+00' };
  let consumed = false, loseResponse = true, connections = 0;
  const releases = [];
  const pool = { async connect() { connections++; return {
    async query(config) {
      assert.match(config.text, /^UPDATE anchor_email_recovery_reservations/);
      assert.match(config.text, /attempt_token=\$3 AND dispatch_started_at IS NULL RETURNING item_id/);
      assert.equal(config.query_timeout, 6000);
      assert.deepEqual(config.values, ['manifest', 'item', authorization.attemptToken]);
      if (consumed) return { rows: [] };
      consumed = true;
      if (loseResponse) { loseResponse = false; throw Object.assign(new Error('Synthetic lost response after durable CAS'), { code: '08006' }); }
      return { rows: [{ item_id: 'item' }] };
    }, release(discard) { releases.push(discard); },
  }; } };
  await assert.rejects(consumeRecoveryDispatch(pool, authorization), { code: '08006' });
  assert.equal(consumed, true);
  await assert.rejects(consumeRecoveryDispatch(pool, authorization), { code: 'anchor_recovery_dispatch_already_consumed' });
  assert.equal(connections, 2);
  assert.deepEqual(releases, [true, true]);
});

test('adversarial: expiry or deactivation during dispatch CAS cannot hand off or refund the attempt', async () => {
  const { consumeRecoveryDispatch } = require('../services/anchorEmailRecoveryIntegration');
  for (const change of ['unchanged', 'expires', 'deactivated', 'program-window']) {
    const f = leafFixture();
    const authorization = await leaf(f);
    assert.equal(authorization.program.id, f.program.id);
    let current = f.now, consumed = false, dispatched = 0;
    const pool = { async connect() { return {
      async query(config) {
        assert.match(config.text, /^UPDATE anchor_email_recovery_reservations/);
        if (consumed) return { rows: [] };
        consumed = true;
        if (change === 'expires') current = new Date(f.manifest.expiresAt);
        if (change === 'deactivated') delete f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID;
        if (change === 'program-window') authorization.program.policy.expiresAt = f.now.toISOString();
        return { rows: [{ item_id: f.item.id }] };
      }, release() {},
    }; } };
    const handoff = async () => {
      await consumeRecoveryDispatch(pool, authorization, { now: () => current, env: f.env });
      dispatched++;
    };
    if (change === 'unchanged') await handoff();
    else await assert.rejects(handoff(), undefined, change);
    assert.equal(consumed, true, change);
    assert.equal(dispatched, change === 'unchanged' ? 1 : 0, change);
    // Even reverting the clock/activation cannot refund the committed CAS.
    current = f.now; f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID = f.manifest.id;
    await assert.rejects(handoff(), { code: 'anchor_recovery_dispatch_already_consumed' });
    assert.equal(dispatched, change === 'unchanged' ? 1 : 0, change);
  }
});

test('adversarial: quarantine subtraction grants no other Anchor program authority and leaves other tenants unchanged', async () => {
  const { uncertaintyState } = require('../services/anchorEmailRecoveryIntegration');
  for (const [tenantId, programId, mayProceed] of [
    ['10', 'local-program', true], ['10', 'unapproved-program', false], ['10', undefined, false],
    ['13', 'local-program', false],
  ]) {
    const f = leafFixture(); f.unresolved = f.unresolved.slice(0, 1);
    const state = await uncertaintyState(f.db, tenantId, { now: f.now, env: f.env, programId });
    if (tenantId !== '10') { assert.equal(state, null); assert.equal(f.calls.length, 0); continue; }
    assert.equal(state.uncertain, 1);
    assert.equal(state.blocking_uncertain === 0, mayProceed, `${tenantId}/${programId}`);
    assert.equal(state.quarantined_uncertain, mayProceed ? 1 : 0);
  }
});

test('adversarial: replacement manifests cannot replay an item and new uncertainty restores the Anchor stop', async () => {
  const { uncertaintyState } = require('../services/anchorEmailRecoveryIntegration');
  for (const consumed of [false, true]) {
    const f = leafFixture();
    if (consumed) f.reservations[0].dispatch_started_at = f.now.toISOString();
    f.manifest.id = 'local-replacement-manifest';
    f.manifest.expiresAt = '2026-10-19T16:45:00.000Z';
    f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID = f.manifest.id;
    await assert.rejects(leaf(f), { code: 'anchor_recovery_reservation_invalid' });
  }
  const f = leafFixture();
  f.item.status = 'uncertain'; f.unresolved[1].status = 'uncertain';
  const state = await uncertaintyState(f.db, '10', { now: f.now, env: f.env, programId: f.program.id,
    ownAttempt: { tenantId: '10', programId: f.program.id, envelopeId: f.envelope.id,
      itemId: f.item.id, attemptToken: f.item.attempt_token } });
  assert.equal(state.uncertain, 2);
  assert.equal(state.quarantined_uncertain, 1);
  assert.equal(state.own_attempt_uncertain, 0);
  assert.equal(state.blocking_uncertain, 1);
});

test('adversarial: maintenance closes ordinary preparation before adapter, reservation, or research work', async () => {
  const { service } = require('../services/governedOutbound');
  for (const operation of ['initializePreparation', 'resumeReservedPreparation', 'replenish', 'runPreparationRefill']) {
    const f = leafFixture(); f.control.manifest = null;
    let adaptersCalled = 0, connections = 0;
    const pool = { query: f.db.query, async connect() { connections++; throw new Error('Unexpected mutation connection'); } };
    const adapters = new Proxy({}, { get() { return async () => { adaptersCalled++; throw new Error('Unexpected adapter work'); }; } });
    const svc = service({ pool, tenantId: '10', adapters, now: () => f.now, enabled: () => false });
    svc.store.lock = async fn => fn();
    svc.store.program = async () => f.program;
    svc.store.event = async () => {};
    if (operation === 'runPreparationRefill') {
      const result = await svc[operation]();
      assert.equal(result.preparedAdded, 0);
      assert.equal(result.prepareSkippedReason, 'anchor_recovery_separate_preparation_approval_required');
    } else await assert.rejects(svc[operation]({}, { id: 'operator', role: 'admin' }),
      { code: 'anchor_recovery_separate_preparation_approval_required' });
    assert.equal(adaptersCalled, 0, operation);
    assert.equal(connections, 0, operation);
  }
});

test('adversarial: real governed Brevo serialization sends only the exact approved payload and managed route', async () => {
  const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
  const integration = require('../services/anchorEmailRecoveryIntegration');
  const load = (file, requireFn, env = {}) => {
    const module = { exports: {} };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '..', file), 'utf8'), {
      module, exports: module.exports, process: { env }, require: requireFn,
    }, { filename: file });
    return module.exports;
  };
  for (const change of ['none', 'recipient', 'copy', 'sender', 'cc', 'bcc', 'apiKey']) {
    const f = leafFixture(); let consumed = false;
    const requests = [];
    const pool = { async connect() { return {
      async query(config) {
        if (/^(?:BEGIN|COMMIT|ROLLBACK)|set_config/.test(config.text)) return { rows: [] };
        if (/^UPDATE anchor_email_recovery_reservations/.test(config.text)) {
          if (consumed) return { rows: [] };
          consumed = true; return { rows: [{ item_id: f.item.id }] };
        }
        return f.db.query(config);
      }, release() {},
    }; } };
    const guard = load('utils/callEligibility.js', id => {
      if (id === '../services/anchorEmailRecovery') return {
        loadRecoveryState: db => recovery.loadRecoveryState(db, { now: f.now, env: f.env }),
        assertRecoveryLeaf: (db, input) => recovery.assertRecoveryLeaf(db, input, { now: f.now, env: f.env }),
      };
      if (id === '../services/anchorEmailRecoveryIntegration') return {
        consumeRecoveryDispatch: (db, auth) => integration.consumeRecoveryDispatch(db, auth, { now: () => f.now, env: f.env }),
      };
      if (id === '../services/anchorScopedResume') return require('../services/anchorScopedResume');
      throw new Error(`Unexpected guard dependency ${id}`);
    });
    const provider = load('packages/providers/brevo/sendEmail.js', id => {
      if (id === '../../../utils/callEligibility') return guard;
      if (id === 'axios') return { async post(url, payload) { requests.push({ url, payload: clone(payload) }); return { data: { messageId: 'synthetic-provider-receipt' } }; } };
      throw new Error(`Unexpected provider dependency ${id}`);
    }, { BREVO_API_KEY: 'synthetic-test-only-managed-key' });
    const adapters = load('services/governedOutboundAdapters.js', id => id === '../packages/providers/brevo/sendEmail' ? provider : {});
    const send = adapters.adapters(pool, { governedContext: {
      tenantId: '10', clientId: 10, transport: { usesBrevoTransport: true },
    } }).sendFor(f.program, { envelope: f.envelope, item: f.item });
    const input = clone(f.input.command);
    if (change === 'recipient') input.toEmail = 'unapproved@example.test';
    if (change === 'copy') input.body += 'Unapproved.';
    if (change === 'sender') input.sender.name = 'Unapproved Sender';
    if (change === 'cc' || change === 'bcc') input[change] = 'unapproved@example.test';
    if (change === 'apiKey') input.apiKey = 'synthetic-test-only-override';
    const result = await send(input);
    assert.equal(result.success, change === 'none', `${change}: ${result.providerErrorCode}`);
    assert.equal(requests.length, change === 'none' ? 1 : 0, change);
    assert.equal(consumed, change === 'none', change);
    if (change === 'none') {
      const payload = requests[0].payload;
      assert.equal(requests[0].url, 'https://api.brevo.com/v3/smtp/email');
      assert.deepEqual(payload.to, [{ email: f.item.email, name: '' }]);
      assert.deepEqual(payload.sender, f.input.command.sender);
      assert.equal(payload.subject, f.manifest.admissions[0].subject);
      assert.equal(payload.textContent, f.manifest.admissions[0].body);
      assert.equal(payload.cc, undefined); assert.equal(payload.bcc, undefined);
    }
  }
});

test('adversarial: existing-artifact preparation requires its own explicit narrow authority before connecting', async () => {
  const prepare = require('../services/anchorEmailRecoveryPrepareExisting');
  const fixture = require('./helpers/anchorRecoveryPrepareExistingFixture').prepareExistingFixture;
  const changes = [
    f => { f.env = {}; }, f => { f.env.ANCHOR_EMAIL_RECOVERY_PREPARE_EXISTING_ID = 'wrong'; },
    f => { f.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE = 'false'; },
    f => { f.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'true'; },
    f => { f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID = 'send'; },
    f => { f.config.maxItems = 2; }, f => { f.config.tenantId = '13'; },
    f => { f.config.operatorRole = 'viewer'; }, f => { f.config.testMode = true; },
    f => { delete f.config.approvalReference; }, f => { f.config.allowPaidResearch = true; },
    f => { f.config.candidate.bcc = 'extra@example.test'; },
  ];
  for (const mutate of changes) {
    const f = fixture(); mutate(f); let connected = false;
    await assert.rejects(prepare.prepareExistingApproved({ async connect() { connected = true; throw new Error('Must reject before connecting'); } }, f.config, { now: f.now, env: f.env }));
    assert.equal(connected, false);
  }
});

test('adversarial: exact current artifacts freeze one distinct-candidate item without execution or send authority', async () => {
  const prepare = require('../services/anchorEmailRecoveryPrepareExisting');
  const f = require('./helpers/anchorRecoveryPrepareExistingFixture').prepareExistingFixture();
  assert.notEqual(f.config.candidate.candidateId, f.config.candidate.prospectId);
  const options = { now: f.now, env: f.env };
  const report = await prepare.reviewExistingPreparation(f.pool, f.config, options);
  assert.equal(report.ready, true, JSON.stringify(report));
  assert.equal(f.inserts.length, 0); assert.equal(report.providerAuthorized, false);
  assert.throws(() => recovery.validateManifest(report.proposedManifest, options));
  assert.equal(f.calls.some(q => /FOR (?:UPDATE|SHARE)|pg_advisory|INSERT|DELETE|ALTER|CREATE/.test(q.sql)), false);
  const frozen = await prepare.prepareExistingApproved(f.pool, f.config, options);
  assert.equal(frozen.frozen, true); assert.equal(frozen.providerAuthorized, false);
  assert.equal(f.committed.length, 3);
  assert.equal(f.control.manifest, null); assert.equal(f.control.maintenance_armed, true);
  assert.equal(frozen.proposedManifest.admissions[0].envelopeApprovalId, null);
  assert.throws(() => recovery.validateManifest(frozen.proposedManifest, options));
  assert.equal(f.committed.some(q => /UPDATE|DELETE|acquisition_mission_contributions|anchor_email_recovery_control/.test(q.sql)), false);
});

test('adversarial: stale artifacts, revoked operator, held contact or new uncertainty cannot write preparation', async () => {
  const prepare = require('../services/anchorEmailRecoveryPrepareExisting');
  const fixture = require('./helpers/anchorRecoveryPrepareExistingFixture').prepareExistingFixture;
  for (const mutate of [
    f => { f.operator.active = false; }, f => { f.operator.role = 'viewer'; }, f => { f.operator.client_id = 13; },
    f => { f.program.authorized_by = 'different-operator'; },
    f => { f.missionRow.updated_at = '2026-10-16T15:00:00.000Z'; },
    f => { f.config.candidate.body += ' changed'; },
    f => { f.prospect.ao_outreach_review_required = true; }, f => { f.prospect.ao_outreach_review_required = null; },
    f => { f.control.manifest = f.manifest; },
    f => { f.unresolved.push({ ...f.unresolved[0], item_id: 'new-unknown', attempt_token: null }); },
  ]) {
    const f = fixture(); mutate(f);
    await assert.rejects(prepare.prepareExistingApproved(f.pool, f.config, { now: f.now, env: f.env }));
    assert.equal(f.inserts.length, 0); assert.equal(f.committed.length, 0);
  }
});
