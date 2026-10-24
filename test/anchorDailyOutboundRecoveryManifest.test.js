'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const recovery = require('../services/anchorEmailRecovery');
const { recoveryFixture, clone } = require('./helpers/anchorRecoveryManifestFixture');
const options = f => ({ now: f.now, env: f.env });
const leaf = f => recovery.assertRecoveryLeaf(f.db, f.input, options(f));

function updatePolicy(f, patch) {
  Object.assign(f.program.policy, patch);
  f.program.policy_hash = hash(f.program.policy);
  f.manifest.policyHash = f.program.policy_hash;
  f.approval.payload.payload.dailyEnvelope.policyHash = f.program.policy_hash;
}

function rebindSnapshot(f) {
  f.envelope.manifest = [clone(f.item.snapshot)];
  f.envelope.manifest_hash = hash(f.envelope.manifest);
  f.manifest.admissions[0].manifestHash = f.envelope.manifest_hash;
  f.approval.payload.payload.dailyEnvelope.manifestHash = f.envelope.manifest_hash;
}

test('recovery module import has no database/provider/credential bootstrap', () => {
  const result = spawnSync(process.execPath, ['-e', `
    const Module = require('node:module'); const original = Module._load;
    Module._load = function(id, parent, ...args) {
      if (id === 'dotenv' || id === 'pg' || id === 'axios' || /(?:^|\\/)db$/.test(id)) throw new Error('Forbidden bootstrap: '+id);
      return original.call(this, id, parent, ...args);
    };
    require('./services/anchorEmailRecovery');
  `], { cwd: require('node:path').resolve(__dirname, '..'), encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, '');
});

test('manifest requires exact explicit keys, types, scope, hashes and one fresh admission', () => {
  const f = recoveryFixture();
  const manifest = recovery.validateManifest(f.manifest, options(f));
  assert.deepEqual(manifest, f.manifest);
  assert.notEqual(manifest, f.manifest);
  assert.equal(Object.isFrozen(manifest.admissions[0]), true);
  for (const key of Object.keys(f.manifest)) {
    const input = clone(f.manifest); delete input[key];
    assert.throws(() => recovery.validateManifest(input, options(f)), { code: 'anchor_recovery_invalid_manifest' }, key);
  }
  for (const change of [{ tenantId: 10 }, { tenantId: '13' }, { channel: 'sms' }, { provider: 'smtp' },
    { transport: 'manual' }, { version: '1' }, { maxFreshSends: 2 }, { maxFreshSends: '1' },
    { admissions: [] }, { admissions: [f.manifest.admissions[0], f.manifest.admissions[0]] },
    { approvalReference: '' }, { senderEmail: null }, { senderEmail: 'Sender@synthetic-recovery.test' },
    { policyHash: '0'.repeat(63) }, { allowUnreviewed: true }]) {
    assert.throws(() => recovery.validateManifest({ ...f.manifest, ...change }, options(f)), JSON.stringify(change));
  }
  for (const key of Object.keys(f.manifest.admissions[0])) {
    const input = clone(f.manifest); delete input.admissions[0][key];
    assert.throws(() => recovery.validateManifest(input, options(f)), key);
  }
  for (const recipientEmail of [null, '', ['fresh@synthetic-recovery.test'], 'fresh@synthetic-recovery.test,other@synthetic-recovery.test',
    'Fresh <fresh@synthetic-recovery.test>', 'fresh@synthetic-recovery.test\r\nBcc: evil@synthetic-recovery.test']) {
    const input = clone(f.manifest); input.admissions[0].recipientEmail = recipientEmail;
    assert.throws(() => recovery.validateManifest(input, options(f)));
  }
});

test('manifest time window rejects expired, future, unbounded and invalid calendar/times', () => {
  const f = recoveryFixture();
  for (const change of [
    { validFrom: '2026-10-19T17:00:00Z' }, { expiresAt: '2026-10-19T16:00:00Z' },
    { validFrom: '2026-10-18T00:00:00Z', expiresAt: '2026-10-20T00:00:00Z' },
    { validFrom: '2026-10-19 15:00:00' }, { validFrom: 1792412400000 },
    { validFrom: '2026-02-30T15:00:00Z' }, { validFrom: '2026-10-18T24:00:00Z' },
  ]) assert.throws(() => recovery.validateManifest({ ...f.manifest, ...change }, options(f)));
  assert.throws(() => recovery.validateManifest(f.manifest, { now: new Date('invalid') }));
  const atStart = recovery.validateManifest(f.manifest, { now: new Date(f.manifest.validFrom) });
  assert.equal(atStart.id, f.manifest.id);
});

test('table absent and environment unarmed is the only legacy state', async () => {
  const f = recoveryFixture(); f.catalog.control_table = null;
  for (const env of [{}, { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'false' }]) {
    assert.deepEqual(await recovery.loadRecoveryState(f.db, { ...options(f), env }), { mode: 'legacy' });
  }
  assert.equal(f.calls.some(call => /FROM anchor_email_recovery_control/.test(call.sql)), false);
  for (const env of [
    { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'yes' }, { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: '1' },
    { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'garbage' }, { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: false },
    { ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: 'configured-without-schema' },
  ]) assert.equal((await recovery.loadRecoveryState(f.db, { ...options(f), env })).mode, 'maintenance');
  assert.equal((await recovery.loadRecoveryState(f.db, { ...options(f), env: { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' } })).mode, 'maintenance');
});

test('durable maintenance survives env removal and missing, malformed or mismatched grants', async () => {
  const variations = [f => { f.control.manifest = null; }, f => { f.control.manifest = {}; },
    f => { f.control.maintenance_armed = false; }, f => { f.control.singleton = false; },
    f => { f.control = null; }, f => { f.env = {}; }, f => { f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID = 'other'; },
    f => { f.now = new Date(f.manifest.expiresAt); }, f => { f.catalog = {}; }];
  for (const mutate of variations) {
    const f = recoveryFixture(); mutate(f);
    const state = await recovery.loadRecoveryState(f.db, options(f));
    assert.equal(state.mode, 'maintenance');
    assert.equal(Object.hasOwn(state, 'manifest'), false);
    await assert.rejects(leaf(f), { code: 'anchor_recovery_maintenance' });
  }
});

test('catalog and control read errors fail closed; state reads have no writes or locks', async () => {
  for (const fail of [/to_regclass/, /FROM anchor_email_recovery_control/]) {
    const f = recoveryFixture(); f.fail = fail;
    assert.equal((await recovery.loadRecoveryState(f.db, options(f))).mode, 'maintenance');
  }
  const f = recoveryFixture();
  assert.equal((await recovery.loadRecoveryState(f.db, options(f))).mode, 'active');
  assert.ok(f.calls.every(call => /^SELECT/.test(call.sql) && !/FOR SHARE|advisory/.test(call.sql)));
});

test('leaf verifies real bindings and retains scoped read locks through caller transaction', async () => {
  const f = recoveryFixture(); const result = await leaf(f);
  assert.equal(result.legacy, false);
  assert.equal(result.prospect.id, f.prospect.id);
  assert.equal(result.item.id, f.item.id);
  assert.equal(result.attemptToken, '2026-10-19 16:00:00.123456+00');
  assert.match(f.calls[0].sql, /pg_advisory_xact_lock_shared/);
  assert.deepEqual(f.calls[0].values, [recovery.RECOVERY_LOCK_KEY]);
  assert.match(f.calls.find(call => /FROM anchor_email_recovery_control/.test(call.sql)).sql, /FOR SHARE/);
  for (const table of ['acquisition_outbound_programs', 'acquisition_outbound_envelopes', 'acquisition_outbound_items', 'prospects p']) {
    assert.match(f.calls.find(call => call.sql.includes(`FROM ${table}`) && !call.sql.includes('AS item_id')).sql, /FOR SHARE/);
  }
  const prospectSql = f.calls.find(call => /FROM prospects p/.test(call.sql)).sql;
  assert.match(prospectSql, /p\.ao_outreach_review_required/);
  assert.doesNotMatch(prospectSql, /ao_call_suppressed/);
  assert.match(f.calls.find(call => /AS attempt_token FROM acquisition_outbound_items/.test(call.sql)).sql, /AT TIME ZONE 'UTC'/);
  assert.doesNotMatch(f.calls.find(call => /FROM anchor_email_recovery_reservations/.test(call.sql)).sql, /FOR SHARE|FOR UPDATE/);
  assert.ok(f.calls.every(call => /^SELECT/.test(call.sql)), 'authorization itself never consumes or refunds a dispatch');
});

test('active recovery rejects SMS/manual/non10/multi-identity/contextless routes', async () => {
  for (const mutate of [f => { f.input.channel = 'sms'; }, f => { f.input.channel = 'call'; },
    f => { f.input.identities[0].clientId = 13; }, f => { f.input.identities.push(f.input.identities[0]); },
    f => { f.input.identities = []; }, f => { f.input.context = null; },
    f => { delete f.input.context.provider; }, f => { f.input.context.transport = 'smtp'; },
    f => { f.input.context.itemId = 'unapproved'; }]) {
    const f = recoveryFixture(); mutate(f); await assert.rejects(leaf(f));
  }
});

test('actual recipient, copy, sender, sender name and idempotency are independently bound', async () => {
  for (const mutate of [f => { f.input.command.toEmail = 'substituted@synthetic-recovery.test'; },
    f => { f.input.command.toEmail = [f.item.email]; }, f => { f.input.command.cc = 'other@synthetic-recovery.test'; },
    f => { f.input.command.subject += '!'; }, f => { f.input.command.body += '!'; },
    f => { f.input.command.sender.email = 'other@synthetic-recovery.test'; }, f => { f.input.command.sender.name = 'Other'; },
    f => { f.input.command.idempotencyKey = 'arbitrary-tag'; }, f => { f.input.identities[0].prospectId = 'substituted'; }]) {
    const f = recoveryFixture(); mutate(f); await assert.rejects(leaf(f));
  }
  const f = recoveryFixture(); f.input.command.toEmail = ` ${f.item.email.toUpperCase()} `;
  assert.equal((await leaf(f)).legacy, false, 'recipient comparison uses one normalized email');
});

test('missing/null/string review, DNC and synthetic flags never imply permission', async () => {
  for (const field of ['ao_outreach_review_required', 'do_not_contact', 'is_synthetic']) {
    for (const value of [undefined, null, true, 'false', 0, {}, []]) {
      const f = recoveryFixture(); f.prospect[field] = value; await assert.rejects(leaf(f), field + ':' + value);
    }
  }
  const f = recoveryFixture(); f.fail = /FROM prospects p/;
  await assert.rejects(leaf(f), { code: '42703' });
});

test('current CRM relink/email change and spreadsheet-held rows fail independent of frozen copy', async () => {
  for (const mutate of [f => { f.prospect.email = 'new@synthetic-recovery.test'; },
    f => { f.prospect.company_id = 'different-company'; }, f => { f.prospect.id = 'different-contact'; },
    f => { f.prospect.client_id = 13; }, f => { f.prospect.ao_outreach_review_required = true; },
    f => { f.prospect.email_verified = false; }, f => { f.prospect.enrichment_provenance = {}; },
    f => { f.prospect = null; }]) {
    const f = recoveryFixture(); mutate(f); await assert.rejects(leaf(f));
  }
});

test('live policy must match known first-touch bounds and frozen approval hash', async () => {
  for (const patch of [{ maxSequenceStep: 2 }, { dailyCap: '5' }, { dailyCap: 0 }, { totalCap: 101 },
    { spacingMinutes: 0 }, { weekdays: [] }, { timeZone: 'UTC' }, { preparationOnly: true },
    { admissionPolicy: 'auto-approve' }, { startsAt: 'invalid' }, { expiresAt: '2026-10-19T16:01:00Z' }]) {
    const f = recoveryFixture(); updatePolicy(f, patch); await assert.rejects(leaf(f), JSON.stringify(patch));
  }
  for (const mutate of [f => { f.program.mode = 'shadow'; }, f => { f.program.policy_hash = 'a'.repeat(64); },
    f => { f.program.tenant_id = '13'; }, f => { f.program.policy.senderEmail = 'other@synthetic-recovery.test'; }]) {
    const f = recoveryFixture(); mutate(f); await assert.rejects(leaf(f));
  }
});

test('live envelope, item, approval and frozen snapshot cannot drift', async () => {
  for (const mutate of [f => { f.envelope.status = 'cancelled'; }, f => { f.envelope.revision = 'new'; },
    f => { f.envelope.local_day = '2026-10-18'; }, f => { f.envelope.approval_id = 'other'; },
    f => { f.envelope.manifest_hash = '0'.repeat(64); }, f => { f.item.snapshot.message.body += '!'; },
    f => { f.item.snapshot.message.subject += '!'; rebindSnapshot(f); },
    f => { f.item.email = 'other@synthetic-recovery.test'; }, f => { f.item.prospect_id = 'other'; },
    f => { f.item.status = 'pending'; }, f => { f.item.attempt_token = null; },
    f => { f.item.provider_message_id = 'already-accepted'; }, f => { f.approval = null; },
    f => { f.approval.payload.payload.approved = false; }, f => { f.approval.payload.payload.invalidated = true; },
    f => { f.approval.payload.payload.preparedArtifactRevision = 'other'; },
    f => { f.approval.payload.payload.dailyEnvelope.candidateIds = ['other']; }]) {
    const f = recoveryFixture(); mutate(f); await assert.rejects(leaf(f));
  }
});

test('canonical sender or inbox changes do not use inferred SMTP identities', async () => {
  for (const mutate of [f => { f.client.sender_email = 'other@synthetic-recovery.test'; },
    f => { f.client.sender_name = 'Other'; }, f => { f.client.sending_domain = 'different.test'; },
    f => { f.client.active = false; }, f => { f.client.autosend_enabled = true; },
    f => { f.integration.mailbox_address = 'other@synthetic-recovery.test'; },
    f => { f.integration.status = 'disabled'; }, f => { f.integration.tenant_id = '13'; }]) {
    const f = recoveryFixture(); mutate(f); await assert.rejects(leaf(f));
  }
  const f = recoveryFixture(); await leaf(f);
  assert.ok(f.calls.every(call => !/tenant_sending_identities/.test(call.sql)));
});

test('quarantine presence, identity exclusions and unresolved counts are checked again at leaf', async () => {
  for (const mutate of [f => { f.quarantine = null; }, f => { f.quarantine.decision_hash = '0'.repeat(64); },
    f => { f.quarantineHits = [{ id: 'held-contact-or-alias' }]; },
    f => { f.unresolved.push({ ...f.unresolved[1], item_id: 'second-unknown' }); },
    f => { f.unresolved.push({ ...f.unresolved[1], item_id: 'null-token', attempt_token: null }); },
    f => { f.unresolved.pop(); }, f => { f.unresolved[1].attempt_token = '2026-10-19 16:00:00.123457+00'; }]) {
    const f = recoveryFixture(); mutate(f); await assert.rejects(leaf(f));
  }
});

test('reservation is exact once-only admission; malformed/consumed/wrong token rows reject', async () => {
  for (const mutate of [f => { f.reservations = []; }, f => { f.reservations.push(clone(f.reservations[0])); },
    f => { f.reservations[0].manifest_id = 'other'; }, f => { f.reservations[0].item_id = 'other'; },
    f => { f.reservations[0].attempt_token = '2026-10-19 16:00:00.123+00'; },
    f => { f.reservations[0].prospect_id = 'other'; }, f => { f.reservations[0].company_id = 'other'; },
    f => { f.reservations[0].recipient_email = 'other@synthetic-recovery.test'; },
    f => { f.reservations[0].used_at = null; }, f => { f.reservations[0].dispatch_started_at = f.now; },
    f => { delete f.reservations[0].dispatch_started_at; }]) {
    const f = recoveryFixture(); mutate(f); await assert.rejects(leaf(f));
  }
});

test('claim admission permits only pending never-consumed durable item and does not write', async () => {
  const f = recoveryFixture(); f.item.status = 'pending'; f.item.attempt_token = null; f.item.attempted_at = null; f.reservations = [];
  const args = { program: f.program, item: f.item, envelope: f.envelope, stage: 'claim' };
  const result = await recovery.assertRecoveryAdmission(f.db, args, options(f));
  assert.equal(result.legacy, false); assert.equal(result.manifest.id, f.manifest.id);
  assert.equal(result.admission.itemId, f.item.id);
  assert.ok(f.calls.every(call => /^SELECT/.test(call.sql)));
  f.reservations.push({ item_id: f.item.id, manifest_id: 'earlier-manifest' });
  await assert.rejects(recovery.assertRecoveryAdmission(f.db, args, options(f)), { code: 'anchor_recovery_admission_consumed' });
});


test('leaf rechecks activation deadline and current environment after contended reads', async () => {
  for (const mutate of [f => { f.now.setTime(Date.parse(f.manifest.expiresAt)); },
    f => { f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID = ''; }]) {
    const f = recoveryFixture(); const query = f.db.query;
    f.db.query = async (...args) => {
      const result = await query(...args);
      if (args[0] === 'SELECT * FROM anchor_email_quarantines WHERE tenant_id=$1') mutate(f);
      return result;
    };
    await assert.rejects(leaf(f));
  }
});

test('active recovery accepts only managed credentials and rejects explicit or malformed overrides', async () => {
  for (const value of [true, 'true', 'false', 1, {}]) {
    const f = recoveryFixture(); f.input.command.credentialOverride = value;
    await assert.rejects(leaf(f), { code: 'anchor_recovery_credential_override' });
  }
  const f = recoveryFixture(); f.input.command.credentialOverride = false;
  assert.equal((await leaf(f)).legacy, false);
});
