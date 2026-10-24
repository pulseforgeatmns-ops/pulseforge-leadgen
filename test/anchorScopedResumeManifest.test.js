'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const resume = require('../services/anchorScopedResume');
const recovery = require('../services/anchorEmailRecovery');
const { scopedResumeFixture, clone } = require('./helpers/anchorScopedResumeFixture');
const options = f => ({ now: f.now, env: f.env });

test('scoped grant requires every explicit field; old exact canary rejects discriminant', () => {
  const f = scopedResumeFixture();
  const m = resume.validateScopedResumeManifest(f.manifest, options(f));
  assert.equal(Object.isFrozen(m.scopes.email), true);
  assert.throws(() => recovery.validateManifest(m, options(f)));
  for (const key of Object.keys(f.manifest)) {
    const m = clone(f.manifest); delete m[key];
    assert.throws(() => resume.validateScopedResumeManifest(m, options(f)), key);
  }
  for (const patch of [{ version: 2 }, { tenantId: 10 }, { arbitrary: true }, { operatorRole: 'viewer' },
    { admissionPolicy: 'all_native_scout' }, { reviewSchemaPolicy: 'default_false' }, { sms: 'enabled' },
    { manualCrmEmail: 'enabled' }, { expiresAt: '2026-02-30T12:00:00Z' }]) {
    assert.throws(() => resume.validateScopedResumeManifest({ ...f.manifest, ...patch }, options(f)));
  }
});

test('independent scopes have no inferred permissions or cap defaults', () => {
  const f = scopedResumeFixture();
  for (const scope of ['research', 'preparation', 'email']) {
    const m = clone(f.manifest); delete m.scopes[scope];
    assert.throws(() => resume.validateScopedResumeManifest(m, options(f)));
  }
  for (const cap of [null, undefined, 0, -1, '2', Infinity, 1.5, Number.MAX_SAFE_INTEGER]) {
    const m = clone(f.manifest); m.scopes.email.dailyCap = cap;
    assert.throws(() => resume.validateScopedResumeManifest(m, options(f)));
  }
  const m = clone(f.manifest); m.scopes.research = { enabled: false, dailyCap: 1 };
  assert.throws(() => resume.validateScopedResumeManifest(m, options(f)));
});

test('identity batch is exact and independent of envelope/day', () => {
  const f = scopedResumeFixture();
  const a = f.manifest.admissions[0];
  assert.equal(Object.hasOwn(a, 'envelopeId'), false);
  for (const key of Object.keys(a)) {
    const m = clone(f.manifest); delete m.admissions[0][key];
    assert.throws(() => resume.validateScopedResumeManifest(m, options(f)), key);
  }
  for (const mutate of [m => m.admissions.push(clone(m.admissions[0])),
    m => { m.admissions[0].recipientEmail = 'Fresh@synthetic-recovery.test'; },
    m => { m.admissions[0].companyDomain = 'gmail.com'; },
    m => { m.admissions[0].identityFingerprint = ''; }]) {
    const m = clone(f.manifest); mutate(m); assert.throws(() => resume.validateScopedResumeManifest(m, options(f)));
  }
});

test('identity fingerprint preserves explicit NULL to false review CAS but rejects relink/provenance change', () => {
  const f = scopedResumeFixture(), p = clone(f.prospect), c = f.companies[0];
  p.ao_outreach_review_required = null;
  assert.equal(resume.identityFingerprint(p, c), f.manifest.admissions[0].identityFingerprint);
  p.ao_outreach_review_required = false;
  assert.equal(resume.identityFingerprint(p, c), f.manifest.admissions[0].identityFingerprint);
  p.enrichment_provenance.email.source = 'mission_bound_admission';
  assert.notEqual(resume.identityFingerprint(p, c), f.manifest.admissions[0].identityFingerprint);
});

test('scoped state requires own explicit activation and kill switch wins', async () => {
  const f = scopedResumeFixture();
  assert.equal((await recovery.loadRecoveryState(f.db, options(f))).mode, 'scoped_resume');
  for (const env of [{}, { ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: f.manifest.id },
    { ...f.env, ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' }]) {
    assert.equal((await recovery.loadRecoveryState(f.db, { ...options(f), env })).mode, 'maintenance');
  }
  f.catalog.control_table = null;
  assert.equal((await recovery.loadRecoveryState(f.db, options(f))).mode, 'maintenance');
});

test('paid research and arbitrary copy generation are unsupported, even with hashes and caps', () => {
  const f = scopedResumeFixture();
  f.manifest.scopes.research = { enabled: true, approvalReference: 'synthetic-research', dailyCap: 1, totalCap: 1,
    operations: ['research_existing_source'], providerBudgetHash: 'a'.repeat(64) };
  assert.throws(() => resume.validateScopedResumeManifest(f.manifest, options(f)), { reason: 'resume_paid_research_unsupported' });
});

test('scoped leaf binds current approved item; reservation query is item-specific', async () => {
  const f = scopedResumeFixture();
  const bound = await recovery.assertRecoveryLeaf(f.db, f.input, options(f));
  assert.equal(bound.resumeGrant.id, f.manifest.id);
  assert.equal(bound.item.id, f.item.id);
  const queries = f.calls.filter(c => /FROM anchor_email_recovery_reservations/.test(c.sql));
  assert.ok(queries.some(c => /WHERE item_id=\$1/.test(c.sql)));
  assert.ok(queries.every(c => !/manifest_id=\$1 OR/.test(c.sql)));
});

test('scoped first-touch blocks new items from previously attempted recipient, contact or company', async () => {
  for (const prior_item_id of ['other-day-item', 'other-program-item', 'permanent-cleaned-reservation']) {
    const f = scopedResumeFixture(); f.priorAttempts = [{ prior_item_id }];
    await assert.rejects(recovery.assertRecoveryLeaf(f.db, f.input, options(f)), { code: 'anchor_scoped_resume_first_touch_consumed' });
  }
});

test('review holds and changed company/provenance remain closed independent of explicit batch', async () => {
  for (const mutate of [f => { f.prospect.ao_outreach_review_required = true; },
    f => { f.prospect.ao_outreach_review_required = null; }, f => { delete f.prospect.ao_outreach_review_required; },
    f => { f.prospect.acquisition_metadata = { mission_bound_admission: { fresh: true } }; },
    f => { f.prospect.company_name = 'Changed Company'; }]) {
    const f = scopedResumeFixture(); mutate(f); await assert.rejects(recovery.assertRecoveryLeaf(f.db, f.input, options(f)));
  }
});

test('missing, altered and revoked durable installation never exposes scoped mode', async () => {
  for (const installation of [null, { manifest_hash: 'b'.repeat(64) }, { revoked_at: '2026-10-19T15:59:00Z' }]) {
    const f = scopedResumeFixture(); f.installation = installation;
    assert.equal((await recovery.loadRecoveryState(f.db, options(f))).mode, 'maintenance');
    await assert.rejects(recovery.assertRecoveryLeaf(f.db, f.input, options(f)));
  }
});

test('actual transport payload rejects recipient/body/sender/HTML/idempotency substitution and extensions', async () => {
  for (const mutate of [f => { delete f.input.command.actualPayload; },
    f => { f.input.command.actualPayload.to[0].email = 'different@synthetic-recovery.test'; },
    f => { f.input.command.actualPayload.textContent += ' Changed'; },
    f => { f.input.command.actualPayload.htmlContent += ' Changed'; },
    f => { f.input.command.actualPayload.sender.email = 'different@synthetic-recovery.test'; },
    f => { f.input.command.actualPayload.headers['Idempotency-Key'] = 'different'; },
    f => { f.input.command.actualPayload.bcc = [{ email: 'different@synthetic-recovery.test' }]; },
    f => { f.input.command.actualPayload.headers.Bcc = 'different@synthetic-recovery.test'; }]) {
    const f = scopedResumeFixture(); mutate(f); await assert.rejects(recovery.assertRecoveryLeaf(f.db, f.input, options(f)));
  }
});

test('universal tenant-10 CRM guard binds actual recipient even without scoped transport authority', async () => {
  const f = scopedResumeFixture();
  assert.deepEqual(await resume.assertCrmEmailQuarantine(f.db, f.input), { recipients: [f.item.email] });
  f.input.command.actualPayload.to[0].email = 'different@synthetic-recovery.test';
  f.input.command.toEmail = 'different@synthetic-recovery.test';
  await assert.rejects(resume.assertCrmEmailQuarantine(f.db, f.input), { code: 'anchor_recovery_payload_changed' });
});

test('SMTP actual recipient/header/envelope checks cannot borrow a clean prospect or enable SMTP', async () => {
  const f = scopedResumeFixture();
  const command = { transport: 'smtp', actualPayload: { from: 'Synthetic Sender <sender@synthetic-recovery.test>',
    replyTo: 'sender@synthetic-recovery.test', to: [f.item.email], subject: 'Synthetic', text: 'Synthetic', headers: { 'Message-ID': '<synthetic@synthetic-recovery.test>' } } };
  await resume.assertCrmEmailQuarantine(f.db, { identities: f.input.identities, command });
  await assert.rejects(recovery.assertRecoveryLeaf(f.db, { ...f.input, command }, options(f)));
  for (const patch of [{ cc: ['different@synthetic-recovery.test'] }, { envelope: { to: ['different@synthetic-recovery.test'] } },
    { headers: { 'Message-ID': '<synthetic@synthetic-recovery.test>', Bcc: 'different@synthetic-recovery.test' } }]) {
    await assert.rejects(resume.assertCrmEmailQuarantine(f.db, { identities: f.input.identities, command: { ...command, actualPayload: { ...command.actualPayload, ...patch } } }));
  }
});

test('source, policy and operator changes invalidate every capability; email never grants preparation', async () => {
  for (const mutate of [f => { f.sourceMission.objective = 'Changed purpose'; },
    f => { f.program.scope_hash = 'b'.repeat(64); }, f => { f.program.policy.dailyCap = 6; },
    f => { f.manifest.expiresAt = '2026-10-20T01:00:00Z'; }, f => { f.operator = { active: false }; },
    f => { f.program.authorized_by = 'another-operator'; }]) {
    const f = scopedResumeFixture(); mutate(f);
    await assert.rejects(resume.assertScopedResumeCapability(f.db, { program: f.program, capability: 'email', operation: 'dispatch' }, options(f)));
  }
  const f = scopedResumeFixture();
  await assert.rejects(resume.assertScopedResumeCapability(f.db, { program: f.program, capability: 'preparation', operation: 'freeze' }, options(f)), { code: 'anchor_scoped_resume_capability_closed' });
  const before = f.calls.length;
  await assert.rejects(resume.assertScopedResumeCapability(f.db, { program: { ...f.program, tenant_id: '13' }, capability: 'email', operation: 'dispatch' }, options(f)), { code: 'anchor_scoped_resume_scope_invalid' });
  assert.equal(f.calls.length, before, 'non-Anchor capability rejection performs no recovery read');
});

test('capability inspection is read-only and independent scopes are explicit', async () => {
  const f = scopedResumeFixture();
  const result = await resume.inspectScopedResumeCapabilities(f.db, { program: f.program }, options(f));
  assert.equal(result.email.enabled, true);
  assert.equal(result.research.enabled, false);
  assert.equal(result.preparation.enabled, false);
  assert.ok(f.calls.every(c => /^SELECT/.test(c.sql) && !/FOR SHARE|FOR UPDATE|pg_advisory/.test(c.sql)));
});
