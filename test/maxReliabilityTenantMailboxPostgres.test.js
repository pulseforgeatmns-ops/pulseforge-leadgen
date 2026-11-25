'use strict';

/**
 * MAX-P0-003 R2 — tenant-mailbox SMTP sends (tenantOutreachScheduler path) go
 * through the durable external-action ledger on real PostgreSQL: one SMTP call
 * under concurrency, no blind retry after an unknown outcome, definitive
 * pre-transfer failures stay retryable, and an acknowledged send whose message
 * row was lost is restored without re-sending.
 */

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const {
  PostgresTenantMailboxStore, ensureTenantMailboxSchema, sendTenantEmail,
  MAILBOX_STATUS, IDENTITY_STATUS, MESSAGE_STATUS,
} = require('../services/tenantMailbox');
const { createExternalActionLedger, STATES } = require('../services/maxReliability/externalActionLedger');

const MIGRATION = path.join(__dirname, '../migrations/2026-11-25-max-p0-003-reliability-core.sql');
const TENANT = '77';
let fixture;

test.before(async () => {
  const instance = await startDisposablePostgres('max-p0-003-mailbox-');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 8 });
  await ensureTenantMailboxSchema(pool);
  await pool.query(fs.readFileSync(MIGRATION, 'utf8'));
  const store = new PostgresTenantMailboxStore(pool);
  const integration = await store.saveIntegration({
    id: 'tmi_p0003', tenantId: TENANT, providerType: 'GENERIC_SMTP_IMAP', mailboxAddress: 'hello@example.test',
    displayName: 'Synthetic', smtpHost: 'smtp.example.test', smtpPort: 465, smtpTlsMode: 'SSL_TLS',
    imapHost: 'imap.example.test', imapPort: 993, imapTlsMode: 'SSL_TLS',
    smtpSecretRef: 'SMTP_REF', imapSecretRef: 'IMAP_REF', status: MAILBOX_STATUS.ACTIVE,
  });
  const identity = await store.saveIdentity({
    id: 'tsi_p0003', tenantId: TENANT, mailboxIntegrationId: integration.id, senderEmail: 'hello@example.test',
    senderDisplayName: 'Synthetic', replyToAddress: 'hello@example.test', status: IDENTITY_STATUS.ACTIVE,
  });
  fixture = { instance, pool, store, identity };
});
test.after(() => closePostgresFixture(fixture.pool, fixture.instance));

// Non-CRM verification send: exercises the PostgreSQL store without CRM authorization.
function send(key, transport, extra = {}) {
  return sendTenantEmail({
    tenantId: TENANT, sendingIdentityId: fixture.identity.id, to: 'buyer@example.test', subject: 'Synthetic',
    body: 'Synthetic body', prospectId: 'safe-test', missionId: 'mailbox-verification', outreachAssetId: 'mailbox-test-send',
    metadata: { idempotencyKey: key },
  }, { store: fixture.store, transport, purpose: 'mailbox_verification', secretResolver: () => 'dummy', ...extra });
}

function transport(behaviour) {
  const t = { calls: 0, async sendMail() { t.calls += 1; return behaviour(t.calls); }, async verify() { return true; } };
  return t;
}

test('concurrent sends of one message reach SMTP exactly once', { timeout: 60000 }, async () => {
  const t = transport(async () => { await new Promise((r) => setTimeout(r, 100)); return { messageId: '<one@example.test>' }; });
  const results = await Promise.allSettled(Array.from({ length: 6 }, () => send('mb-concurrent', t)));
  assert.equal(t.calls, 1);
  assert.equal(results.filter((r) => r.status === 'fulfilled' && r.value.sent === true).length, 1);
  for (const r of results.filter((x) => x.status === 'rejected')) {
    assert.ok(['external_action_in_progress', 'external_action_outcome_unknown'].includes(r.reason.code), r.reason.code);
  }
  const again = await send('mb-concurrent', t);
  assert.equal(again.duplicate, true);
  assert.equal(t.calls, 1);
});

test('mid-transfer failure is outcome-unknown and is never retried', { timeout: 60000 }, async () => {
  // nodemailer 8.x tags an unexpected close with command 'CONN' at ANY stage, including after DATA.
  const t = transport(async () => { throw Object.assign(new Error('Connection closed unexpectedly'), { code: 'ECONNECTION', command: 'CONN' }); });
  await assert.rejects(send('mb-unknown', t), { code: 'ECONNECTION' });
  const row = (await fixture.pool.query("SELECT status, failure_code, metadata FROM tenant_outreach_messages WHERE metadata->>'idempotencyKey' = 'mb-unknown'")).rows[0];
  assert.equal(row.status, MESSAGE_STATUS.FAILED);
  assert.equal(row.failure_code, 'external_action_outcome_unknown');
  assert.equal(row.metadata.providerAccepted, null, 'never recorded as definitely not accepted');
  await assert.rejects(send('mb-unknown', t), { code: 'external_action_outcome_unknown' });
  assert.equal(t.calls, 1, 'no blind retry');
  const receipt = await createExternalActionLedger(fixture.pool).get(TENANT, 'tenant-mailbox:mb-unknown');
  assert.equal(receipt.state, STATES.OUTCOME_UNKNOWN);
});

test('socket reset and timeout (real nodemailer CONN shapes) are unknown, never retried', { timeout: 60000 }, async () => {
  for (const [key, err] of [
    ['mb-reset', Object.assign(new Error('read ECONNRESET'), { code: 'ESOCKET', command: 'CONN', syscall: 'read' })],
    ['mb-timeout', Object.assign(new Error('Timeout'), { code: 'ETIMEDOUT', command: 'CONN' })],
  ]) {
    const t = transport(async () => { throw err; });
    await assert.rejects(send(key, t));
    await assert.rejects(send(key, t), { code: 'external_action_outcome_unknown' });
    assert.equal(t.calls, 1, `${key}: no blind retry`);
  }
});

test('failure after the provider returned is never read as "not sent"', { timeout: 60000 }, async () => {
  const real = createExternalActionLedger(fixture.pool);
  let failOnce = true;
  const ledger = { ...real, recordProviderResult: async (...args) => {
    if (failOnce) { failOnce = false; throw Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:5432'), { code: 'ECONNREFUSED', syscall: 'connect' }); }
    return real.recordProviderResult(...args);
  } };
  const t = transport(async () => ({ messageId: '<accepted@example.test>' }));
  await assert.rejects(send('mb-post-provider', t, { externalActionLedger: ledger }), { code: 'ECONNREFUSED' });
  const row = (await fixture.pool.query("SELECT metadata FROM tenant_outreach_messages WHERE metadata->>'idempotencyKey' = 'mb-post-provider'")).rows[0];
  assert.notEqual(row.metadata.providerAccepted, false, 'never recorded as definitely not accepted');
  await assert.rejects(send('mb-post-provider', t, { externalActionLedger: ledger }), { code: 'external_action_outcome_unknown' });
  assert.equal(t.calls, 1, 'the accepted message is not sent again');
});

test('connection never established is definitive and may be retried once', { timeout: 60000 }, async () => {
  const t = transport(async (n) => {
    if (n === 1) throw Object.assign(new Error('connect ENETUNREACH 192.0.2.1:465'), { code: 'ESOCKET', command: 'CONN', syscall: 'connect' });
    return { messageId: '<retry@example.test>' };
  });
  await assert.rejects(send('mb-definitive', t), { code: 'ESOCKET' });
  const retry = await send('mb-definitive', t);
  assert.equal(retry.sent, true);
  assert.equal(t.calls, 2);
  assert.equal((await createExternalActionLedger(fixture.pool).get(TENANT, 'tenant-mailbox:mb-definitive')).attempt, 2);
});

test('acknowledged send whose message row was lost is restored without re-sending', { timeout: 60000 }, async () => {
  const t = transport(async () => ({ messageId: '<crash@example.test>' }));
  await send('mb-crash', t);
  // Simulate a crash after the ledger recorded the acknowledgement but before the row became SENT.
  await fixture.pool.query("UPDATE tenant_outreach_messages SET status = 'PENDING', sent_at = NULL WHERE metadata->>'idempotencyKey' = 'mb-crash'");
  const restored = await send('mb-crash', t);
  assert.equal(restored.duplicate, true);
  assert.equal(restored.message.status, MESSAGE_STATUS.SENT);
  assert.equal(t.calls, 1);
});

test('mailbox sends fail closed without the reliability migration', { timeout: 60000 }, async () => {
  await fixture.pool.query('CREATE SCHEMA IF NOT EXISTS mb_no_migration');
  const bare = createTrackedPostgresPool({ connectionString: fixture.instance.connectionString, max: 2, options: '-c search_path=mb_no_migration' });
  try {
    await ensureTenantMailboxSchema(bare);
    const store = new PostgresTenantMailboxStore(bare);
    const integration = await store.saveIntegration({ id: 'tmi_bare', tenantId: TENANT, providerType: 'GENERIC_SMTP_IMAP',
      mailboxAddress: 'hello@example.test', smtpHost: 'smtp.example.test', smtpPort: 465, smtpTlsMode: 'SSL_TLS',
      smtpSecretRef: 'SMTP_REF', status: MAILBOX_STATUS.ACTIVE });
    const identity = await store.saveIdentity({ id: 'tsi_bare', tenantId: TENANT, mailboxIntegrationId: integration.id,
      senderEmail: 'hello@example.test', status: IDENTITY_STATUS.ACTIVE });
    const t = transport(async () => ({ messageId: '<never@example.test>' }));
    await assert.rejects(sendTenantEmail({ tenantId: TENANT, sendingIdentityId: identity.id, to: 'buyer@example.test',
      subject: 's', body: 'b', prospectId: 'safe-test', missionId: 'mailbox-verification', outreachAssetId: 'mailbox-test-send',
      metadata: { idempotencyKey: 'mb-bare' } }, { store, transport: t, purpose: 'mailbox_verification', secretResolver: () => 'dummy' }),
    { code: 'reliability_schema_missing' });
    assert.equal(t.calls, 0);
  } finally {
    await bare.end();
  }
});

test('legitimate sequence steps to the same prospect in the same mission all send (not constrained by R7)', { timeout: 60000 }, async () => {
  const t = transport(async (n) => ({ messageId: `<step-${n}@example.test>` }));
  // Same tenant, identity, mission and prospect; distinct steps → distinct idempotency keys and content.
  const step = (n) => sendTenantEmail({
    tenantId: TENANT, sendingIdentityId: fixture.identity.id, to: 'buyer@example.test', subject: `Step ${n}`,
    body: `Sequence step ${n}`, prospectId: 'safe-test', missionId: 'mailbox-verification', outreachAssetId: 'mailbox-test-send',
    sequenceStepRef: String(n), metadata: { idempotencyKey: `mb-seq-step-${n}` },
  }, { store: fixture.store, transport: t, purpose: 'mailbox_verification', secretResolver: () => 'dummy' });
  const results = [await step(1), await step(2), await step(3)];
  assert.ok(results.every((r) => r.sent === true), JSON.stringify(results.map((r) => r.sent)));
  assert.equal(t.calls, 3);
  const claims = (await fixture.pool.query(
    "SELECT state FROM max_external_action_claims WHERE tenant_id = $1 AND idempotency_key LIKE 'tenant-mailbox:mb-seq-step-%'", [TENANT])).rows;
  assert.deepEqual(claims.map((c) => c.state), ['provider_acknowledged', 'provider_acknowledged', 'provider_acknowledged']);
  // A retry of step 2 is still deduplicated (replay, no SMTP call).
  assert.equal((await step(2)).duplicate, true);
  assert.equal(t.calls, 3);
});
