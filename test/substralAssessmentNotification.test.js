'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const axios = require('axios');

function setEnv(t, key, value) {
  const before = process.env[key];
  t.after(() => { if (before === undefined) delete process.env[key]; else process.env[key] = before; });
  if (value === undefined) delete process.env[key]; else process.env[key] = value;
}

const {
  DEFAULT_NOTIFY_EMAIL,
  notificationBlockReason,
  productionDeliveryRequired,
  resolveRecipient,
  resolveSender,
  syntheticSubmission,
  notifyAssessmentRequest,
} = require('../lib/substralAssessmentNotification');

const production = {
  NODE_ENV: 'production',
  RAILWAY_ENVIRONMENT_NAME: 'production',
  BREVO_API_KEY: 'fake-key',
};

const values = {
  domain: 'acmecleaning.com',
  email: 'owner@acmecleaning.com',
  context: 'Redesign decision',
};

function mockNotificationPool(actionId, { claimRows = [{ claim_id: 'claim-1', status: 'claimed' }], claims = [] } = {}) {
  const queries = [];
  return {
    queries,
    async query(sql, params) {
      queries.push({ sql, params });
      if (/FROM agent_actions/i.test(sql)) {
        return { rows: [{ id: String(actionId) }] };
      }
      if (/FROM substral_assessment_notification_claims/i.test(sql) && /SELECT status/i.test(sql)) {
        return { rows: claims.slice(0, 1) };
      }
      if (/INSERT INTO substral_assessment_notification_claims/i.test(sql)) {
        return { rows: claimRows };
      }
      if (/failed_or_uncertain/i.test(sql)) return { rows: [] };
      if (/SET status = 'sent'/i.test(sql)) return { rows: [] };
      return { rows: [] };
    },
  };
}

test('default operator recipient is hello@studiosubstral.com', () => {
  delete process.env.SUBSTRAL_ASSESSMENT_NOTIFY_EMAIL;
  assert.equal(resolveRecipient(), DEFAULT_NOTIFY_EMAIL);
  assert.equal(DEFAULT_NOTIFY_EMAIL, 'hello@studiosubstral.com');
});

test('production delivery requires Brevo and is not suppressed in production runtime', () => {
  assert.equal(notificationBlockReason(values, production), null);
  assert.equal(productionDeliveryRequired(production), true);
  assert.equal(notificationBlockReason(values, { ...production, BREVO_API_KEY: '' }), 'missing_provider_key');
});

test('test runner suppresses provider calls even with production credentials', async (t) => {
  t.mock.method(axios, 'post', async () => { throw new Error('must not call provider'); });
  setEnv(t, 'NODE_ENV', 'production');
  setEnv(t, 'BREVO_API_KEY', 'production-looking-key');
  assert.ok(process.env.NODE_TEST_CONTEXT);
  const result = await notifyAssessmentRequest(
    { query() { throw new Error('must not query'); } },
    values,
    'action-1',
    17
  );
  assert.equal(result.reason, 'test_runtime');
  assert.equal(axios.post.mock.callCount(), 0);
});

test('synthetic example.com submissions skip notification', () => {
  assert.equal(syntheticSubmission({ ...values, email: 'qa@example.com' }), true);
  assert.equal(notificationBlockReason({ ...values, email: 'qa@example.com' }, production), 'synthetic_submission');
});

test('production provider failure fails closed and retains claim', async (t) => {
  t.mock.method(axios, 'post', async () => { throw new Error('timeout'); });
  setEnv(t, 'NODE_TEST_CONTEXT', undefined);
  setEnv(t, 'NODE_ENV', 'production');
  setEnv(t, 'RAILWAY_ENVIRONMENT_NAME', 'production');
  setEnv(t, 'BREVO_API_KEY', 'fake-key');
  const pool = mockNotificationPool('77');
  await assert.rejects(
    notifyAssessmentRequest(pool, values, '77', 17),
    /claim retained/
  );
  assert.equal(axios.post.mock.callCount(), 1);
  assert.match(pool.queries.find((q) => /failed_or_uncertain/i.test(q.sql)).sql, /failed_or_uncertain/);
});

test('successful send logs canonical recipient and uses reply-to visitor email', async (t) => {
  let payload;
  t.mock.method(axios, 'post', async (_url, body) => {
    payload = body;
    return { data: { messageId: '<provider-msg-1>' } };
  });
  setEnv(t, 'NODE_TEST_CONTEXT', undefined);
  setEnv(t, 'NODE_ENV', 'production');
  setEnv(t, 'RAILWAY_ENVIRONMENT_NAME', 'production');
  setEnv(t, 'BREVO_API_KEY', 'fake-key');
  delete process.env.SUBSTRAL_ASSESSMENT_NOTIFY_EMAIL;

  const pool = mockNotificationPool('88', { claimRows: [{ claim_id: 'claim-2', status: 'claimed' }] });

  const result = await notifyAssessmentRequest(pool, values, '88', 17);
  assert.equal(result.status, 'sent');
  assert.equal(result.recipient, 'hello@studiosubstral.com');
  assert.equal(payload.to[0].email, 'hello@studiosubstral.com');
  assert.equal(payload.replyTo.email, values.email);
  assert.match(payload.subject, /Website assessment request — acmecleaning.com/);
  assert.equal(result.providerMessageId, '<provider-msg-1>');
});

test('legacy duplicate action without claim still sends the first notification', async (t) => {
  t.mock.method(axios, 'post', async () => ({ data: { messageId: '<legacy-first-send>' } }));
  setEnv(t, 'NODE_TEST_CONTEXT', undefined);
  setEnv(t, 'NODE_ENV', 'production');
  setEnv(t, 'RAILWAY_ENVIRONMENT_NAME', 'production');
  setEnv(t, 'BREVO_API_KEY', 'fake-key');
  const pool = mockNotificationPool('legacy-action', { claimRows: [{ claim_id: 'claim-legacy', status: 'claimed' }] });
  const result = await notifyAssessmentRequest(pool, values, 'legacy-action', 17);
  assert.equal(result.status, 'sent');
  assert.equal(result.providerMessageId, '<legacy-first-send>');
  assert.equal(axios.post.mock.callCount(), 1);
});

test('duplicate resubmit suppresses after notification claim is sent', async (t) => {
  t.mock.method(axios, 'post', async () => { throw new Error('must not call provider'); });
  setEnv(t, 'NODE_TEST_CONTEXT', undefined);
  setEnv(t, 'NODE_ENV', 'production');
  setEnv(t, 'RAILWAY_ENVIRONMENT_NAME', 'production');
  setEnv(t, 'BREVO_API_KEY', 'fake-key');
  const pool = mockNotificationPool('legacy-action', {
    claims: [{ status: 'sent', provider_message_id: '<already-delivered>' }],
  });
  const result = await notifyAssessmentRequest(pool, values, 'legacy-action', 17);
  assert.equal(result.status, 'suppressed');
  assert.equal(result.reason, 'notification_already_sent');
  assert.equal(result.providerMessageId, '<already-delivered>');
  assert.equal(axios.post.mock.callCount(), 0);
});

test('concurrent duplicate notification attempts call provider once', async (t) => {
  t.mock.method(axios, 'post', async () => ({ data: { messageId: '<concurrent-send>' } }));
  setEnv(t, 'NODE_TEST_CONTEXT', undefined);
  setEnv(t, 'NODE_ENV', 'production');
  setEnv(t, 'RAILWAY_ENVIRONMENT_NAME', 'production');
  setEnv(t, 'BREVO_API_KEY', 'fake-key');

  let claimState = null;
  const pool = {
    async query(sql, params) {
      if (/FROM agent_actions/i.test(sql)) return { rows: [{ id: 'concurrent-action' }] };
      if (/FROM substral_assessment_notification_claims/i.test(sql) && /SELECT status/i.test(sql)) {
        return { rows: claimState ? [claimState] : [] };
      }
      if (/INSERT INTO substral_assessment_notification_claims/i.test(sql)) {
        if (!claimState) {
          claimState = { status: 'claimed', provider_message_id: null };
          return { rows: [{ claim_id: 'claim-concurrent', status: 'claimed' }] };
        }
        if (claimState.status === 'failed_or_uncertain') {
          claimState = { status: 'claimed', provider_message_id: null };
          return { rows: [{ claim_id: 'claim-retry', status: 'claimed' }] };
        }
        return { rows: [] };
      }
      if (/SET status = 'sent'/i.test(sql)) {
        claimState = { status: 'sent', provider_message_id: '<concurrent-send>' };
        return { rows: [] };
      }
      return { rows: [] };
    },
  };

  const results = await Promise.all(Array.from({ length: 12 }, () => notifyAssessmentRequest(
    pool,
    values,
    'concurrent-action',
    17
  )));
  assert.equal(results.filter((r) => r.status === 'sent').length, 1);
  assert.equal(
    results.filter((r) => r.reason === 'notification_in_progress' || r.reason === 'notification_already_sent').length,
    11
  );
  assert.equal(axios.post.mock.callCount(), 1);
});

test('fingerprint matches intake request_fingerprint when context is absent', () => {
  const { fingerprint } = require('../lib/substralAssessmentNotification');
  const { createHash } = require('node:crypto');
  const noContext = { domain: 'acme.com', email: 'a@acme.com', context: null };
  const intakeFp = createHash('sha256').update(JSON.stringify([
    noContext.domain, noContext.email, null,
  ])).digest('hex');
  assert.equal(fingerprint(noContext), intakeFp);
});
