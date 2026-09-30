'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const axios = require('axios');
function setEnv(t, key, value) {
  const before = process.env[key];
  t.after(() => { if (before === undefined) delete process.env[key]; else process.env[key] = before; });
  if (value === undefined) delete process.env[key]; else process.env[key] = value;
}

const { notificationBlockReason, syntheticSubmission, fingerprint, notifyWalkthrough } = require('../lib/walkthroughNotification');
const { validateWalkthroughPayload } = require('../lib/walkthroughValidate');
const contact = { name: 'Morgan Jones', business_name: 'Office One', email: 'morgan@customer.com', phone: '6034202430', city: 'Manchester', space_type: 'law_office', space_type_label: 'Law office' };
const production = { NODE_ENV: 'production', BREVO_API_KEY: 'fake-key' };

test('live credentials never override test, staging, or disabled delivery', () => {
  assert.equal(notificationBlockReason(contact, production), null);
  for (const marker of ['NODE_TEST_CONTEXT', 'JEST_WORKER_ID', 'VITEST']) {
    assert.equal(notificationBlockReason(contact, { ...production, [marker]: '1' }), 'test_runtime');
  }
  for (const NODE_ENV of [undefined, 'development', 'staging']) {
    assert.equal(notificationBlockReason(contact, { ...production, NODE_ENV }), 'non_production_runtime');
  }
  assert.equal(notificationBlockReason(contact, { ...production, RAILWAY_ENVIRONMENT_NAME: 'staging' }), 'non_production_environment');
  assert.equal(notificationBlockReason(contact, { ...production, ANCHOR_WALKTHROUGH_NOTIFY_ENABLED: 'false' }), 'disabled');
});

test('reserved contacts and explicit test/demo markers suppress production notifications', () => {
  for (const email of ['alex@riverside.example', 'a@example.com', 'a@sub.example.org', 'a@demo.test', 'a@x.invalid', 'a@localhost']) {
    assert.equal(syntheticSubmission({ ...contact, email }), true);
  }
  for (const phone of ['(603) 555-0142', '+1 603 555 0100', '6035550199']) {
    assert.equal(syntheticSubmission({ ...contact, phone }), true);
  }
  assert.equal(syntheticSubmission({ ...contact, phone: '6035550200' }), false);
  for (const marker of ['is_test', 'is_demo', 'is_synthetic']) {
    const validated = validateWalkthroughPayload({ ...contact, [marker]: 'true' });
    assert.equal(validated.ok, true);
    assert.equal(notificationBlockReason(validated.values, production), 'synthetic_submission');
  }
  const validated = validateWalkthroughPayload({ ...contact, submission_mode: 'PREVIEW' });
  assert.equal(notificationBlockReason(validated.values, production), 'synthetic_submission');
});

test('node test runner with production credentials makes zero DB or provider calls', async t => {
  t.mock.method(axios, 'post', async () => { throw new Error('must not call provider'); });
  setEnv(t, 'NODE_ENV', 'production');
  setEnv(t, 'BREVO_API_KEY', 'production-looking-key');
  assert.ok(process.env.NODE_TEST_CONTEXT);
  const result = await notifyWalkthrough({ query() { throw new Error('must not query'); } }, contact, '77');
  assert.equal(result.reason, 'test_runtime');
  assert.equal(axios.post.mock.callCount(), 0);
});

test('duplicate key normalizes case, whitespace and phone formatting', () => {
  assert.equal(fingerprint(contact), fingerprint({ ...contact, name: ' MORGAN  JONES ', email: 'MORGAN@CUSTOMER.COM', phone: '+1 (603) 420-2430' }));
  assert.notEqual(fingerprint(contact), fingerprint({ ...contact, email: 'other@customer.com' }));
});

test('production provider boundary fails closed and retains ambiguous claims', async t => {
  // Install the provider mock BEFORE simulating the production runtime.
  t.mock.method(axios, 'post', async () => { throw new Error('timeout with secret config'); });
  setEnv(t, 'NODE_TEST_CONTEXT', undefined);
  setEnv(t, 'NODE_ENV', 'production');
  setEnv(t, 'BREVO_API_KEY', 'fake-key');
  setEnv(t, 'RAILWAY_ENVIRONMENT_NAME', 'production');
  setEnv(t, 'ANCHOR_WALKTHROUGH_NOTIFY_ENABLED', 'true');
  const queries = [];
  const pool = { async query(sql, params) { queries.push({ sql, params }); return { rows: [{ claim_id: 'claim' }] }; } };
  await assert.rejects(notifyWalkthrough(pool, contact, '77'), /claim retained/);
  assert.equal(axios.post.mock.callCount(), 1);
  assert.match(queries[1].sql, /failed_or_uncertain/);
  await assert.rejects(notifyWalkthrough({ async query() { throw new Error('schema unavailable'); } }, contact, '77'), /schema unavailable/);
  assert.equal(axios.post.mock.callCount(), 1);
  assert.equal((await notifyWalkthrough({ async query() { return { rows: [] }; } }, contact, '77')).status, 'suppressed');
  assert.equal(axios.post.mock.callCount(), 1);
  assert.equal((await notifyWalkthrough(pool, { ...contact, email: 'alex@riverside.example' }, '77')).reason, 'synthetic_submission');
  assert.equal(axios.post.mock.callCount(), 1);
});
