'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const axios = require('axios');
const { Pool } = require('pg');
const { randomUUID } = require('node:crypto');

function setEnv(t, key, value) {
  const before = process.env[key];
  t.after(() => { if (before === undefined) delete process.env[key]; else process.env[key] = before; });
  if (value === undefined) delete process.env[key]; else process.env[key] = value;
}

const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { captureAssessmentRequest } = require('../lib/substralAssessmentIntake');
const { notifyAssessmentRequest } = require('../lib/substralAssessmentNotification');

test('legacy duplicate intake sends once then suppresses; concurrency sends once', {
  skip: process.env.SUBSTRAL_NOTIFICATION_TEST_POSTGRES !== 'true',
  timeout: 120000,
}, async (t) => {
  const pg = await startDisposablePostgres('substral-notify-');
  const pool = new Pool({ connectionString: pg.connectionString });
  t.after(async () => { await pool.end(); await pg.stop(); });

  await pool.query(`CREATE TABLE clients (id INTEGER PRIMARY KEY);
    INSERT INTO clients VALUES (17);
    CREATE TABLE agent_actions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      created_by TEXT,
      action_type TEXT NOT NULL,
      title TEXT NOT NULL,
      description TEXT NOT NULL,
      payload JSONB,
      status TEXT DEFAULT 'pending',
      client_id INTEGER REFERENCES clients(id),
      created_at TIMESTAMPTZ DEFAULT NOW()
    );`);
  await pool.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-09-29-substral-assessment-idempotency.sql'), 'utf8'));
  await pool.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-04-substral-assessment-notification-claims.sql'), 'utf8'));

  setEnv(t, 'STUDIO_SUBSTRAL_CLIENT_ID', '17');
  setEnv(t, 'NODE_TEST_CONTEXT', undefined);
  setEnv(t, 'NODE_ENV', 'production');
  setEnv(t, 'RAILWAY_ENVIRONMENT_NAME', 'production');
  setEnv(t, 'BREVO_API_KEY', 'fake-key');

  t.mock.method(axios, 'post', async () => ({ data: { messageId: '<provider-concurrent>' } }));

  const values = {
    domain: 'legacy-resubmit.example',
    email: 'owner@legacy-resubmit.example',
    context: 'Pre-notification legacy row',
    request_key: randomUUID(),
  };

  const firstCapture = await captureAssessmentRequest(pool, values);
  assert.equal(firstCapture.duplicate, false);

  const legacyNotify = await notifyAssessmentRequest(
    pool,
    values,
    firstCapture.id,
    firstCapture.client_id
  );
  assert.equal(legacyNotify.status, 'sent');
  assert.equal(axios.post.mock.callCount(), 1);

  const duplicateCapture = await captureAssessmentRequest(pool, values);
  assert.equal(duplicateCapture.duplicate, true);
  assert.equal(String(duplicateCapture.id), String(firstCapture.id));

  const resubmitNotify = await notifyAssessmentRequest(
    pool,
    values,
    duplicateCapture.id,
    duplicateCapture.client_id
  );
  assert.equal(resubmitNotify.status, 'suppressed');
  assert.equal(resubmitNotify.reason, 'notification_already_sent');
  assert.equal(axios.post.mock.callCount(), 1);

  const thirdNotify = await notifyAssessmentRequest(
    pool,
    values,
    duplicateCapture.id,
    duplicateCapture.client_id
  );
  assert.equal(thirdNotify.reason, 'notification_already_sent');
  assert.equal(axios.post.mock.callCount(), 1);

  const concurrentValues = {
    domain: 'concurrent-notify.example',
    email: 'owner@concurrent-notify.example',
    context: null,
    request_key: randomUUID(),
  };
  const concurrentCapture = await captureAssessmentRequest(pool, concurrentValues);
  const concurrentResults = await Promise.all(Array.from({ length: 20 }, () => notifyAssessmentRequest(
    pool,
    concurrentValues,
    concurrentCapture.id,
    concurrentCapture.client_id
  )));
  assert.equal(concurrentResults.filter((r) => r.status === 'sent').length, 1);
  assert.equal(
    concurrentResults.filter((r) => r.reason === 'notification_already_sent' || r.reason === 'notification_in_progress').length,
    19
  );
  assert.equal(axios.post.mock.callCount(), 2);
});
