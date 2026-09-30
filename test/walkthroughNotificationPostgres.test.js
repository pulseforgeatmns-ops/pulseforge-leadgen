'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Pool } = require('pg');
const axios = require('axios');
function setEnv(t, key, value) {
  const before = process.env[key];
  t.after(() => { if (before === undefined) delete process.env[key]; else process.env[key] = before; });
  if (value === undefined) delete process.env[key]; else process.env[key] = value;
}

const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { notifyWalkthrough } = require('../lib/walkthroughNotification');

test('notification claims survive concurrency, reloads, retries and expiry', { skip: process.env.WALKTHROUGH_NOTIFICATION_TEST_POSTGRES !== 'true', timeout: 60000 }, async t => {
  const pg = await startDisposablePostgres('walkthrough-mail-');
  const pool = new Pool({ connectionString: pg.connectionString });
  t.after(async () => { await pool.end(); await pg.stop(); });
  await pool.query('CREATE TABLE agent_actions (id INTEGER PRIMARY KEY, client_id INTEGER, action_type TEXT)');
  await pool.query("INSERT INTO agent_actions VALUES (1,10,'walkthrough_request'), (2,10,'walkthrough_request'), (3,11,'walkthrough_request')");
  const migration = fs.readFileSync(path.join(__dirname, '../migrations/2026-09-30-walkthrough-notification-claims.sql'), 'utf8');
  await pool.query(migration);
  await pool.query(migration);
  t.mock.method(axios, 'post', async () => ({ data: { messageId: 'stubbed-provider-message' } }));
  setEnv(t, 'NODE_TEST_CONTEXT', undefined);
  setEnv(t, 'NODE_ENV', 'production');
  setEnv(t, 'BREVO_API_KEY', 'fake-key');
  setEnv(t, 'RAILWAY_ENVIRONMENT_NAME', 'production');
  setEnv(t, 'ANCHOR_WALKTHROUGH_NOTIFY_ENABLED', 'true');
  const values = { name: 'Morgan Jones', business_name: 'Office One', email: 'morgan@customer.com', phone: '6034202430', city: 'Manchester', space_type: 'law_office' };
  const results = await Promise.all(Array.from({ length: 20 }, (_, i) => notifyWalkthrough(pool, values, i % 2 + 1)));
  assert.equal(results.filter(x => x.status === 'sent').length, 1);
  assert.equal(axios.post.mock.callCount(), 1);
  delete require.cache[require.resolve('../lib/walkthroughNotification')];
  assert.equal((await require('../lib/walkthroughNotification').notifyWalkthrough(pool, values, 2)).status, 'suppressed');
  assert.equal((await notifyWalkthrough(pool, { ...values, email: 'other@customer.com' }, 3)).status, 'suppressed');
  assert.equal((await notifyWalkthrough(pool, values, 999)).status, 'suppressed');
  assert.equal(axios.post.mock.callCount(), 1);
  await pool.query("UPDATE walkthrough_notification_claims SET claimed_at = NOW() - INTERVAL '25 hours'");
  assert.equal((await notifyWalkthrough(pool, values, 2)).status, 'sent');
  assert.equal(axios.post.mock.callCount(), 2);
  axios.post.mock.mockImplementation(async () => { throw new Error('provider timeout'); });
  const failure = { ...values, email: 'new@customer.com' };
  await assert.rejects(notifyWalkthrough(pool, failure, 1), /claim retained/);
  assert.equal((await notifyWalkthrough(pool, failure, 2)).status, 'suppressed');
  assert.equal(axios.post.mock.callCount(), 3);
  assert.deepEqual((await pool.query('SELECT status FROM walkthrough_notification_claims ORDER BY status')).rows.map(x=>x.status), ['failed_or_uncertain', 'sent']);
});
