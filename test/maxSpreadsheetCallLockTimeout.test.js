'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { callLockKey, withCallAuthorization } = require('../utils/callEligibility');

test('real PostgreSQL bounds call lock waits and never leaks transaction-local timeout settings', { timeout: 30000 }, async t => {
  const instance = await startDisposablePostgres('spreadsheet-call-timeout-');
  assert.equal(new URL(instance.connectionString).hostname, '127.0.0.1');
  const db = new Pool({ connectionString: instance.connectionString, max: 1 });
  const holder = new Pool({ connectionString: instance.connectionString, max: 1 });
  const swallowPoolError = () => {};
  db.on('error', swallowPoolError);
  holder.on('error', swallowPoolError);
  t.after(async () => {
    db.off('error', swallowPoolError);
    holder.off('error', swallowPoolError);
    await holder.end();
    await db.end();
    await instance.stop();
  });
  await db.query(`CREATE TABLE prospects(id text, client_id integer, do_not_contact boolean, is_synthetic boolean, ao_call_suppressed boolean, ao_outreach_review_required boolean);
    INSERT INTO prospects VALUES('fixture',10,false,false,false,false);
    SET lock_timeout='42s'; SET statement_timeout='43s'; SET idle_in_transaction_session_timeout='44s';`);
  const settings = async () => (await db.query(`SELECT pg_backend_pid() AS pid,
    current_setting('lock_timeout') AS lock_timeout,
    current_setting('statement_timeout') AS statement_timeout,
    current_setting('idle_in_transaction_session_timeout') AS idle_timeout`)).rows[0];
  const before = await settings();
  const identities = [{ prospectId: 'fixture', clientId: 10 }];
  assert.equal(await withCallAuthorization(db, identities, async () => 'local provider stub'), 'local provider stub');
  assert.deepEqual(await settings(), before, 'successful handoff must restore all preexisting connection settings');

  const key = callLockKey('fixture', 10);
  await holder.query('SELECT pg_advisory_lock(hashtextextended($1,0))', [key]);
  let providerCalls = 0;
  const started = Date.now();
  try {
    await assert.rejects(withCallAuthorization(db, identities, async () => { providerCalls++; }), error => ['55P03', '57014'].includes(error.code));
    assert.equal(providerCalls, 0);
    assert.ok(Date.now() - started < 7000, 'server lock deadline must stop waiting before driver fallback');
    const after = await settings();
    assert.notEqual(after.pid, before.pid, 'failed/uncertain session must be destroyed');
    assert.equal(after.lock_timeout, '0');
    assert.equal(after.statement_timeout, '0');
    assert.equal(after.idle_timeout, '0');
  } finally {
    await holder.query('SELECT pg_advisory_unlock(hashtextextended($1,0))', [key]);
  }
  assert.equal(await withCallAuthorization(db, identities, async () => 'retry stub'), 'retry stub');
});
