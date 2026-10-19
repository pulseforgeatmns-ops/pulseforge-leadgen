'use strict';

// Never uses DATABASE_URL: every fixture lives in this disposable local cluster.
const test = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { GovernedOutboundStore } = require('../services/governedOutboundStore');
const { reconcileUncertainItemFromEvidence } = require('../services/governedUncertainSendReconciliation');
const { EXECUTOR_LOCK_NAMESPACE, withTenantOutreachExecutorLock } = require('../services/tenantOutreachExecutorLock');

test('reconciliation attempt identity, dispatch exclusion and CAS on native PostgreSQL', { timeout: 60000 }, async t => {
  const pg = await startDisposablePostgres('anchor-reconciliation-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 10 });
  t.after(async () => { await pool.end(); await pg.stop(); });
  await pool.query(`
    CREATE TABLE acquisition_outbound_envelopes(id text PRIMARY KEY,tenant_id text,mission_id text,local_day date,program_id text);
    CREATE TABLE acquisition_outbound_items(id text PRIMARY KEY,tenant_id text,envelope_id text,candidate_id text,prospect_id text,email text,status text,reason text,attempted_at timestamptz,provider_message_id text);
    CREATE TABLE acquisition_outbound_events(id text PRIMARY KEY,tenant_id text,program_id text,envelope_id text,item_id text,event_type text,payload jsonb,created_at timestamptz DEFAULT now());
    CREATE TABLE acquisition_mission_outbound_executions(id text PRIMARY KEY,tenant_id text,mission_id text,prospect_id text,status text,provider_message_id text,sent_at timestamptz,attempted_at timestamptz,provider_error_code text,provider_error_message text,payload jsonb,updated_at timestamptz);
    CREATE TABLE tenant_outreach_scheduled_sends(id text,tenant_id text,status text,outbound_message_id text,prospect_id text,recipient_email text,mission_id text,skip_reason text,failure_code text,failure_message text,executed_at timestamptz,created_at timestamptz);
    CREATE TABLE tenant_outreach_messages(id text,tenant_id text,direction text,status text,provider_message_id text,rfc_message_id text,sent_at timestamptz,prospect_id text,recipients jsonb,created_at timestamptz);
    CREATE TABLE agent_log(id text,client_id int,agent_name text,prospect_id text,ran_at timestamptz,action text,status text,payload jsonb);
  `);
  async function reset() {
    await pool.query(`TRUNCATE acquisition_outbound_events,acquisition_outbound_items,acquisition_outbound_envelopes,acquisition_mission_outbound_executions,tenant_outreach_scheduled_sends,tenant_outreach_messages,agent_log;
      INSERT INTO acquisition_outbound_envelopes VALUES('env','10','mission','2026-10-16','program');
      INSERT INTO acquisition_outbound_items VALUES('item','10','env','prospect','prospect','fixture@example.invalid','uncertain','ak_observed_provenance_required','2026-10-16T17:08:03.123456Z',NULL);
      INSERT INTO acquisition_mission_outbound_executions(id,tenant_id,mission_id,prospect_id,status,attempted_at,payload) VALUES('execution','10','mission','prospect','attempted','2026-10-16T17:08:03.123456Z','{"email":"fixture@example.invalid"}');
      INSERT INTO acquisition_outbound_events(id,tenant_id,envelope_id,item_id,event_type,payload)
        SELECT 'proof','10','env','item','pre_provider_proof',jsonb_build_object('itemId',id,'envelopeId',envelope_id,'attemptToken',attempted_at::text,'providerBoundaryCrossed',false,'reason',reason)
        FROM acquisition_outbound_items WHERE id='item';`);
  }
  const item = async () => (await pool.query("SELECT *,attempted_at::text AS attempt_token FROM acquisition_outbound_items WHERE id='item'")).rows[0];
  const events = async () => Number((await pool.query('SELECT count(*) AS n FROM acquisition_outbound_events')).rows[0].n);
  const store = new GovernedOutboundStore(pool, '10');

  await t.test('inspection succeeds inside a database-enforced read-only transaction', async () => {
    await reset();
    const db = await pool.connect();
    try {
      await db.query('BEGIN READ ONLY');
      const result = await reconcileUncertainItemFromEvidence(db, '10', 'item', { dryRun: true });
      assert.equal(result.classification.outcome, 'PROVEN_UNSENT');
      assert.equal(result.applied, false);
      await db.query('ROLLBACK');
    } finally { db.release(); }
    assert.equal(await events(), 1);
    assert.equal((await item()).status, 'uncertain');
  });

  await t.test('exact microsecond attempt proof applies once and releases both locks', async () => {
    await reset();
    const result = await reconcileUncertainItemFromEvidence(pool, '10', 'item');
    assert.equal(result.applied, true);
    assert.equal((await item()).status, 'pending');
    assert.equal((await reconcileUncertainItemFromEvidence(pool, '10', 'item')).reason, 'already_reconciled');
    assert.equal(await withTenantOutreachExecutorLock(pool, async () => 'released'), 'released');
    assert.equal(await store.lock(async () => 'released'), 'released');
  });

  for (const scheduler of [false, true]) {
    await t.test(`${scheduler ? 'scheduler' : 'tenant'} overlap performs no inspection writes`, async () => {
      await reset();
      const db = await pool.connect();
      const keys = scheduler ? [EXECUTOR_LOCK_NAMESPACE, 0] : [store.ctx.advisoryLockNamespace, store.ctx.advisoryLockKey];
      try {
        await db.query('SELECT pg_advisory_lock($1,$2)', keys);
        const result = await reconcileUncertainItemFromEvidence(pool, '10', 'item');
        assert.equal(result.halted, scheduler ? 'scheduler_overlap' : 'overlap');
        assert.equal(result.applied, false);
        assert.equal(result.retryAllowed, false);
        assert.equal(await events(), 1);
      } finally { await db.query('SELECT pg_advisory_unlock($1,$2)', keys); db.release(); }
    });
  }

  await t.test('old proof cannot release a newer abandoned attempt', async () => {
    await reset();
    await pool.query("UPDATE acquisition_outbound_items SET attempted_at=attempted_at+interval '1 hour',reason='abandoned_attempt'");
    const result = await reconcileUncertainItemFromEvidence(pool, '10', 'item');
    assert.equal(result.classification.outcome, 'UNKNOWN');
    assert.equal(result.applied, false);
    assert.equal((await item()).status, 'uncertain');
  });

  await t.test('failed mailbox status with durable receipt stays held', async () => {
    await reset();
    await pool.query("INSERT INTO tenant_outreach_messages(id,tenant_id,direction,status,provider_message_id,prospect_id) VALUES('mail','10','OUTBOUND','failed','accepted','prospect')");
    const result = await reconcileUncertainItemFromEvidence(pool, '10', 'item');
    assert.equal(result.classification.outcome, 'SKIP');
    assert.equal(result.applied, false);
  });

  await t.test('failed canonical execution with sent_at stays held', async () => {
    await reset();
    await pool.query("UPDATE acquisition_mission_outbound_executions SET status='failed',sent_at=attempted_at+interval '1 second'");
    const result = await reconcileUncertainItemFromEvidence(pool, '10', 'item');
    assert.equal(result.classification.outcome, 'SKIP');
    assert.equal(result.applied, false);
    assert.equal((await item()).status, 'uncertain');
  });

  await t.test('diagnostic limits cannot hide an older durable receipt behind newer failures', async () => {
    await reset();
    await pool.query(`INSERT INTO tenant_outreach_messages(id,tenant_id,direction,status,provider_message_id,prospect_id,created_at)
      VALUES('old-mail','10','OUTBOUND','failed','accepted','prospect','2026-10-01');
      INSERT INTO tenant_outreach_messages(id,tenant_id,direction,status,prospect_id,created_at)
      SELECT 'new-mail-'||n,'10','OUTBOUND','failed','prospect','2026-10-16'::timestamptz+n*interval '1 second' FROM generate_series(1,25) n;`);
    const result = await reconcileUncertainItemFromEvidence(pool, '10', 'item');
    assert.equal(result.classification.outcome, 'SKIP');
    assert.equal(result.applied, false);
  });

  for (const mutation of [
    "provider_message_id='late-receipt'",
    "attempted_at=attempted_at+interval '0.000001 second'",
    "status='sent'",
    "reason='provider_acceptance_unknown'",
  ]) {
    await t.test(`release CAS rejects concurrent ${mutation}`, async () => {
      await reset();
      const stale = await item();
      // Mutate from another connection after the inspected version was read.
      await pool.query(`UPDATE acquisition_outbound_items SET ${mutation} WHERE id='item'`);
      const before = await item();
      await assert.rejects(store.releaseUnsent(stale, 'reconciled_not_sent', {
        reconciled: true, providerOutcome: 'PROVIDER_CONFIRMED_NOT_SENT',
        canonicalPreProviderRejection: true, missionId: 'mission',
        expectedAttempt: { status: stale.status, reason: stale.reason, attemptedAt: stale.attempt_token },
      }), error => error.code === 'item_not_uncertain');
      assert.deepEqual(await item(), before);
      assert.equal((await pool.query("SELECT status FROM acquisition_mission_outbound_executions WHERE id='execution'")).rows[0].status, 'attempted', 'canonical mutation must roll back with failed CAS');
      assert.equal(await events(), 1);
    });
  }

  await t.test('scheduler cannot dispatch while reconciliation is inspecting under both locks', async () => {
    await reset();
    let enter; const entered = new Promise(resolve => { enter = resolve; });
    let resume; const resumed = new Promise(resolve => { resume = resolve; });
    const wrapped = {
      connect: pool.connect.bind(pool),
      async query(sql, params) {
        if (/SELECT i\.\*/.test(sql)) { enter(); await resumed; }
        return pool.query(sql, params);
      },
    };
    const applying = reconcileUncertainItemFromEvidence(wrapped, '10', 'item');
    await entered;
    try {
      const dispatch = await withTenantOutreachExecutorLock(pool, async () => assert.fail('provider dispatch must remain excluded'));
      assert.equal(dispatch.halted, 'scheduler_overlap');
    } finally { resume(); }
    assert.equal((await applying).applied, true);
  });
});
