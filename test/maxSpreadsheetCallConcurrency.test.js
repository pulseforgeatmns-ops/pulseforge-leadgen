'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { withCallAuthorization, withEmailAuthorization } = require('../utils/callEligibility');
const { PostgresSpreadsheetProposalStore } = require('../packages/max/stateIngestion/spreadsheetProposalStore');
function barrier() { let release; const promise = new Promise(resolve => { release = resolve; }); return { promise, release }; }

test('real PostgreSQL serializes call provider handoff against approved suppression in both orders', { timeout: 60000 }, async t => {
  const instance = await startDisposablePostgres('spreadsheet-call-lock-');
  assert.equal(new URL(instance.connectionString).hostname, '127.0.0.1');
  const db = new Pool({ connectionString: instance.connectionString, max: 6 });
  t.after(async () => { await db.end(); await instance.stop(); });
  await db.query(fs.readFileSync(path.join(__dirname, 'fixtures/maxSpreadsheetBaseSchema.sql'), 'utf8'));
  await db.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-07-max-spreadsheet-reliability.sql'), 'utf8'));
  await db.query(`ALTER TABLE prospects ADD COLUMN IF NOT EXISTS do_not_contact boolean DEFAULT false;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS is_synthetic boolean DEFAULT false;
    INSERT INTO clients VALUES(1);
    INSERT INTO users(id,client_id,name,role) VALUES(10,1,'AO','ao'),(11,1,'Test Jake','admin');`);
  const store = new PostgresSpreadsheetProposalStore(db, { clientId: 1, aoId: 10, approverUserId: 11 });
  async function setup({ admissionHold = false } = {}) {
    const company = await db.query("INSERT INTO companies(client_id,name) VALUES(1,'Call concurrency fixture') RETURNING id");
    const account = await db.query('INSERT INTO prospects(client_id,assigned_ao_id,company_id) VALUES(1,10,$1) RETURNING id', [company.rows[0].id]);
    const accountId = account.rows[0].id;
    const scope = { actorId: 10, conversationId: randomUUID(), sourceHash: 'a'.repeat(64) };
    const operation = { id: randomUUID(), type: 'SUPPRESS_CALL', target: { accountId }, before: null,
      after: { channel: 'call', reason: 'Explicit remove from call list' }, blocked: false,
      evidence: [{ fileHash: scope.sourceHash, sheet: 'Sheet1', row: 11, cell: 'K11', rawValue: 'No - Remove from call list' }] };
    if (admissionHold) Object.assign(operation, {
      type: 'SET_ACCOUNT_FIELD', field: 'email', after: 'imported@example.test', outreachReviewRequired: true,
      evidence: [{ fileHash: scope.sourceHash, sheet: 'Sheet1', row: 4, cell: 'D4', rawValue: 'imported@example.test' }],
    });
    const proposal = await store.createProposal({ ...scope, plan: { operations: [operation] }, baseline: await store.snapshotContext() });
    const commit = targetStore => (targetStore || store).commitProposal({ ...scope, proposalId: proposal.id,
      selectedOperationIds: [operation.id], expectedDigest: proposal.digest, approvedBy: 11, idempotencyKey: randomUUID() });
    return { accountId, identities: [{ prospectId: accountId, clientId: 1 }], commit };
  }
  async function waitForAdvisoryWaiter() {
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      const result = await db.query("SELECT count(*)::int AS count FROM pg_locks WHERE locktype='advisory' AND NOT granted");
      if (result.rows[0].count > 0) return;
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.fail('Expected second PostgreSQL connection to wait for matching advisory lock');
  }
  await t.test('provider handoff first: suppression waits, commits afterward, blocks every subsequent call', async () => {
    const { accountId, identities, commit } = await setup();
    const entered = barrier(), finishHandoff = barrier(); let calls = 0;
    const call = withCallAuthorization(db, identities, async () => { calls++; entered.release(); await finishHandoff.promise; return 'local fake provider accepted'; });
    await entered.promise;
    const suppression = commit();
    try {
      await waitForAdvisoryWaiter();
      assert.equal((await db.query('SELECT ao_call_suppressed FROM prospects WHERE id=$1', [accountId])).rows[0].ao_call_suppressed, false);
    } finally { finishHandoff.release(); }
    assert.equal(await call, 'local fake provider accepted');
    assert.equal((await suppression).committed, true);
    await assert.rejects(withCallAuthorization(db, identities, async () => { calls++; }), { code: 'CALL_SUPPRESSED' });
    assert.equal(calls, 1);
    let emails = 0;
    await withEmailAuthorization(db, identities, async () => { emails++; });
    assert.equal(emails, 1, 'call-only suppression must not become email suppression');
  });
  await t.test('suppression first: provider waits until commit, rereads suppression and is never invoked', async () => {
    const { identities, commit } = await setup();
    const acquired = barrier(), continueCommit = barrier(); let calls = 0;
    // Instrument only the scheduling boundary; every SQL statement still runs
    // against the real disposable database using separate connections.
    const pausedDb = { query: db.query.bind(db), async connect() {
      const client = await db.connect();
      return { release: client.release.bind(client), async query(sql, values) {
        const result = await client.query(sql, values);
        if (/pg_advisory_xact_lock/.test(typeof sql === 'string' ? sql : sql.text)) { acquired.release(); await continueCommit.promise; }
        return result;
      } };
    } };
    const pausedStore = new PostgresSpreadsheetProposalStore(pausedDb, { clientId: 1, aoId: 10, approverUserId: 11 });
    const suppression = commit(pausedStore);
    await acquired.promise;
    const call = withCallAuthorization(db, identities, async () => { calls++; });
    // Attach rejection handler before releasing the barrier.
    const rejectedCall = assert.rejects(call, { code: 'CALL_SUPPRESSED' });
    try { await waitForAdvisoryWaiter(); assert.equal(calls, 0); }
    finally { continueCommit.release(); }
    assert.equal((await suppression).committed, true);
    await rejectedCall; assert.equal(calls, 0);
  });
  await t.test('email handoff first: imported contact hold waits and blocks every later email', async () => {
    const { accountId, identities, commit } = await setup({ admissionHold: true });
    const entered = barrier(), finishHandoff = barrier(); let emails = 0;
    const email = withEmailAuthorization(db, identities, async () => { emails++; entered.release(); await finishHandoff.promise; return 'local fake SMTP accepted'; });
    await entered.promise;
    const hold = commit();
    try {
      await waitForAdvisoryWaiter();
      assert.equal((await db.query('SELECT ao_outreach_review_required FROM prospects WHERE id=$1', [accountId])).rows[0].ao_outreach_review_required, false);
    } finally { finishHandoff.release(); }
    assert.equal(await email, 'local fake SMTP accepted');
    assert.equal((await hold).committed, true);
    await assert.rejects(withEmailAuthorization(db, identities, async () => { emails++; }), { code: 'outreach_review_required' });
    assert.equal(emails, 1);
  });
  await t.test('imported contact hold first: pending email rereads the hold and never reaches its provider', async () => {
    const { identities, commit } = await setup({ admissionHold: true });
    const acquired = barrier(), continueCommit = barrier(); let emails = 0;
    const pausedDb = { query: db.query.bind(db), async connect() {
      const client = await db.connect();
      return { release: client.release.bind(client), async query(sql, values) {
        const result = await client.query(sql, values);
        if (/pg_advisory_xact_lock/.test(typeof sql === 'string' ? sql : sql.text)) { acquired.release(); await continueCommit.promise; }
        return result;
      } };
    } };
    const pausedStore = new PostgresSpreadsheetProposalStore(pausedDb, { clientId: 1, aoId: 10, approverUserId: 11 });
    const hold = commit(pausedStore);
    await acquired.promise;
    const email = withEmailAuthorization(db, identities, async () => { emails++; });
    const rejectedEmail = assert.rejects(email, { code: 'outreach_review_required' });
    try { await waitForAdvisoryWaiter(); assert.equal(emails, 0); }
    finally { continueCommit.release(); }
    assert.equal((await hold).committed, true);
    await rejectedEmail;
    assert.equal(emails, 0);
  });

});
