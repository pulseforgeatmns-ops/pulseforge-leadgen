'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { performance } = require('node:perf_hooks');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { PostgresSpreadsheetProposalStore } = require('../packages/max/stateIngestion/spreadsheetProposalStore');
const { withCallAuthorization, withEmailAuthorization, withSmsAuthorization } = require('../utils/callEligibility');

// These are bounded correctness regressions for the real, global table lock.
// They are not a load test or evidence of a production latency/throughput SLO.
const LOCKED_TABLES = [
  'users', 'prospects', 'companies', 'ao_leads', 'ao_contacts', 'ao_follow_up_tasks',
  'ao_prospect_activity', 'touchpoints', 'ao_prospect_tasks', 'max_ao_follow_up_tasks',
  'tenant_outreach_suppressions', 'prospect_notes', 'prospect_lifecycle_events',
  'max_spreadsheet_contacts', 'max_spreadsheet_suppressions', 'max_spreadsheet_relationships',
].sort();
const isImportLock = sql => /^LOCK TABLE\b/.test(typeof sql === 'string' ? sql : sql.text);
const jsonValue = value => JSON.parse(JSON.stringify(value));

function barrier() {
  let release;
  const promise = new Promise(resolve => { release = resolve; });
  return { promise, release };
}

async function bounded(promise, description, milliseconds = 10000) {
  let timer;
  try {
    return await Promise.race([promise, new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`Timed out: ${description}`)), milliseconds);
    })]);
  } finally { clearTimeout(timer); }
}

async function eventually(check, description, milliseconds = 3000) {
  const deadline = performance.now() + milliseconds;
  do {
    const value = await check();
    if (value) return value;
    // Poll an observed database condition; elapsed time never establishes order.
    await new Promise(resolve => setTimeout(resolve, 15));
  } while (performance.now() < deadline);
  assert.fail(description);
}

async function waitingOn(observer, waiter, blocker, table, mode) {
  return eventually(async () => {
    const { rows } = await observer.query(`SELECT l.pid, l.mode, a.query, pg_blocking_pids(l.pid) AS blockers
      FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
      WHERE l.pid=$1 AND l.locktype='relation' AND l.relation=$2::regclass
        AND l.mode=$3 AND NOT l.granted AND $4=ANY(pg_blocking_pids(l.pid))`,
    [waiter, table, mode, blocker]);
    return rows[0];
  }, `Backend ${waiter} must wait on ${blocker} for ${table} ${mode}`);
}

async function setupCase(t, observer, connectionString, firstClientId) {
  const db = new Pool({ connectionString, max: 8, connectionTimeoutMillis: 3000, query_timeout: 10000 });
  const pending = [], barriers = [], borrowed = new Set(), pids = new Set();
  const context = {
    db, observer, pids,
    track(promise) {
      // Attach a rejection handler in the same turn, including expected timeouts.
      const settled = promise.then(value => ({ value }), error => ({ error }));
      pending.push(settled);
      return promise;
    },
    barrier() { const value = barrier(); barriers.push(value); return value; },
    async connect(ownedByOperation = false) {
      const client = await db.connect();
      const entry = { client, released: false, ownedByOperation };
      borrowed.add(entry); pids.add(client.processID);
      return {
        pid: client.processID,
        query: client.query.bind(client),
        release(discard = false) {
          assert.equal(entry.released, false, 'each checked-out client must be released exactly once');
          entry.released = true; borrowed.delete(entry); client.release(discard);
        },
      };
    },
  };
  t.after(async () => {
    // Failure cleanup first opens every scheduling barrier, then rolls back any
    // held fixture writer. Production queries retain their own finite deadlines.
    for (const value of barriers) value.release();
    await Promise.allSettled([...borrowed].filter(entry => !entry.ownedByOperation).map(async entry => {
      try { await entry.client.query('ROLLBACK'); } catch (_) { /* discard below */ }
    }));
    await Promise.allSettled(pending);
    for (const entry of borrowed) {
      if (!entry.released) { entry.released = true; entry.client.release(true); }
    }
    borrowed.clear();
    await db.end();
  });
  context.instrument = ({ pause = false, inspectTimeout = false } = {}) => {
    const attempted = context.barrier(), acquired = context.barrier(), resume = context.barrier();
    const monitor = { attempted, acquired, resume, settings: null, lockStartedAt: null, releases: [] };
    monitor.pool = {
      query: db.query.bind(db),
      async connect() {
        const client = await context.connect(true);
        return {
          release(discard) { monitor.releases.push({ pid: client.pid, discard }); client.release(discard); },
          async query(sql, values) {
            const lock = isImportLock(sql);
            if (lock) {
              if (inspectTimeout) monitor.settings = (await client.query(`SELECT
                current_setting('lock_timeout') AS lock_timeout,
                current_setting('statement_timeout') AS statement_timeout`)).rows[0];
              monitor.lockStartedAt = performance.now();
              attempted.release(client.pid);
            }
            // Never replace, shorten, or mock the actual import SQL/lock.
            const result = await client.query(sql, values);
            if (lock) {
              acquired.release(client.pid);
              if (pause) await bounded(resume.promise, 'release acquired import lock', 12000);
            }
            return result;
          },
        };
      },
    };
    return monitor;
  };
  context.writer = async () => {
    const client = await context.connect();
    await client.query('BEGIN');
    await client.query("SET LOCAL statement_timeout='10s'");
    return client;
  };
  context.assertClean = async () => {
    await eventually(async () => {
      const { rows } = await observer.query(`SELECT
        (SELECT count(*)::int FROM pg_locks WHERE pid=ANY($1::int[])
          AND locktype IN ('relation','advisory')) AS locks,
        (SELECT count(*)::int FROM pg_stat_activity WHERE pid=ANY($1::int[])
          AND (xact_start IS NOT NULL OR state LIKE 'idle in transaction%')) AS transactions`, [[...pids]]);
      return rows[0].locks === 0 && rows[0].transactions === 0;
    }, 'All participating backends must release their transaction and advisory/table locks');
    assert.equal(borrowed.size, 0, 'no checked-out connection may leak');
    assert.equal(db.waitingCount, 0, 'no queued connection checkout may leak');
    assert.equal(db.idleCount, db.totalCount, 'all surviving pool clients must be idle');
  };
  for (const [name, clientId] of [['a', firstClientId], ['b', firstClientId + 1]]) {
    const aoId = clientId * 10, approverUserId = aoId + 1;
    await observer.query('INSERT INTO clients(id) VALUES($1)', [clientId]);
    await observer.query(`INSERT INTO users(id,client_id,name,role) VALUES
      ($1,$3,'Fixture AO','ao'),($2,$3,'Fixture approver','admin')`, [aoId, approverUserId, clientId]);
    const company = (await observer.query('INSERT INTO companies(client_id,name) VALUES($1,$2) RETURNING id',
      [clientId, `Tenant ${name} fixture`])).rows[0];
    const accountId = randomUUID();
    await observer.query(`INSERT INTO prospects(id,client_id,company_id,assigned_ao_id,first_name,phone)
      VALUES($1,$2,$3,$4,$5,$6)`, [accountId, clientId, company.id, aoId, `Tenant ${name}`, `fixture-${name}`]);
    const options = { clientId, aoId, approverUserId };
    const store = new PostgresSpreadsheetProposalStore(context.instrument().pool, options);
    const scope = { actorId: aoId, conversationId: randomUUID(), sourceHash: (name === 'a' ? 'a' : 'b').repeat(64) };
    const tenant = { clientId, aoId, accountId, options, store, scope };
    tenant.note = { id: 'note', type: 'ADD_NOTE', target: { accountId }, before: null,
      after: { text: `Tenant ${name} approved note` }, blocked: false,
      evidence: [{ sheet: 'Sheet1', row: 3, cell: 'L3', rawValue: `Tenant ${name} approved note` }] };
    tenant.hold = { ...tenant.note, id: 'email', type: 'SET_ACCOUNT_FIELD', field: 'email',
      after: `${name}-${clientId}@example.test`, outreachReviewRequired: true };
    tenant.prepare = async (operations = [tenant.note], idempotencyKey = randomUUID()) => {
      const proposal = await store.createProposal({ ...scope, plan: { operations }, baseline: await store.snapshotContext() });
      const request = { ...scope, proposalId: proposal.id, expectedDigest: proposal.digest,
        selectedOperationIds: operations.map(operation => operation.id), approvedBy: approverUserId, idempotencyKey };
      return { proposal, request, commit: (monitor) => context.track((monitor
        ? new PostgresSpreadsheetProposalStore(monitor.pool, options) : store).commitProposal(request)) };
    };
    tenant.read = async () => (await observer.query('SELECT * FROM prospects WHERE id=$1 AND client_id=$2', [accountId, clientId])).rows[0];
    tenant.initial = await tenant.read();
    assert.deepEqual((await store.snapshotContext()).prospects.map(row => row.id), [accountId]);
    context[name] = tenant;
  }
  return context;
}

async function assertPending(context, tenant, proposal) {
  const { rows } = await context.observer.query(`SELECT status,receipt,approved_by,approved_at,
    (SELECT count(*)::int FROM max_spreadsheet_effects WHERE proposal_id=$1) AS effects,
    (SELECT count(*)::int FROM max_spreadsheet_commit_requests WHERE proposal_id=$1) AS requests
    FROM max_spreadsheet_proposals WHERE id=$1`, [proposal.id]);
  assert.deepEqual(rows[0], { status: 'pending', receipt: null, approved_by: null, approved_at: null, effects: 0, requests: 0 });
  assert.deepEqual(await tenant.read(), tenant.initial, 'failed/blocked import must not partially change its prospect');
  assert.equal((await context.observer.query('SELECT count(*)::int AS count FROM ao_prospect_activity WHERE tenant_id=$1', [tenant.clientId])).rows[0].count, 0);
}

async function assertCommitted(context, tenant, proposal, receipt, operations = 1) {
  assert.equal(receipt.committed, true);
  assert.equal(receipt.proposalId, proposal.id);
  assert.equal(receipt.results.length, operations);
  const { rows } = await context.observer.query(`SELECT status,receipt,
    (SELECT count(*)::int FROM max_spreadsheet_effects WHERE proposal_id=$1 AND client_id=$2) AS effects,
    (SELECT count(*)::int FROM max_spreadsheet_commit_requests WHERE proposal_id=$1 AND client_id=$2) AS requests
    FROM max_spreadsheet_proposals WHERE id=$1 AND client_id=$2`, [proposal.id, tenant.clientId]);
  assert.equal(rows[0].status, 'committed');
  const { replayed, ...originalReceipt } = receipt;
  assert.deepEqual(rows[0].receipt, jsonValue(originalReceipt));
  assert.equal(rows[0].effects, operations);
  assert.equal(rows[0].requests, 1);
  const activities = (await context.observer.query('SELECT * FROM ao_prospect_activity WHERE tenant_id=$1', [tenant.clientId])).rows;
  assert.equal(activities.length, 1, 'an approved note must be applied exactly once');
  assert.equal(activities[0].prospect_id, tenant.accountId);
  assert.equal(activities[0].ao_id, tenant.aoId);
  const snapshot = await tenant.store.snapshotContext();
  assert.deepEqual(snapshot.prospects.map(row => row.id), [tenant.accountId]);
  assert.ok(snapshot.effects.every(effect => effect.operation.target.accountId === tenant.accountId && effect.verifiedPresent));
}

async function assertNoImportWrites(context, tenant) {
  const { rows } = await context.observer.query(`SELECT
    (SELECT count(*)::int FROM max_spreadsheet_proposals WHERE client_id=$1) AS proposals,
    (SELECT count(*)::int FROM max_spreadsheet_effects WHERE client_id=$1) AS effects,
    (SELECT count(*)::int FROM max_spreadsheet_commit_requests WHERE client_id=$1) AS requests,
    (SELECT count(*)::int FROM ao_prospect_activity WHERE tenant_id=$1) AS activities`, [tenant.clientId]);
  assert.deepEqual(rows[0], { proposals: 0, effects: 0, requests: 0, activities: 0 });
}

test('real PostgreSQL cross-tenant import locking has bounded, isolated outcomes', { timeout: 90000 }, async t => {
  const instance = await startDisposablePostgres('spreadsheet-cross-tenant-');
  assert.equal(new URL(instance.connectionString).hostname, '127.0.0.1');
  const observer = new Pool({ connectionString: instance.connectionString, max: 2, query_timeout: 10000 });
  t.after(async () => { try { await observer.end(); } finally { await instance.stop(); } });
  await observer.query(fs.readFileSync(path.join(__dirname, 'fixtures/maxSpreadsheetBaseSchema.sql'), 'utf8'));
  await observer.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-07-max-spreadsheet-reliability.sql'), 'utf8'));
  await observer.query('ALTER TABLE prospects ADD COLUMN do_not_contact boolean DEFAULT false, ADD COLUMN is_synthetic boolean DEFAULT false');
  let nextClientId = 1;
  const setup = async subtest => {
    const firstClientId = nextClientId; nextClientId += 2;
    return setupCase(subtest, observer, instance.connectionString, firstClientId);
  };

  await t.test('tenant B CRM transaction blocks tenant A actual table lock, then A commits without B contamination', async subtest => {
    const context = await setup(subtest), { a, b } = context;
    const prepared = await a.prepare();
    const writer = await context.writer();
    await writer.query('UPDATE prospects SET phone=$1 WHERE id=$2 AND client_id=$3', ['B CRM edit', b.accountId, b.clientId]);
    const monitor = context.instrument(), importing = prepared.commit(monitor);
    const pid = await bounded(monitor.attempted.promise, 'import to attempt table lock');
    const wait = await waitingOn(observer, pid, writer.pid, 'prospects', 'ShareRowExclusiveLock');
    assert.match(wait.query, /^LOCK TABLE .* IN SHARE ROW EXCLUSIVE MODE$/);
    await assertPending(context, a, prepared.proposal);
    await writer.query('COMMIT'); writer.release();
    const receipt = await importing;
    await assertCommitted(context, a, prepared.proposal, receipt);
    assert.deepEqual(await b.read(), { ...b.initial, phone: 'B CRM edit' });
    await assertNoImportWrites(context, b);
    await context.assertClean();
  });

  await t.test('tenant A acquired full table lock times out a B writer; rollback and later retry preserve B', async subtest => {
    const context = await setup(subtest), { a, b } = context;
    const prepared = await a.prepare(), monitor = context.instrument({ pause: true });
    const importing = prepared.commit(monitor);
    const pid = await bounded(monitor.acquired.promise, 'import to acquire full table lock');
    const locks = await observer.query(`SELECT c.relname FROM pg_locks l JOIN pg_class c ON c.oid=l.relation
      WHERE l.pid=$1 AND l.locktype='relation' AND l.mode='ShareRowExclusiveLock' AND l.granted ORDER BY c.relname`, [pid]);
    assert.deepEqual(locks.rows.map(row => row.relname), LOCKED_TABLES);
    const writer = await context.writer();
    // Bound only the fixture writer, with more time than the observation window.
    // The production importer keeps its unchanged five-second lock timeout.
    await writer.query("SET LOCAL lock_timeout='5s'");
    const writeSql = 'UPDATE prospects SET phone=$1 WHERE id=$2 AND client_id=$3';
    const values = ['B retried CRM edit', b.accountId, b.clientId];
    const writing = context.track(writer.query(writeSql, values));
    await waitingOn(observer, writer.pid, pid, 'prospects', 'RowExclusiveLock');
    await assert.rejects(writing, error => error.code === '55P03' && /lock timeout/.test(error.message));
    await writer.query('ROLLBACK'); writer.release();
    assert.deepEqual(await b.read(), b.initial);
    monitor.resume.release();
    await assertCommitted(context, a, prepared.proposal, await importing);
    const retry = await context.writer();
    assert.equal((await retry.query(writeSql, values)).rowCount, 1);
    await retry.query('COMMIT'); retry.release();
    assert.deepEqual(await b.read(), { ...b.initial, phone: values[0] });
    await assertNoImportWrites(context, b);
    await context.assertClean();
  });

  await t.test('production five-second import lock timeout rolls back every partial lock/write and permits exact retry', async subtest => {
    const context = await setup(subtest), { a, b } = context;
    const prepared = await a.prepare([a.note, a.hold]);
    const writer = await context.writer();
    await writer.query('UPDATE prospects SET phone=$1 WHERE id=$2 AND client_id=$3', ['B held edit', b.accountId, b.clientId]);
    const monitor = context.instrument({ inspectTimeout: true }), importing = prepared.commit(monitor);
    const pid = await bounded(monitor.attempted.promise, 'import to attempt table lock');
    await waitingOn(observer, pid, writer.pid, 'prospects', 'ShareRowExclusiveLock');
    assert.deepEqual(monitor.settings, { lock_timeout: '5s', statement_timeout: '15s' });
    const partial = await observer.query(`SELECT locktype, mode FROM pg_locks WHERE pid=$1 AND granted
      AND (locktype='advisory' OR (relation='users'::regclass AND mode='ShareRowExclusiveLock'))`, [pid]);
    assert.equal(partial.rows.filter(row => row.locktype === 'advisory').length, 1);
    assert.equal(partial.rows.filter(row => row.mode === 'ShareRowExclusiveLock').length, 1);
    await assert.rejects(importing, error => error.code === '55P03' && /lock timeout/.test(error.message));
    assert.ok(performance.now() - monitor.lockStartedAt >= 4500, 'production five-second lock deadline must not be shortened by the test');
    assert.equal((await observer.query('SELECT xact_start IS NOT NULL AS held FROM pg_stat_activity WHERE pid=$1', [writer.pid])).rows[0].held, true);
    await assertPending(context, a, prepared.proposal);
    await eventually(async () => (await observer.query(`SELECT count(*)::int AS count FROM pg_locks
      WHERE pid=$1 AND locktype IN ('relation','advisory')`, [pid])).rows[0].count === 0,
    'failed import must release even locks acquired before its timeout');
    assert.equal(monitor.releases.length, 1);
    assert.equal(monitor.releases[0].discard, true, 'uncertain timeout session must be discarded');
    await writer.query('COMMIT'); writer.release();
    const receipt = await prepared.commit(); // Identical proposal, digest, selection and idempotency key.
    await assertCommitted(context, a, prepared.proposal, receipt, 2);
    assert.equal((await a.read()).ao_outreach_review_required, true);
    assert.equal((await a.read()).email, a.hold.after);
    const replay = await prepared.commit();
    assert.deepEqual(replay, { ...jsonValue(receipt), replayed: true });
    await assertCommitted(context, a, prepared.proposal, receipt, 2);
    assert.deepEqual(await b.read(), { ...b.initial, phone: 'B held edit' });
    await assertNoImportWrites(context, b);
    await context.assertClean();
  });

  await t.test('independent tenant imports serialize, preserve tenant scope and replay without duplicate effects', async subtest => {
    const context = await setup(subtest), { a, b } = context;
    const first = await a.prepare([a.note], 'same-key-different-tenant');
    const second = await b.prepare([b.note], 'same-key-different-tenant');
    const aMonitor = context.instrument({ pause: true }), bMonitor = context.instrument();
    const aImport = first.commit(aMonitor);
    const aPid = await bounded(aMonitor.acquired.promise, 'first tenant import to acquire table locks');
    const bImport = second.commit(bMonitor);
    const bPid = await bounded(bMonitor.attempted.promise, 'second tenant import to attempt table locks');
    await waitingOn(observer, bPid, aPid, 'users', 'ShareRowExclusiveLock');
    await assertPending(context, b, second.proposal);
    aMonitor.resume.release();
    const [aReceipt, bReceipt] = await Promise.all([aImport, bImport]);
    await assertCommitted(context, a, first.proposal, aReceipt);
    await assertCommitted(context, b, second.proposal, bReceipt);
    await assert.rejects(a.store.getProposal({ ...a.scope, proposalId: second.proposal.id }), { code: 'PROPOSAL_NOT_FOUND' });
    await assert.rejects(b.store.getProposal({ ...b.scope, proposalId: first.proposal.id }), { code: 'PROPOSAL_NOT_FOUND' });
    const replays = await Promise.all([first.commit(), second.commit()]);
    assert.deepEqual(replays, [{ ...jsonValue(aReceipt), replayed: true }, { ...jsonValue(bReceipt), replayed: true }]);
    await assertCommitted(context, a, first.proposal, aReceipt);
    await assertCommitted(context, b, second.proposal, bReceipt);
    await context.assertClean();
  });

  for (const [channel, authorize] of [['call', withCallAuthorization], ['email', withEmailAuthorization], ['sms', withSmsAuthorization]]) {
    await t.test(`${channel} authorization and local provider stub remain tenant-scoped while result persistence waits for an import`, async subtest => {
      const context = await setup(subtest), { a, b } = context;
      const prepared = await a.prepare([a.note, a.hold]);
      const monitor = context.instrument({ pause: true });
      const importing = prepared.commit(monitor);
      const aPid = await bounded(monitor.acquired.promise, 'import to acquire table locks');
      const providerCalls = [];
      const providerPool = context.instrument().pool;
      const identity = { prospectId: b.accountId, clientId: b.clientId };
      // Exercise real authorization, with a deliberately local provider stub.
      // This does not claim end-to-end service/provider retry correctness.
      const accepted = await authorize(providerPool, [identity], async () => {
        providerCalls.push(identity);
        return { reference: `local-${channel}-${randomUUID()}` };
      });
      assert.deepEqual(providerCalls, [identity]);
      const writer = await context.writer();
      const persisting = context.track(writer.query(`INSERT INTO touchpoints
        (client_id,prospect_id,channel,action_type,outcome,content_summary)
        VALUES($1,$2,$3,'provider_stub_result','accepted',$4) RETURNING *`,
      [b.clientId, b.accountId, channel, accepted.reference]));
      await waitingOn(observer, writer.pid, aPid, 'touchpoints', 'RowExclusiveLock');
      assert.equal((await observer.query('SELECT count(*)::int AS count FROM touchpoints WHERE client_id=$1', [b.clientId])).rows[0].count, 0);
      monitor.resume.release();
      const receipt = await importing;
      const persisted = await persisting;
      await writer.query('COMMIT'); writer.release();
      assert.equal(persisted.rowCount, 1);
      assert.equal(persisted.rows[0].client_id, b.clientId);
      assert.equal(persisted.rows[0].prospect_id, b.accountId);
      assert.deepEqual(providerCalls, [identity], 'persistence contention must not invoke the stub twice');
      await assertCommitted(context, a, prepared.proposal, receipt, 2);
      await assertNoImportWrites(context, b);
      assert.deepEqual(await b.read(), b.initial, 'A admission hold must not alter B outreach eligibility');
      const forbidden = async () => { assert.fail('wrong-scope/held prospect must not cross the provider boundary'); };
      const code = channel === 'call' ? 'CALL_SUPPRESSED' : 'outreach_review_required';
      await assert.rejects(authorize(providerPool, [{ prospectId: a.accountId, clientId: b.clientId }], forbidden), { code });
      await assert.rejects(authorize(providerPool, [{ prospectId: a.accountId, clientId: a.clientId }], forbidden), { code });
      const rows = (await observer.query('SELECT * FROM touchpoints WHERE client_id=ANY($1::int[])', [[a.clientId, b.clientId]])).rows;
      assert.equal(rows.length, 1);
      assert.equal(rows[0].content_summary, accepted.reference);
      assert.deepEqual((await b.store.snapshotContext()).prospects.map(row => row.id), [b.accountId]);
      await context.assertClean();
    });
  }
});
