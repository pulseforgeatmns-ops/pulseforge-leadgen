'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createTrackedPostgresPool, closePostgresPool } = require('./helpers/closePostgresPool');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

function freshHelpers() {
  for (const file of ['../utils/businessNameShort', '../scripts/promoteUnenriched']) {
    delete require.cache[require.resolve(file)];
  }
  return {
    ...require('../utils/businessNameShort'),
    ...require('../scripts/promoteUnenriched'),
  };
}

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

const columnsSql = `SELECT count(*)::int AS n FROM pg_catalog.pg_attribute
  WHERE attrelid = to_regclass('companies') AND NOT attisdropped
    AND attname IN ('business_name_short', 'business_name_short_confidence', 'business_name_short_flags')`;
const addColumnsSql = `ALTER TABLE companies ADD COLUMN business_name_short TEXT,
  ADD COLUMN business_name_short_confidence TEXT,
  ADD COLUMN business_name_short_flags TEXT[] DEFAULT ARRAY[]::TEXT[]`;

// This suite must use native PostgreSQL: pg-mem cannot establish lock ownership,
// autocommit visibility, rollback of DDL, or the original 40P01/42703 failure.
test('promotion schema lifecycle on disposable native PostgreSQL', { timeout: 60000 }, async t => {
  const server = await startDisposablePostgres('promotion-schema-');
  const admin = createTrackedPostgresPool({ connectionString: server.connectionString });
  const pools = [];
  let sequence = 0;
  async function fixture({ max = 4, existing = false } = {}) {
    const schema = `promotion_schema_${++sequence}`;
    await admin.query(`CREATE SCHEMA ${schema}`);
    const pool = createTrackedPostgresPool({ connectionString: server.connectionString, max,
      connectionTimeoutMillis: 2000, options: `-c search_path=${schema},public -c statement_timeout=10000 -c lock_timeout=5000` });
    pools.push(pool);
    await pool.query(`CREATE TABLE companies (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER, name TEXT,
      domain TEXT, website TEXT, industry TEXT, location TEXT,
      created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)`);
    if (existing) await pool.query(addColumnsSql);
    return { pool, schema };
  }
  t.after(async () => {
    // pg-pool.end() may resolve before physical sockets close. Do not stop the
    // native server until every tracked client has actually ended.
    const results = await Promise.allSettled([...pools, admin].map(pool => closePostgresPool(pool)));
    const errors = results.filter(result => result.status === 'rejected').map(result => result.reason);
    try { await server.stop(); } catch (error) { errors.push(error); }
    if (errors.length) throw new AggregateError(errors, 'Promotion schema fixture shutdown failed');
  });

  for (const existing of [false, true]) {
    await t.test(`forced concurrent promotion with ${existing ? 'existing' : 'absent'} columns`, async () => {
      const { pool } = await fixture({ existing });
      const { findOrCreateCompanyForClient } = freshHelpers();
      const firstAtLock = deferred();
      const secondHasLock = deferred();
      const events = [];
      let connectionCount = 0;
      const db = {
        query: (...args) => pool.query(...args),
        connect: async () => {
          const client = await pool.connect();
          const label = ++connectionCount;
          return {
            release: () => client.release(),
            query: async (sql, params) => {
              const advisory = /SELECT pg_advisory_xact_lock/.test(sql);
              if (label === 1 && advisory) {
                firstAtLock.resolve();
                await secondHasLock.promise;
              }
              events.push({ label, sql: String(sql).trim().split(/\s+/).slice(0, 3).join(' ') });
              const result = await client.query(sql, params);
              if (label === 2 && advisory) secondHasLock.resolve();
              return result;
            },
          };
        },
      };
      const first = findOrCreateCompanyForClient({ name: 'Granite PM', domain: 'granite.example', clientId: 10 }, db);
      // Attach rejection handlers immediately while the deterministic lock barrier runs.
      const results = Promise.allSettled([first, firstAtLock.promise.then(() =>
        findOrCreateCompanyForClient({ name: 'Granite Property Management', domain: 'granite.example', clientId: 10 }, db))]);
      const settled = await results;
      assert.deepEqual(settled.map(result => result.status === 'fulfilled' ? 'ok' : result.reason.code), ['ok', 'ok']);
      assert.equal(settled[0].value, settled[1].value);
      assert.equal((await pool.query('SELECT count(*)::int AS n FROM companies')).rows[0].n, 1);
      assert.equal((await pool.query(columnsSql)).rows[0].n, 3);
      assert.equal(events.filter(event => event.sql === 'BEGIN').length, 2);
      assert.equal(events.filter(event => event.sql === 'COMMIT').length, 2);
      assert.equal(events.filter(event => event.sql.startsWith('ALTER TABLE')).length, 0,
        'schema DDL must finish before promotion transactions acquire relation/advisory locks');
    });
  }

  await t.test('one-connection pool completes schema ensure and company transaction without starvation', async () => {
    const { pool } = await fixture({ max: 1 });
    const { findOrCreateCompanyForClient } = freshHelpers();
    const id = await findOrCreateCompanyForClient({ name: 'One Connection LLC', domain: 'one.example', clientId: 10 }, pool);
    assert.ok(id);
    assert.equal(pool.waitingCount, 0);
    assert.equal((await pool.query(columnsSql)).rows[0].n, 3);
  });

  await t.test('existing columns do not acquire an unnecessary ALTER relation lock', async () => {
    const { pool } = await fixture({ existing: true });
    const { ensureBusinessNameShortColumns } = freshHelpers();
    const statements = [];
    await ensureBusinessNameShortColumns({ query: (sql, params) => {
      statements.push(sql);
      return pool.query(sql, params);
    } });
    assert.ok(statements.length > 0);
    assert.equal(statements.some(sql => /ALTER TABLE/.test(sql)), false);
  });

  await t.test('in-flight pool ensure coalesces and resolves only after committed visibility', async () => {
    const { pool } = await fixture();
    const { ensureBusinessNameShortColumns } = freshHelpers();
    const blocker = await pool.connect();
    await blocker.query('BEGIN');
    await blocker.query('LOCK TABLE companies IN ACCESS SHARE MODE');
    const ddlStarted = deferred();
    let ddlCount = 0;
    let finished = false;
    const db = { query: async (sql, params) => {
      if (/ALTER TABLE/.test(sql)) { ddlCount += 1; ddlStarted.resolve(); }
      return pool.query(sql, params);
    } };
    const first = ensureBusinessNameShortColumns(db);
    const second = ensureBusinessNameShortColumns(db);
    first.then(() => { finished = true; });
    await ddlStarted.promise;
    assert.equal(finished, false);
    assert.equal((await blocker.query(columnsSql)).rows[0].n, 0);
    await blocker.query('ROLLBACK');
    blocker.release();
    await Promise.all([first, second]);
    assert.equal(ddlCount, 1);
    const observer = await pool.connect();
    try { assert.equal((await observer.query(columnsSql)).rows[0].n, 3); }
    finally { observer.release(); }
  });

  await t.test('independent pools and visible relations never inherit another ensure success', async () => {
    const first = await fixture();
    const second = await fixture();
    const { ensureBusinessNameShortColumns } = freshHelpers();
    await ensureBusinessNameShortColumns(first.pool);
    await ensureBusinessNameShortColumns(second.pool);
    assert.equal((await first.pool.query(columnsSql)).rows[0].n, 3);
    assert.equal((await second.pool.query(columnsSql)).rows[0].n, 3);
  });

  await t.test('distinct one-connection pools concurrently ensure and promote into the same schema', async () => {
    const { pool: firstPool, schema } = await fixture({ max: 1 });
    const secondPool = createTrackedPostgresPool({ connectionString: server.connectionString, max: 1,
      connectionTimeoutMillis: 2000, options: `-c search_path=${schema},public -c statement_timeout=10000 -c lock_timeout=5000` });
    pools.push(secondPool);
    const { findOrCreateCompanyForClient } = freshHelpers();
    const bothProbed = deferred();
    let probes = 0;
    let alters = 0;
    const wrap = pool => ({
      connect: () => pool.connect(),
      query: async (sql, params) => {
        const result = await pool.query(sql, params);
        if (/pg_catalog.pg_attribute/.test(sql) && !result.rows[0]?.ready) {
          if (++probes === 2) bothProbed.resolve();
          await bothProbed.promise;
        }
        if (/ALTER TABLE/.test(sql)) alters += 1;
        return result;
      },
    });
    const ids = await Promise.all([firstPool, secondPool].map((pool, index) =>
      findOrCreateCompanyForClient({ name: `Shared Schema Alias ${index}`, domain: 'shared.example', clientId: 10 }, wrap(pool))));
    assert.equal(probes, 2, 'both independent pools observed missing columns before either ALTER');
    assert.equal(alters, 2);
    assert.equal(ids[0], ids[1]);
    assert.equal((await firstPool.query('SELECT count(*)::int AS n FROM companies')).rows[0].n, 1);
    assert.equal((await secondPool.query(columnsSql)).rows[0].n, 3);
  });

  await t.test('transaction-local DDL success is discarded after rollback and same-client retry', async () => {
    const { pool } = await fixture();
    const { ensureBusinessNameShortColumns } = freshHelpers();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await ensureBusinessNameShortColumns(client);
      assert.equal((await client.query(columnsSql)).rows[0].n, 3);
      assert.equal((await pool.query(columnsSql)).rows[0].n, 0);
      await client.query('ROLLBACK');
      assert.equal((await pool.query(columnsSql)).rows[0].n, 0);
      await client.query('BEGIN');
      await ensureBusinessNameShortColumns(client);
      await client.query('COMMIT');
      assert.equal((await pool.query(columnsSql)).rows[0].n, 3);
    } finally { await client.query('ROLLBACK'); client.release(); }
  });

  await t.test('another client cannot consume uncommitted schema success and recovers after rollback', async () => {
    const { pool } = await fixture();
    const { ensureBusinessNameShortColumns } = freshHelpers();
    const first = await pool.connect();
    const second = await pool.connect();
    let pending;
    try {
      await first.query('BEGIN');
      await ensureBusinessNameShortColumns(first);
      const { rows: [{ pid }] } = await second.query('SELECT pg_backend_pid() AS pid');
      let finished = false;
      pending = ensureBusinessNameShortColumns(second).then(() => { finished = true; });
      // Observe the native relation-lock wait, rather than relying on a sleep to
      // infer that the second caller has reached the uncommitted DDL boundary.
      const deadline = Date.now() + 3000;
      let waiting = false;
      while (!waiting && !finished && Date.now() < deadline) {
        waiting = (await pool.query('SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted) AS waiting', [pid])).rows[0].waiting;
        if (!waiting) await new Promise(resolve => setTimeout(resolve, 5));
      }
      assert.equal(finished, false, 'a different client must not inherit uncommitted success');
      assert.equal(waiting, true);
      await first.query('ROLLBACK');
      await pending;
      assert.equal((await pool.query(columnsSql)).rows[0].n, 3);
    } finally {
      await first.query('ROLLBACK');
      if (pending) await pending.catch(() => {});
      first.release(); second.release();
    }
  });

  await t.test('explicit transaction-client promotion rolls back cleanly and retries on the same client', async () => {
    const { pool } = await fixture();
    const { findOrCreateCompanyForClient } = freshHelpers();
    const client = await pool.connect();
    const input = { name: 'Explicit Transaction', domain: 'explicit.example', clientId: 13 };
    try {
      await client.query('BEGIN');
      await findOrCreateCompanyForClient(input, client, true);
      await client.query('ROLLBACK');
      assert.equal((await pool.query(columnsSql)).rows[0].n, 0);
      await client.query('BEGIN');
      const id = await findOrCreateCompanyForClient(input, client, true);
      await client.query('COMMIT');
      assert.equal((await pool.query('SELECT id FROM companies')).rows[0].id, id);
      assert.equal((await pool.query(columnsSql)).rows[0].n, 3);
    } finally { await client.query('ROLLBACK'); client.release(); }
  });

  await t.test('a rejected native ALTER is propagated, cleared, and retried', async () => {
    const { pool } = await fixture();
    const { ensureBusinessNameShortColumns } = freshHelpers();
    const client = await pool.connect();
    try {
      await client.query('BEGIN READ ONLY');
      await assert.rejects(ensureBusinessNameShortColumns(client), { code: '25006' });
      await client.query('ROLLBACK');
      await ensureBusinessNameShortColumns(client);
      assert.equal((await pool.query(columnsSql)).rows[0].n, 3);
    } finally { await client.query('ROLLBACK'); client.release(); }
  });

  await t.test('canonical dedup, tenant isolation, identity failures, and SQL rollback stay intact', async () => {
    const { pool } = await fixture();
    const { findOrCreateCompanyForClient } = freshHelpers();
    const row = { name: 'Granite Realty', domain: 'granite.example', clientId: 10 };
    const first = await findOrCreateCompanyForClient(row, pool);
    assert.equal(await findOrCreateCompanyForClient({ ...row, name: 'Granite Realty LLC' }, pool), first);
    const otherTenant = await findOrCreateCompanyForClient({ ...row, clientId: 13 }, pool);
    assert.notEqual(otherTenant, first);
    await assert.rejects(findOrCreateCompanyForClient({ ...row, domain: 'other.example' }, pool), /company_identity_domain_conflict/);
    await pool.query("INSERT INTO companies (name, domain, client_id) VALUES ('Duplicate', 'granite.example', 10)");
    await assert.rejects(findOrCreateCompanyForClient(row, pool), /ambiguous_canonical_company/);
    await pool.query("ALTER TABLE companies ADD CONSTRAINT synthetic_company_failure CHECK (name <> 'Rejected')");
    await assert.rejects(findOrCreateCompanyForClient({ name: 'Rejected', domain: 'rejected.example', clientId: 10 }, pool), { code: '23514' });
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM companies WHERE domain='rejected.example'")).rows[0].n, 0);
    const recovered = await findOrCreateCompanyForClient({ name: 'Retry', domain: 'rejected.example', clientId: 10 }, pool);
    assert.ok(recovered);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM companies WHERE client_id=13')).rows[0].n, 1);
  });
});
