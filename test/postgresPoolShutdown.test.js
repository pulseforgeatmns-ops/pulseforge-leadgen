'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { EventEmitter, once } = require('node:events');
const { setImmediate: nextTurn } = require('node:timers/promises');
const { Pool } = require('pg');
const { createTrackedPostgresPool, trackUnopenedPostgresPool, closePostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');

class DeferredClient extends EventEmitter {
  constructor() { super(); this._queryable = true; this._ending = false; }
  connect(callback) { queueMicrotask(() => callback(null)); }
  end(callback) {
    this._ending = true;
    this.finishClose = () => { callback(); this.emit('end'); };
    this.emit('closeStarted');
  }
}

async function seededPool(count = 3, options = {}) {
  const pool = createTrackedPostgresPool({ Client: DeferredClient, idleTimeoutMillis: 0, ...options });
  const clients = await Promise.all(Array.from({ length: count }, () => pool.connect()));
  for (const client of clients) client.release();
  return { pool, clients };
}

function noListeners(pool) {
  assert.equal(pool.listenerCount('remove'), 0);
  assert.equal(pool.listenerCount('error'), 0);
}

test('installed pg-pool end resolves before physical closure; helper waits for every client', async () => {
  const { pool, clients } = await seededPool();
  assert.ok(pool instanceof Pool);
  let finished = false;
  const shutdown = closePostgresPool(pool).then(() => { finished = true; });
  await nextTurn();
  assert.equal(pool.ended, true);
  assert.equal(pool.totalCount, 0, 'pool bookkeeping alone does not establish socket closure');
  assert.equal(finished, false);
  clients[0].finishClose();
  clients[1].finishClose();
  await nextTurn();
  assert.equal(finished, false, 'last socket is still closing');
  clients[2].finishClose();
  await shutdown;
  noListeners(pool);
  await closePostgresPool(pool);
});

test('already evicted client is tracked even when totalCount is zero before shutdown', async () => {
  const { pool, clients } = await seededPool(1, { idleTimeoutMillis: 1 });
  await once(clients[0], 'closeStarted');
  assert.equal(pool.totalCount, 0);
  let finished = false;
  const shutdown = closePostgresPool(pool).then(() => { finished = true; });
  await nextTurn();
  assert.equal(finished, false);
  clients[0].finishClose();
  await shutdown;
  assert.equal(finished, true);
  noListeners(pool);
});

test('preexisting eviction cannot be mistaken for a different client closure', async () => {
  const pool = createTrackedPostgresPool({ Client: DeferredClient, idleTimeoutMillis: 1 });
  const evicted = await pool.connect();
  const active = await pool.connect();
  evicted.release();
  await once(evicted, 'closeStarted');
  assert.equal(pool.totalCount, 1);
  let finished = false;
  const shutdown = closePostgresPool(pool).then(() => { finished = true; });
  active.release();
  evicted.finishClose();
  await nextTurn();
  assert.equal(finished, false, 'the other socket is still closing');
  active.finishClose();
  await shutdown;
  noListeners(pool);
});

test('zero-client pool and concurrent helper calls complete without hanging', async () => {
  const pool = createTrackedPostgresPool();
  const first = closePostgresPool(pool);
  assert.equal(closePostgresPool(pool), first);
  await first;
  noListeners(pool);
});

test('client tracking is attached before synchronous end callbacks', async () => {
  class SynchronousClient extends DeferredClient {
    end(callback) { callback(); this.emit('end'); }
  }
  const { pool } = await seededPool(2, { Client: SynchronousClient });
  await closePostgresPool(pool);
  noListeners(pool);
});

for (const mode of ['reject', 'throw', 'pool error']) {
  test(`shutdown surfaces ${mode} and removes temporary listeners`, async () => {
    const pool = createTrackedPostgresPool();
    const error = new Error(`failed ${mode}`);
    pool.end = () => {
      if (mode === 'throw') throw error;
      if (mode === 'pool error') { pool.emit('error', error); return; }
      return Promise.reject(error);
    };
    await assert.rejects(closePostgresPool(pool), err => err === error);
    noListeners(pool);
  });
}

test('missing client closure times out visibly instead of hanging or suppressing failure', async () => {
  const { pool, clients } = await seededPool(1);
  await assert.rejects(closePostgresPool(pool, { timeoutMs: 20 }), /1 client closures still pending/);
  noListeners(pool);
  clients[0].finishClose();
});

test('externally ended tracked pool still waits for physical socket closure without ending twice', async () => {
  const { pool, clients } = await seededPool(1);
  await pool.end();
  assert.equal(pool.ended, true);
  pool.end = () => assert.fail('must not invoke end twice');
  let finished = false;
  const shutdown = closePostgresPool(pool).then(() => { finished = true; });
  await nextTurn();
  assert.equal(finished, false);
  clients[0].finishClose();
  await shutdown;
  noListeners(pool);
});

test('invalid timeout and an untracked pool fail closed', async () => {
  const pool = new Pool();
  await assert.rejects(closePostgresPool(pool, { timeoutMs: 0 }), /timeout must be positive/);
  await assert.rejects(closePostgresPool(pool), /created with createTrackedPostgresPool/);
  noListeners(pool);
  await pool.end();
});

test('failed pool shutdown still stops the disposable server and preserves the failure', async () => {
  const pool = createTrackedPostgresPool();
  const error = new Error('shutdown failed');
  pool.end = () => Promise.reject(error);
  let stops = 0;
  await assert.rejects(closePostgresFixture(pool, { stop: async () => { stops += 1; } }), err => err === error);
  assert.equal(stops, 1);
  noListeners(pool);
});

test('timed-out physical closure still stops the disposable server and remains a failure', async () => {
  const { pool, clients } = await seededPool(1);
  let stops = 0;
  await assert.rejects(closePostgresFixture(pool, { stop: async () => {
    stops += 1;
    clients[0].finishClose();
  } }, { timeoutMs: 20 }), /client closures still pending/);
  assert.equal(stops, 1);
  noListeners(pool);
});

test('pool and server cleanup errors are both preserved', async () => {
  const pool = createTrackedPostgresPool();
  const poolError = new Error('pool failure');
  const stopError = new Error('server failure');
  pool.end = () => Promise.reject(poolError);
  await assert.rejects(closePostgresFixture(pool, { stop: async () => { throw stopError; } }), error => {
    assert.ok(error instanceof AggregateError);
    assert.deepEqual(error.errors, [poolError, stopError]);
    return true;
  });
  noListeners(pool);
});


test('fresh factory-created pool is tracked in place without changing its options or TLS identity checks', async () => {
  const ssl = { rejectUnauthorized: true, checkServerIdentity() {} };
  const pool = new Pool({ Client: DeferredClient, idleTimeoutMillis: 0, max: 5, ssl });
  assert.equal(trackUnopenedPostgresPool(pool), pool);
  assert.equal(trackUnopenedPostgresPool(pool), pool);
  assert.equal(pool.options.ssl, ssl); assert.equal(pool.options.max, 5);
  const client = await pool.connect(); client.release();
  let closed = false; const pending = closePostgresPool(pool).then(() => { closed = true; });
  await nextTurn(); assert.equal(closed, false);
  client.finishClose(); await pending; noListeners(pool);
});

test('late tracking rejects an already connected or ending pool', async () => {
  const pool = new Pool({ Client: DeferredClient, idleTimeoutMillis: 0 });
  const client = await pool.connect();
  assert.throws(() => trackUnopenedPostgresPool(pool), /unopened/);
  client.release(); const ending = pool.end();
  assert.throws(() => trackUnopenedPostgresPool(pool), /unopened/);
  client.finishClose(); await ending;
});
