'use strict';

const { Pool, Client } = require('pg');
const tracked = new WeakMap();
const closing = new WeakMap();

// Call immediately on a newly constructed pool, before its first connection.
// Supports exercising a production pool factory unchanged in isolated tests.
function trackUnopenedPostgresPool(pool) {
  if (tracked.has(pool)) return pool;
  if (!(pool instanceof Pool) || pool.totalCount !== 0 || pool.waitingCount !== 0 || pool.ending || pool.ended) {
    throw new Error('Tracking requires a newly constructed, unopened PostgreSQL pool');
  }
  const clients = new Map();
  const BaseClient = pool.Client || Client;
  class TrackedClient extends BaseClient {
    constructor(config) {
      super(config);
      let resolveClosed;
      const closed = new Promise(resolve => { resolveClosed = resolve; });
      clients.set(this, closed);
      this.once('end', () => { clients.delete(this); resolveClosed(); });
    }
  }
  pool.Client = TrackedClient;
  tracked.set(pool, clients);
  return pool;
}

/** Test-only pool: track physical client closure before any connection is opened. */
function createTrackedPostgresPool(options = {}) {
  return trackUnopenedPostgresPool(new Pool(options));
}

/**
 * pg-pool can resolve end() before client sockets finish closing, including
 * clients removed earlier by idle eviction. Await their end events before the
 * disposable harness's synchronous pg_ctl stop. No shutdown errors are ignored.
 */
function closePostgresPool(pool, { timeoutMs = 10000 } = {}) {
  if (closing.has(pool)) return closing.get(pool);
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return Promise.reject(new Error('Pool shutdown timeout must be positive'));
  }
  const clients = tracked.get(pool);
  if (!clients) return Promise.reject(new Error('Pool must be created with createTrackedPostgresPool'));
  const closures = [...clients.values()];
  const promise = new Promise((resolve, reject) => {
    let settled = false;
    const finish = error => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      pool.removeListener('error', onError);
      if (error) reject(error);
      else resolve();
    };
    const onError = error => finish(error);
    const timer = setTimeout(() => finish(new Error(
      `Pool shutdown timed out: ${clients.size} client closures still pending`
    )), timeoutMs);
    pool.on('error', onError);
    try {
      // If a tracked pool is already ending, its physical closures still provide
      // evidence. Do not invoke end twice. Controlled helper calls are idempotent.
      const end = pool.ending || pool.ended ? undefined : pool.end();
      Promise.all([end, ...closures]).then(() => finish(), onError);
    } catch (error) {
      finish(error);
    }
  });
  closing.set(pool, promise);
  return promise;
}

async function closePostgresFixture(pool, postgres, options) {
  let shutdownError;
  try {
    await closePostgresPool(pool, options);
  } catch (error) {
    shutdownError = error;
  } finally {
    try {
      await postgres.stop();
    } catch (error) {
      if (shutdownError) {
        throw new AggregateError([shutdownError, error], 'Pool shutdown and PostgreSQL stop both failed');
      }
      throw error;
    }
  }
  if (shutdownError) throw shutdownError;
}

module.exports = { createTrackedPostgresPool, trackUnopenedPostgresPool, closePostgresPool, closePostgresFixture };
