'use strict';

const { Pool, Client } = require('pg');
const tracked = new WeakMap();
const closing = new WeakMap();

/** Test-only pool: track physical client closure before any connection is opened. */
function createTrackedPostgresPool(options = {}) {
  const clients = new Map();
  const BaseClient = options.Client || Client;
  class TrackedClient extends BaseClient {
    constructor(config) {
      super(config);
      let resolveClosed;
      const closed = new Promise(resolve => { resolveClosed = resolve; });
      clients.set(this, closed);
      this.once('end', () => { clients.delete(this); resolveClosed(); });
    }
  }
  const pool = new Pool({ ...options, Client: TrackedClient });
  tracked.set(pool, clients);
  return pool;
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

module.exports = { createTrackedPostgresPool, closePostgresPool, closePostgresFixture };
