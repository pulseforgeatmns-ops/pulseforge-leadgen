'use strict';

const { insertShadowEvent } = require('./ShadowEventRepository');

function integer(value, fallback, min, max) {
  const n = Number(value);
  return Number.isInteger(n) && n >= min && n <= max ? n : fallback;
}

// This pool belongs only to shadow review. Never consume/close the request pool.
function createShadowPool(env = process.env) {
  const { Pool } = require('pg');
  const timeout = integer(env.DECISION_SHADOW_DB_TIMEOUT_MS, 1000, 50, 10000);
  return new Pool({
    connectionString: env.DATABASE_URL,
    ssl: String(env.DATABASE_SSL || '').toLowerCase() === 'false' ? false : { rejectUnauthorized: false },
    application_name: 'pulseforge-decision-shadow', max: 2,
    connectionTimeoutMillis: timeout, statement_timeout: timeout,
    query_timeout: timeout + 250, idleTimeoutMillis: 1000, allowExitOnIdle: true,
  });
}

function createShadowEventSink({ env = process.env, createPool = createShadowPool,
  warn = row => console.warn('[DECISION_SHADOW_PERSISTENCE]', JSON.stringify(row)) } = {}) {
  const enabled = env.DECISION_SHADOW_ENABLED === 'true' && env.DECISION_SHADOW_PERSIST_ENABLED !== 'false';
  const maxPending = integer(env.DECISION_SHADOW_DB_MAX_PENDING, 64, 1, 1000);
  const pending = new Set();
  const counts = { persisted: 0, failed: 0, dropped: 0 };
  let pool, lastWarning = -Infinity;
  function warning(reason) {
    if (Date.now() - lastWarning < 30000) return;
    lastWarning = Date.now();
    // Fixed codes/counters only: never include DB exceptions, SQL, or secrets.
    try { Promise.resolve(warn({ event: 'DECISION_SHADOW_PERSISTENCE', spec: 'SPEC-JEV-002',
      status: 'error', reason, ...counts })).catch(() => {}); } catch (_) { /* optional diagnostics */ }
  }
  return {
    write(row) {
      if (!enabled || row?.mode !== 'shadow' || row?.event !== 'DECISION_SHADOW_EVALUATED') return;
      if (pending.size >= maxPending) {
        counts.dropped += 1;
        warning('capacity_limit');
        return;
      }
      // Keep the slot until the actual write settles. A hung driver cannot
      // escape the bound by racing a timer and accepting unlimited new work.
      const task = new Promise(resolve => setImmediate(resolve)).then(async () => {
        if (!env.DATABASE_URL) {
          counts.failed += 1;
          warning('database_unconfigured');
          return;
        }
        if (!pool) {
          pool = createPool(env);
          pool.on('error', () => warning('idle_connection_failed'));
        }
        await insertShadowEvent(pool, row);
        counts.persisted += 1;
      }).catch(() => { counts.failed += 1; warning('write_failed'); });
      pending.add(task);
      task.then(() => pending.delete(task));
    },
    async drain() { await Promise.all([...pending]); },
    stats() { return { ...counts, pending: pending.size }; },
    // Explicit shutdown/test use only. This is never the application's db.js pool.
    async close() { await this.drain(); if (pool) await pool.end(); },
  };
}

let defaultSink;
function getDefaultShadowEventSink() { return defaultSink ||= createShadowEventSink(); }

module.exports = { createShadowEventSink, createShadowPool, getDefaultShadowEventSink };
