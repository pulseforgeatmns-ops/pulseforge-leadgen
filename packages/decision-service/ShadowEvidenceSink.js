'use strict';

const { insertShadowEvidence } = require('./ShadowEvidenceRepository');
const { createShadowPool } = require('./ShadowEventSink');

function createShadowEvidenceSink({ env = process.env, createPool = createShadowPool,
  warn = row => console.warn('[DECISION_SHADOW_EVIDENCE]', JSON.stringify(row)) } = {}) {
  const enabled = env.DECISION_SHADOW_ENABLED === 'true' && env.DECISION_SHADOW_PERSIST_ENABLED !== 'false';
  const pending = new Set();
  let pool;
  return {
    write(row) {
      if (!enabled || !row?.event) return;
      const task = Promise.resolve().then(async () => {
        if (!env.DATABASE_URL) return;
        if (!pool) {
          pool = createPool(env);
          pool.on('error', () => {});
        }
        await insertShadowEvidence(pool, row);
      }).catch(() => {
        try { warn({ status: 'error', reason: 'evidence_write_failed' }); } catch (_) { /* noop */ }
      });
      pending.add(task);
      task.finally(() => pending.delete(task));
    },
    async drain() { await Promise.all([...pending]); },
    async close() { await this.drain(); if (pool) await pool.end(); },
  };
}

let defaultEvidenceSink;
function getDefaultShadowEvidenceSink() {
  return defaultEvidenceSink ||= createShadowEvidenceSink();
}

module.exports = { createShadowEvidenceSink, getDefaultShadowEvidenceSink };
