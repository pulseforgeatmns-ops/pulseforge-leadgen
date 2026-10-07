'use strict';

const fs = require('fs');
const path = require('path');

/**
 * Apply Signal V1 PostgreSQL schema (idempotent).
 *
 * @param {{ query: Function }} pool
 */
async function ensureSignalSchema(pool) {
  if (!pool || typeof pool.query !== 'function') {
    throw new Error('ensureSignalSchema requires a pg pool');
  }
  const baseDir = path.join(__dirname, '../../../migrations');
  for (const file of [
    '2026-10-04-signal-v1.sql',
    '2026-10-04-signal-v1-market-observations.sql',
    '2026-10-04-signal-v1-research.sql',
    '2026-10-04-signal-v1-research-candidates.sql',
    '2026-10-05-signal-v1-prospective-shadow.sql',
    '2026-10-07-signal-v1-prospective-persistence.sql',
  ]) {
    const sql = fs.readFileSync(path.join(baseDir, file), 'utf8');
    await pool.query(sql);
  }
}

module.exports = {
  ensureSignalSchema,
};
