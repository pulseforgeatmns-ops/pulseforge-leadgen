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
  const sqlPath = path.join(__dirname, '../../../migrations/2026-10-04-signal-v1.sql');
  const sql = fs.readFileSync(sqlPath, 'utf8');
  await pool.query(sql);
}

module.exports = {
  ensureSignalSchema,
};
