'use strict';

const fs = require('fs');
const path = require('path');

/**
 * Apply the graph-owned knowledge schema to a Postgres pool/client.
 * Used by tests and explicit migrate; not invoked from KnowledgeService.
 *
 * @param {{ query: Function }} pool
 * @param {{ sqlPath?: string }} [options]
 */
async function ensureKnowledgeSchema(pool, options = {}) {
  if (!pool || typeof pool.query !== 'function') {
    throw new Error('ensureKnowledgeSchema requires a pg pool');
  }
  const sqlPath =
    options.sqlPath ||
    path.join(__dirname, '../../../migrations/2026-07-26-knowledge-graph-persistent.sql');
  const sql = fs.readFileSync(sqlPath, 'utf8');
  await pool.query(sql);
}

module.exports = {
  ensureKnowledgeSchema,
};
