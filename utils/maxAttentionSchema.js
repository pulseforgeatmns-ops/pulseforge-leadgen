'use strict';

const pool = require('../db');
const fs = require('node:fs');
const path = require('node:path');
const { ensureClientArchitecture } = require('./clientContext');
const { ensureUsersTable } = require('../middleware/auth');
const { ensureMaxDecisionExecutionSchema } = require('./maxDecisionExecutionSchema');

const schemaInitPromises = new WeakMap();

async function ensureMaxAttentionSchemaOnce(db) {
  if (db === pool) {
    await ensureClientArchitecture();
    await ensureUsersTable();
  }
  await ensureMaxDecisionExecutionSchema(db);
  const migration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-10-05-max-reliability-attention.sql'),
    'utf8'
  );
  await db.query(migration);
}

async function ensureMaxAttentionSchema(db = pool) {
  if (!schemaInitPromises.has(db)) {
    const promise = ensureMaxAttentionSchemaOnce(db).catch(err => {
      schemaInitPromises.delete(db);
      throw err;
    });
    schemaInitPromises.set(db, promise);
  }
  return schemaInitPromises.get(db);
}

module.exports = {
  ensureMaxAttentionSchema,
};
