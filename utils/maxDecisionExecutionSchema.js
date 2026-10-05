'use strict';

const pool = require('../db');
const fs = require('node:fs');
const path = require('node:path');
const { ensureClientArchitecture } = require('./clientContext');
const { ensureUsersTable } = require('../middleware/auth');
const { ensureMaxStateIngestionSchema } = require('./maxStateIngestionSchema');

const schemaInitPromises = new WeakMap();

async function ensureMaxDecisionExecutionSchemaOnce(db) {
  if (db === pool) {
    await ensureClientArchitecture();
    await ensureUsersTable();
    await ensureMaxStateIngestionSchema(db);
  }
  const migration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-10-05-max-reliability-decision-execution.sql'),
    'utf8'
  );
  await db.query(migration);
}

async function ensureMaxDecisionExecutionSchema(db = pool) {
  if (!schemaInitPromises.has(db)) {
    const promise = ensureMaxDecisionExecutionSchemaOnce(db).catch(err => {
      schemaInitPromises.delete(db);
      throw err;
    });
    schemaInitPromises.set(db, promise);
  }
  return schemaInitPromises.get(db);
}

module.exports = {
  ensureMaxDecisionExecutionSchema,
};
