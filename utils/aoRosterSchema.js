'use strict';

const fs = require('node:fs');
const path = require('node:path');
const pool = require('../db');
const { ensureUsersTable } = require('../middleware/auth');

const schemaInitPromises = new WeakMap();

const AO_OPERATIONAL_STATUSES = Object.freeze(['active', 'paused', 'inactive']);
const AO_REVIEW_BUCKETS = Object.freeze(['needs_reassignment', 'transferred_from_inactive_ao']);

async function ensureAoRosterSchemaOnce(db) {
  if (db === pool) {
    await ensureUsersTable();
  }
  const migration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-10-05-ao-roster-reassign-001.sql'),
    'utf8'
  );
  await db.query(migration);
}

async function ensureAoRosterSchema(db = pool) {
  if (!schemaInitPromises.has(db)) {
    const promise = ensureAoRosterSchemaOnce(db).catch(err => {
      schemaInitPromises.delete(db);
      throw err;
    });
    schemaInitPromises.set(db, promise);
  }
  return schemaInitPromises.get(db);
}

module.exports = {
  ensureAoRosterSchema,
  AO_OPERATIONAL_STATUSES,
  AO_REVIEW_BUCKETS,
};
