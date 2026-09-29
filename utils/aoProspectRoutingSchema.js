'use strict';

const pool = require('../db');
const fs = require('node:fs');
const path = require('node:path');
const { ensureClientArchitecture } = require('./clientContext');
const { ensureUsersTable } = require('../middleware/auth');

const schemaInitPromises = new WeakMap();

async function ensureAoProspectRoutingSchemaOnce(db) {
  if (db === pool) {
    await ensureClientArchitecture();
    await ensureUsersTable();
  }
  const migration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-09-22-ao-prospect-routing.sql'),
    'utf8'
  );
  await db.query(migration);
}

async function ensureAoProspectRoutingSchema(db = pool) {
  if (!schemaInitPromises.has(db)) {
    const promise = ensureAoProspectRoutingSchemaOnce(db).catch(err => {
      schemaInitPromises.delete(db);
      throw err;
    });
    schemaInitPromises.set(db, promise);
  }
  return schemaInitPromises.get(db);
}

module.exports = {
  ensureAoProspectRoutingSchema,
};
