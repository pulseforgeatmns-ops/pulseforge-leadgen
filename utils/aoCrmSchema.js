'use strict';

const pool = require('../db');
const fs = require('node:fs');
const path = require('node:path');
const { ensureClientArchitecture } = require('./clientContext');
const { ensureUsersTable } = require('../middleware/auth');
const { ensureAoProspectRoutingSchema } = require('./aoProspectRoutingSchema');

const schemaInitPromises = new WeakMap();

async function ensureAoCrmSchemaOnce(db) {
  if (db === pool) {
    await ensureClientArchitecture();
    await ensureUsersTable();
    await ensureAoProspectRoutingSchema(db);
  }
  const migration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-09-28-ao-crm-001.sql'),
    'utf8'
  );
  await db.query(migration);
}

async function ensureAoCrmSchema(db = pool) {
  if (!schemaInitPromises.has(db)) {
    const promise = ensureAoCrmSchemaOnce(db).catch(err => {
      schemaInitPromises.delete(db);
      throw err;
    });
    schemaInitPromises.set(db, promise);
  }
  return schemaInitPromises.get(db);
}

module.exports = {
  ensureAoCrmSchema,
};
