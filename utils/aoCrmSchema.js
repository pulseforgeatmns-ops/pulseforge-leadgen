'use strict';

const pool = require('../db');
const fs = require('node:fs');
const path = require('node:path');
const { ensureClientArchitecture } = require('./clientContext');
const { ensureUsersTable } = require('../middleware/auth');
const { ensureAoProspectRoutingSchema } = require('./aoProspectRoutingSchema');
const { ensureAoRosterSchema } = require('./aoRosterSchema');

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
  const followupMigration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-09-29-ao-followup-001.sql'),
    'utf8'
  );
  await db.query(followupMigration);
  const workflowMigration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-09-30-ao-workflow-002-flags.sql'),
    'utf8'
  );
  await db.query(workflowMigration);
  const flagInboxMigration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-10-06-ao-flag-inbox-001.sql'),
    'utf8'
  );
  await db.query(flagInboxMigration);
  await ensureAoRosterSchema(db);
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
