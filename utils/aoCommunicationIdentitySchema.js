'use strict';

const fs = require('node:fs');
const path = require('node:path');

const schemaInitPromises = new WeakMap();

async function ensureAoCommunicationIdentitySchemaOnce(db) {
  const migration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-10-06-ao-mailbox-001.sql'),
    'utf8'
  );
  await db.query(migration);
}

async function ensureAoCommunicationIdentitySchema(db) {
  if (!schemaInitPromises.has(db)) {
    const promise = ensureAoCommunicationIdentitySchemaOnce(db).catch(err => {
      schemaInitPromises.delete(db);
      throw err;
    });
    schemaInitPromises.set(db, promise);
  }
  return schemaInitPromises.get(db);
}

module.exports = {
  ensureAoCommunicationIdentitySchema,
};
