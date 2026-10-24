'use strict';

const pool = require('../db');
const fs = require('node:fs');
const path = require('node:path');
const { ensureClientArchitecture } = require('./clientContext');
const { ensureUsersTable } = require('../middleware/auth');

const schemaInitPromises = new WeakMap();

async function ensureMaxVoiceSchemaOnce(db) {
  if (db === pool) {
    await ensureClientArchitecture();
    await ensureUsersTable();
  }
  const migration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-10-06-max-voice-recordings.sql'),
    'utf8'
  );
  await db.query(migration);
}

async function ensureMaxVoiceSchema(db = pool) {
  if (!schemaInitPromises.has(db)) {
    const promise = ensureMaxVoiceSchemaOnce(db).catch(err => {
      schemaInitPromises.delete(db);
      throw err;
    });
    schemaInitPromises.set(db, promise);
  }
  return schemaInitPromises.get(db);
}

module.exports = {
  ensureMaxVoiceSchema,
};
