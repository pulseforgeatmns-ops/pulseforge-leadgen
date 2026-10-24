'use strict';

const pool = require('../db');
const fs = require('node:fs');
const path = require('node:path');
const { ensureClientArchitecture } = require('./clientContext');

const schemaInitPromises = new WeakMap();

async function ensureMaxConversationMemorySchemaOnce(db) {
  if (db === pool) {
    await ensureClientArchitecture();
  }
  const migration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-10-06-max-conversation-memory.sql'),
    'utf8'
  );
  await db.query(migration);
}

async function ensureMaxConversationMemorySchema(db = pool) {
  if (!schemaInitPromises.has(db)) {
    const promise = ensureMaxConversationMemorySchemaOnce(db).catch(err => {
      schemaInitPromises.delete(db);
      throw err;
    });
    schemaInitPromises.set(db, promise);
  }
  return schemaInitPromises.get(db);
}

module.exports = {
  ensureMaxConversationMemorySchema,
};
