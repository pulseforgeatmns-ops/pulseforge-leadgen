'use strict';
// Signal database routing is opt-in until data, auth and application cutover are verified.
// SIGNAL_DB_SEPARATION_REQUIRED=1 makes the legacy shared-DB fallback a hard error.
const { Pool } = require('pg');
let dedicatedPool;

function resolveSignalConnection(env = process.env) {
  const separateRequired = env.SIGNAL_DB_SEPARATION_REQUIRED === '1';
  const raw = env.SIGNAL_DATABASE_URL;
  const signalUrl = typeof raw === 'string' ? raw.trim() : '';
  if (!signalUrl) {
    if (separateRequired) throw new Error('signal_database_url_required');
    return null; // Temporary legacy route, until migration is explicitly activated.
  }
  if (!/^postgres(?:ql)?:\/\//i.test(signalUrl)) throw new Error('invalid_signal_database_url');
  if (env.DATABASE_URL && signalUrl === env.DATABASE_URL.trim()) {
    throw new Error('signal_database_must_be_separate');
  }
  return signalUrl;
}

function getSignalPool(env = process.env) {
  const connectionString = resolveSignalConnection(env);
  if (!connectionString) return require('./db');
  if (dedicatedPool) return dedicatedPool;
  dedicatedPool = new Pool({
    connectionString,
    ssl: String(env.SIGNAL_DATABASE_SSL || '').toLowerCase() === 'false'
      ? false : { rejectUnauthorized: false },
    max: 6,
    connectionTimeoutMillis: 5000,
    query_timeout: 10000,
  });
  return dedicatedPool;
}

module.exports = { getSignalPool, resolveSignalConnection };
