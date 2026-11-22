'use strict';
// Signal database routing is opt-in. A configured URL alone cannot switch workloads.
// SIGNAL_DB_SEPARATION_REQUIRED=1 requires an independent destination and fails closed.
const { Pool } = require('pg');
let dedicatedPool;
let dedicatedConnection;

function endpoint(urlString) {
  let url;
  try { url = new URL(urlString); }
  catch { throw new Error('invalid_signal_database_url'); }
  if (!['postgres:', 'postgresql:'].includes(url.protocol) || !url.hostname ||
      !url.pathname || url.pathname === '/') {
    throw new Error('invalid_signal_database_url');
  }
  return { host: url.hostname.toLowerCase(), port: url.port || '5432', database: decodeURIComponent(url.pathname.slice(1)) };
}

function resolveSignalConnection(env = process.env) {
  // This intentional fallback allows storing the dedicated secret before cutover.
  if (env.SIGNAL_DB_SEPARATION_REQUIRED !== '1') return null;
  const raw = env.SIGNAL_DATABASE_URL;
  const signalUrl = typeof raw === 'string' ? raw.trim() : '';
  if (!signalUrl) throw new Error('signal_database_url_required');
  const signal = endpoint(signalUrl);
  if (env.DATABASE_URL) {
    // Different credentials do not imply a different database.
    const core = endpoint(String(env.DATABASE_URL).trim());
    if (core.host === signal.host && core.port === signal.port && core.database === signal.database)
      throw new Error('signal_database_must_be_separate');
  }
  return signalUrl;
}

function getSignalPool(env = process.env) {
  const connectionString = resolveSignalConnection(env);
  if (!connectionString) return require('./db');
  if (dedicatedPool) {
    if (dedicatedConnection !== connectionString) throw new Error('signal_database_connection_changed');
    return dedicatedPool;
  }
  dedicatedPool = new Pool({
    connectionString,
    ssl: String(env.SIGNAL_DATABASE_SSL || '').toLowerCase() === 'false'
      ? false : { rejectUnauthorized: false },
    max: 6,
    connectionTimeoutMillis: 5000,
    query_timeout: 10000,
  });
  dedicatedConnection = connectionString;
  return dedicatedPool;
}
module.exports = { getSignalPool, resolveSignalConnection };
