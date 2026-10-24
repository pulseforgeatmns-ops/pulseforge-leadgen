const crypto = require('crypto');
const pool = require('../db');
const { normalizeVertical } = require('./normalize');

const LOCK_KEY = 'global';
const POLL_MS = 5000;
const WAIT_MAX_MS = 5 * 60 * 1000;
const LOCK_TTL_MS = 2 * 60 * 60 * 1000; // 2 hours

async function ensureScoutLockTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scout_lock (
      lock_key TEXT PRIMARY KEY,
      holder_id TEXT NOT NULL,
      client_id INTEGER,
      industry TEXT,
      vertical TEXT,
      location TEXT,
      acquired_at TIMESTAMPTZ DEFAULT NOW(),
      expires_at TIMESTAMPTZ
    )
  `);
  await pool.query(`
    ALTER TABLE scout_lock
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ
  `);
}

async function clearStaleScoutLock() {
  await pool.query(`
    DELETE FROM scout_lock
    WHERE lock_key = $1
      AND expires_at IS NOT NULL
      AND expires_at < NOW()
  `, [LOCK_KEY]);
}

async function tryAcquireScoutLock(meta) {
  await ensureScoutLockTable();
  await clearStaleScoutLock();

  const holderId = crypto.randomUUID();
  const res = await pool.query(`
    INSERT INTO scout_lock (lock_key, holder_id, client_id, industry, vertical, location, acquired_at, expires_at)
    VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW() + INTERVAL '2 hours')
    ON CONFLICT (lock_key) DO NOTHING
    RETURNING holder_id
  `, [
    LOCK_KEY,
    holderId,
    meta.clientId ?? null,
    meta.industry ?? null,
    normalizeVertical(meta.vertical),
    meta.location ?? null,
  ]);

  return res.rows.length ? holderId : null;
}

async function releaseScoutLock(holderId) {
  if (!holderId) return false;
  const res = await pool.query(`
    DELETE FROM scout_lock
    WHERE lock_key = $1 AND holder_id = $2
  `, [LOCK_KEY, holderId]);
  return (res.rowCount || 0) > 0;
}

async function getActiveScoutLock() {
  await ensureScoutLockTable();
  const res = await pool.query(`
    SELECT holder_id, client_id, industry, vertical, location, acquired_at, expires_at
    FROM scout_lock
    WHERE lock_key = $1 AND (expires_at IS NULL OR expires_at > NOW())
  `, [LOCK_KEY]);
  return res.rows[0] || null;
}

async function acquireScoutLockWithWait(meta, { logFn } = {}) {
  const deadline = Date.now() + WAIT_MAX_MS;

  while (Date.now() < deadline) {
    const holderId = await tryAcquireScoutLock(meta);
    if (holderId) return holderId;

    const active = await getActiveScoutLock();
    if (logFn && active) {
      console.log(`[Scout] Waiting for lock held by client ${active.client_id} (${active.vertical || active.industry}) since ${active.acquired_at}`);
    }
    await new Promise(r => setTimeout(r, POLL_MS));
  }

  return null;
}

module.exports = {
  ensureScoutLockTable,
  acquireScoutLockWithWait,
  releaseScoutLock,
  getActiveScoutLock,
  POLL_MS,
  WAIT_MAX_MS,
};
