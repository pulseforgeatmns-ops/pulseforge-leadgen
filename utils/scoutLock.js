const crypto = require('crypto');
const pool = require('../db');

const LOCK_KEY = 'global';
const POLL_MS = 5000;
const WAIT_MAX_MS = 5 * 60 * 1000;
const STALE_LOCK_MS = 15 * 60 * 1000;

async function ensureScoutLockTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scout_lock (
      lock_key TEXT PRIMARY KEY,
      holder_id TEXT NOT NULL,
      client_id INTEGER,
      industry TEXT,
      vertical TEXT,
      location TEXT,
      acquired_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

async function clearStaleScoutLock() {
  await pool.query(`
    DELETE FROM scout_lock
    WHERE lock_key = $1
      AND acquired_at < NOW() - INTERVAL '15 minutes'
  `, [LOCK_KEY]);
}

async function tryAcquireScoutLock(meta) {
  await ensureScoutLockTable();
  await clearStaleScoutLock();

  const holderId = crypto.randomUUID();
  const res = await pool.query(`
    INSERT INTO scout_lock (lock_key, holder_id, client_id, industry, vertical, location, acquired_at)
    VALUES ($1, $2, $3, $4, $5, $6, NOW())
    ON CONFLICT (lock_key) DO NOTHING
    RETURNING holder_id
  `, [
    LOCK_KEY,
    holderId,
    meta.clientId ?? null,
    meta.industry ?? null,
    meta.vertical ?? null,
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
    SELECT holder_id, client_id, industry, vertical, location, acquired_at
    FROM scout_lock
    WHERE lock_key = $1
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
  STALE_LOCK_MS,
};
