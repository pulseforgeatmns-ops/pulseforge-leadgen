'use strict';

/**
 * MAX-P0-003 — single-flight worker lock that cannot leak.
 *
 * Lock, work and unlock all happen while ONE dedicated connection is held, so
 * the unlock can never land on a different pooled connection (where it would
 * return false, leave the lock on an idle pooled connection, and let the next
 * borrower of that connection re-enter). If the unlock fails, the connection is
 * destroyed instead of returned to the pool, which releases the lock.
 *
 * @param {import('pg').Pool} pool
 * @param {number|string} key  bigint advisory key
 * @param {() => Promise<any>} fn
 * @param {{ label?: string }} [opts]
 */
async function withWorkerAdvisoryLock(pool, key, fn, opts = {}) {
  const label = opts.label || 'worker';
  const client = await pool.connect();
  let locked = false;
  let releaseError;
  try {
    locked = (await client.query('SELECT pg_try_advisory_lock($1) AS locked', [key])).rows[0]?.locked === true;
    if (!locked) return { skipped: true, reason: 'worker_already_running' };
    return await fn();
  } finally {
    if (locked) {
      try {
        const r = await client.query('SELECT pg_advisory_unlock($1) AS unlocked', [key]);
        if (r.rows[0]?.unlocked !== true) releaseError = new Error('advisory unlock returned false');
      } catch (err) {
        releaseError = err;
      }
      if (releaseError) console.error(`[${label}] advisory unlock failed; discarding connection:`, releaseError.message);
    }
    client.release(releaseError || undefined);
  }
}

module.exports = { withWorkerAdvisoryLock };
