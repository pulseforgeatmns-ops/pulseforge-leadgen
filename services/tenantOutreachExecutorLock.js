'use strict';

// Shared with both durable scheduler entrypoints. Reconciliation must exclude
// a durable dispatch as well as the governed tenant clock before reading proof.
const EXECUTOR_LOCK_NAMESPACE = 701252;

async function withTenantOutreachExecutorLock(pool, fn) {
  const db = await pool.connect();
  let locked = false;
  try {
    locked = (await db.query('SELECT pg_try_advisory_lock($1,$2) AS locked', [EXECUTOR_LOCK_NAMESPACE, 0])).rows[0]?.locked === true;
    if (!locked) return { halted: 'scheduler_overlap' };
    return await fn();
  } finally {
    if (locked) await db.query('SELECT pg_advisory_unlock($1,$2)', [EXECUTOR_LOCK_NAMESPACE, 0]);
    db.release();
  }
}

module.exports = { EXECUTOR_LOCK_NAMESPACE, withTenantOutreachExecutorLock };
