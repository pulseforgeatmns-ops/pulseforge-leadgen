'use strict';

/**
 * Bounded retention for max_interpretation_shadow (P0-MAX-REASONING-001 PR 2).
 *
 * Observations are triage metadata: frame enums, candidate ids, latency,
 * tokens and failure reasons. They are kept for RETENTION_DAYS and then
 * physically deleted by a governed maintenance run
 * (scripts/maxInterpretationShadowPurge.js). Deletion is batched so a large
 * backlog never holds a long lock, and is bounded by observed_at only, so a
 * purge can never remove a row newer than the cutoff for any tenant.
 */

const RETENTION_DAYS = 90;
const MAX_BATCH = 10000;

function cutoffFor(now, retentionDays) {
  if (!Number.isInteger(retentionDays) || retentionDays < 1 || retentionDays > 3650) {
    throw new Error('shadow_retention_days_invalid');
  }
  return new Date(now.getTime() - retentionDays * 86400000);
}

/** Count what a purge would delete (read-only). */
async function countExpired(db, { now = new Date(), retentionDays = RETENTION_DAYS } = {}) {
  const cutoff = cutoffFor(now, retentionDays);
  const { rows } = await db.query(
    'SELECT COUNT(*)::integer AS n, MIN(observed_at) AS oldest FROM max_interpretation_shadow WHERE observed_at < $1',
    [cutoff]
  );
  return { cutoff: cutoff.toISOString(), expired: rows[0].n, oldest: rows[0].oldest };
}

/** Delete expired observations in bounded batches. */
async function purgeExpired(db, { now = new Date(), retentionDays = RETENTION_DAYS, batchSize = 5000, maxBatches = 1000 } = {}) {
  const cutoff = cutoffFor(now, retentionDays);
  if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > MAX_BATCH) throw new Error('shadow_retention_batch_invalid');
  let deleted = 0;
  let batches = 0;
  for (; batches < maxBatches; batches += 1) {
    const { rowCount } = await db.query(
      `DELETE FROM max_interpretation_shadow WHERE id IN (
         SELECT id FROM max_interpretation_shadow WHERE observed_at < $1 ORDER BY observed_at LIMIT $2)`,
      [cutoff, batchSize]
    );
    deleted += rowCount;
    if (rowCount < batchSize) { batches += 1; break; }
  }
  return { cutoff: cutoff.toISOString(), deleted, batches };
}

module.exports = { RETENTION_DAYS, countExpired, purgeExpired };
