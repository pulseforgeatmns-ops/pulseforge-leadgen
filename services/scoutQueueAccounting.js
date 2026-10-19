'use strict';

const BUCKETS = Object.freeze(['eligible_now', 'unsupported_vertical', 'attempt_limit_hold', 'retry_cooldown', 'unknown']);
// Same predicates as the enrichment selector, evaluated by PostgreSQL at its
// timestamp precision. A JS Date comparison would lose microseconds at boundaries.
// First failure is vertical, attempts, then retry; predicates are not additive.
const ELIGIBILITY_SQL = `CASE
  WHEN (u.vertical = ANY($5::text[])) IS NOT TRUE THEN 'unsupported_vertical'
  WHEN COALESCE(u.enrichment_attempts, 0) >= 6
    AND (COALESCE(u.last_attempt_at, NOW() - INTERVAL '100 years')
      <= NOW() - INTERVAL '7 days') IS NOT TRUE THEN 'attempt_limit_hold'
  WHEN COALESCE(u.enrichment_attempts, 0) >= 3
    AND COALESCE(u.enrichment_attempts, 0) < 6
    AND (COALESCE(u.last_attempt_at, NOW() - INTERVAL '100 years')
      <= NOW() - INTERVAL '24 hours') IS NOT TRUE THEN 'retry_cooldown'
  WHEN COALESCE(u.enrichment_attempts, 0) < 3
    AND (COALESCE(u.last_attempt_at, NOW() - INTERVAL '100 years')
      <= NOW() - INTERVAL '1 hour') IS NOT TRUE THEN 'retry_cooldown'
  ELSE 'eligible_now' END`;
function createQueueAccounting() {
  const matched = new Map();
  let duplicateCandidateMatches = 0;
  let unobservedCandidateMatches = 0;
  return {
    observe(rows, { tenantId } = {}) {
      duplicateCandidateMatches++;
      if (!Array.isArray(rows) || !rows.length) { unobservedCandidateMatches++; return; }
      let found = false;
      for (const row of rows) {
        if (String(row.client_id) !== String(tenantId) || row.id == null) continue;
        found = true;
        const id = String(row.id).toLowerCase();
        const bucket = BUCKETS.includes(row.eligibility_bucket) ? row.eligibility_bucket : 'unknown';
        // A row seen changing across candidate observations is not a stable snapshot.
        matched.set(id, matched.has(id) && matched.get(id) !== bucket ? 'unknown' : bucket);
      }
      if (!found) unobservedCandidateMatches++;
    },
    snapshot() {
      const firstFailureCounts = Object.fromEntries(BUCKETS.map(key => [key, 0]));
      for (const bucket of matched.values()) firstFailureCounts[bucket]++;
      return { scope: 'distinct_matched_queue_rows', duplicateCandidateMatches,
        unobservedCandidateMatches, distinctMatchedRows: matched.size, firstFailureCounts };
    },
  };
}
module.exports = { ELIGIBILITY_SQL, createQueueAccounting, BUCKETS };
