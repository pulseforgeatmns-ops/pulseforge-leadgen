'use strict';

const BUCKETS = Object.freeze(['eligible_now', 'unsupported_vertical', 'attempt_limit_hold', 'retry_cooldown', 'unknown']);
const { buildEnrichmentEligibilitySql, buildEnrichmentRetryAfterSql } = require('../utils/scoutEnrichmentEligibility');
const ELIGIBILITY_SQL = buildEnrichmentEligibilitySql({ alias: 'u.', verticals: '$5::text[]' });
const RETRY_AFTER_SQL = buildEnrichmentRetryAfterSql();
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
module.exports = { ELIGIBILITY_SQL, RETRY_AFTER_SQL, createQueueAccounting, BUCKETS };
