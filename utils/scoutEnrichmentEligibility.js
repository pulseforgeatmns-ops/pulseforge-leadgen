'use strict';

// Shared SQL preserves the existing enrichment policy. Classification and ticket
// scheduling must not create authority for more frequent paid research.
const LONG_HOLD_AFTER_ATTEMPT = 3;

function buildEnrichmentEligibilitySql({ alias = '', verticals = '$5::text[]',
  unrestrictedVerticals = false, maxAttempts = String(LONG_HOLD_AFTER_ATTEMPT),
  retryInterval = "INTERVAL '1 hour'", longInterval = "INTERVAL '7 days'" } = {}) {
  const attempts = `COALESCE(${alias}enrichment_attempts, 0)`;
  const vertical = `${alias}vertical = ANY(${verticals})`;
  const allowed = unrestrictedVerticals ? `(${verticals} IS NULL OR ${vertical})` : vertical;
  return `CASE
  WHEN (${allowed}) IS NOT TRUE THEN 'unsupported_vertical'
  WHEN (${attempts} < ${maxAttempts}
    OR ${alias}last_attempt_at <= NOW() - ${longInterval}) IS NOT TRUE THEN 'attempt_limit_hold'
  WHEN (COALESCE(${alias}last_attempt_at, NOW() - INTERVAL '100 years')
    <= NOW() - ${retryInterval}) IS NOT TRUE THEN 'retry_cooldown'
  ELSE 'eligible_now' END`;
}

function buildEnrichmentRetryAfterSql({ alias = 'u.', verticals = '$5::text[]' } = {}) {
  const attempts = `COALESCE(${alias}enrichment_attempts, 0)`;
  // Missing last-attempt evidence cannot establish that a long hold has elapsed.
  return `CASE WHEN ${alias}vertical = ANY(${verticals})
    AND NOT (${attempts} >= ${LONG_HOLD_AFTER_ATTEMPT} AND ${alias}last_attempt_at IS NULL) THEN
    GREATEST(0, EXTRACT(EPOCH FROM (GREATEST(
      CASE WHEN ${attempts} >= ${LONG_HOLD_AFTER_ATTEMPT}
        THEN ${alias}last_attempt_at + INTERVAL '7 days' ELSE NOW() END,
      COALESCE(${alias}last_attempt_at + INTERVAL '1 hour', NOW())) - NOW())) * 1000)::bigint
    ELSE NULL END`;
}

module.exports = { buildEnrichmentEligibilitySql, buildEnrichmentRetryAfterSql, LONG_HOLD_AFTER_ATTEMPT };
