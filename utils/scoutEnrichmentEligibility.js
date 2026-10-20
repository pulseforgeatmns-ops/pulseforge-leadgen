'use strict';

// PostgreSQL evaluates both selection and accounting at full timestamp precision.
// Parameters below are internal SQL expressions, never user-provided strings.
const DAILY_RETRY_AFTER_ATTEMPT = 3;
const LONG_HOLD_AFTER_ATTEMPT = 6;

function buildEnrichmentEligibilitySql({ alias = '', verticals = '$5::text[]',
  unrestrictedVerticals = false, maxAttempts = String(LONG_HOLD_AFTER_ATTEMPT), retryInterval = "INTERVAL '1 hour'",
  dailyInterval = "INTERVAL '24 hours'", longInterval = "INTERVAL '7 days'" } = {}) {
  const attempts = `COALESCE(${alias}enrichment_attempts, 0)`;
  const elapsed = interval => `(COALESCE(${alias}last_attempt_at, NOW() - INTERVAL '100 years') <= NOW() - ${interval})`;
  const vertical = `${alias}vertical = ANY(${verticals})`;
  const allowed = unrestrictedVerticals ? `(${verticals} IS NULL OR ${vertical})` : vertical;
  return `CASE
  WHEN (${allowed}) IS NOT TRUE THEN 'unsupported_vertical'
  WHEN ${attempts} >= ${maxAttempts}
    AND ${elapsed(longInterval)} IS NOT TRUE THEN 'attempt_limit_hold'
  WHEN ${attempts} >= ${DAILY_RETRY_AFTER_ATTEMPT} AND ${attempts} < ${maxAttempts}
    AND ${elapsed(dailyInterval)} IS NOT TRUE THEN 'retry_cooldown'
  WHEN ${attempts} < ${DAILY_RETRY_AFTER_ATTEMPT} AND ${attempts} < ${maxAttempts}
    AND ${elapsed(retryInterval)} IS NOT TRUE THEN 'retry_cooldown'
  ELSE 'eligible_now' END`;
}

// Durable tickets must wake on the same tier boundary as queue selection.
function buildEnrichmentRetryAfterSql({ alias = 'u.', verticals = '$5::text[]' } = {}) {
  const attempts = `COALESCE(${alias}enrichment_attempts, 0)`;
  return `CASE WHEN ${alias}vertical = ANY(${verticals}) THEN
    GREATEST(0, EXTRACT(EPOCH FROM (COALESCE(${alias}last_attempt_at + CASE
      WHEN ${attempts} >= ${LONG_HOLD_AFTER_ATTEMPT} THEN INTERVAL '7 days'
      WHEN ${attempts} >= ${DAILY_RETRY_AFTER_ATTEMPT} THEN INTERVAL '24 hours'
      ELSE INTERVAL '1 hour' END, NOW()) - NOW())) * 1000)::bigint
    ELSE NULL END`;
}

module.exports = { buildEnrichmentEligibilitySql, buildEnrichmentRetryAfterSql,
  DAILY_RETRY_AFTER_ATTEMPT, LONG_HOLD_AFTER_ATTEMPT };
