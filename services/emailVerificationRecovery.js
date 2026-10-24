'use strict';

/**
 * Safe retry of email_not_verified inventory. Never promotes invalid or risky
 * addresses merely to fill inventory.
 */

const { withResearchProviderScope, rethrowResearchProviderControl } = require('../utils/anchorResearchProviderPause');
const { invalidOutreachEmailReason } = require('../utils/emailGuard');
const { resolveEmailVerification } = require('../leadgen');

const STALE_VERIFICATION_MS = 24 * 60 * 60 * 1000;
const RETRYABLE_STATES = Object.freeze([
  'verification_never_attempted',
  'verifier_unavailable',
  'verifier_timeout',
  'stale_verification',
]);
// Fixed branch labels only. Never copy provider reasons, errors or candidate
// data into telemetry. A failed CAS does not reveal which snapshot field moved.
const VERIFICATION_RETRY_FAILURE_REASONS = Object.freeze([
  'verification_failed',
  'adapter_unavailable',
  'candidate_ineligible',
  'persistence_not_confirmed',
  'verification_exception',
  'persistence_exception',
  'unexpected_exception',
]);

function verificationRetryFailureReasons(counts = {}) {
  return Object.fromEntries(VERIFICATION_RETRY_FAILURE_REASONS.map(reason => {
    const count = counts?.[reason];
    return [reason, Number.isSafeInteger(count) && count >= 0 ? count : 0];
  }));
}

function recordRetryFailure(telemetry, reason) {
  telemetry.verificationRetryFailed += 1;
  telemetry.verificationRetryFailureReasons[reason] += 1;
}

function emptyVerificationRetryTelemetry() {
  return {
    emailNotVerified: 0,
    verificationRetryAttempted: 0,
    verificationRetryValid: 0,
    verificationRetryRisky: 0,
    verificationRetryInvalid: 0,
    verificationRetryFailed: 0,
    verificationRetryFailureReasons: verificationRetryFailureReasons(),
    // A failed candidate read happens before attempts, so it must not inflate
    // verificationRetryFailed or the per-attempt failure reason totals.
    verificationRetryLoadFailed: 0,
    skipped: {
      verification_never_attempted: 0,
      verifier_unavailable: 0,
      verifier_timeout: 0,
      risky_catchall: 0,
      invalid: 0,
      stale_verification: 0,
      malformed_email: 0,
    },
  };
}

function mergeVerificationRetryTelemetry(target, delta = {}) {
  if (!target || !delta || target === delta) return target;
  for (const key of [
    'emailNotVerified',
    'verificationRetryAttempted',
    'verificationRetryValid',
    'verificationRetryRisky',
    'verificationRetryInvalid',
    'verificationRetryFailed',
    'verificationRetryLoadFailed',
  ]) {
    target[key] = Number(target[key] || 0) + Number(delta[key] || 0);
  }
  const failures = verificationRetryFailureReasons(target.verificationRetryFailureReasons);
  const added = verificationRetryFailureReasons(delta.verificationRetryFailureReasons);
  for (const reason of VERIFICATION_RETRY_FAILURE_REASONS) {
    failures[reason] = Math.min(Number.MAX_SAFE_INTEGER, failures[reason] + added[reason]);
  }
  target.verificationRetryFailureReasons = failures;
  target.skipped = target.skipped || emptyVerificationRetryTelemetry().skipped;
  for (const [reason, count] of Object.entries(delta.skipped || {})) {
    target.skipped[reason] = Number(target.skipped[reason] || 0) + Number(count || 0);
  }
  return target;
}

function classifyUnverifiedReason(row = {}) {
  const email = String(row.email || '').trim();
  if (!email || invalidOutreachEmailReason(email)) return 'malformed_email';

  const status = String(row.email_status || '').toLowerCase();
  if (status === 'invalid' || status === 'undeliverable') return 'invalid';
  if (status === 'risky' || status === 'catchall' || status === 'catch_all' || status === 'accept_all') {
    return 'risky_catchall';
  }

  const method = String(row.email_verification_method || row.verifier_response?.method || '').toLowerCase();
  const reason = String(row.verifier_response?.reason || row.verifier_response?.error || '').toLowerCase();
  if (reason.includes('timeout') || method.includes('timeout') || reason === 'verifier_timeout') {
    return 'verifier_timeout';
  }
  if (
    reason.includes('unavailable')
    || reason === 'fetch_unavailable'
    || reason === 'http_error'
    || method.includes('unavailable')
  ) {
    return 'verifier_unavailable';
  }

  const checkedAt = row.verifier_checked_at || row.verified_at;
  if (!checkedAt && !method && !status) return 'verification_never_attempted';
  if (!checkedAt && (status === 'unknown' || status === 'unverified' || status === 'unverified_legacy' || !status)) {
    return 'verification_never_attempted';
  }
  if (checkedAt) {
    const age = Date.now() - +new Date(checkedAt);
    if (Number.isFinite(age) && age > STALE_VERIFICATION_MS) return 'stale_verification';
  }
  if (status === 'unknown' || !status) return 'verification_never_attempted';
  return 'stale_verification';
}

function isRetryableUnverifiedReason(reason) {
  return RETRYABLE_STATES.includes(reason);
}

function classifyRetryOutcome(verification = {}) {
  const status = String(verification.emailStatus || '').toLowerCase();
  if (verification.emailVerified === true && verification.reject !== true) return 'valid';
  if (status === 'invalid' || status === 'undeliverable') return 'invalid';
  if (status === 'risky' || status === 'catchall' || status === 'catch_all') return 'risky';
  if (verification.reject) return 'invalid';
  return 'failed';
}

function validatedClientId(value) {
  const parsed = typeof value === 'number' || (typeof value === 'string' && /^[1-9]\d*$/.test(value))
    ? Number(value) : NaN;
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError('Email verification retry requires a positive integer clientId');
  }
  return parsed;
}

async function loadUnverifiedProspects(pool, { clientId, limit = 15 } = {}) {
  const { rows } = await pool.query(`
    SELECT id, client_id, email, email_verified, email_status, email_verification_method,
      verifier_response, verifier_response::text AS verifier_response_snapshot,
      verifier_checked_at::text AS verifier_checked_at,
      verified_at::text AS verified_at, do_not_contact, company_id
    FROM prospects
    WHERE client_id = $1
      AND email IS NOT NULL
      AND COALESCE(do_not_contact, false) = false
      AND COALESCE(email_verified, false) = false
    ORDER BY prospects.verifier_checked_at ASC NULLS FIRST, updated_at ASC NULLS LAST, id ASC
    LIMIT $2
  `, [clientId, Math.max(1, Number(limit) || 15)]);
  return rows;
}

async function persistVerifiedRetry(pool, clientId, row, email, verification) {
  // Compare the verification snapshot at write time: an in-flight verifier must
  // not overwrite a changed address, owner, opt-out or newer verification.
  // The SELECT preserves timestamp microseconds as text for these comparisons.
  const result = await pool.query(`
    UPDATE prospects
    SET email_verified = $2,
        email_verification_method = $3,
        verified_at = $4,
        email_status = $5,
        verifier_response = $6::jsonb,
        verifier_checked_at = $7,
        do_not_contact = $8,
        notes = COALESCE(notes, '') || $9
    WHERE id = $1 AND client_id = $10
      AND LOWER(BTRIM(email)) = $11
      AND COALESCE(do_not_contact, false) = false
      AND COALESCE(email_verified, false) = false
      AND email_verified IS NOT DISTINCT FROM $12
      AND email_verification_method IS NOT DISTINCT FROM $13
      AND verified_at IS NOT DISTINCT FROM $14::timestamptz
      AND email_status IS NOT DISTINCT FROM $15
      AND verifier_response IS NOT DISTINCT FROM $16::jsonb
      AND verifier_checked_at IS NOT DISTINCT FROM $17::timestamptz
      AND company_id IS NOT DISTINCT FROM $18
      AND do_not_contact IS NOT DISTINCT FROM $19
  `, [
    row.id,
    verification.emailVerified === true,
    verification.emailVerificationMethod,
    verification.verifiedAt,
    verification.emailStatus,
    JSON.stringify(verification.verifierResponse || null),
    verification.verifierCheckedAt,
    verification.doNotContact === true,
    ' | email verification retried during Scout replenishment.',
    clientId,
    email,
    row.email_verified ?? null,
    row.email_verification_method ?? null,
    row.verified_at ?? null,
    row.email_status ?? null,
    row.verifier_response_snapshot ?? null,
    row.verifier_checked_at ?? null,
    row.company_id ?? null,
    row.do_not_contact ?? null,
  ]);
  return result.rowCount === 1;
}

async function retryUnverifiedEmails(pool, options = {}) {
  const tenantId = options.clientId === undefined ? 10 : options.clientId;
  return withResearchProviderScope(tenantId, () => retryUnverifiedEmailsScoped(pool, options));
}

async function retryUnverifiedEmailsScoped(pool, {
  clientId = 10,
  limit = 15,
  verify = resolveEmailVerification,
  now = Date.now(),
} = {}) {
  const tenantId = validatedClientId(clientId);
  const telemetry = emptyVerificationRetryTelemetry();
  const rows = await loadUnverifiedProspects(pool, { clientId: tenantId, limit }).catch(error => {
    rethrowResearchProviderControl(error);
    telemetry.verificationRetryLoadFailed += 1;
    return [];
  });
  telemetry.emailNotVerified = rows.length;

  for (const row of rows) {
    const reason = classifyUnverifiedReason(row);
    if (!isRetryableUnverifiedReason(reason)) {
      telemetry.skipped[reason] = Number(telemetry.skipped[reason] || 0) + 1;
      continue;
    }

    telemetry.verificationRetryAttempted += 1;
    let failureReason = 'unexpected_exception';
    try {
      if (Number(row.client_id) !== tenantId || row.do_not_contact === true || row.email_verified === true) {
        recordRetryFailure(telemetry, 'candidate_ineligible');
        continue;
      }
      const email = String(row.email).trim().toLowerCase();
      if (typeof verify !== 'function') {
        recordRetryFailure(telemetry, 'adapter_unavailable');
        continue;
      }
      failureReason = 'verification_exception';
      const verification = await withResearchProviderScope(row.client_id, () => verify(email, {
        email,
        source: ['verification_retry'],
        now,
      }));
      failureReason = 'unexpected_exception';
      const outcome = classifyRetryOutcome(verification);
      if (outcome === 'failed') {
        recordRetryFailure(telemetry, 'verification_failed');
        continue;
      }
      failureReason = 'persistence_exception';
      const persisted = await persistVerifiedRetry(pool, tenantId, row, email, outcome === 'valid'
        ? verification
        : { ...verification, emailVerified: false, doNotContact: true });
      if (!persisted) {
        recordRetryFailure(telemetry, 'persistence_not_confirmed');
      } else if (outcome === 'valid') {
        telemetry.verificationRetryValid += 1;
      } else if (outcome === 'risky') {
        telemetry.verificationRetryRisky += 1;
      } else if (outcome === 'invalid') {
        telemetry.verificationRetryInvalid += 1;
      }
    } catch (_err) {
      rethrowResearchProviderControl(_err);
      recordRetryFailure(telemetry, failureReason);
    }
  }

  return telemetry;
}

module.exports = {
  RETRYABLE_STATES,
  STALE_VERIFICATION_MS,
  VERIFICATION_RETRY_FAILURE_REASONS,
  verificationRetryFailureReasons,
  emptyVerificationRetryTelemetry,
  mergeVerificationRetryTelemetry,
  classifyUnverifiedReason,
  isRetryableUnverifiedReason,
  retryUnverifiedEmails,
};
