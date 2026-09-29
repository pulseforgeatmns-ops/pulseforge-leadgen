'use strict';

/**
 * Safe retry of email_not_verified inventory. Never promotes invalid or risky
 * addresses merely to fill inventory.
 */

const { invalidOutreachEmailReason } = require('../utils/emailGuard');
const { resolveEmailVerification } = require('../leadgen');

const STALE_VERIFICATION_MS = 24 * 60 * 60 * 1000;
const RETRYABLE_STATES = Object.freeze([
  'verification_never_attempted',
  'verifier_unavailable',
  'verifier_timeout',
  'stale_verification',
]);

function emptyVerificationRetryTelemetry() {
  return {
    emailNotVerified: 0,
    verificationRetryAttempted: 0,
    verificationRetryValid: 0,
    verificationRetryRisky: 0,
    verificationRetryInvalid: 0,
    verificationRetryFailed: 0,
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
  ]) {
    target[key] = Number(target[key] || 0) + Number(delta[key] || 0);
  }
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

async function loadUnverifiedProspects(pool, { clientId = 10, limit = 15 } = {}) {
  const { rows } = await pool.query(`
    SELECT id, email, email_verified, email_status, email_verification_method,
      verifier_response, verifier_checked_at, verified_at, do_not_contact, company_id
    FROM prospects
    WHERE client_id = $1
      AND email IS NOT NULL
      AND COALESCE(do_not_contact, false) = false
      AND COALESCE(email_verified, false) = false
    ORDER BY verifier_checked_at ASC NULLS FIRST, updated_at ASC NULLS LAST, id ASC
    LIMIT $2
  `, [Number(clientId), Math.max(1, Number(limit) || 15)]);
  return rows;
}

async function persistVerifiedRetry(pool, row, verification) {
  await pool.query(`
    UPDATE prospects
    SET email_verified = $2,
        email_verification_method = $3,
        verified_at = $4,
        email_status = $5,
        verifier_response = $6::jsonb,
        verifier_checked_at = $7,
        do_not_contact = $8,
        notes = COALESCE(notes, '') || $9
    WHERE id = $1 AND client_id = 10
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
  ]);
}

async function retryUnverifiedEmails(pool, {
  clientId = 10,
  limit = 15,
  verify = resolveEmailVerification,
  now = Date.now(),
} = {}) {
  const telemetry = emptyVerificationRetryTelemetry();
  const rows = await loadUnverifiedProspects(pool, { clientId, limit }).catch(() => []);
  telemetry.emailNotVerified = rows.length;

  for (const row of rows) {
    const reason = classifyUnverifiedReason(row);
    if (!isRetryableUnverifiedReason(reason)) {
      telemetry.skipped[reason] = Number(telemetry.skipped[reason] || 0) + 1;
      continue;
    }

    telemetry.verificationRetryAttempted += 1;
    try {
      const verification = await verify(row.email, {
        email: row.email,
        source: ['verification_retry'],
        now,
      });
      const outcome = classifyRetryOutcome(verification);
      if (outcome === 'valid') {
        await persistVerifiedRetry(pool, row, verification);
        telemetry.verificationRetryValid += 1;
      } else if (outcome === 'risky') {
        await persistVerifiedRetry(pool, row, {
          ...verification,
          emailVerified: false,
          doNotContact: true,
        });
        telemetry.verificationRetryRisky += 1;
      } else if (outcome === 'invalid') {
        await persistVerifiedRetry(pool, row, {
          ...verification,
          emailVerified: false,
          doNotContact: true,
        });
        telemetry.verificationRetryInvalid += 1;
      } else {
        telemetry.verificationRetryFailed += 1;
      }
    } catch (_err) {
      telemetry.verificationRetryFailed += 1;
    }
  }

  return telemetry;
}

module.exports = {
  RETRYABLE_STATES,
  STALE_VERIFICATION_MS,
  emptyVerificationRetryTelemetry,
  mergeVerificationRetryTelemetry,
  classifyUnverifiedReason,
  isRetryableUnverifiedReason,
  retryUnverifiedEmails,
};
