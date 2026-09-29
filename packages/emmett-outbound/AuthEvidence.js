'use strict';

/**
 * SPEC-255 — canonical authentication evidence normalization.
 * PASS / FAIL / UNKNOWN epistemic semantics for SPF, DKIM, DMARC, SMTP.
 */

const AUTH_STATE = Object.freeze({
  PASS: 'PASS',
  FAIL: 'FAIL',
  UNKNOWN: 'UNKNOWN',
});

const PASS_STATUSES = new Set(['present', 'verified', 'pass', 'valid', 'yes', 'authenticated']);
const FAIL_STATUSES = new Set(['missing', 'failed', 'fail', 'invalid', 'no']);

function authStateOf(value) {
  if (value && typeof value === 'object' && value.state) {
    const state = String(value.state).toUpperCase();
    if (state === AUTH_STATE.PASS || state === AUTH_STATE.FAIL || state === AUTH_STATE.UNKNOWN) {
      return state;
    }
  }
  if (value === true) return AUTH_STATE.PASS;
  if (value === false) return AUTH_STATE.FAIL;
  const v = String(value || '').toLowerCase();
  if (!v) return AUTH_STATE.UNKNOWN;
  if (PASS_STATUSES.has(v) || v === 'reject' || v === 'quarantine') return AUTH_STATE.PASS;
  if (FAIL_STATUSES.has(v)) return AUTH_STATE.FAIL;
  if (v === 'none' || v === 'p=none') return AUTH_STATE.PASS;
  if (v === 'not_checked' || v === 'unknown') return AUTH_STATE.UNKNOWN;
  return AUTH_STATE.UNKNOWN;
}

function authPass(value) {
  return authStateOf(value) === AUTH_STATE.PASS;
}

function authFail(value) {
  return authStateOf(value) === AUTH_STATE.FAIL;
}

function authUnknown(value) {
  return authStateOf(value) === AUTH_STATE.UNKNOWN;
}

function mapVerificationField(field = {}, key = null) {
  const status = String(field.status || '').toLowerCase();
  let state = AUTH_STATE.UNKNOWN;
  if (PASS_STATUSES.has(status)) state = AUTH_STATE.PASS;
  else if (FAIL_STATUSES.has(status)) state = AUTH_STATE.FAIL;
  else if (!status || status === 'not_checked') state = AUTH_STATE.UNKNOWN;

  return {
    state,
    provenance: {
      source: 'verification_state',
      key,
      status: field.status || 'not_checked',
      code: field.code || null,
      message: field.message || null,
      reason: field.reason || null,
    },
  };
}

function authenticationFromVerificationState(verificationState = {}) {
  const vs = verificationState || {};
  return {
    spf: mapVerificationField(vs.spf, 'spf'),
    dkim: mapVerificationField(vs.dkim, 'dkim'),
    dmarc: mapVerificationField(vs.dmarc, 'dmarc'),
    smtp: mapVerificationField(vs.smtp, 'smtp'),
    imap: mapVerificationField(vs.imap, 'imap'),
    provenance: { source: 'tenant_mailbox_integrations.verification_state' },
  };
}

function dmarcScore(value) {
  const state = authStateOf(value);
  if (state === AUTH_STATE.FAIL) return 0;
  if (state === AUTH_STATE.UNKNOWN) return 4;
  const v = String(value?.provenance?.status || value || '').toLowerCase();
  if (v === 'none' || v === 'p=none') return 4;
  return 8;
}

function authFactorForCapacity(auth = {}) {
  const keys = ['spf', 'dkim', 'dmarc'];
  const states = keys.map((key) => authStateOf(auth[key]));
  if (states.every((s) => s === AUTH_STATE.PASS)) return 1;
  if (states.some((s) => s === AUTH_STATE.FAIL)) return 0.55;
  return 0.55;
}

function allAuthPass(auth = {}, keys = ['spf', 'dkim', 'dmarc']) {
  return keys.every((key) => authPass(auth[key]));
}

function anyAuthFail(auth = {}, keys = ['spf', 'dkim', 'dmarc', 'smtp']) {
  return keys.some((key) => authFail(auth[key]));
}

function anyAuthUnknown(auth = {}, keys = ['spf', 'dkim', 'dmarc']) {
  return keys.some((key) => authUnknown(auth[key]));
}

module.exports = {
  AUTH_STATE,
  authStateOf,
  authPass,
  authFail,
  authUnknown,
  mapVerificationField,
  authenticationFromVerificationState,
  dmarcScore,
  authFactorForCapacity,
  allAuthPass,
  anyAuthFail,
  anyAuthUnknown,
};
