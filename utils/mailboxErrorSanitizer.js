'use strict';

/**
 * Canonical mailbox/runtime error sanitizer (IMAP, SMTP, OAuth refresh, activation).
 * Redacts explicit credential assignments; preserves OAuth diagnostic terminology.
 */

const CREDENTIAL_ASSIGNMENT =
  /\b(password|passwd|pass|auth|access[_-]?token|refresh[_-]?token|client[_-]?secret)\s*=\s*(?:\[[^\]]*\]|[^&\s,\n]+)/gi;

const SMTP_AUTH_FAILED_PASSWORD = /\bsmtp auth failed password=(?:\[[^\]]*\]|[^\s]+)/gi;
const AUTH_FAILED_PASSWORD = /\bauth failed password=(?:\[[^\]]*\]|[^\s]+)/gi;

const BEARER_TOKEN = /Bearer\s+[A-Za-z0-9._-]+/gi;

const OBJECT_OBJECT = /\[object Object\]/gi;

function normalizeCredentialKey(key) {
  const k = String(key || '').toLowerCase().replace(/-/g, '_');
  if (k === 'pass') return 'pass';
  if (k === 'passwd') return 'passwd';
  if (k === 'password') return 'password';
  if (k === 'auth') return 'auth';
  if (k === 'access_token' || k === 'accesstoken') return 'access_token';
  if (k === 'refresh_token' || k === 'refreshtoken') return 'refresh_token';
  if (k === 'client_secret' || k === 'clientsecret') return 'client_secret';
  return k;
}

function appendSafeGoogleDiagnostics(parts, err) {
  if (!err || typeof err !== 'object') return;
  if (err.httpStatus != null && Number.isFinite(Number(err.httpStatus))) {
    parts.push(`httpStatus=${err.httpStatus}`);
  }
  if (typeof err.error === 'string' && err.error.trim()) {
    parts.push(`error=${err.error.trim()}`);
  }
  if (typeof err.error_description === 'string' && err.error_description.trim()) {
    parts.push(`error_description=${err.error_description.trim()}`);
  }
}

function rawErrorText(err) {
  if (err == null) return 'unknown_error';
  if (typeof err === 'string') return err;
  const parts = [];
  if (err instanceof Error || typeof err.message === 'string') {
    if (err.message) parts.push(String(err.message));
  } else if (typeof err === 'object') {
    try {
      parts.push(JSON.stringify(err));
    } catch (_e) {
      parts.push(String(err));
    }
  } else {
    parts.push(String(err));
  }
  appendSafeGoogleDiagnostics(parts, err);
  return parts.filter(Boolean).join(' ') || 'unknown_error';
}

function redactCredentialAssignments(text) {
  return String(text).replace(CREDENTIAL_ASSIGNMENT, (match, key) => {
    if (/\=\[redacted\]$/i.test(match)) return match;
    const normalized = normalizeCredentialKey(key);
    return `${normalized}=[redacted]`;
  });
}

function collapseAuthFailurePhrases(text) {
  return String(text)
    .replace(SMTP_AUTH_FAILED_PASSWORD, 'smtp auth=[redacted]')
    .replace(AUTH_FAILED_PASSWORD, 'auth=[redacted]');
}

function sanitizeMailboxErrorText(raw) {
  let text = String(raw ?? 'unknown_error');
  text = redactCredentialAssignments(text);
  text = text.replace(OBJECT_OBJECT, '[redacted]');
  text = redactCredentialAssignments(text);
  text = collapseAuthFailurePhrases(text);
  text = text.replace(BEARER_TOKEN, 'Bearer [redacted]');
  return text.slice(0, 500);
}

function sanitizeMailboxError(err) {
  return sanitizeMailboxErrorText(rawErrorText(err));
}

module.exports = {
  sanitizeMailboxError,
  sanitizeMailboxErrorText,
  rawErrorText,
  redactCredentialAssignments,
};
