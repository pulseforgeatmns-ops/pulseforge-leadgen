'use strict';

/**
 * MAX-P0-003 — redaction for durable reliability audit records.
 *
 * Audit rows must explain what happened without retaining message content,
 * recipients or credentials. Keys that carry content or secrets are replaced;
 * e-mail addresses inside any remaining string are masked; long strings and
 * deep structures are truncated.
 */

const crypto = require('crypto');

const SECRET_KEY = /(pass(word)?|secret|token|api[-_]?key|authorization|cookie|session|credential)/i;
const CONTENT_KEY = /^(body|html|htmlcontent|text|textcontent|subject|message|content|to|toemail|toname|email|recipient|recipients|cc|bcc|phone)$/i;
const EMAIL = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
const MAX_STRING = 300;
const MAX_DEPTH = 5;
const MAX_KEYS = 40;

function shortHash(value) {
  return crypto.createHash('sha256').update(String(value)).digest('hex').slice(0, 16);
}

function redactString(value) {
  const masked = String(value).replace(EMAIL, (m) => `[email:${shortHash(m.toLowerCase())}]`);
  return masked.length > MAX_STRING ? `${masked.slice(0, MAX_STRING)}…[truncated]` : masked;
}

function redact(value, depth = 0) {
  if (value == null) return value;
  if (typeof value === 'string') return redactString(value);
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (value instanceof Date) return value.toISOString();
  if (depth >= MAX_DEPTH) return '[depth_limit]';
  if (Array.isArray(value)) return value.slice(0, MAX_KEYS).map((v) => redact(v, depth + 1));
  if (typeof value === 'object') {
    const out = {};
    for (const [key, v] of Object.entries(value).slice(0, MAX_KEYS)) {
      if (SECRET_KEY.test(key)) out[key] = '[redacted:secret]';
      else if (CONTENT_KEY.test(key)) out[key] = v == null ? v : '[redacted:content]';
      else out[key] = redact(v, depth + 1);
    }
    return out;
  }
  return String(value);
}

module.exports = { redact, shortHash };
