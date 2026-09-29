'use strict';

const SECRET_PATTERNS = [
  /\b(?:password|passwd|pwd)\s*[:=]\s*\S+/gi,
  /\b(?:api[_-]?key|apikey|secret|token|auth(?:orization)?)\s*[:=]\s*\S+/gi,
  /\bbearer\s+[a-z0-9._-]+/gi,
  /\bsk-[a-z0-9]{10,}\b/gi,
  /\b(?:cookie|session(?:_id)?)\s*[:=]\s*\S+/gi,
  /\b(?:DATABASE_URL|SMTP|IMAP|BREVO|ANTHROPIC|PROSPEO|GOOGLE)[_\w]*\s*[:=]\s*\S+/gi,
];

function redactString(value) {
  let text = String(value ?? '');
  for (const pattern of SECRET_PATTERNS) {
    text = text.replace(pattern, '[REDACTED]');
  }
  return text;
}

function redactValue(value) {
  if (typeof value === 'string') return redactString(value);
  if (Array.isArray(value)) return value.map(redactValue);
  if (value && typeof value === 'object') return redactObject(value);
  return value;
}

function redactObject(obj) {
  if (!obj || typeof obj !== 'object') return obj;
  const out = Array.isArray(obj) ? [] : {};
  for (const [key, val] of Object.entries(obj)) {
    if (/password|token|secret|cookie|authorization|api_key|apikey/i.test(key)) {
      out[key] = '[REDACTED]';
    } else {
      out[key] = redactValue(val);
    }
  }
  return out;
}

module.exports = {
  redactString,
  redactValue,
  redactObject,
};
