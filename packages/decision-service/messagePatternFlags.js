'use strict';

const { createHash } = require('node:crypto');

const STATUS_INSPECTION_PATTERNS = [
  /\bstatus\b/i,
  /\bconfidence\b/i,
  /\bcurrent\b.*\bmission\b/i,
  /\bwhat\b.*\bwaiting\b/i,
  /\bblocked\b/i,
  /\bshow\b.*\bmission\b/i,
  /\bhow many\b.*\bprospects\b/i,
  /\bmission state\b/i,
];

const APPROVAL_PATTERNS = [
  /^yes\b/i,
  /\bapprove\b/i,
  /\bgo ahead\b/i,
  /\bdo it\b/i,
];

const REJECTION_PATTERNS = [
  /^no\b/i,
  /\breject\b/i,
  /\bnot yet\b/i,
  /\bcancel\b/i,
];

function normalizeMessage(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function matchesAny(message, patterns) {
  return patterns.some((pattern) => pattern.test(message));
}

function wordCountBucket(message) {
  const words = message ? message.split(/\s+/).length : 0;
  if (words <= 8) return 'short';
  if (words <= 40) return 'medium';
  return 'long';
}

/** Privacy-safe pattern flags and intent bucket hash — never stores raw message text. */
function messagePatternFlags(question) {
  const message = normalizeMessage(question);
  const flags = {
    status_inspection: matchesAny(message, STATUS_INSPECTION_PATTERNS),
    approval_like: matchesAny(message, APPROVAL_PATTERNS),
    rejection_like: matchesAny(message, REJECTION_PATTERNS),
    has_question_mark: message.includes('?'),
    word_count_bucket: wordCountBucket(message),
  };
  const intent_bucket = message
    ? createHash('sha256').update(message.toLowerCase()).digest('hex').slice(0, 16)
    : null;
  return { ...flags, intent_bucket };
}

module.exports = { messagePatternFlags, normalizeMessage };
