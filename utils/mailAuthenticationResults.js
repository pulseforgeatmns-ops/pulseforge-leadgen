'use strict';

/**
 * Parse Authentication-Results / ARC-Authentication-Results from delivered mail.
 * Used to record SPF/DKIM/DMARC PASS from real messages — not DNS presence alone.
 */

const { getHeaderValue, parseRawMailHeaders } = require('./mailHeaders');

const AUTH_HEADER_NAMES = ['authentication-results', 'arc-authentication-results'];

function normalizeAuthToken(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeHeadersInput(headers) {
  if (typeof headers === 'string') {
    const map = parseRawMailHeaders(headers);
    return Object.fromEntries(map.entries());
  }
  return headers;
}

function extractAuthResultsBlocks(headers) {
  const normalized = normalizeHeadersInput(headers);
  const blocks = [];
  for (const name of AUTH_HEADER_NAMES) {
    const raw = getHeaderValue(normalized, name);
    if (!raw) continue;
    blocks.push(...String(raw).split(/\s*;\s*(?=smtp\.|spf=|dkim=|dmarc=)/i));
  }
  return blocks.length ? blocks : [String(getHeaderValue(headers, 'authentication-results') || '')];
}

function parseMechanismResult(block, mechanism) {
  const re = new RegExp(`\\b${mechanism}\\s*=\\s*(pass|fail|neutral|none|softfail|permerror|temperror|bypass|unknown)`, 'i');
  const match = String(block || '').match(re);
  return match ? normalizeAuthToken(match[1]) : null;
}

function parseFromHeader(headers) {
  const from = getHeaderValue(headers, 'from') || '';
  const match = from.match(/<([^>]+)>/) || from.match(/([^\s<>]+@[^\s<>]+)/);
  return match ? String(match[1]).trim().toLowerCase() : from.trim().toLowerCase();
}

function parseReplyToHeader(headers) {
  const replyTo = getHeaderValue(headers, 'reply-to');
  if (!replyTo) return null;
  const match = replyTo.match(/<([^>]+)>/) || replyTo.match(/([^\s<>]+@[^\s<>]+)/);
  return match ? String(match[1]).trim().toLowerCase() : replyTo.trim().toLowerCase();
}

/**
 * @returns {{ spf: string|null, dkim: string|null, dmarc: string|null, from: string|null, replyTo: string|null }}
 */
function parseDeliveredAuthenticationEvidence(headers) {
  const normalized = normalizeHeadersInput(headers);
  const blocks = extractAuthResultsBlocks(normalized);
  const combined = blocks.join(' ');
  return {
    spf: parseMechanismResult(combined, 'spf'),
    dkim: parseMechanismResult(combined, 'dkim'),
    dmarc: parseMechanismResult(combined, 'dmarc'),
    from: parseFromHeader(normalized),
    replyTo: parseReplyToHeader(normalized),
  };
}

function verificationFieldFromDeliveredResult(result, mechanism) {
  const value = normalizeAuthToken(result[mechanism]);
  if (value === 'pass') {
    return {
      status: 'pass',
      provenance: { source: 'delivered_message', mechanism, result: value },
    };
  }
  if (value === 'fail' || value === 'softfail' || value === 'permerror') {
    return {
      status: 'failed',
      provenance: { source: 'delivered_message', mechanism, result: value },
    };
  }
  return {
    status: 'not_checked',
    provenance: { source: 'delivered_message', mechanism, result: value || 'missing' },
  };
}

function verificationStateFromDeliveredHeaders(headers) {
  const parsed = parseDeliveredAuthenticationEvidence(headers);
  return {
    spf: verificationFieldFromDeliveredResult(parsed, 'spf'),
    dkim: verificationFieldFromDeliveredResult(parsed, 'dkim'),
    dmarc: verificationFieldFromDeliveredResult(parsed, 'dmarc'),
    from: parsed.from,
    replyTo: parsed.replyTo,
    capturedAt: new Date().toISOString(),
  };
}

function deliveredAuthenticationPasses(state = {}) {
  for (const key of ['spf', 'dkim', 'dmarc']) {
    const status = normalizeAuthToken(state[key]?.status);
    const source = state[key]?.provenance?.source;
    if (status !== 'pass' || source !== 'delivered_message') return false;
  }
  return true;
}

module.exports = {
  parseDeliveredAuthenticationEvidence,
  verificationStateFromDeliveredHeaders,
  deliveredAuthenticationPasses,
};
