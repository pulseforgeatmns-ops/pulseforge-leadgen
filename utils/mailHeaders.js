'use strict';

/**
 * Canonical mail header access for IMAP fetch normalization (SPEC-253).
 * Supports imapflow raw header Buffers, Map-like objects, plain objects,
 * and Gmail-style { name, value } arrays used in tests.
 */

function clean(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function joinHeaderValue(value) {
  if (value == null || value === '') return null;
  if (Array.isArray(value)) {
    const joined = value.map((row) => String(row)).filter(Boolean).join(' ');
    return joined || null;
  }
  return String(value);
}

function appendHeaderValue(map, key, value) {
  const lower = clean(key).toLowerCase();
  if (!lower || value == null || value === '') return;
  const existing = map.get(lower);
  if (existing == null) {
    map.set(lower, value);
    return;
  }
  if (Array.isArray(existing)) {
    existing.push(value);
    return;
  }
  map.set(lower, [existing, value]);
}

function parseRawMailHeaders(raw) {
  const map = new Map();
  if (raw == null) return map;

  const text = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
  const lines = text.split(/\r?\n/);
  let currentKey = null;
  let currentValue = '';

  for (const line of lines) {
    if (line === '') break;
    if (/^[ \t]/.test(line) && currentKey) {
      currentValue += ` ${line.trim()}`;
      continue;
    }
    if (currentKey) {
      appendHeaderValue(map, currentKey, currentValue.trim());
    }
    const idx = line.indexOf(':');
    if (idx === -1) {
      currentKey = null;
      currentValue = '';
      continue;
    }
    currentKey = line.slice(0, idx).trim();
    currentValue = line.slice(idx + 1).trim();
  }
  if (currentKey) appendHeaderValue(map, currentKey, currentValue.trim());
  return map;
}

function getHeaderValueFromMap(map, name) {
  if (!map || typeof map.get !== 'function') return null;
  const target = clean(name).toLowerCase();
  if (!target) return null;
  return joinHeaderValue(map.get(target) ?? map.get(name));
}

function getHeaderValue(headers, name) {
  try {
    const target = clean(name).toLowerCase();
    if (!target || headers == null) return null;

    if (Buffer.isBuffer(headers)) {
      return getHeaderValueFromMap(parseRawMailHeaders(headers), target);
    }

    if (typeof headers.getFirst === 'function') {
      return joinHeaderValue(headers.getFirst(name) ?? headers.getFirst(target));
    }

    if (typeof headers.get === 'function' && !Array.isArray(headers)) {
      return joinHeaderValue(headers.get(target) ?? headers.get(name));
    }

    if (Array.isArray(headers)) {
      const matches = headers
        .filter((row) => clean(row?.name).toLowerCase() === target)
        .map((row) => row?.value)
        .filter((value) => value != null && value !== '');
      return joinHeaderValue(matches.length <= 1 ? matches[0] : matches);
    }

    if (typeof headers === 'object') {
      for (const [key, value] of Object.entries(headers)) {
        if (clean(key).toLowerCase() === target) return joinHeaderValue(value);
      }
    }

    return null;
  } catch (_err) {
    return null;
  }
}

function normalizeImapFlowFetchMessage(msg, opts = {}) {
  if (!msg || msg.uid == null) return null;
  const headers = msg.headers;
  const envelope = msg.envelope || {};
  const nowIso = typeof opts.now === 'function'
    ? new Date(opts.now()).toISOString()
    : (opts.now ? new Date(opts.now).toISOString() : new Date().toISOString());

  return {
    uid: msg.uid,
    providerMessageId: String(msg.uid),
    rfcMessageId: getHeaderValue(headers, 'message-id'),
    inReplyTo: getHeaderValue(headers, 'in-reply-to'),
    referencesHeader: getHeaderValue(headers, 'references'),
    subject: envelope.subject || getHeaderValue(headers, 'subject') || '',
    from: envelope.from?.[0]?.address || getHeaderValue(headers, 'from') || '',
    to: Array.isArray(envelope.to) ? envelope.to.map((row) => row.address).filter(Boolean) : [],
    receivedAt: envelope.date ? new Date(envelope.date).toISOString() : nowIso,
    body: msg.source ? msg.source.toString('utf8') : '',
  };
}

module.exports = {
  getHeaderValue,
  parseRawMailHeaders,
  normalizeImapFlowFetchMessage,
};
