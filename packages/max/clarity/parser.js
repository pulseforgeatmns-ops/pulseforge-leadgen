'use strict';

const crypto = require('node:crypto');
const { Parser } = require('htmlparser2');

const PARSER_VERSION = 'pf-clarity-001/v1';
const MAX_BODY_BYTES = 512 * 1024;
const METRICS = Object.freeze({
  sessions: /(?:total\s+)?sessions/i,
  pages_per_session: /pages\s*(?:per|\/)\s*session/i,
  scroll_depth: /(?:average\s+)?scroll\s+depth/i,
  session_duration: /(?:average\s+)?session\s+(?:duration|time)/i,
  dead_clicks: /dead\s+clicks?/i,
  rage_clicks: /rage\s+clicks?/i,
  quick_backs: /quick\s+backs?/i,
  js_errors: /(?:javascript|js)\s+errors?/i,
});
const PERCENT_METRICS = new Set(['scroll_depth', 'dead_clicks', 'rage_clicks', 'quick_backs', 'js_errors']);

class ClarityParseError extends Error {
  constructor(code, provenance = {}) {
    super(code);
    this.name = 'ClarityParseError';
    this.code = code;
    this.provenance = provenance;
  }
}

function header(message, name) {
  return String((message.payload?.headers || []).find((h) => String(h.name).toLowerCase() === name)?.value || '');
}

function extractAddress(value) {
  const text = String(value || '').trim();
  const match = text.match(/^(?:[^<>]*<)?([^\s<>@]+@[^\s<>@]+)>?$/);
  return match ? match[1].toLowerCase() : null;
}

function htmlToText(html) {
  let text = '';
  let hiddenDepth = 0;
  const hidden = new Set(['script', 'style', 'head', 'noscript']);
  const blocks = new Set(['br', 'p', 'div', 'tr', 'td', 'th', 'li', 'h1', 'h2', 'h3', 'table']);
  const parser = new Parser({
    onopentag(name) {
      if (hidden.has(name)) hiddenDepth += 1;
      if (!hiddenDepth && blocks.has(name)) text += '\n';
    },
    ontext(value) { if (!hiddenDepth) text += value; },
    onclosetag(name) {
      if (hidden.has(name)) hiddenDepth = Math.max(0, hiddenDepth - 1);
      if (!hiddenDepth && blocks.has(name)) text += '\n';
    },
  }, { decodeEntities: true });
  parser.write(html);
  parser.end();
  return text;
}

function projectIdsFromContent(content, isHtml) {
  const urls = [];
  if (isHtml) {
    const parser = new Parser({ onopentag(_name, attributes) { if (attributes.href) urls.push(attributes.href); } }, { decodeEntities: true });
    parser.write(content); parser.end();
  }
  urls.push(...(content.match(/https?:\/\/[^\s<>"']+/gi) || []));
  const ids = new Set();
  for (const value of urls) {
    try {
      const url = new URL(value.replace(/[),.\]]+$/, ''));
      if (url.protocol !== 'https:' || url.hostname !== 'clarity.microsoft.com' || url.username || url.password || url.port) continue;
      const match = url.pathname.match(/^\/projects\/view\/([A-Za-z0-9]{3,64})(?:\/|$)/);
      if (match) ids.add(match[1]);
    } catch (_) { /* Non-URL text is not project evidence. */ }
  }
  return ids;
}

function assertGmailAuthentication(message) {
  // Gmail's first Authentication-Results header is the receiving provider's
  // result. Never search a later sender-supplied or ARC header for a pass.
  const first = header(message, 'authentication-results').replace(/\r?\n\s*/g, ' ');
  const clauses = first.split(';').map((value) => value.trim());
  if (!/^mx\.google\.com(?:\s+\d+)?$/i.test(clauses[0] || '')) throw new ClarityParseError('digest_authentication_missing');
  const dmarc = clauses.some((clause) => /^dmarc=pass\b/i.test(clause) && /\bheader\.from=microsoft\.com(?:\s|$)/i.test(clause));
  const dkim = clauses.some((clause) => {
    if (!/^dkim=pass\b/i.test(clause)) return false;
    const match = clause.match(/\bheader\.(?:d|i)=([^\s;]+)/i);
    const domain = match?.[1].split('@').pop().toLowerCase();
    return domain === 'microsoft.com' || domain?.endsWith('.microsoft.com');
  });
  if (!dmarc || !dkim) throw new ClarityParseError('digest_authentication_failed');
}

function extractBody(message) {
  const plain = [];
  const html = [];
  const projectIds = new Set();
  let size = 0;
  let count = 0;
  function visit(part, depth) {
    if (!part || depth > 20 || ++count > 100) throw new ClarityParseError('mime_limit_exceeded');
    // Attachments and nested forwarded mail are not digest evidence.
    if (part.filename || part.mimeType === 'message/rfc822') return;
    const type = String(part.mimeType || '').toLowerCase();
    if (['text/plain', 'text/html'].includes(type) && part.body?.data) {
      const encoded = part.body.data;
      if (typeof encoded !== 'string' || !/^[A-Za-z0-9_\-+/]*={0,2}$/.test(encoded)) {
        throw new ClarityParseError('invalid_body_encoding');
      }
      if (encoded.length > MAX_BODY_BYTES * 1.4) throw new ClarityParseError('body_limit_exceeded');
      const value = Buffer.from(encoded, 'base64url').toString('utf8');
      size += Buffer.byteLength(value);
      if (size > MAX_BODY_BYTES) throw new ClarityParseError('body_limit_exceeded');
      (type === 'text/plain' ? plain : html).push(value);
      for (const id of projectIdsFromContent(value, type === 'text/html')) projectIds.add(id);
    }
    for (const child of part.parts || []) visit(child, depth + 1);
  }
  visit(message.payload, 0);
  if (!plain.length && !html.length) throw new ClarityParseError('digest_body_missing');
  // Use one MIME alternative, never concatenate duplicate metrics from both.
  return plain.length ? { text: plain.join('\n'), type: 'gmail_plain', projectIds: [...projectIds] }
    : { text: htmlToText(html.join('\n')), type: 'gmail_html', projectIds: [...projectIds] };
}

function normalizeText(value) {
  return value.replace(/\r/g, '').replace(/[\u00a0\u202f]/g, ' ')
    .replace(/[−–—]/g, '-').replace(/[ \t]+/g, ' ').replace(/\n\s*\n/g, '\n').trim();
}

const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
function dateString(year, month, day) {
  const date = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
  if (date.getUTCFullYear() !== Number(year) || date.getUTCMonth() !== Number(month) - 1 || date.getUTCDate() !== Number(day)) return null;
  return date.toISOString().slice(0, 10);
}

function parseWindow(text, receivedAt) {
  let start;
  let end;
  let match = text.match(/\b(20\d{2}-\d{2}-\d{2})\s*(?:-|to|through)\s*(20\d{2}-\d{2}-\d{2})\b/i);
  if (match) {
    start = dateString(...match[1].split('-'));
    end = dateString(...match[2].split('-'));
  } else {
    match = text.match(/\b([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:,?\s+(20\d{2}))?\s*(?:-|to|through)\s*(?:([A-Za-z]{3,9})\.?\s+)?(\d{1,2})(?:,?\s+(20\d{2}))?\b/i);
    if (!match) throw new ClarityParseError('reporting_window_missing');
    const firstMonth = MONTHS.indexOf(match[1].slice(0, 3).toLowerCase()) + 1;
    const lastMonth = match[4] ? MONTHS.indexOf(match[4].slice(0, 3).toLowerCase()) + 1 : firstMonth;
    if (!firstMonth || !lastMonth) throw new ClarityParseError('reporting_window_invalid');
    let lastYear = Number(match[6] || match[3] || (receivedAt && new Date(receivedAt).getUTCFullYear()));
    if (!lastYear) throw new ClarityParseError('reporting_year_missing');
    if (!match[6] && !match[3] && receivedAt && lastMonth > new Date(receivedAt).getUTCMonth() + 1) lastYear -= 1;
    const firstYear = Number(match[3] || lastYear - (firstMonth > lastMonth ? 1 : 0));
    start = dateString(firstYear, firstMonth, match[2]);
    end = dateString(lastYear, lastMonth, match[5]);
  }
  if (!start || !end || Date.parse(end) - Date.parse(start) !== 6 * 86400000) {
    throw new ClarityParseError('reporting_window_invalid');
  }
  if (receivedAt && Date.parse(end) > Date.parse(receivedAt) + 86400000) throw new ClarityParseError('reporting_window_future');
  return { window_start: start, window_end: end };
}

function parseDelta(segment) {
  // Only a complete immediate change line/tail is evidence. Never search
  // promotional footers, later cards or arbitrary prose for signed numbers.
  const normalized = String(segment || '').trim().replace(/↑|▲|increased?\s+by|up\s+by/gi, '+').replace(/↓|▼|decreased?\s+by|down\s+by/gi, '-');
  const match = normalized.match(/^([+-])\s*(\d+(?:\.\d+)?)\s*%(?:\s*(?:vs\.?|compared (?:to|with)|from)\s+(?:the\s+)?(?:previous|last)\s+week)?$/i);
  if (match) return Number(`${match[1]}${match[2]}`);
  if (/^(?:unchanged|no change)$/i.test(normalized)) return 0;
  return null; // Unsigned or unbound changes are ambiguous.
}

function parseValue(segment, key) {
  const lines = segment.replace(/^[\s:|=]+/, '').trim().split('\n');
  const value = lines[0].trim();
  const finish = (result, matchLength) => {
    const tail = value.slice(matchLength).trim();
    if (tail && parseDelta(tail) === null) throw new ClarityParseError(`metric_invalid_${key}`);
    return { value: result, deltaCandidate: tail || lines[1] || '' };
  };
  const missing = value.match(/^(?:n\/?a|not available|no data|--|—)(?:\s|$)/i);
  if (missing || !value) return finish(null, missing?.[0].length || 0);
  if (key === 'session_duration') {
    let m = value.match(/^(\d+):(\d{2}(?:\.\d+)?)(?:\s|$)/);
    if (m) {
      if (Number(m[2]) >= 60) throw new ClarityParseError(`metric_invalid_${key}`);
      return finish(Number(m[1]) * 60 + Number(m[2]), m[0].length);
    }
    m = value.match(/^(?:(\d+(?:\.\d+)?)\s*(?:m|min(?:ute)?s?)\s*)?(?:(\d+(?:\.\d+)?)\s*(?:s|sec(?:ond)?s?))\b/i);
    if (m) return finish(Number(m[1] || 0) * 60 + Number(m[2]), m[0].length);
    m = value.match(/^(\d+(?:\.\d+)?)\s*(?:m|min(?:ute)?s?)(?:\s|$)/i);
    if (m) return finish(Number(m[1]) * 60, m[0].length);
  }
  const m = value.match(/^(\d+(?:,\d{3})*(?:\.\d+)?)\s*(%|seconds?|secs?|s)?(?=\s|$|[↑↓▲▼+-])/i);
  if (!m) throw new ClarityParseError(`metric_invalid_${key}`);
  const result = Number(m[1].replace(/,/g, ''));
  if (!Number.isFinite(result) || result < 0 || (key === 'sessions' && !Number.isSafeInteger(result)) || (PERCENT_METRICS.has(key) && (m[2] !== '%' || result > 100)) || (!PERCENT_METRICS.has(key) && m[2] === '%') || (['sessions', 'pages_per_session'].includes(key) && m[2])) {
    throw new ClarityParseError(`metric_invalid_${key}`);
  }
  return finish(result, m[0].length);
}

function parseMetrics(text) {
  const labels = [];
  for (const [key, pattern] of Object.entries(METRICS)) {
    const regex = new RegExp(`\\b${pattern.source}\\b`, 'gi');
    for (const match of text.matchAll(regex)) labels.push({ key, start: match.index, end: match.index + match[0].length });
  }
  labels.sort((a, b) => a.start - b.start);
  const metrics = Object.fromEntries(Object.keys(METRICS).map((key) => [key, null]));
  const deltas_pct = { ...metrics };
  const seen = new Set();
  let valuesBeforeLabels;
  for (let index = 0; index < labels.length; index += 1) {
    const label = labels[index];
    if (seen.has(label.key)) throw new ClarityParseError(`metric_duplicate_${label.key}`);
    seen.add(label.key);
    const after = text.slice(label.end, labels[index + 1]?.start ?? text.length).trim();
    const precedingLine = text.slice(0, label.start).trimEnd().split('\n').pop()?.trim() || '';
    const precedingSameLine = text.slice(text.lastIndexOf('\n', label.start - 1) + 1, label.start).trim();
    // Some Clarity templates display the value above its label. Only accept a
    // whole numeric line, not numbers from earlier prose or reporting dates.
    const before = precedingSameLine || precedingLine;
    const valueBefore = /^(?:\d[\d,.]*(?:%|\s*(?:s|sec(?:onds?)?|m|min(?:utes?)?))?|\d+:\d{2}(?:\.\d+)?|n\/?a|not available|no data|--)$/i.test(before);
    if (valuesBeforeLabels === undefined) valuesBeforeLabels = valueBefore;
    if (valuesBeforeLabels && !valueBefore) throw new ClarityParseError(`metric_invalid_${label.key}`);
    const parsedValue = parseValue(valuesBeforeLabels ? before : after, label.key);
    metrics[label.key] = parsedValue.value;
    deltas_pct[label.key] = parseDelta(valuesBeforeLabels ? after.split('\n')[0] : parsedValue.deltaCandidate);
  }
  if (Object.values(metrics).filter((value) => value !== null).length < 2) throw new ClarityParseError('digest_metrics_missing');
  return { metrics, deltas_pct };
}

/** Returns aggregate evidence only. Never retains or logs email bodies. */
function parseClarityDigest(message, { expectedSender, expectedProjectId, requireAuthentication = false } = {}) {
  const provenance = { parser_version: PARSER_VERSION, gmail_message_id: message?.id || null, gmail_thread_id: message?.threadId || null, received_at: null, content_sha256: null, source_type: null };
  try {
    if (!message || !/^[A-Za-z0-9_-]{1,128}$/.test(message.id || '')) throw new ClarityParseError('message_id_invalid');
    const received = Number(message.internalDate);
    if (Number.isFinite(received) && received > 0 && !Number.isNaN(new Date(received).getTime())) provenance.received_at = new Date(received).toISOString();
    if (expectedSender && extractAddress(header(message, 'from')) !== String(expectedSender).toLowerCase()) throw new ClarityParseError('digest_sender_mismatch');
    if (requireAuthentication) assertGmailAuthentication(message);
    const body = extractBody(message);
    const text = normalizeText(body.text);
    provenance.content_sha256 = crypto.createHash('sha256').update(text).digest('hex');
    provenance.source_type = body.type;
    provenance.project_id = body.projectIds.length === 1 ? body.projectIds[0] : null;
    if (expectedProjectId && (!/^[A-Za-z0-9]{3,64}$/.test(expectedProjectId) || body.projectIds.length !== 1 || body.projectIds[0] !== expectedProjectId)) throw new ClarityParseError('digest_project_mismatch');
    const subject = header(message, 'subject');
    const identity = `${subject}\n${text.slice(0, 1000)}`;
    if (!/\bclarity\b/i.test(identity) || !/\b(?:weekly|digest|week(?:ly)?\s+(?:report|summary)|performance\s+(?:report|summary))\b/i.test(identity)) {
      throw new ClarityParseError('not_clarity_digest');
    }
    return { project_id: provenance.project_id, ...parseWindow(`${subject}\n${text}`, provenance.received_at), ...parseMetrics(text), provenance };
  } catch (error) {
    if (error instanceof ClarityParseError) { error.provenance = provenance; throw error; }
    throw new ClarityParseError('digest_parse_failed', provenance);
  }
}

module.exports = { PARSER_VERSION, METRICS, ClarityParseError, extractAddress, htmlToText, parseClarityDigest };
