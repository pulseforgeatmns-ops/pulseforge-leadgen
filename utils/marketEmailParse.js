'use strict';

const { normalizeEmailAddress } = require('./marketCompanyResolve');

const URL_RE = /https?:\/\/[^\s<>"')\]]+/gi;
const ANGLE_URL_RE = /<\s*(https?:\/\/[^>\s]+)\s*>/gi;

function decodeGmailBody(data) {
  if (!data) return '';
  return Buffer.from(String(data).replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
}

function walkParts(payload, visit) {
  if (!payload) return;
  visit(payload);
  for (const part of payload.parts || []) walkParts(part, visit);
}

function extractBodies(payload) {
  let bodyText = '';
  let bodyHtml = '';

  walkParts(payload, (part) => {
    if (!part?.body?.data) return;
    const decoded = decodeGmailBody(part.body.data);
    if (part.mimeType === 'text/plain' && !bodyText) bodyText = decoded;
    if (part.mimeType === 'text/html' && !bodyHtml) bodyHtml = decoded;
  });

  if (!bodyText && !bodyHtml && payload?.body?.data) {
    const decoded = decodeGmailBody(payload.body.data);
    if (payload.mimeType === 'text/html') bodyHtml = decoded;
    else bodyText = decoded;
  }

  if (!bodyText && bodyHtml) {
    bodyText = bodyHtml
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>')
      .replace(/\s+/g, ' ')
      .trim();
  }

  return { bodyText, bodyHtml: bodyHtml || undefined };
}

function extractLinks(...sources) {
  const links = new Set();
  for (const source of sources) {
    const text = String(source || '');
    for (const match of text.matchAll(ANGLE_URL_RE)) {
      links.add(match[1].replace(/[.,;:]+$/g, ''));
    }
    for (const match of text.matchAll(URL_RE)) {
      links.add(match[0].replace(/[.,;:]+$/g, ''));
    }
  }
  return [...links];
}

function extractAttachments(payload) {
  const attachments = [];
  walkParts(payload, (part) => {
    const filename = part?.filename;
    if (filename) attachments.push(filename);
  });
  return attachments;
}

function headerMap(headers) {
  const map = {};
  for (const header of headers || []) {
    const name = String(header.name || '').trim();
    if (!name) continue;
    map[name] = header.value || '';
  }
  return map;
}

function headerValue(headers, name) {
  const target = String(name || '').toLowerCase();
  for (const [key, value] of Object.entries(headers || {})) {
    if (key.toLowerCase() === target) return value;
  }
  return '';
}

function parseFrom(raw) {
  const text = String(raw || '').trim();
  const email = normalizeEmailAddress(text);
  let fromName;
  const nameMatch = text.match(/^"?([^"<]+)"?\s*</);
  if (nameMatch) {
    fromName = nameMatch[1].trim() || undefined;
  } else if (text && !text.includes('@')) {
    fromName = text;
  }
  return { fromName, fromEmail: email || '' };
}

function parseDate(value) {
  if (!value) return undefined;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? undefined : date;
}

/**
 * Convert a Gmail users.messages.get (format=full) payload into MarketEmail.
 * Basic extraction only — no summarization or scoring.
 */
function parseGmailMessage(fullMessage, { importedAt = new Date() } = {}) {
  const payload = fullMessage?.payload || {};
  const headers = headerMap(payload.headers);
  const { fromName, fromEmail } = parseFrom(headerValue(headers, 'From'));
  const { bodyText, bodyHtml } = extractBodies(payload);
  const links = extractLinks(bodyText, bodyHtml, headerValue(headers, 'List-Unsubscribe'));
  const attachments = extractAttachments(payload);
  const messageId = String(headerValue(headers, 'Message-ID') || headerValue(headers, 'Message-Id') || '').trim() || null;
  const sentAt = parseDate(headerValue(headers, 'Date'));
  const internalMs = Number(fullMessage?.internalDate);
  const receivedAt = Number.isFinite(internalMs) && internalMs > 0
    ? new Date(internalMs)
    : (sentAt || importedAt);

  return {
    gmailId: String(fullMessage.id || ''),
    threadId: String(fullMessage.threadId || '') || null,
    messageId,
    receivedAt,
    sentAt,
    fromName,
    fromEmail,
    subject: headerValue(headers, 'Subject') || '(no subject)',
    bodyText: bodyText || '',
    bodyHtml,
    headers,
    links,
    attachments,
    importedAt,
  };
}

function buildLabelQuery({ label = 'MARKET_INTEL', days = 365 } = {}) {
  const safeLabel = String(label || 'MARKET_INTEL').trim().replace(/"/g, '');
  const safeDays = Math.max(1, Math.min(Number(days) || 365, 3650));
  return `label:${safeLabel} newer_than:${safeDays}d`;
}

module.exports = {
  buildLabelQuery,
  decodeGmailBody,
  extractAttachments,
  extractBodies,
  extractLinks,
  headerMap,
  headerValue,
  parseFrom,
  parseGmailMessage,
};
