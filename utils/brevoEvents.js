const crypto = require('crypto');
const pool = require('../db');

const BREVO_EVENT_MAP = {
  request: 'sent',
  sent: 'sent',
  delivered: 'delivered',
  open: 'opened',
  opens: 'opened',
  opened: 'opened',
  loaded_by_proxy: 'opened',
  loadedByProxy: 'opened',
  loadedbyproxy: 'opened',
  clicks: 'clicked',
  click: 'clicked',
  clicked: 'clicked',
  soft_bounce: 'soft_bounce',
  softBounces: 'soft_bounce',
  softbounces: 'soft_bounce',
  bounce: 'hard_bounce',
  hard_bounce: 'hard_bounce',
  hardBounces: 'hard_bounce',
  hardbounces: 'hard_bounce',
  blocked: 'blocked',
  spam: 'spam',
  unsubscribed: 'unsubscribed',
  replied: 'replied',
  reply: 'replied',
};

const TWO_PART_SUFFIXES = new Set([
  'co.uk',
  'com.au',
  'com.br',
  'com.mx',
  'co.nz',
  'co.in',
  'co.jp',
  'com.sg',
]);

function normalizeEmail(value) {
  return value === undefined || value === null ? '' : String(value).trim().toLowerCase();
}

function brevoMessageId(payload = {}) {
  return payload.messageId ||
    payload.message_id ||
    payload['message-id'] ||
    payload['Message-ID'] ||
    payload['messageId'] ||
    payload.uuid ||
    null;
}

function textValue(value) {
  if (value === undefined || value === null) return null;
  const str = String(value).trim();
  return str || null;
}

function extractEmail(value) {
  if (!value) return '';
  if (typeof value === 'object') {
    return normalizeEmail(value.email || value.address || value.mail);
  }
  const raw = String(value).trim();
  const match = raw.match(/<([^<>@\s]+@[^<>\s]+)>/) || raw.match(/([^<>\s]+@[^<>\s]+)/);
  return normalizeEmail(match ? match[1] : raw);
}

function recipientEmail(payload = {}) {
  return extractEmail(
    payload.email ||
    payload.recipient ||
    payload.rcpt ||
    payload.to?.[0] ||
    payload.to
  );
}

function senderEmail(payload = {}) {
  return extractEmail(
    payload.from ||
    payload.sender ||
    payload.sender_email ||
    payload.from_email ||
    payload.metadata?.from_email
  );
}

function rootDomainFromEmail(email) {
  const domain = normalizeEmail(email).split('@').pop();
  if (!domain) return null;
  return normalizeRootDomain(domain);
}

function normalizeRootDomain(value) {
  if (!value) return null;
  let host = String(value).trim().toLowerCase();
  host = host.replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split(':')[0];
  host = host.replace(/[<>"'()]/g, '');
  const labels = host.split('.').filter(Boolean);
  if (labels.length <= 2) return labels.join('.') || null;
  const suffix = labels.slice(-2).join('.');
  if (TWO_PART_SUFFIXES.has(suffix) && labels.length >= 3) {
    return labels.slice(-3).join('.');
  }
  return labels.slice(-2).join('.');
}

function eventAt(payload = {}) {
  const value = payload.date || payload.ts || payload.timestamp || payload.event_at || payload.created_at;
  if (!value) return new Date();
  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) return parsed;
  const normalized = new Date(String(value).replace(' ', 'T'));
  return Number.isNaN(normalized.getTime()) ? new Date() : normalized;
}

function eventName(payload = {}) {
  return String(payload.event || payload.event_type || payload.type || '').trim();
}

function internalEventType(payload = {}) {
  return BREVO_EVENT_MAP[eventName(payload)] || BREVO_EVENT_MAP[eventName(payload).toLowerCase()] || null;
}

function metadataValue(payload, key) {
  return payload?.metadata?.[key] ?? payload?.params?.[key] ?? payload?.[key] ?? null;
}

function eventId(payload = {}, eventType, email, messageId) {
  const explicit = payload.event_id || payload.eventId;
  if (explicit) return String(explicit);
  const raw = [
    payload.id || payload.uuid || '',
    messageId || '',
    eventName(payload),
    eventType || '',
    email || '',
    payload.date || payload.ts || payload.timestamp || '',
    payload.link || '',
  ].join('|');
  return crypto.createHash('sha256').update(raw).digest('hex');
}

function parseStep(value) {
  if (value === undefined || value === null || value === '') return null;
  const match = String(value).match(/\d+/);
  if (!match) return null;
  const n = Number.parseInt(match[0], 10);
  return Number.isFinite(n) ? n : null;
}

function parseJsonMaybe(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return {};
  }
}

async function findMatchingSend({ email, clientId, messageId, subject }) {
  if (messageId) {
    const byMessage = await pool.query(`
      SELECT payload, client_id, prospect_id
      FROM agent_log
      WHERE agent_name = 'emmett'
        AND action = 'email_sent'
        AND payload->>'message_id' = $1
      ORDER BY ran_at DESC
      LIMIT 1
    `, [messageId]);
    if (byMessage.rows.length) return byMessage.rows[0];
  }

  const params = [email, subject || null];
  if (clientId) params.push(clientId);
  const byEmail = await pool.query(`
    SELECT al.payload, al.client_id, al.prospect_id
    FROM agent_log al
    JOIN prospects p ON p.id = al.prospect_id AND p.client_id = al.client_id
    WHERE al.agent_name = 'emmett'
      AND al.action = 'email_sent'
      AND LOWER(p.email) = $1
      AND ($2::text IS NULL OR al.payload->>'subject' = $2)
      ${clientId ? 'AND al.client_id = $3' : ''}
    ORDER BY al.ran_at DESC
    LIMIT 1
  `, params);
  return byEmail.rows[0] || null;
}

async function resolveProspect(email, clientId, sendMatch) {
  if (sendMatch?.prospect_id) {
    const byId = await pool.query(`
      SELECT id, client_id, email, vertical
      FROM prospects
      WHERE id = $1
        AND client_id = $2
      LIMIT 1
    `, [sendMatch.prospect_id, sendMatch.client_id]);
    if (byId.rows.length) return byId.rows[0];
  }

  const params = [email];
  if (clientId) params.push(clientId);
  const byEmail = await pool.query(`
    SELECT id, client_id, email, vertical
    FROM prospects
    WHERE LOWER(email) = $1
      ${clientId ? 'AND client_id = $2' : ''}
    ORDER BY created_at DESC
    LIMIT 1
  `, params);
  return byEmail.rows[0] || null;
}

async function sendingDomainForPayload(payload, sendPayload, prospect) {
  const fromEmail = senderEmail(payload) || normalizeEmail(sendPayload.from_email);
  const fromDomain = rootDomainFromEmail(fromEmail);
  if (fromDomain) return fromDomain;

  const payloadDomain = normalizeRootDomain(payload.sending_domain || metadataValue(payload, 'sending_domain'));
  if (payloadDomain) return payloadDomain;

  if (prospect?.client_id) {
    const clientRes = await pool.query('SELECT sending_domain FROM clients WHERE id = $1 LIMIT 1', [prospect.client_id]);
    const clientDomain = normalizeRootDomain(clientRes.rows[0]?.sending_domain);
    if (clientDomain) return clientDomain;
  }

  return 'unknown.local';
}

async function logBrevoEvent({ eventId, eventType, email, prospectId, clientId, inserted }) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
    VALUES ('riley', 'brevo_event_received', $1, $2, 'success', NOW(), $3)
  `, [
    prospectId || null,
    JSON.stringify({
      event_id: eventId,
      event_type: eventType,
      recipient_email: email,
      inserted,
    }),
    clientId || null,
  ]);
}

async function insertBrevoEvent(rawPayload = {}) {
  const payload = rawPayload || {};
  const recipient = recipientEmail(payload);
  const type = internalEventType(payload);
  if (!recipient || !type) {
    return { inserted: false, skipped: true, reason: 'missing_email_or_event_type' };
  }

  const messageId = brevoMessageId(payload);
  const payloadClientId = Number(
    metadataValue(payload, 'client_id') ||
    payload.clientId ||
    payload.client_id
  ) || null;
  const subject = textValue(payload.subject || metadataValue(payload, 'subject'));
  const sendMatch = await findMatchingSend({
    email: recipient,
    clientId: payloadClientId,
    messageId,
    subject,
  });
  const sendPayload = parseJsonMaybe(sendMatch?.payload);
  const clientId = Number(sendMatch?.client_id || payloadClientId) || null;
  const prospect = await resolveProspect(recipient, clientId, sendMatch);
  const finalClientId = Number(prospect?.client_id || clientId) || null;
  const domain = await sendingDomainForPayload(payload, sendPayload, prospect);
  const id = eventId(payload, type, recipient, messageId);
  const tags = Array.isArray(payload.tags) ? payload.tags : [];
  const sequence = textValue(metadataValue(payload, 'sequence') || sendPayload.sequence || tags[0]);
  const step = parseStep(metadataValue(payload, 'step') || sendPayload.step || tags.find(tag => /^step_/i.test(String(tag))));

  const insert = await pool.query(`
    INSERT INTO email_events (
      event_id,
      prospect_id,
      client_id,
      sending_domain,
      recipient_email,
      event_type,
      subject_line,
      sequence,
      step,
      brevo_message_id,
      raw_payload,
      event_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING id
  `, [
    id,
    prospect?.id || null,
    finalClientId,
    domain,
    recipient,
    type,
    subject,
    sequence,
    step,
    messageId,
    JSON.stringify(payload),
    eventAt(payload),
  ]);

  const inserted = insert.rowCount > 0;
  await logBrevoEvent({
    eventId: id,
    eventType: type,
    email: recipient,
    prospectId: prospect?.id || null,
    clientId: finalClientId,
    inserted,
  });

  return {
    inserted,
    duplicate: !inserted,
    event_id: id,
    event_type: type,
    recipient_email: recipient,
    prospect_id: prospect?.id || null,
    client_id: finalClientId,
    sending_domain: domain,
  };
}

module.exports = {
  BREVO_EVENT_MAP,
  brevoMessageId,
  insertBrevoEvent,
  internalEventType,
  normalizeRootDomain,
  recipientEmail,
  rootDomainFromEmail,
  senderEmail,
};
