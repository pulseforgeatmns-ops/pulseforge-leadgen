const crypto = require('crypto');
const pool = require('../db');

const BREVO_EVENT_MAP = {
  request: 'sent',
  requests: 'sent',
  sent: 'sent',
  delivered: 'delivered',
  open: 'opened',
  opens: 'opened',
  opened: 'opened',
  loaded_by_proxy: 'opened_proxy',
  loadedByProxy: 'opened_proxy',
  loadedbyproxy: 'opened_proxy',
  proxyOpen: 'opened_proxy',
  proxyopen: 'opened_proxy',
  proxy_open: 'opened_proxy',
  unique_opened: 'opened',
  uniqueOpened: 'opened',
  uniqueopened: 'opened',
  unique_loaded_by_proxy: 'opened_proxy',
  uniqueLoadedByProxy: 'opened_proxy',
  uniqueloadedbyproxy: 'opened_proxy',
  uniqueProxyOpen: 'opened_proxy',
  uniqueproxyopen: 'opened_proxy',
  unique_proxy_open: 'opened_proxy',
  clicks: 'clicked',
  click: 'clicked',
  clicked: 'clicked',
  soft_bounce: 'soft_bounce',
  softBounce: 'soft_bounce',
  softbounce: 'soft_bounce',
  softBounces: 'soft_bounce',
  softbounces: 'soft_bounce',
  bounce: 'hard_bounce',
  hard_bounce: 'hard_bounce',
  hardBounce: 'hard_bounce',
  hardbounce: 'hard_bounce',
  hardBounces: 'hard_bounce',
  hardbounces: 'hard_bounce',
  blocked: 'blocked',
  deferred: 'deferred',
  invalid: 'invalid',
  invalid_email: 'invalid',
  invalidEmail: 'invalid',
  invalidemail: 'invalid',
  error: 'error',
  spam: 'spam',
  complaint: 'spam',
  complaints: 'spam',
  spamReport: 'spam',
  spamreport: 'spam',
  spamReports: 'spam',
  spamreports: 'spam',
  unsubscribe: 'unsubscribed',
  unsubscribed: 'unsubscribed',
  replied: 'replied',
  reply: 'replied',
};

// These lifecycle states can occur at most once for a Brevo message. The live
// webhook and the history API use different raw names (for example `request`
// versus `requests`) and can timestamp the same event one second apart, so the
// message id + canonical type is the stable cross-source identity.
const SINGLETON_MESSAGE_EVENT_TYPES = new Set([
  'sent',
  'delivered',
  'hard_bounce',
  'blocked',
  'invalid',
  'spam',
  'unsubscribed',
]);

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
  const timestamp = Number(payload.ts);
  if (payload.ts != null && payload.ts !== '' && Number.isFinite(timestamp)) {
    return new Date(timestamp * 1000);
  }

  if (payload.date) {
    const match = String(payload.date).match(
      /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?$/
    );
    if (match) {
      const [, year, month, day, hour, minute, second, milliseconds = '0'] = match;
      const localAsUtc = Date.UTC(
        Number(year),
        Number(month) - 1,
        Number(day),
        Number(hour),
        Number(minute),
        Number(second),
        Number(milliseconds.padEnd(3, '0'))
      );
      const formatter = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hourCycle: 'h23',
      });
      const offsetAt = utcMillis => {
        const probe = Math.floor(utcMillis / 1000) * 1000;
        const parts = formatter.formatToParts(new Date(probe));
        const part = type => Number(parts.find(item => item.type === type).value);
        return Date.UTC(
          part('year'),
          part('month') - 1,
          part('day'),
          part('hour'),
          part('minute'),
          part('second')
        ) - probe;
      };
      let utc = localAsUtc - offsetAt(localAsUtc);
      utc = localAsUtc - offsetAt(utc);
      return new Date(utc);
    }
  }

  const value = payload.date || payload.timestamp || payload.event_at || payload.created_at;
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
  const name = eventName(payload);
  return BREVO_EVENT_MAP[name] || BREVO_EVENT_MAP[name.toLowerCase()] || null;
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
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12
    WHERE NOT (
      (
        $13::boolean
        AND $10::text IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM email_events existing
          WHERE existing.brevo_message_id = $10
            AND existing.event_type = $6
        )
      )
      OR EXISTS (
        SELECT 1
        FROM email_events existing
        WHERE existing.event_type = $6
          AND existing.raw_payload = $11::jsonb
      )
    )
    ON CONFLICT (event_id) DO UPDATE
      SET event_type = EXCLUDED.event_type
      WHERE email_events.event_type IS DISTINCT FROM EXCLUDED.event_type
    RETURNING id, (xmax = 0) AS inserted
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
    SINGLETON_MESSAGE_EVENT_TYPES.has(type),
  ]);

  const inserted = insert.rows[0]?.inserted === true;
  const updated = insert.rowCount > 0 && !inserted;
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
    updated,
    duplicate: insert.rowCount === 0,
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
  eventAt,
  eventId,
  insertBrevoEvent,
  internalEventType,
  normalizeRootDomain,
  recipientEmail,
  rootDomainFromEmail,
  senderEmail,
};
