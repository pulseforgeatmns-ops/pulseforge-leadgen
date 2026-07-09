const OPEN_SOURCE = {
  HUMAN: 'human',
  PROXY: 'proxy',
  UNKNOWN: 'unknown',
};

const DELIVERY_COINCIDENT_THRESHOLD_SECONDS = 15;
const BATCH_OPEN_WINDOW_SECONDS = 10;
const BATCH_OPEN_DISTINCT_PROSPECT_THRESHOLD = 5;

const PROXY_EVENT_TYPES = new Set(['opened_proxy']);
const OPEN_EVENT_TYPES = new Set(['opened', 'open', 'opened_proxy']);
const CLICK_EVENT_TYPES = new Set(['clicked', 'click']);
const SEND_EVENT_TYPES = ['sent', 'delivered'];

const KNOWN_PROXY_UA_RE = /googleimageproxy|mailprivacy|mail privacy|apple.*mail.*proxy|duckduckgo.*email|yahoo.*proxy|proxy/i;
const PROXY_RAW_EVENT_RE = /^(loaded_by_proxy|loadedbyproxy|proxyopen|proxy_open|unique_loaded_by_proxy|uniqueloadedbyproxy|uniqueproxyopen|unique_proxy_open)$/i;

let schemaReady = false;
let schemaReadyPromise = null;

function isOpenEventType(eventType) {
  return OPEN_EVENT_TYPES.has(String(eventType || '').toLowerCase());
}

function isClickEventType(eventType) {
  return CLICK_EVENT_TYPES.has(String(eventType || '').toLowerCase());
}

function isOpenOrClickEventType(eventType) {
  return isOpenEventType(eventType) || isClickEventType(eventType);
}

function extractText(payload, keys) {
  for (const key of keys) {
    const value = payload?.[key] ?? payload?.metadata?.[key] ?? payload?.params?.[key];
    if (value === undefined || value === null) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return null;
}

function extractOpenMetadata(payload = {}) {
  return {
    userAgent: extractText(payload, ['user_agent', 'userAgent', 'user-agent', 'User-Agent']),
    ipAddress: extractText(payload, ['ip', 'ip_address', 'remote_ip', 'client_ip']),
  };
}

function ipv4Block(value) {
  const match = String(value || '').trim().match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.\d{1,3}$/);
  if (!match) return null;
  return `${match[1]}.${match[2]}.${match[3]}.0/24`;
}

function sourceKey({ ipAddress, userAgent }) {
  const block = ipv4Block(ipAddress);
  if (block) return `ip:${block}`;
  const ua = String(userAgent || '').replace(/\s+/g, ' ').trim().toLowerCase();
  return ua ? `ua:${ua.slice(0, 180)}` : null;
}

function rawPayloadEventName(payload = {}) {
  return String(payload.event || payload.event_type || payload.type || '').trim();
}

function normalizeDate(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

async function ensureOpenSignalSchema(pool) {
  if (schemaReady) return;
  if (!schemaReadyPromise) {
    schemaReadyPromise = (async () => {
      await pool.query(`
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'open_source') THEN
            CREATE TYPE open_source AS ENUM ('human', 'proxy', 'unknown');
          END IF;
        END $$;
      `);
      await pool.query(`
        ALTER TABLE email_events
          ADD COLUMN IF NOT EXISTS open_source open_source NOT NULL DEFAULT 'unknown',
          ADD COLUMN IF NOT EXISTS open_source_reason TEXT,
          ADD COLUMN IF NOT EXISTS open_source_classified_at TIMESTAMPTZ,
          ADD COLUMN IF NOT EXISTS user_agent TEXT,
          ADD COLUMN IF NOT EXISTS ip_address TEXT
      `);
      await pool.query(`
        CREATE INDEX IF NOT EXISTS email_events_open_source_idx
          ON email_events (client_id, prospect_id, event_at, open_source)
          WHERE event_type IN ('opened', 'open', 'opened_proxy')
      `);
      await pool.query(`
        CREATE INDEX IF NOT EXISTS email_events_message_event_idx
          ON email_events (client_id, prospect_id, brevo_message_id, event_type, event_at)
          WHERE brevo_message_id IS NOT NULL
      `);
      schemaReady = true;
    })().catch(err => {
      schemaReadyPromise = null;
      throw err;
    });
  }
  await schemaReadyPromise;
}

async function findDeliveryTimestamp(pool, { prospectId, clientId, recipientEmail, messageId, subject, sendMatch }) {
  const sentAt = normalizeDate(sendMatch?.ran_at || sendMatch?.event_at);
  if (sentAt) return sentAt;
  if (!clientId || !prospectId) return null;

  const params = [
    clientId,
    prospectId,
    messageId || null,
    recipientEmail || null,
    subject || null,
  ];
  const { rows } = await pool.query(`
    SELECT MIN(event_at) AS delivered_at
    FROM email_events
    WHERE client_id = $1
      AND prospect_id = $2
      AND event_type = ANY($6::text[])
      AND (
        ($3::text IS NOT NULL AND brevo_message_id = $3)
        OR (
          $3::text IS NULL
          AND LOWER(recipient_email) = LOWER($4)
          AND ($5::text IS NULL OR subject_line = $5)
        )
      )
  `, [...params, SEND_EVENT_TYPES]);
  return normalizeDate(rows[0]?.delivered_at);
}

async function hasCorrespondingSend(pool, { prospectId, clientId, recipientEmail, messageId, subject, eventAt }) {
  if (!clientId || !prospectId) return false;
  const occurredAt = normalizeDate(eventAt) || new Date();
  const { rows } = await pool.query(`
    SELECT (
      EXISTS (
      SELECT 1
      FROM email_events ee
      WHERE ee.client_id = $1
        AND ee.prospect_id = $2
        AND ee.event_type = ANY($6::text[])
        AND (
          ($3::text IS NOT NULL AND ee.brevo_message_id = $3)
          OR (
            $3::text IS NULL
            AND LOWER(ee.recipient_email) = LOWER($4)
            AND ($5::text IS NULL OR ee.subject_line = $5)
            AND ee.event_at <= $7::timestamptz
          )
        )
      )
      OR EXISTS (
      SELECT 1
      FROM agent_log al
      JOIN prospects p ON p.id = al.prospect_id AND p.client_id = al.client_id
      WHERE al.client_id = $1
        AND al.prospect_id = $2
        AND al.agent_name = 'emmett'
        AND al.action = 'email_sent'
        AND (
          ($3::text IS NOT NULL AND al.payload->>'message_id' = $3)
          OR (
            $3::text IS NULL
            AND LOWER(p.email) = LOWER($4)
            AND ($5::text IS NULL OR al.payload->>'subject' = $5)
            AND al.ran_at <= $7::timestamptz
          )
        )
      )
    ) AS has_send
  `, [clientId, prospectId, messageId || null, recipientEmail || null, subject || null, SEND_EVENT_TYPES, occurredAt]);
  return rows[0]?.has_send === true;
}

async function markBatchProxyEvents(pool, { clientId, eventAt, source, insertedEventId }) {
  if (!clientId || !source || !eventAt) return 0;
  const { rows } = await pool.query(`
    WITH source_events AS (
      SELECT id, prospect_id
      FROM email_events
      WHERE client_id = $1
        AND event_type IN ('opened', 'open')
        AND event_at >= $2::timestamptz - ($3::int * INTERVAL '1 second')
        AND event_at <= $2::timestamptz
        AND (
          CASE
            WHEN ip_address ~ '^([0-9]{1,3}\\.){3}[0-9]{1,3}$'
              THEN 'ip:' || split_part(ip_address, '.', 1) || '.' || split_part(ip_address, '.', 2) || '.' || split_part(ip_address, '.', 3) || '.0/24'
            WHEN COALESCE(user_agent, '') <> ''
              THEN 'ua:' || LEFT(LOWER(regexp_replace(user_agent, '\\s+', ' ', 'g')), 180)
            ELSE NULL
          END
        ) = $4
    ), grouped AS (
      SELECT COUNT(DISTINCT prospect_id)::int AS distinct_prospects
      FROM source_events
    ), updated AS (
      UPDATE email_events ee
      SET open_source = 'proxy',
          open_source_reason = 'batch_fire',
          open_source_classified_at = NOW()
      FROM source_events source, grouped
      WHERE ee.id = source.id
        AND grouped.distinct_prospects >= $5
        AND ee.open_source IS DISTINCT FROM 'proxy'::open_source
      RETURNING ee.id
    )
    SELECT COUNT(*)::int AS updated_count FROM updated
  `, [
    clientId,
    eventAt,
    BATCH_OPEN_WINDOW_SECONDS,
    source,
    BATCH_OPEN_DISTINCT_PROSPECT_THRESHOLD,
  ]);
  return Number(rows[0]?.updated_count || 0);
}

async function classifyOpenSource(pool, {
  eventType,
  eventAt,
  payload,
  prospectId,
  clientId,
  recipientEmail,
  messageId,
  subject,
  sendMatch,
}) {
  const { userAgent, ipAddress } = extractOpenMetadata(payload);
  const eventDate = normalizeDate(eventAt) || new Date();
  const rawEvent = rawPayloadEventName(payload);

  if (!isOpenEventType(eventType)) {
    return { openSource: OPEN_SOURCE.UNKNOWN, reason: null, userAgent, ipAddress, hasSend: false };
  }

  if (PROXY_EVENT_TYPES.has(eventType) || PROXY_RAW_EVENT_RE.test(rawEvent)) {
    return { openSource: OPEN_SOURCE.PROXY, reason: 'brevo_proxy_event', userAgent, ipAddress, hasSend: false };
  }

  if (userAgent && KNOWN_PROXY_UA_RE.test(userAgent)) {
    return { openSource: OPEN_SOURCE.PROXY, reason: 'known_proxy_user_agent', userAgent, ipAddress, hasSend: false };
  }

  const hasSend = Boolean(sendMatch) || await hasCorrespondingSend(pool, {
    prospectId,
    clientId,
    recipientEmail,
    messageId,
    subject,
    eventAt: eventDate,
  });

  if (!hasSend) {
    return { openSource: OPEN_SOURCE.UNKNOWN, reason: 'no_corresponding_send', userAgent, ipAddress, hasSend };
  }

  const deliveredAt = await findDeliveryTimestamp(pool, {
    prospectId,
    clientId,
    recipientEmail,
    messageId,
    subject,
    sendMatch,
  });
  if (deliveredAt) {
    const deltaSeconds = Math.abs((eventDate.getTime() - deliveredAt.getTime()) / 1000);
    if (deltaSeconds <= DELIVERY_COINCIDENT_THRESHOLD_SECONDS) {
      return {
        openSource: OPEN_SOURCE.PROXY,
        reason: 'delivery_coincident',
        userAgent,
        ipAddress,
        hasSend,
        deliveredAt,
      };
    }
  }

  return {
    openSource: OPEN_SOURCE.HUMAN,
    reason: 'sent_non_proxy_open',
    userAgent,
    ipAddress,
    hasSend,
    deliveredAt,
    source: sourceKey({ ipAddress, userAgent }),
  };
}

async function logZeroSendSuppression(pool, {
  source = 'riley',
  eventId,
  eventType,
  prospectId,
  clientId,
  recipientEmail,
  messageId,
  subject,
}) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
    VALUES ($1, 'signal_dropped_zero_send', $2, $3::jsonb, 'skipped', NOW(), $4)
  `, [
    source,
    prospectId || null,
    JSON.stringify({
      event_id: eventId || null,
      event_type: eventType || null,
      recipient_email: recipientEmail || null,
      brevo_message_id: messageId || null,
      subject: subject || null,
      reason: 'no_corresponding_send',
      prospect_id: prospectId || null,
      client_id: clientId || null,
    }),
    clientId || null,
  ]);
}

module.exports = {
  OPEN_SOURCE,
  DELIVERY_COINCIDENT_THRESHOLD_SECONDS,
  BATCH_OPEN_WINDOW_SECONDS,
  BATCH_OPEN_DISTINCT_PROSPECT_THRESHOLD,
  classifyOpenSource,
  ensureOpenSignalSchema,
  extractOpenMetadata,
  hasCorrespondingSend,
  isClickEventType,
  isOpenEventType,
  isOpenOrClickEventType,
  logZeroSendSuppression,
  markBatchProxyEvents,
};
