require('dotenv').config();

const axios = require('axios');
const { randomUUID } = require('crypto');
const pool = require('./db');
const { getRuntimeClientId } = require('./utils/clientContext');
const { ensureMiraSchema } = require('./utils/miraSchema');
const { reportAgentRun } = require('./utils/agentObservability');

const AGENT_NAME = 'warm_routing';
const CLIENT_ID = getRuntimeClientId();
const WORKER_INTERVAL_MS = 10 * 60 * 1000;
const ADVISORY_LOCK_KEY = 91720260617;
const TODOIST_API_BASE = 'https://api.todoist.com/api/v1';
const TODOIST_PROJECT_ID = '6ggVcJrgX9QgWwFW';
const TODOIST_SECTION_ID = '6ggVcMrcVVCqvFv4';
const DAILY_PING_CAP = 10;
const LOCAL_TIME_ZONE = 'America/New_York';
const TRIGGER_REASONS = [
  'ICP_JUMP_15',
  'REPLY_RECEIVED',
  'ENGAGEMENT_CLUSTER',
  'ICP_CROSS_80_RECENT',
];

let intervalHandle = null;
let intervalRunning = false;

function makeRunId(clientId) {
  return `warm_routing_run-${clientId || 'none'}-${new Date().toISOString()}-${randomUUID()}`;
}

async function reportWarmRoutingRun({ clientId, runId, attempts, successes, skipped, errorSample = null }) {
  try {
    return await reportAgentRun({
      agent: 'warm_routing_run',
      clientId,
      runId,
      attempts,
      successes,
      skipped,
      errorSample,
    });
  } catch (err) {
    console.error('[warm_routing] Observability report failed:', err.message);
    return null;
  }
}

function truncate(value, max = 1200) {
  const text = value === undefined || value === null ? '' : String(value);
  return text.length > max ? text.slice(0, max) : text;
}

function normalizeId(value) {
  return String(value || '').trim();
}

function todoistToken() {
  return process.env.TODOIST_API_TOKEN || process.env.TODOIST_TOKEN || null;
}

function telegramConfig() {
  return {
    botToken: process.env.MIRA_TELEGRAM_BOT_TOKEN,
    chatId: process.env.JACOB_TELEGRAM_CHAT_ID,
  };
}

function dashboardUrl(path = '/dashboard/warm') {
  const base = process.env.DASHBOARD_URL || process.env.APP_URL || 'https://pulseforge-leadgen-production.up.railway.app';
  return `${String(base).replace(/\/+$/, '')}${path}`;
}

function localDateKey(date = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: LOCAL_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

function localTimeParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: LOCAL_TIME_ZONE,
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(date);
  return Object.fromEntries(parts.map(part => [part.type, part.value]));
}

function localClock(date = new Date()) {
  const parts = localTimeParts(date);
  return `${parts.hour}:${parts.minute}`;
}

function isDigestWindow(date = new Date()) {
  const parts = localTimeParts(date);
  return Number(parts.hour) === 20 && Number(parts.minute) < 10;
}

function relTime(value) {
  if (!value) return 'unknown';
  const then = new Date(value).getTime();
  if (!Number.isFinite(then)) return 'unknown';
  const diffMs = Math.max(0, Date.now() - then);
  const minutes = Math.floor(diffMs / 60000);
  if (minutes < 60) return `${Math.max(1, minutes)}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

function companyLabel(prospect = {}) {
  return prospect.company_name || prospect.business_name || prospect.company || prospect.email || `Prospect ${prospect.id}`;
}

function triggerLabel(reason, prospect = {}) {
  if (reason === 'ICP_JUMP_15') {
    const delta = Number(prospect.icp_delta_7d || 0);
    return `ICP jumped +${delta}, now ${Number(prospect.icp_score || 0)}`;
  }
  if (reason === 'REPLY_RECEIVED') return 'Inbound reply received';
  if (reason === 'ENGAGEMENT_CLUSTER') {
    const opens = Number(prospect.opens_24h || 0);
    const clicks = Number(prospect.clicks_24h || 0);
    if (clicks > 0) return `${clicks} click${clicks === 1 ? '' : 's'} in 24h`;
    return `${opens} opens in 24h`;
  }
  if (reason === 'ICP_CROSS_80_RECENT') return `ICP crossed 80, now ${Number(prospect.icp_score || 0)}`;
  return reason;
}

function lastTouchSummary(prospect = {}) {
  const step = prospect.last_email_step ? `, email step ${prospect.last_email_step}` : '';
  return `${relTime(prospect.last_touch_at || prospect.email_touched_at || prospect.last_contacted_at)}${step}`;
}

function prospectContext(prospect = {}) {
  return [prospect.vertical, prospect.service_area_match || prospect.location || prospect.company_location]
    .filter(Boolean)
    .join(', ') || 'no context';
}

function buildWarmTelegramText(prospect, reason) {
  return [
    `Warm signal: ${companyLabel(prospect)}`,
    '',
    `Contact: ${prospect.email || prospect.phone || 'unknown'}`,
    '',
    `Trigger: ${triggerLabel(reason, prospect)}`,
    '',
    `Last touch: ${lastTouchSummary(prospect)}`,
    '',
    `Context: ${prospectContext(prospect)}`,
  ].join('\n');
}

function warmKeyboard(prospectId) {
  return {
    inline_keyboard: [[
      { text: 'Working now', callback_data: `working:${prospectId}` },
      { text: 'Today', callback_data: `today:${prospectId}` },
      { text: 'Tomorrow', callback_data: `tomorrow:${prospectId}` },
    ]],
  };
}

function taskDescription(prospect, reason) {
  return [
    `Contact: ${prospect.email || prospect.phone || 'unknown'}`,
    `ICP: ${Number(prospect.icp_score || 0)}${prospect.icp_delta_7d ? `, delta +${prospect.icp_delta_7d}` : ''}`,
    `Trigger: ${triggerLabel(reason, prospect)}`,
    `Last touch: ${lastTouchSummary(prospect)}`,
    `Sequence step: ${prospect.last_email_step || 'unknown'}`,
    `Vertical: ${prospect.vertical || 'unknown'}`,
    `City: ${prospect.service_area_match || prospect.location || prospect.company_location || 'unknown'}`,
  ].join('\n');
}

function warmCaptureText(prospect, reason) {
  return `Warm signal: ${companyLabel(prospect)}. ${triggerLabel(reason, prospect)}. ${prospectContext(prospect)}.`;
}

function resolutionCaptureText(action, prospect) {
  const company = companyLabel(prospect);
  if (action === 'working_now') return `Working on it: ${company}.`;
  if (action === 'today') return `Queued for today: ${company}.`;
  if (action === 'tomorrow') return `Queued for tomorrow: ${company}.`;
  if (action === 'auto_escalated') return `Auto-escalated: ${company}.`;
  return `Resolved: ${company}.`;
}

async function insertMiraPrimaryCapture({
  captureType,
  text,
  prospectId,
  capturedAt,
  linkedCaptureId = null,
}) {
  const { rows } = await pool.query(`
    INSERT INTO capture_inbox (
      received_at, content_type, raw_text, classification, routed_to_table,
      routed_to_id, status, raw_metadata, processed_at, capture_type, source,
      linked_entity_type, linked_entity_id, linked_capture_id, captured_at
    )
    VALUES (
      $1, 'text', $2, $3, 'mira_primary_log', $4, 'routed', $5::jsonb, $1,
      $3, 'warm_routing', 'prospect', $4, $6, $1
    )
    RETURNING id
  `, [
    capturedAt,
    text,
    captureType,
    String(prospectId),
    JSON.stringify({ source: 'warm_routing', capture_type: captureType }),
    linkedCaptureId,
  ]);
  return rows[0]?.id || null;
}

async function ensureWarmRoutingSchema() {
  await ensureMiraSchema();
  await pool.query(`
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS email_touched_at TIMESTAMPTZ
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS warm_trigger_fires (
      id BIGSERIAL PRIMARY KEY,
      prospect_id UUID REFERENCES prospects(id),
      client_id INTEGER,
      trigger_reason TEXT NOT NULL CHECK (trigger_reason IN ('ICP_JUMP_15','REPLY_RECEIVED','ENGAGEMENT_CLUSTER','ICP_CROSS_80_RECENT')),
      fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolved_action TEXT CHECK (resolved_action IS NULL OR resolved_action IN ('working_now','today','tomorrow','auto_escalated')),
      resolved_at TIMESTAMPTZ,
      todoist_task_id TEXT
    )
  `);
  await pool.query(`ALTER TABLE warm_trigger_fires ADD COLUMN IF NOT EXISTS client_id INTEGER`);
  await pool.query(`ALTER TABLE warm_trigger_fires ADD COLUMN IF NOT EXISTS trigger_payload JSONB`);
  await pool.query(`ALTER TABLE warm_trigger_fires ADD COLUMN IF NOT EXISTS ping_sent BOOLEAN NOT NULL DEFAULT false`);
  await pool.query(`ALTER TABLE warm_trigger_fires ADD COLUMN IF NOT EXISTS digest_sent BOOLEAN NOT NULL DEFAULT false`);
  await pool.query(`ALTER TABLE warm_trigger_fires ADD COLUMN IF NOT EXISTS digest_date DATE`);
  await pool.query(`ALTER TABLE warm_trigger_fires ADD COLUMN IF NOT EXISTS telegram_chat_id TEXT`);
  await pool.query(`ALTER TABLE warm_trigger_fires ADD COLUMN IF NOT EXISTS telegram_message_id TEXT`);
  await pool.query(`ALTER TABLE warm_trigger_fires ADD COLUMN IF NOT EXISTS mira_capture_id BIGINT`);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS warm_trigger_fires_recent_idx
      ON warm_trigger_fires (prospect_id, trigger_reason, fired_at DESC)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS warm_trigger_fires_unresolved_idx
      ON warm_trigger_fires (client_id, resolved_action, fired_at DESC)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS icp_score_snapshots (
      id BIGSERIAL PRIMARY KEY,
      prospect_id UUID REFERENCES prospects(id),
      client_id INTEGER,
      icp_score INTEGER,
      snapshot_date DATE NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (prospect_id, snapshot_date)
    )
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS icp_score_snapshots_lookup_idx
      ON icp_score_snapshots (prospect_id, snapshot_date DESC)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS mira_warm_capture_log (
      id BIGSERIAL PRIMARY KEY,
      warm_trigger_fire_id BIGINT UNIQUE REFERENCES warm_trigger_fires(id),
      prospect_id UUID,
      company TEXT,
      trigger_reason TEXT,
      icp_score INTEGER,
      fired_at TIMESTAMPTZ,
      resolved_action TEXT,
      resolved_at TIMESTAMPTZ,
      time_to_resolution_minutes INTEGER,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`ALTER TABLE capture_inbox ADD COLUMN IF NOT EXISTS capture_type TEXT`);
  await pool.query(`ALTER TABLE capture_inbox ADD COLUMN IF NOT EXISTS source TEXT`);
  await pool.query(`ALTER TABLE capture_inbox ADD COLUMN IF NOT EXISTS linked_entity_type TEXT`);
  await pool.query(`ALTER TABLE capture_inbox ADD COLUMN IF NOT EXISTS linked_entity_id TEXT`);
  await pool.query(`ALTER TABLE capture_inbox ADD COLUMN IF NOT EXISTS linked_capture_id BIGINT`);
  await pool.query(`ALTER TABLE capture_inbox ADD COLUMN IF NOT EXISTS captured_at TIMESTAMPTZ`);
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'capture_inbox_linked_capture_id_fkey'
          AND conrelid = 'capture_inbox'::regclass
      ) THEN
        ALTER TABLE capture_inbox
          ADD CONSTRAINT capture_inbox_linked_capture_id_fkey
          FOREIGN KEY (linked_capture_id) REFERENCES capture_inbox(id);
      END IF;
    END $$;
  `);
}

async function backfillEmailTouchedAt(clientId) {
  await pool.query(`
    UPDATE prospects p
    SET email_touched_at = latest.last_touch
    FROM (
      SELECT p2.id, MAX(t.created_at) AS last_touch
      FROM prospects p2
      JOIN touchpoints t ON t.prospect_id = p2.id AND t.client_id = p2.client_id
      WHERE p2.client_id = $1
        AND p2.email_touched_at IS NULL
        AND t.channel = 'email'
      GROUP BY p2.id
    ) latest
    WHERE p.id = latest.id
      AND p.client_id = $1
      AND p.email_touched_at IS NULL
  `, [clientId]);
}

async function snapshotIcpScores(clientId) {
  await pool.query(`
    INSERT INTO icp_score_snapshots (prospect_id, client_id, icp_score, snapshot_date)
    SELECT id, client_id, icp_score, CURRENT_DATE
    FROM prospects
    WHERE client_id = $1
    ON CONFLICT (prospect_id, snapshot_date) DO NOTHING
  `, [clientId]);
}

async function getWarmProspects(clientId) {
  const { rows } = await pool.query(`
    SELECT
      p.id,
      p.client_id,
      p.first_name,
      p.last_name,
      p.email,
      p.phone,
      p.vertical,
      p.status,
      p.icp_score,
      p.email_touched_at,
      p.last_contacted_at,
      p.service_area_match,
      COALESCE(c.name, NULLIF(SPLIT_PART(COALESCE(p.notes, ''), ' - ', 1), ''), p.email) AS company_name,
      COALESCE(snap.icp_score, hist.old_score, p.icp_score) AS icp_score_7d_ago,
      (p.icp_score - COALESCE(snap.icp_score, hist.old_score, p.icp_score))::int AS icp_delta_7d,
      COALESCE(evt.opens_24h, 0)::int + COALESCE(tp.opens_24h, 0)::int AS opens_24h,
      COALESCE(evt.clicks_24h, 0)::int + COALESCE(tp.clicks_24h, 0)::int AS clicks_24h,
      COALESCE(evt.reply_count, 0)::int + COALESCE(tp.reply_count, 0)::int AS reply_count,
      last_tp.created_at AS last_touch_at,
      last_tp.channel AS last_touch_channel,
      last_tp.action_type AS last_touch_action,
      last_tp.content_summary AS last_touch_summary,
      last_email.payload->>'step' AS last_email_step
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN LATERAL (
      SELECT icp_score
      FROM icp_score_snapshots s
      WHERE s.prospect_id = p.id
        AND s.snapshot_date <= CURRENT_DATE - INTERVAL '7 days'
      ORDER BY s.snapshot_date DESC
      LIMIT 1
    ) snap ON TRUE
    LEFT JOIN LATERAL (
      SELECT old_score
      FROM icp_score_history h
      WHERE h.prospect_id = p.id
        AND h.created_at >= NOW() - INTERVAL '7 days'
      ORDER BY h.created_at ASC
      LIMIT 1
    ) hist ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) FILTER (WHERE event_type IN ('opened', 'open') AND event_at >= NOW() - INTERVAL '24 hours') AS opens_24h,
        COUNT(*) FILTER (WHERE event_type IN ('clicked', 'click') AND event_at >= NOW() - INTERVAL '24 hours') AS clicks_24h,
        COUNT(*) FILTER (WHERE event_type IN ('replied', 'reply')) AS reply_count
      FROM email_events ee
      WHERE ee.client_id = p.client_id
        AND LOWER(ee.recipient_email) = LOWER(p.email)
    ) evt ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) FILTER (WHERE action_type IN ('open', 'email_opened') AND created_at >= NOW() - INTERVAL '24 hours') AS opens_24h,
        COUNT(*) FILTER (WHERE action_type IN ('click', 'email_clicked') AND created_at >= NOW() - INTERVAL '24 hours') AS clicks_24h,
        COUNT(*) FILTER (WHERE action_type IN ('inbound', 'reply', 'email_reply', 'inbound_reply')) AS reply_count
      FROM touchpoints t
      WHERE t.client_id = p.client_id
        AND t.prospect_id = p.id
    ) tp ON TRUE
    LEFT JOIN LATERAL (
      SELECT channel, action_type, content_summary, created_at
      FROM touchpoints t
      WHERE t.client_id = p.client_id
        AND t.prospect_id = p.id
      ORDER BY created_at DESC
      LIMIT 1
    ) last_tp ON TRUE
    LEFT JOIN LATERAL (
      SELECT payload
      FROM agent_log al
      WHERE al.client_id = p.client_id
        AND al.prospect_id = p.id
        AND al.agent_name = 'emmett'
        AND al.action = 'email_sent'
      ORDER BY al.ran_at DESC
      LIMIT 1
    ) last_email ON TRUE
    WHERE p.client_id = $1
      AND COALESCE(p.mira_archived, false) = false
      AND COALESCE(p.do_not_contact, false) = false
      AND COALESCE(p.status, '') NOT IN ('dead', 'bounced', 'disqualified', 'closed', 'do_not_email', 'auto_responder')
      AND p.email IS NOT NULL
      AND TRIM(p.email) <> ''
      AND NOT EXISTS (
        SELECT 1 FROM email_events bad
        WHERE bad.client_id = p.client_id
          AND LOWER(bad.recipient_email) = LOWER(p.email)
          AND bad.event_type IN ('hard_bounce', 'blocked', 'unsubscribed', 'spam')
      )
    ORDER BY p.icp_score DESC NULLS LAST, p.created_at DESC
    LIMIT 500
  `, [clientId]);
  return rows;
}

function evaluateWarmTriggers(prospect) {
  const reasons = [];
  if (Number(prospect.icp_delta_7d || 0) >= 15) reasons.push('ICP_JUMP_15');
  if (Number(prospect.reply_count || 0) > 0) reasons.push('REPLY_RECEIVED');
  if (Number(prospect.opens_24h || 0) >= 3 || Number(prospect.clicks_24h || 0) > 0) reasons.push('ENGAGEMENT_CLUSTER');
  const touchedAt = prospect.email_touched_at ? new Date(prospect.email_touched_at).getTime() : 0;
  if (Number(prospect.icp_score || 0) >= 80 && touchedAt && touchedAt >= Date.now() - 14 * 24 * 60 * 60 * 1000) {
    reasons.push('ICP_CROSS_80_RECENT');
  }
  return reasons;
}

async function hasRecentFire(prospectId, reason) {
  const { rows } = await pool.query(`
    SELECT 1
    FROM warm_trigger_fires
    WHERE prospect_id = $1
      AND trigger_reason = $2
      AND fired_at >= NOW() - INTERVAL '72 hours'
    LIMIT 1
  `, [prospectId, reason]);
  return rows.length > 0;
}

async function pingsSentToday(clientId) {
  const { rows } = await pool.query(`
    SELECT COUNT(*)::int AS count
    FROM warm_trigger_fires
    WHERE client_id = $1
      AND ping_sent = true
      AND (fired_at AT TIME ZONE $2)::date = (NOW() AT TIME ZONE $2)::date
  `, [clientId, LOCAL_TIME_ZONE]);
  return Number(rows[0]?.count || 0);
}

async function sendWarmTelegramMessage(prospect, reason) {
  const { botToken, chatId } = telegramConfig();
  if (!botToken || !chatId) {
    console.warn('[warm_routing] Telegram env missing. Warm ping skipped.');
    return null;
  }

  const response = await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    chat_id: chatId,
    text: buildWarmTelegramText(prospect, reason),
    reply_markup: warmKeyboard(prospect.id),
  }, { timeout: 5000 });

  return {
    chatId: normalizeId(chatId),
    messageId: normalizeId(response.data?.result?.message_id),
  };
}

async function editWarmTelegramMessage(callbackQuery, appendText) {
  const { botToken } = telegramConfig();
  const message = callbackQuery?.message;
  if (!botToken || !message?.chat?.id || !message?.message_id) return;
  const baseText = String(message.text || '').replace(/\n\n→ .+$/s, '');
  await axios.post(`https://api.telegram.org/bot${botToken}/editMessageText`, {
    chat_id: message.chat.id,
    message_id: message.message_id,
    text: `${baseText}\n\n${appendText}`,
    reply_markup: { inline_keyboard: [] },
  }, { timeout: 5000 });
}

async function editWarmTelegramMessageDirect(chatId, messageId, baseText, appendText) {
  const { botToken } = telegramConfig();
  if (!botToken || !chatId || !messageId) return;
  const cleanBaseText = String(baseText || '').replace(/\n\n→ .+$/s, '');
  await axios.post(`https://api.telegram.org/bot${botToken}/editMessageText`, {
    chat_id: chatId,
    message_id: messageId,
    text: `${cleanBaseText}\n\n${appendText}`,
    reply_markup: { inline_keyboard: [] },
  }, { timeout: 5000 });
}

async function answerWarmCallback(callbackQueryId, text = null) {
  const { botToken } = telegramConfig();
  if (!botToken || !callbackQueryId) return;
  await axios.post(`https://api.telegram.org/bot${botToken}/answerCallbackQuery`, {
    callback_query_id: callbackQueryId,
    ...(text ? { text } : {}),
  }, { timeout: 3000 });
}

async function createTodoistTask({ prospect, reason, due, auto = false }) {
  const token = todoistToken();
  if (!token) throw new Error('TODOIST_API_TOKEN not set');

  const titlePrefix = auto ? '[auto] ' : '';
  const payload = {
    content: `${titlePrefix}Touch ${companyLabel(prospect)}: ${triggerLabel(reason, prospect)}`,
    description: taskDescription(prospect, reason),
    project_id: TODOIST_PROJECT_ID,
    section_id: TODOIST_SECTION_ID,
    due_string: due,
  };

  const response = await axios.post(`${TODOIST_API_BASE}/tasks`, payload, {
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    timeout: 8000,
  });

  if (!response.data?.id) throw new Error('Todoist task create response did not include id');
  return String(response.data.id);
}

async function insertMiraWarmCaptureLog(fireId, prospect, reason) {
  await pool.query(`
    INSERT INTO mira_warm_capture_log (
      warm_trigger_fire_id, prospect_id, company, trigger_reason, icp_score, fired_at
    )
    SELECT id, prospect_id, $2, trigger_reason, $3, fired_at
    FROM warm_trigger_fires
    WHERE id = $1
    ON CONFLICT (warm_trigger_fire_id) DO NOTHING
  `, [fireId, companyLabel(prospect), Number(prospect.icp_score || 0)]);
}

async function writeFireCapture(fireId, prospect, reason, firedAt) {
  const captureId = await insertMiraPrimaryCapture({
    captureType: 'warm_signal',
    text: warmCaptureText(prospect, reason),
    prospectId: prospect.id,
    capturedAt: firedAt,
  });
  if (captureId) {
    await pool.query('UPDATE warm_trigger_fires SET mira_capture_id = $2 WHERE id = $1', [fireId, captureId]);
  }
  return captureId;
}

async function writeResolutionCapture(fireId, action, prospect, resolvedAt = new Date()) {
  const { rows } = await pool.query('SELECT mira_capture_id FROM warm_trigger_fires WHERE id = $1', [fireId]);
  const linkedCaptureId = rows[0]?.mira_capture_id || null;
  const captureId = await insertMiraPrimaryCapture({
    captureType: 'warm_signal_resolved',
    text: resolutionCaptureText(action, prospect),
    prospectId: prospect.id || prospect.prospect_id,
    capturedAt: resolvedAt,
    linkedCaptureId,
  });
  return captureId;
}

async function syncMiraWarmCaptureResolution(fireId) {
  await pool.query(`
    UPDATE mira_warm_capture_log log
    SET resolved_action = fire.resolved_action,
        resolved_at = fire.resolved_at,
        time_to_resolution_minutes = CASE
          WHEN fire.resolved_at IS NULL THEN NULL
          ELSE FLOOR(EXTRACT(EPOCH FROM (fire.resolved_at - fire.fired_at)) / 60)::int
        END
    FROM warm_trigger_fires fire
    WHERE log.warm_trigger_fire_id = fire.id
      AND fire.id = $1
  `, [fireId]);
}

async function logAgent(action, payload, status = 'success', errorMsg = null, clientId = CLIENT_ID, prospectId = null) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, error_msg, ran_at, client_id)
    VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)
  `, [
    AGENT_NAME,
    action,
    prospectId,
    JSON.stringify(payload || {}),
    status,
    errorMsg ? truncate(errorMsg, 500) : null,
    clientId,
  ]).catch(err => console.error('[warm_routing] agent_log write failed:', err.message));
}

async function recordFire(prospect, reason, pingResult) {
  const payload = {
    prospect_id: prospect.id,
    company: companyLabel(prospect),
    contact: prospect.email || prospect.phone || null,
    trigger_reason: reason,
    trigger_label: triggerLabel(reason, prospect),
    icp_score: Number(prospect.icp_score || 0),
    icp_delta_7d: Number(prospect.icp_delta_7d || 0),
    last_touch: lastTouchSummary(prospect),
    vertical: prospect.vertical || null,
    city: prospect.service_area_match || prospect.location || prospect.company_location || null,
  };

  const { rows } = await pool.query(`
    INSERT INTO warm_trigger_fires (
      prospect_id, client_id, trigger_reason, trigger_payload, ping_sent,
      telegram_chat_id, telegram_message_id, digest_date
    )
    VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, CURRENT_DATE)
    RETURNING id, fired_at
  `, [
    prospect.id,
    prospect.client_id,
    reason,
    JSON.stringify(payload),
    Boolean(pingResult),
    pingResult?.chatId || null,
    pingResult?.messageId || null,
  ]);
  const fire = rows[0];
  await insertMiraWarmCaptureLog(fire.id, prospect, reason);
  const miraCaptureId = await writeFireCapture(fire.id, prospect, reason, fire.fired_at);
  await logAgent('warm_trigger_fire', { fire_id: fire.id, ...payload, ping_sent: Boolean(pingResult) }, 'success', null, prospect.client_id, prospect.id);
  return { ...fire, mira_capture_id: miraCaptureId };
}

async function processProspectTrigger(prospect, reason) {
  if (await hasRecentFire(prospect.id, reason)) {
    return { fired: false, reason: 'recent_fire' };
  }

  let pingResult = null;
  if ((await pingsSentToday(prospect.client_id)) < DAILY_PING_CAP) {
    pingResult = await sendWarmTelegramMessage(prospect, reason);
  }

  const fire = await recordFire(prospect, reason, pingResult);
  return { fired: true, fire_id: fire.id, ping_sent: Boolean(pingResult) };
}

async function autoEscalateStaleFires(clientId) {
  const { rows } = await pool.query(`
    SELECT fire.id AS fire_id, fire.trigger_reason, fire.trigger_payload,
      fire.telegram_chat_id, fire.telegram_message_id,
      p.*, COALESCE(c.name, fire.trigger_payload->>'company', p.email) AS company_name
    FROM warm_trigger_fires fire
    JOIN prospects p ON p.id = fire.prospect_id AND p.client_id = fire.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE fire.client_id = $1
      AND fire.resolved_action IS NULL
      AND fire.fired_at <= NOW() - INTERVAL '24 hours'
    ORDER BY fire.fired_at ASC
    LIMIT 25
  `, [clientId]);

  let escalated = 0;
  for (const row of rows) {
    const payload = row.trigger_payload || {};
    const prospect = { ...row, ...payload, id: row.id, company_name: row.company_name };
    try {
      const taskId = await createTodoistTask({ prospect, reason: row.trigger_reason, due: 'today', auto: true });
      await pool.query(`
        UPDATE warm_trigger_fires
        SET resolved_action = 'auto_escalated',
            resolved_at = NOW(),
            todoist_task_id = $2
        WHERE id = $1
      `, [row.fire_id, taskId]);
      await syncMiraWarmCaptureResolution(row.fire_id);
      await writeResolutionCapture(row.fire_id, 'auto_escalated', prospect);
      if (row.telegram_chat_id && row.telegram_message_id) {
        try {
          await editWarmTelegramMessageDirect(
            row.telegram_chat_id,
            row.telegram_message_id,
            buildWarmTelegramText(prospect, row.trigger_reason),
            `→ auto-escalated to today's queue at ${localClock()}`
          );
        } catch (err) {
          await logAgent(
            'warm_trigger_telegram_edit_failed',
            {
              fire_id: row.fire_id,
              telegram_chat_id: row.telegram_chat_id,
              telegram_message_id: row.telegram_message_id,
            },
            'failed',
            err.message,
            clientId,
            row.prospect_id
          );
        }
      }
      await logAgent('warm_trigger_auto_escalated', { fire_id: row.fire_id, todoist_task_id: taskId }, 'success', null, clientId, row.prospect_id);
      escalated++;
    } catch (err) {
      await logAgent('warm_trigger_auto_escalation_failed', { fire_id: row.fire_id }, 'failed', err.message, clientId, row.prospect_id);
    }
  }
  return escalated;
}

async function sendOverflowDigest(clientId) {
  if (!isDigestWindow()) return { sent: false, reason: 'outside_digest_window' };

  const dateKey = localDateKey();
  const already = await pool.query(`
    SELECT 1
    FROM agent_log
    WHERE agent_name = $1
      AND action = 'warm_overflow_digest'
      AND client_id = $2
      AND payload->>'digest_date' = $3
    LIMIT 1
  `, [AGENT_NAME, clientId, dateKey]);
  if (already.rows.length) return { sent: false, reason: 'already_sent' };

  const { rows } = await pool.query(`
    SELECT fire.id, fire.prospect_id, fire.trigger_reason, fire.trigger_payload,
      COALESCE((fire.trigger_payload->>'icp_score')::int, p.icp_score, 0) AS score,
      COALESCE(fire.trigger_payload->>'company', c.name, p.email) AS company
    FROM warm_trigger_fires fire
    JOIN prospects p ON p.id = fire.prospect_id AND p.client_id = fire.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE fire.client_id = $1
      AND fire.ping_sent = false
      AND fire.digest_sent = false
      AND (fire.fired_at AT TIME ZONE $2)::date = (NOW() AT TIME ZONE $2)::date
    ORDER BY score DESC, fire.fired_at ASC
    LIMIT 5
  `, [clientId, LOCAL_TIME_ZONE]);

  if (!rows.length) return { sent: false, reason: 'no_overflow' };

  const lines = rows.map(row => {
    const payload = row.trigger_payload || {};
    const label = payload.trigger_label || row.trigger_reason;
    return `${row.company}: ICP ${row.score}, ${label}`;
  });
  const text = [
    'Warm signal overflow digest',
    '',
    ...lines,
    '',
    `Review: ${dashboardUrl('/dashboard/warm')}`,
  ].join('\n');

  const { botToken, chatId } = telegramConfig();
  if (!botToken || !chatId) {
    await logAgent('warm_overflow_digest', { digest_date: dateKey, skipped: 'telegram_env_missing', count: rows.length }, 'skipped', null, clientId);
    return { sent: false, reason: 'telegram_env_missing' };
  }

  await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    chat_id: chatId,
    text,
  }, { timeout: 5000 });

  await pool.query(`
    UPDATE warm_trigger_fires
    SET digest_sent = true
    WHERE id = ANY($1::bigint[])
  `, [rows.map(row => row.id)]);
  await logAgent('warm_overflow_digest', { digest_date: dateKey, count: rows.length, ids: rows.map(row => row.id) }, 'success', null, clientId);
  return { sent: true, count: rows.length };
}

async function withWorkerLock(fn) {
  const lock = await pool.query('SELECT pg_try_advisory_lock($1) AS locked', [ADVISORY_LOCK_KEY]);
  if (!lock.rows[0]?.locked) return { skipped: true, reason: 'worker_already_running' };
  try {
    return await fn();
  } finally {
    await pool.query('SELECT pg_advisory_unlock($1)', [ADVISORY_LOCK_KEY]).catch(err => {
      console.error('[warm_routing] advisory unlock failed:', err.message);
    });
  }
}

async function run(params = {}) {
  const clientId = getRuntimeClientId(params);
  const runId = makeRunId(clientId);
  let result;
  try {
    result = await withWorkerLock(async () => {
    await ensureWarmRoutingSchema();
    await backfillEmailTouchedAt(clientId);
    await snapshotIcpScores(clientId);

    const prospects = await getWarmProspects(clientId);
    let fires = 0;
    let skipped = 0;
    const results = [];

    for (const prospect of prospects) {
      const reasons = evaluateWarmTriggers(prospect);
      for (const reason of reasons) {
        try {
          const result = await processProspectTrigger(prospect, reason);
          if (result.fired) fires++;
          else skipped++;
          results.push({ prospect_id: prospect.id, reason, ...result });
        } catch (err) {
          skipped++;
          results.push({ prospect_id: prospect.id, reason, fired: false, error: err.message });
          await logAgent('warm_trigger_failed', { prospect_id: prospect.id, reason }, 'failed', err.message, clientId, prospect.id);
        }
      }
    }

    const autoEscalated = await autoEscalateStaleFires(clientId);
    const digest = await sendOverflowDigest(clientId);
    await logAgent('warm_routing_run', {
      scanned: prospects.length,
      fires,
      skipped,
      auto_escalated: autoEscalated,
      digest,
    }, 'success', null, clientId);

    return { scanned: prospects.length, fires, skipped, auto_escalated: autoEscalated, digest, results };
  });
  } catch (err) {
    const failed = { attempts: 1, successes: 0, skipped: 0, errorSample: { error: err.message }, failed: true };
    await reportWarmRoutingRun({ clientId, runId, ...failed });
    return failed;
  }
  const idleLockSkip = result.skipped === true && result.reason === 'worker_already_running';
  if (idleLockSkip) {
    const idle = { ...result, attempts: 0, successes: 0, skipped: 0, errorSample: null, idle: true };
    await reportWarmRoutingRun({ clientId, runId, attempts: idle.attempts, successes: idle.successes, skipped: idle.skipped, errorSample: idle.errorSample });
    return idle;
  }
  const routeSkips = Number.isFinite(Number(result.skipped)) ? Number(result.skipped) : 0;
  const attempts = Number(result.attempts ?? ((result.fires || 0) + routeSkips));
  const successes = Number(result.successes ?? (result.fires || 0));
  const skipped = routeSkips;
  const errorSample = result.errorSample || result.results?.find(row => row.error) || null;
  await reportWarmRoutingRun({ clientId, runId, attempts, successes, skipped, errorSample });
  return { ...result, attempts, successes, skipped, errorSample };
}

function startWarmRoutingScheduler(options = {}) {
  const intervalMs = Number(options.intervalMs || WORKER_INTERVAL_MS);
  if (intervalHandle) return intervalHandle;
  intervalHandle = setInterval(() => {
    if (intervalRunning) return;
    intervalRunning = true;
    run({ client_id: options.client_id || CLIENT_ID })
      .catch(err => console.error('[warm_routing] scheduler error:', err.message))
      .finally(() => { intervalRunning = false; });
  }, intervalMs);
  if (intervalHandle.unref) intervalHandle.unref();
  console.log(`[warm_routing] scheduler started interval=${intervalMs}ms`);
  return intervalHandle;
}

async function getLatestUnresolvedFire(prospectId) {
  const { rows } = await pool.query(`
    SELECT fire.*, p.email, p.phone, p.vertical, p.icp_score, p.service_area_match,
      c.location AS location,
      c.location AS company_location,
      COALESCE(c.name, fire.trigger_payload->>'company', p.email) AS company_name
    FROM warm_trigger_fires fire
    JOIN prospects p ON p.id = fire.prospect_id AND p.client_id = fire.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE fire.prospect_id = $1
      AND fire.resolved_action IS NULL
    ORDER BY fire.fired_at DESC
    LIMIT 1
  `, [prospectId]);
  return rows[0] || null;
}

async function handleWarmTelegramCallback(callbackQuery) {
  const data = String(callbackQuery?.data || '');
  const match = data.match(/^(working|today|tomorrow):([0-9a-f-]+)$/i);
  if (!match) return false;

  const action = match[1].toLowerCase();
  const prospectId = match[2];
  await answerWarmCallback(callbackQuery.id);
  await ensureWarmRoutingSchema();

  const fire = await getLatestUnresolvedFire(prospectId);
  if (!fire) {
    await editWarmTelegramMessage(callbackQuery, `→ already resolved at ${localClock()}`);
    return true;
  }

  const payload = fire.trigger_payload || {};
  const prospect = { ...fire, ...payload, id: fire.prospect_id, company_name: fire.company_name };

  if (action === 'working') {
    await pool.query(`
      UPDATE warm_trigger_fires
      SET resolved_action = 'working_now',
          resolved_at = NOW()
      WHERE id = $1
    `, [fire.id]);
    await syncMiraWarmCaptureResolution(fire.id);
    await writeResolutionCapture(fire.id, 'working_now', prospect);
    await editWarmTelegramMessage(callbackQuery, `→ Jacob is on it at ${localClock()}`);
    await logAgent('warm_trigger_resolved', { fire_id: fire.id, action: 'working_now' }, 'success', null, fire.client_id, fire.prospect_id);
    return true;
  }

  const due = action === 'tomorrow' ? 'tomorrow' : 'today';
  const taskId = await createTodoistTask({ prospect, reason: fire.trigger_reason, due, auto: false });
  await pool.query(`
    UPDATE warm_trigger_fires
    SET resolved_action = $2,
        resolved_at = NOW(),
        todoist_task_id = $3
    WHERE id = $1
  `, [fire.id, action, taskId]);
  await syncMiraWarmCaptureResolution(fire.id);
  await writeResolutionCapture(fire.id, action, prospect);
  await editWarmTelegramMessage(callbackQuery, action === 'tomorrow' ? `→ added to tomorrow's queue` : `→ added to today's queue`);
  await logAgent('warm_trigger_resolved', { fire_id: fire.id, action, todoist_task_id: taskId }, 'success', null, fire.client_id, fire.prospect_id);
  return true;
}

module.exports = {
  ensureWarmRoutingSchema,
  evaluateWarmTriggers,
  run,
  startWarmRoutingScheduler,
  handleWarmTelegramCallback,
  editWarmTelegramMessageDirect,
  createTodoistTask,
};

if (require.main === module) {
  run().then(result => {
    console.log(JSON.stringify(result, null, 2));
  }).catch(err => {
    console.error('[warm_routing] fatal:', err.stack || err.message);
    process.exit(1);
  });
}
