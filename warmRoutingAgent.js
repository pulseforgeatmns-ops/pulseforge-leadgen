require('dotenv').config();

const axios = require('axios');
const { randomUUID } = require('crypto');
const pool = require('./db');
const { getRuntimeClientId } = require('./utils/clientContext');
const { reportAgentRun } = require('./utils/agentObservability');
const { OPEN_SOURCE, ensureOpenSignalSchema } = require('./utils/openSignalGate');
const { resolveVerticalTier } = require('./utils/verticalTiers');

const AGENT_NAME = 'warm_routing';
const CLIENT_ID = getRuntimeClientId();
const WORKER_INTERVAL_MS = 10 * 60 * 1000;
const ADVISORY_LOCK_KEY = 91720260617;
const TODOIST_API_BASE = 'https://api.todoist.com/api/v1';
const TODOIST_PROJECT_ID = '6ggVcJrgX9QgWwFW';
const TODOIST_SECTION_ID = '6ggVcMrcVVCqvFv4';
const DAILY_PING_CAP = 10;
const LOCAL_TIME_ZONE = 'America/New_York';
const WARM_ROUTING_SEED_VERSION = '2026-07-04-edge-v1';
const SIGNAL_TYPES = {
  ICP_ELIGIBILITY: 'ICP_ELIGIBILITY',
  ICP_SCORE: 'ICP_SCORE',
  ENGAGEMENT_CLUSTER: 'ENGAGEMENT_CLUSTER',
  REPLY: 'REPLY',
};
const TRIGGER_REASONS = [
  'ICP_JUMP_15',
  'ICP_CROSS_90',
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
  const storedLabel = prospect.trigger_label || prospect.trigger_payload?.trigger_label;
  if (storedLabel) return String(storedLabel);
  if (reason === 'ICP_JUMP_15') {
    const delta = Number(prospect.icp_delta_7d || 0);
    return `ICP jumped +${delta}, now ${Number(prospect.icp_score || 0)}`;
  }
  if (reason === 'ICP_CROSS_90') return `ICP crossed 90, now ${Number(prospect.icp_score || 0)}`;
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

function isWarmRoutingEnabled(env = process.env) {
  return String(env.WARM_ROUTING_ENABLED || '').toLowerCase() === 'true';
}

async function isWarmRoutingSeeded(clientId) {
  try {
    const { rows } = await pool.query(`
      SELECT 1
      FROM warm_routing_control
      WHERE client_id = $1
        AND seed_version = $2
        AND seeded_at IS NOT NULL
      LIMIT 1
    `, [clientId, WARM_ROUTING_SEED_VERSION]);
    return rows.length > 0;
  } catch (err) {
    console.warn(`[warm_routing] seed gate unavailable: ${err.message}`);
    return false;
  }
}

async function getWarmProspects(clientId) {
  await ensureOpenSignalSchema(pool);
  const { rows } = await pool.query(`
    SELECT
      p.id,
      p.client_id,
      p.first_name,
      p.last_name,
      p.email,
      p.phone,
      p.vertical,
      client.vertical_tiers,
      p.status,
      p.icp_score,
      GREATEST(p.email_touched_at, email_touch.latest_touch_at) AS email_touched_at,
      p.last_contacted_at,
      p.service_area_match,
      COALESCE(c.name, NULLIF(SPLIT_PART(COALESCE(p.notes, ''), ' - ', 1), ''), p.email) AS company_name,
      p.icp_score AS icp_score_7d_ago,
      0::int AS icp_delta_7d,
      COALESCE(eng.opens_24h, 0)::int AS opens_24h,
      COALESCE(eng.clicks_24h, 0)::int AS clicks_24h,
      eng.latest_event_key AS engagement_event_key,
      last_tp.created_at AS last_touch_at,
      last_tp.channel AS last_touch_channel,
      last_tp.action_type AS last_touch_action,
      last_tp.content_summary AS last_touch_summary,
      last_email.payload->>'step' AS last_email_step
    FROM prospects p
    JOIN clients client ON client.id = p.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN LATERAL (
      SELECT MAX(t.created_at) AS latest_touch_at
      FROM touchpoints t
      WHERE t.client_id = p.client_id
        AND t.prospect_id = p.id
        AND t.channel = 'email'
    ) email_touch ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) FILTER (WHERE kind = 'open') AS opens_24h,
        COUNT(*) FILTER (WHERE kind = 'click') AS clicks_24h,
        (ARRAY_AGG(event_key ORDER BY occurred_at DESC, event_key DESC))[1] AS latest_event_key
      FROM (
        SELECT DISTINCT ON (kind, message_key, DATE_TRUNC('second', occurred_at))
          kind, event_key, message_key, occurred_at
        FROM (
          SELECT
            CASE
              WHEN ee.event_type IN ('opened', 'open') AND ee.open_source = $2::open_source THEN 'open'
              WHEN ee.event_type IN ('clicked', 'click') THEN 'click'
            END AS kind,
            'email_event:' || ee.id::text AS event_key,
            COALESCE(NULLIF(ee.brevo_message_id, ''), LOWER(ee.recipient_email)) AS message_key,
            ee.event_at AS occurred_at
          FROM email_events ee
          WHERE ee.client_id = p.client_id
            AND LOWER(ee.recipient_email) = LOWER(p.email)
            AND ee.event_type IN ('opened', 'open', 'clicked', 'click')
            AND ee.event_at >= NOW() - INTERVAL '24 hours'
            AND (
              EXISTS (
                SELECT 1
                FROM email_events sent
                WHERE sent.client_id = ee.client_id
                  AND sent.prospect_id = ee.prospect_id
                  AND sent.event_type IN ('sent', 'delivered')
                  AND (
                    (ee.brevo_message_id IS NOT NULL AND sent.brevo_message_id = ee.brevo_message_id)
                    OR (
                      ee.brevo_message_id IS NULL
                      AND LOWER(sent.recipient_email) = LOWER(ee.recipient_email)
                      AND sent.subject_line IS NOT DISTINCT FROM ee.subject_line
                      AND sent.event_at <= ee.event_at
                    )
                  )
              )
              OR EXISTS (
                SELECT 1
                FROM agent_log al
                WHERE al.client_id = ee.client_id
                  AND al.prospect_id = ee.prospect_id
                  AND al.agent_name = 'emmett'
                  AND al.action = 'email_sent'
                  AND (
                    (ee.brevo_message_id IS NOT NULL AND al.payload->>'message_id' = ee.brevo_message_id)
                    OR (
                      ee.brevo_message_id IS NULL
                      AND al.payload->>'subject' IS NOT DISTINCT FROM ee.subject_line
                      AND al.ran_at <= ee.event_at
                    )
                  )
              )
            )
            AND (
              ee.event_type IN ('clicked', 'click')
              OR ee.open_source = $2::open_source
            )
            AND NOT EXISTS (
              SELECT 1
              FROM email_events suppressed
              WHERE suppressed.id = ee.id
                AND suppressed.event_type IN ('opened', 'open')
                AND suppressed.open_source <> $2::open_source
            )
        ) raw_events
        WHERE kind IS NOT NULL
        ORDER BY kind, message_key, DATE_TRUNC('second', occurred_at), occurred_at, event_key
      ) canonical_events
    ) eng ON TRUE
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
  `, [clientId, OPEN_SOURCE.HUMAN]);
  return rows;
}

function evaluateWarmTriggers(prospect) {
  const tier = resolveVerticalTier(prospect.vertical, { vertical_tiers: prospect.vertical_tiers });
  if (!tier.warm_eligible) return [];
  const reasons = [];
  if (Number(prospect.opens_24h || 0) >= 3 || Number(prospect.clicks_24h || 0) > 0) reasons.push('ENGAGEMENT_CLUSTER');
  const touchedAt = prospect.email_touched_at ? new Date(prospect.email_touched_at).getTime() : 0;
  if (Number(prospect.icp_score || 0) >= 80 && touchedAt && touchedAt >= Date.now() - 14 * 24 * 60 * 60 * 1000) {
    reasons.push('ICP_CROSS_80_RECENT');
  }
  return reasons;
}

function classifyIcpScoreChange(row) {
  if (row.old_score === null || row.old_score === undefined) return null;
  const oldScore = Number(row.old_score);
  const newScore = Number(row.new_score);
  const delta = newScore - oldScore;
  const crossed80 = oldScore < 80 && newScore >= 80;
  const crossed90 = oldScore >= 80 && oldScore < 90 && newScore >= 90;
  const jumped15 = delta >= 15;
  if (!crossed80 && !crossed90 && !jumped15) return null;

  let reason = 'ICP_JUMP_15';
  let label = `ICP jumped +${delta}, now ${newScore}`;
  if (crossed80) {
    reason = 'ICP_CROSS_80_RECENT';
    label = `ICP crossed 80, now ${newScore}`;
  } else if (crossed90) {
    reason = 'ICP_CROSS_90';
    label = `ICP crossed 90, now ${newScore}`;
  }

  return {
    signal_type: SIGNAL_TYPES.ICP_SCORE,
    reason,
    event_key: `icp_history:${row.id}`,
    observed_at: row.created_at,
    trigger_label: label,
    evidence: {
      history_id: Number(row.id),
      old_score: oldScore,
      new_score: newScore,
      delta,
      crossed_80: crossed80,
      crossed_90: crossed90,
      jumped_15: jumped15,
      source_reason: row.reason || null,
    },
  };
}

function groupSignalEventsByProspect(events) {
  const grouped = new Map();
  for (const event of events) {
    const key = String(event.prospect_id);
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(event);
  }
  for (const rows of grouped.values()) {
    rows.sort((a, b) => new Date(a.observed_at || 0) - new Date(b.observed_at || 0));
  }
  return grouped;
}

async function getWarmSignalStates(clientId) {
  const { rows } = await pool.query(`
    SELECT *
    FROM warm_signal_state
    WHERE client_id = $1
  `, [clientId]);
  return new Map(rows.map(row => [`${row.prospect_id}:${row.signal_type}`, row]));
}

async function upsertWarmSignalState(update) {
  await pool.query(`
    INSERT INTO warm_signal_state (
      client_id, prospect_id, signal_type, is_active, last_observed_value,
      last_source_event_key, last_fired_value, last_fired_at, updated_at
    )
    VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb, $8, NOW())
    ON CONFLICT (client_id, prospect_id, signal_type) DO UPDATE SET
      is_active = EXCLUDED.is_active,
      last_observed_value = EXCLUDED.last_observed_value,
      last_source_event_key = COALESCE(EXCLUDED.last_source_event_key, warm_signal_state.last_source_event_key),
      last_fired_value = COALESCE(EXCLUDED.last_fired_value, warm_signal_state.last_fired_value),
      last_fired_at = COALESCE(EXCLUDED.last_fired_at, warm_signal_state.last_fired_at),
      updated_at = NOW()
  `, [
    update.client_id,
    update.prospect_id,
    update.signal_type,
    Boolean(update.is_active),
    JSON.stringify(update.last_observed_value || {}),
    update.last_source_event_key || null,
    update.last_fired_value ? JSON.stringify(update.last_fired_value) : null,
    update.last_fired_at || null,
  ]);
}

function buildCurrentEdgeEvents(prospect, states) {
  const events = [];
  const stateUpdates = [];
  const tier = resolveVerticalTier(prospect.vertical, { vertical_tiers: prospect.vertical_tiers });
  if (!tier.warm_eligible) {
    for (const signalType of [SIGNAL_TYPES.ICP_ELIGIBILITY, SIGNAL_TYPES.ENGAGEMENT_CLUSTER]) {
      stateUpdates.push({
        client_id: prospect.client_id,
        prospect_id: prospect.id,
        signal_type: signalType,
        is_active: false,
        last_observed_value: { tier: tier.tier, vertical: tier.vertical, tier_blocked: true },
        last_source_event_key: null,
      });
    }
    return { events, stateUpdates };
  }
  const touchedAt = prospect.email_touched_at ? new Date(prospect.email_touched_at) : null;
  const eligibilityActive = Number(prospect.icp_score || 0) >= 80
    && touchedAt
    && touchedAt.getTime() >= Date.now() - 14 * 24 * 60 * 60 * 1000;
  const eligibilityState = states.get(`${prospect.id}:${SIGNAL_TYPES.ICP_ELIGIBILITY}`);
  if (eligibilityActive && !eligibilityState?.is_active) {
    events.push({
      prospect_id: prospect.id,
      signal_type: SIGNAL_TYPES.ICP_ELIGIBILITY,
      reason: 'ICP_CROSS_80_RECENT',
      event_key: `icp_eligibility:${prospect.id}:${touchedAt.toISOString()}`,
      observed_at: touchedAt,
      trigger_label: `ICP crossed 80, now ${Number(prospect.icp_score || 0)}`,
      evidence: { icp_score: Number(prospect.icp_score || 0), email_touched_at: touchedAt.toISOString() },
    });
  }
  stateUpdates.push({
    client_id: prospect.client_id,
    prospect_id: prospect.id,
    signal_type: SIGNAL_TYPES.ICP_ELIGIBILITY,
    is_active: Boolean(eligibilityActive),
    last_observed_value: {
      icp_score: Number(prospect.icp_score || 0),
      email_touched_at: touchedAt?.toISOString() || null,
    },
    last_source_event_key: touchedAt ? `email_touch:${touchedAt.toISOString()}` : null,
  });

  const engagementActive = Number(prospect.opens_24h || 0) >= 3 || Number(prospect.clicks_24h || 0) > 0;
  const engagementState = states.get(`${prospect.id}:${SIGNAL_TYPES.ENGAGEMENT_CLUSTER}`);
  if (engagementActive && !engagementState?.is_active && prospect.engagement_event_key) {
    const trigger = Number(prospect.clicks_24h || 0) > 0
      ? `${Number(prospect.clicks_24h)} click${Number(prospect.clicks_24h) === 1 ? '' : 's'} in 24h`
      : `${Number(prospect.opens_24h)} opens in 24h`;
    events.push({
      prospect_id: prospect.id,
      signal_type: SIGNAL_TYPES.ENGAGEMENT_CLUSTER,
      reason: 'ENGAGEMENT_CLUSTER',
      event_key: `engagement_edge:${prospect.id}:${prospect.engagement_event_key}`,
      observed_at: new Date(),
      trigger_label: trigger,
      evidence: {
        opens_24h: Number(prospect.opens_24h || 0),
        clicks_24h: Number(prospect.clicks_24h || 0),
        source_event_key: prospect.engagement_event_key,
      },
    });
  }
  stateUpdates.push({
    client_id: prospect.client_id,
    prospect_id: prospect.id,
    signal_type: SIGNAL_TYPES.ENGAGEMENT_CLUSTER,
    is_active: engagementActive,
    last_observed_value: {
      opens_24h: Number(prospect.opens_24h || 0),
      clicks_24h: Number(prospect.clicks_24h || 0),
    },
    last_source_event_key: prospect.engagement_event_key || null,
  });

  return { events, stateUpdates };
}

async function getPendingIcpScoreChanges(clientId, prospectIds) {
  if (!prospectIds.length) return [];
  const { rows } = await pool.query(`
    SELECT h.*
    FROM icp_score_history h
    JOIN prospects p ON p.id = h.prospect_id AND p.client_id = $1
    LEFT JOIN warm_signal_state state
      ON state.client_id = p.client_id
     AND state.prospect_id = p.id
     AND state.signal_type = $2
    WHERE h.prospect_id = ANY($3::uuid[])
      AND h.id > COALESCE(NULLIF(state.last_source_event_key, '')::bigint, 0)
    ORDER BY h.prospect_id, h.id
  `, [clientId, SIGNAL_TYPES.ICP_SCORE, prospectIds]);
  return rows;
}

async function getPendingReplyEvents(clientId, prospectIds) {
  if (!prospectIds.length) return [];
  const { rows } = await pool.query(`
    WITH reply_sources AS (
      SELECT ee.prospect_id,
        'email_event:' || ee.id::text AS event_key,
        ee.event_at AS observed_at,
        jsonb_build_object('source', 'email_events', 'source_id', ee.id, 'event_type', ee.event_type) AS evidence
      FROM email_events ee
      WHERE ee.client_id = $1
        AND ee.prospect_id = ANY($2::uuid[])
        AND ee.event_type IN ('replied', 'reply')

      UNION ALL

      SELECT t.prospect_id,
        'touchpoint:' || t.id::text AS event_key,
        t.created_at AS observed_at,
        jsonb_build_object('source', 'touchpoints', 'source_id', t.id, 'action_type', t.action_type) AS evidence
      FROM touchpoints t
      WHERE t.client_id = $1
        AND t.prospect_id = ANY($2::uuid[])
        AND t.action_type IN ('inbound', 'reply', 'email_reply', 'inbound_reply')
        AND NOT EXISTS (
          SELECT 1 FROM email_events ee
          WHERE ee.client_id = t.client_id
            AND ee.prospect_id = t.prospect_id
            AND ee.event_type IN ('replied', 'reply')
            AND ABS(EXTRACT(EPOCH FROM (ee.event_at - t.created_at))) <= 10
        )
    )
    SELECT source.*
    FROM reply_sources source
    LEFT JOIN warm_signal_state state
      ON state.client_id = $1
     AND state.prospect_id = source.prospect_id
     AND state.signal_type = $3
    WHERE source.observed_at >= COALESCE(
      NULLIF(state.last_observed_value->>'cursor_at', '')::timestamptz,
      'epoch'::timestamptz
    )
    ORDER BY source.prospect_id, source.observed_at, source.event_key
  `, [clientId, prospectIds, SIGNAL_TYPES.REPLY]);
  return rows;
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

async function recordFire(prospect, event, pingResult, evidenceEvents = [event]) {
  const reason = event.reason;
  const payload = {
    prospect_id: prospect.id,
    company: companyLabel(prospect),
    contact: prospect.email || prospect.phone || null,
    trigger_reason: reason,
    trigger_label: event.trigger_label || triggerLabel(reason, prospect),
    icp_score: Number(prospect.icp_score || 0),
    icp_delta_7d: Number(prospect.icp_delta_7d || 0),
    last_touch: lastTouchSummary(prospect),
    vertical: prospect.vertical || null,
    city: prospect.service_area_match || prospect.location || prospect.company_location || null,
    opens_24h: Number(prospect.opens_24h || event.evidence?.opens_24h || 0),
    clicks_24h: Number(prospect.clicks_24h || event.evidence?.clicks_24h || 0),
    evidence: evidenceEvents.map(item => ({
      event_key: item.event_key,
      signal_type: item.signal_type,
      trigger_reason: item.reason,
      trigger_label: item.trigger_label,
      observed_at: item.observed_at,
      details: item.evidence || {},
    })),
  };

  const { rows } = await pool.query(`
    INSERT INTO warm_trigger_fires (
      prospect_id, client_id, trigger_reason, trigger_payload, ping_sent,
      telegram_chat_id, telegram_message_id, digest_date, event_key
    )
    VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, CURRENT_DATE, $8)
    RETURNING id, fired_at
  `, [
    prospect.id,
    prospect.client_id,
    reason,
    JSON.stringify(payload),
    Boolean(pingResult),
    pingResult?.chatId || null,
    pingResult?.messageId || null,
    event.event_key,
  ]);
  const fire = rows[0];
  await insertMiraWarmCaptureLog(fire.id, prospect, reason);
  const miraCaptureId = await writeFireCapture(fire.id, prospect, reason, fire.fired_at);
  await logAgent('warm_trigger_fire', { fire_id: fire.id, ...payload, ping_sent: Boolean(pingResult) }, 'success', null, prospect.client_id, prospect.id);
  return { ...fire, mira_capture_id: miraCaptureId };
}

async function claimSignalEvent(prospect, event) {
  const { rows } = await pool.query(`
    INSERT INTO warm_signal_events (
      client_id, prospect_id, signal_type, event_key, observed_at, evidence, status
    )
    VALUES ($1, $2, $3, $4, $5, $6::jsonb, 'pending')
    ON CONFLICT (client_id, event_key) DO NOTHING
    RETURNING *
  `, [
    prospect.client_id,
    prospect.id,
    event.signal_type,
    event.event_key,
    event.observed_at || new Date(),
    JSON.stringify({
      trigger_reason: event.reason,
      trigger_label: event.trigger_label,
      ...(event.evidence || {}),
    }),
  ]);
  if (rows[0]) return { claimed: true, row: rows[0] };

  const existing = await pool.query(`
    SELECT * FROM warm_signal_events
    WHERE client_id = $1 AND event_key = $2
  `, [prospect.client_id, event.event_key]);
  const row = existing.rows[0];
  return { claimed: Boolean(row && row.status !== 'consumed'), row, duplicate: row?.status === 'consumed' };
}

async function getOpenWarmIncident(prospect) {
  const { rows } = await pool.query(`
    SELECT *
    FROM warm_trigger_fires
    WHERE client_id = $1
      AND prospect_id = $2
      AND resolved_action IS NULL
    ORDER BY fired_at DESC
    LIMIT 1
  `, [prospect.client_id, prospect.id]);
  return rows[0] || null;
}

async function appendIncidentEvidence(fireId, events) {
  const evidence = events.map(event => ({
    event_key: event.event_key,
    signal_type: event.signal_type,
    trigger_reason: event.reason,
    trigger_label: event.trigger_label,
    observed_at: event.observed_at,
    details: event.evidence || {},
  }));
  await pool.query(`
    UPDATE warm_trigger_fires
    SET trigger_payload = jsonb_set(
      COALESCE(trigger_payload, '{}'::jsonb),
      '{evidence}',
      COALESCE(trigger_payload->'evidence', '[]'::jsonb) || $2::jsonb,
      true
    )
    WHERE id = $1
  `, [fireId, JSON.stringify(evidence)]);
}

async function markSignalEventsConsumed(clientId, events, fireId) {
  await pool.query(`
    UPDATE warm_signal_events
    SET status = 'consumed', routed_fire_id = $3, consumed_at = NOW()
    WHERE client_id = $1
      AND event_key = ANY($2::text[])
  `, [clientId, events.map(event => event.event_key), fireId]);
}

async function processProspectEvents(prospect, events) {
  const claimedEvents = [];
  for (const event of events) {
    const claim = await claimSignalEvent(prospect, event);
    if (claim.claimed) claimedEvents.push(event);
  }
  if (!claimedEvents.length) return { fired: false, reason: 'events_already_consumed', consumed: 0 };

  const openIncident = await getOpenWarmIncident(prospect);
  if (openIncident) {
    await appendIncidentEvidence(openIncident.id, claimedEvents);
    await markSignalEventsConsumed(prospect.client_id, claimedEvents, openIncident.id);
    return { fired: false, reason: 'evidence_appended', fire_id: openIncident.id, consumed: claimedEvents.length };
  }

  const primary = claimedEvents[0];
  const displayProspect = {
    ...prospect,
    trigger_label: primary.trigger_label,
    icp_delta_7d: primary.evidence?.delta || 0,
    opens_24h: primary.evidence?.opens_24h ?? prospect.opens_24h,
    clicks_24h: primary.evidence?.clicks_24h ?? prospect.clicks_24h,
  };
  let pingResult = null;
  if ((await pingsSentToday(prospect.client_id)) < DAILY_PING_CAP) {
    pingResult = await sendWarmTelegramMessage(displayProspect, primary.reason);
  }

  const fire = await recordFire(displayProspect, primary, pingResult, claimedEvents);
  await markSignalEventsConsumed(prospect.client_id, claimedEvents, fire.id);
  return {
    fired: true,
    fire_id: fire.id,
    ping_sent: Boolean(pingResult),
    consumed: claimedEvents.length,
  };
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
  if (!isWarmRoutingEnabled()) {
    return { disabled: true, reason: 'WARM_ROUTING_ENABLED_not_true', fires: 0 };
  }
  if (!(await isWarmRoutingSeeded(clientId))) {
    return { disabled: true, reason: 'warm_routing_seed_incomplete', fires: 0 };
  }
  let result;
  try {
    result = await withWorkerLock(async () => {
    const prospects = (await getWarmProspects(clientId)).filter(prospect => {
      const tier = resolveVerticalTier(prospect.vertical, { vertical_tiers: prospect.vertical_tiers });
      if (tier.warm_eligible) return true;
      console.log(`[warm_routing] Tier-blocked prospect ${prospect.id}: ${tier.vertical || '(blank)'} -> ${tier.tier}`);
      return false;
    });
    const prospectMap = new Map(prospects.map(prospect => [String(prospect.id), prospect]));
    const states = await getWarmSignalStates(clientId);
    const events = [];
    const stateUpdates = new Map();
    const queueStateUpdate = update => {
      stateUpdates.set(`${update.prospect_id}:${update.signal_type}`, update);
    };

    for (const prospect of prospects) {
      const current = buildCurrentEdgeEvents(prospect, states);
      events.push(...current.events);
      current.stateUpdates.forEach(queueStateUpdate);
    }

    const icpRows = await getPendingIcpScoreChanges(clientId, prospects.map(row => row.id));
    const latestIcpByProspect = new Map();
    for (const row of icpRows) {
      const prospect = prospectMap.get(String(row.prospect_id));
      if (!prospect) continue;
      const event = classifyIcpScoreChange(row);
      if (event) events.push({ ...event, prospect_id: row.prospect_id });
      const prior = latestIcpByProspect.get(String(row.prospect_id));
      latestIcpByProspect.set(String(row.prospect_id), {
        ...row,
        observed_high_water: Math.max(
          Number(prior?.observed_high_water || 0),
          Number(row.old_score || 0),
          Number(row.new_score || 0)
        ),
      });
    }
    for (const [prospectId, row] of latestIcpByProspect) {
      const existing = states.get(`${prospectId}:${SIGNAL_TYPES.ICP_SCORE}`);
      const previousHigh = Number(existing?.last_observed_value?.high_water_score || 0);
      queueStateUpdate({
        client_id: clientId,
        prospect_id: prospectId,
        signal_type: SIGNAL_TYPES.ICP_SCORE,
        is_active: Number(row.new_score || 0) >= 80,
        last_observed_value: {
          old_score: row.old_score,
          current_score: Number(row.new_score || 0),
          high_water_score: Math.max(previousHigh, Number(row.observed_high_water || 0)),
        },
        last_source_event_key: String(row.id),
      });
    }

    const replyRows = await getPendingReplyEvents(clientId, prospects.map(row => row.id));
    const latestReplyByProspect = new Map();
    for (const row of replyRows) {
      events.push({
        prospect_id: row.prospect_id,
        signal_type: SIGNAL_TYPES.REPLY,
        reason: 'REPLY_RECEIVED',
        event_key: row.event_key,
        observed_at: row.observed_at,
        trigger_label: 'Inbound reply received',
        evidence: row.evidence || {},
      });
      latestReplyByProspect.set(String(row.prospect_id), row);
    }
    for (const [prospectId, row] of latestReplyByProspect) {
      queueStateUpdate({
        client_id: clientId,
        prospect_id: prospectId,
        signal_type: SIGNAL_TYPES.REPLY,
        is_active: false,
        last_observed_value: { cursor_at: row.observed_at },
        last_source_event_key: row.event_key,
      });
    }

    const groupedEvents = groupSignalEventsByProspect(events);
    let fires = 0;
    let skipped = 0;
    const results = [];

    for (const prospect of prospects) {
      const prospectEvents = groupedEvents.get(String(prospect.id)) || [];
      try {
        let routeResult = { fired: false, reason: 'no_new_edge', consumed: 0 };
        if (prospectEvents.length) {
          routeResult = await processProspectEvents(prospect, prospectEvents);
          if (routeResult.fired) fires++;
          else skipped++;
        }
        for (const update of stateUpdates.values()) {
          if (String(update.prospect_id) !== String(prospect.id)) continue;
          const firedEvent = prospectEvents.find(event => event.signal_type === update.signal_type);
          await upsertWarmSignalState({
            ...update,
            ...(firedEvent ? {
              last_fired_value: firedEvent.evidence || {},
              last_fired_at: new Date(),
            } : {}),
          });
        }
        if (prospectEvents.length) {
          results.push({
            prospect_id: prospect.id,
            event_keys: prospectEvents.map(event => event.event_key),
            ...routeResult,
          });
        }
      } catch (err) {
        skipped++;
        results.push({ prospect_id: prospect.id, fired: false, error: err.message });
        await logAgent(
          'warm_trigger_failed',
          { prospect_id: prospect.id, event_keys: prospectEvents.map(event => event.event_key) },
          'failed',
          err.message,
          clientId,
          prospect.id
        );
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
  if (!isWarmRoutingEnabled()) {
    console.log('[warm_routing] scheduler disabled; set WARM_ROUTING_ENABLED=true only after migration + seed verification');
    return null;
  }
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
  evaluateWarmTriggers,
  triggerLabel,
  classifyIcpScoreChange,
  buildCurrentEdgeEvents,
  groupSignalEventsByProspect,
  processProspectEvents,
  isWarmRoutingEnabled,
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
