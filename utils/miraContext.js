// utils/miraContext.js
//
// READ-ONLY helpers for the GET /api/mira/context endpoint (routes/api.js).
//
// This endpoint exposes a snapshot of Jacob's current Mira state so an external
// Claude.ai conversation can read live context. Everything in this file is
// strictly read-only: it performs outbound Todoist GET requests and SELECT-only
// DB reads. It never creates/updates Todoist tasks, never mutates any Mira
// table, and never alters schema. Intended for Claude.ai consumption only.

const TODOIST_API_BASE = 'https://api.todoist.com/api/v1';
const pool = require('../db');
const { isActiveTodoistContextItem } = require('./miraWorld');
const { LIVE_WORKSTREAMS } = require('./miraWorld');

function todoistToken() {
  // Matches the env var names used by miraRouterAgent.js.
  return process.env.TODOIST_API_TOKEN || process.env.TODOIST_TOKEN || null;
}

// Thin GET wrapper around the Todoist API. Mirrors the request/parse pattern in
// miraRouterAgent.js so behavior stays consistent across the codebase.
async function todoistGet(path, params = {}) {
  const token = todoistToken();
  if (!token) throw new Error('TODOIST_API_TOKEN not set');

  const url = new URL(`${TODOIST_API_BASE}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) url.searchParams.set(key, value);
  }

  const response = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const bodyText = await response.text();
  let body;
  try {
    body = bodyText ? JSON.parse(bodyText) : {};
  } catch (_) {
    body = { raw: bodyText };
  }
  if (!response.ok) {
    throw new Error(body?.error || body?.raw || `Todoist GET ${path} failed with HTTP ${response.status}`);
  }
  return body;
}

// Returns { configured, open_tasks_count, stale_tasks } where stale_tasks are
// open tasks older than `staleDays`. The GET /tasks endpoint returns only active
// (non-completed) tasks, so the count IS the open-task count. Project names are
// resolved best-effort; if the projects fetch fails we still return tasks with a
// null project rather than failing the whole snapshot.
async function getTodoistSnapshot(staleDays = 5) {
  if (!todoistToken()) {
    return { configured: false, open_tasks_count: 0, stale_tasks: [] };
  }

  // Best-effort project id -> name map for the `project` field on stale tasks.
  const projectMap = new Map();
  try {
    let cursor = null;
    do {
      const body = await todoistGet('/projects', { limit: 200, cursor });
      for (const project of body.results || []) {
        if (project?.id) projectMap.set(String(project.id), project.name || null);
      }
      cursor = body.next_cursor || null;
    } while (cursor);
  } catch (err) {
    console.error('[mira_context] Todoist projects fetch failed:', err.message);
  }

  // Page through all active (open) tasks.
  const tasks = [];
  let cursor = null;
  do {
    const body = await todoistGet('/tasks', { limit: 200, cursor });
    for (const task of body.results || []) tasks.push(task);
    cursor = body.next_cursor || null;
  } while (cursor);

  const now = Date.now();
  const staleMs = staleDays * 86400000;
  const active_tasks = [];
  const stale_tasks = [];
  for (const task of tasks) {
    // v1 unified API uses added_at; REST v2 used created_at — accept either.
    const createdRaw = task.added_at || task.created_at || null;
    const created = createdRaw ? new Date(createdRaw).getTime() : NaN;
    if (!Number.isFinite(created)) continue;
    const ageMs = now - created;
    const project = task.project_id ? (projectMap.get(String(task.project_id)) || null) : null;
    if (!isActiveTodoistContextItem(project, task.content)) continue;
    const row = {
      id: String(task.id),
      content: task.content || '',
      days_open: Math.floor(ageMs / 86400000),
      project,
      due: task?.due?.datetime || task?.due?.date || null,
    };
    active_tasks.push(row);
    if (ageMs > staleMs) stale_tasks.push(row);
  }
  active_tasks.sort((a, b) => a.days_open - b.days_open || a.project.localeCompare(b.project) || a.id.localeCompare(b.id));
  stale_tasks.sort((a, b) => b.days_open - a.days_open);

  return { configured: true, open_tasks_count: active_tasks.length, active_tasks, stale_tasks };
}

// Returns today's daily_anchors row, or null if none is set. The daily_anchors
// table is not created by utils/miraSchema.js, so we fail soft: if the table is
// absent we return null instead of throwing. We discover a date-ish column at
// query time so this works regardless of whether the table scopes "today" via
// anchor_date / date / for_date / day / created_at.
async function getCurrentAnchor(pool) {
  const reg = await pool.query("SELECT to_regclass('public.daily_anchors') AS tbl");
  if (!reg.rows[0]?.tbl) return null;

  const cols = await pool.query(`
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'daily_anchors'
  `);
  const names = cols.rows.map(r => r.column_name);
  const dateCol = ['anchor_date', 'date', 'for_date', 'day', 'created_at'].find(c => names.includes(c));

  if (!dateCol) {
    const res = await pool.query('SELECT * FROM daily_anchors ORDER BY id DESC LIMIT 1');
    return res.rows[0] || null;
  }
  const res = await pool.query(
    `SELECT * FROM daily_anchors
     WHERE ${dateCol}::date = (NOW() AT TIME ZONE 'America/New_York')::date
     ORDER BY id DESC LIMIT 1`
  );
  return res.rows[0] || null;
}

function number(value) {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function shortText(value, maxLength = 120) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  return text ? text.slice(0, maxLength) : null;
}

function contentSafeAnchor(anchor, clientId) {
  if (!anchor) return null;
  const primary = shortText(anchor.primary_anchor || anchor.anchor || anchor.content);
  if (!primary) return null;
  const foreignContext = LIVE_WORKSTREAMS
    .filter(workstream => workstream.client_id != null && Number(workstream.client_id) !== Number(clientId))
    .flatMap(workstream => [workstream.name, workstream.key, workstream.market])
    .filter(Boolean);
  if (foreignContext.some(term => primary.toLowerCase().includes(String(term).toLowerCase()))) {
    return null;
  }
  return {
    anchor_date: anchor.anchor_date || anchor.date || anchor.for_date || anchor.day || null,
    primary_anchor: primary,
  };
}

function contentSafeHealth(metrics = {}, trend = []) {
  const sends = number(metrics.send_count_24h);
  const bounces = number(metrics.bounce_count_24h);
  const baseline = number(metrics.send_daily_average_previous_7d);
  const bounceRate = sends > 0 ? bounces / sends : 0;
  const sendTrend = baseline > 0
    ? Math.round(((sends - baseline) / baseline) * 100)
    : null;

  return {
    send_volume_status: sends === 0 ? 'dark' : 'active',
    send_volume_trend_pct: sendTrend,
    deliverability_status: bounceRate > 0.04 ? 'alert' : bounceRate > 0.02 ? 'watch' : 'healthy',
    bounce_rate_pct: Number((bounceRate * 100).toFixed(2)),
    daily_send_trend_7d: trend.map(row => ({
      date: row.activity_date,
      sends: number(row.send_count),
    })),
  };
}

function buildActivitySummaries(metrics = {}, client = null) {
  const market = [client?.city, client?.state].filter(Boolean).join(', ');
  const suffix = market ? ` in ${market}` : '';
  const summaries = [];
  const sends = number(metrics.send_count_24h);
  const opens = number(metrics.open_count_24h);
  const replies = number(metrics.reply_count_24h);
  const warmSignals = number(metrics.warm_signal_count_24h);
  if (sends) summaries.push(`${sends} emails sent over the past 24 hours${suffix}`);
  if (opens) summaries.push(`${opens} email opens recorded over the past 24 hours${suffix}`);
  if (replies) summaries.push(`${replies} replies received over the past 24 hours${suffix}`);
  if (warmSignals) summaries.push(`${warmSignals} warm signals fired over the past 24 hours${suffix}`);
  return summaries;
}

async function buildContentSafeClientContext(clientId, { query, channel, errors }) {
  const safeRows = async (sql, params = []) => {
    try {
      const result = await query(sql, params);
      return result.rows || [];
    } catch (err) {
      errors.push(err.message);
      console.error('[mira_context] content-safe query failed:', err.message);
      return [];
    }
  };

  const [clientRows, metricRows, trendRows, anchor] = await Promise.all([
    safeRows(`
      SELECT id, COALESCE(NULLIF(business_name, ''), name) AS name, city, state
      FROM clients
      WHERE id = $1 AND active = true
    `, [clientId]),
    safeRows(`
      SELECT
        (SELECT COUNT(*)::int
           FROM agent_log
          WHERE client_id = $1
            AND agent_name = 'emmett'
            AND action = 'email_sent'
            AND status IN ('success', 'completed')
            AND ran_at >= NOW() - INTERVAL '24 hours') AS send_count_24h,
        (SELECT COUNT(*)::int
           FROM email_events
          WHERE client_id = $1
            AND event_type IN ('opened', 'opened_proxy')
            AND event_at >= NOW() - INTERVAL '24 hours') AS open_count_24h,
        (SELECT COUNT(*)::int
           FROM email_events
          WHERE client_id = $1
            AND event_type IN ('replied', 'reply')
            AND event_at >= NOW() - INTERVAL '24 hours'
            AND LOWER(COALESCE(raw_payload->>'classification', '')) <> 'out_of_office') AS reply_count_24h,
        (SELECT COUNT(*)::int
           FROM email_events
          WHERE client_id = $1
            AND event_type IN ('bounced', 'hard_bounce', 'soft_bounce')
            AND event_at >= NOW() - INTERVAL '24 hours') AS bounce_count_24h,
        (SELECT COUNT(*)::int
           FROM capture_inbox ci
           JOIN prospects p
             ON p.id::text = ci.linked_entity_id
            AND p.client_id = $1
          WHERE ci.capture_type IN ('warm_signal', 'warm_signal_resolved')
            AND COALESCE(ci.archived, false) = false
            AND COALESCE(p.mira_archived, false) = false
            AND COALESCE(ci.captured_at, ci.received_at) >= NOW() - INTERVAL '24 hours') AS warm_signal_count_24h,
        (SELECT COUNT(*)::numeric / 7
           FROM agent_log
          WHERE client_id = $1
            AND agent_name = 'emmett'
            AND action = 'email_sent'
            AND status IN ('success', 'completed')
            AND ran_at >= NOW() - INTERVAL '8 days'
            AND ran_at < NOW() - INTERVAL '24 hours') AS send_daily_average_previous_7d
    `, [clientId]),
    safeRows(`
      SELECT (ran_at AT TIME ZONE 'America/New_York')::date AS activity_date,
             COUNT(*)::int AS send_count
      FROM agent_log
      WHERE client_id = $1
        AND agent_name = 'emmett'
        AND action = 'email_sent'
        AND status IN ('success', 'completed')
        AND ran_at >= NOW() - INTERVAL '7 days'
      GROUP BY (ran_at AT TIME ZONE 'America/New_York')::date
      ORDER BY activity_date DESC
    `, [clientId]),
    getCurrentAnchor({ query }).catch(err => {
      errors.push(err.message);
      console.error('[mira_context] anchor failed:', err.message);
      return null;
    }),
  ]);

  const client = clientRows[0] || null;
  const metrics = metricRows[0] || {};
  const currentAnchor = contentSafeAnchor(anchor, clientId);

  return {
    now: new Date().toISOString(),
    client: client ? { id: client.id, name: client.name, city: client.city, state: client.state } : null,
    channel: channel || null,
    current_anchor: currentAnchor,
    metrics: {
      sends_24h: number(metrics.send_count_24h),
      opens_24h: number(metrics.open_count_24h),
      replies_24h: number(metrics.reply_count_24h),
      warm_signals_24h: number(metrics.warm_signal_count_24h),
    },
    recent_activity_summaries: buildActivitySummaries(metrics, client),
    client_health: contentSafeHealth(metrics, trendRows),
    available: Boolean(client && metricRows.length && errors.length === 0),
  };
}

async function buildMiraContext(clientId = null, options = {}) {
  const includeCrossClient = options.includeCrossClient ?? clientId == null;
  const query = options.query || pool.query.bind(pool);
  const errors = [];

  // A client-scoped request never receives the cross-client/full payload. Even
  // if a future caller forgets contentSafe:true, fail closed to the scoped shape.
  if (clientId != null && !includeCrossClient) {
    return buildContentSafeClientContext(Number(clientId), {
      query,
      channel: options.channel,
      errors,
    });
  }

  const safeRows = (promise, fallback = []) => promise.then(r => r.rows).catch(err => {
    console.error('[mira_context] query failed:', err.message);
    return fallback;
  });

  const [
    recentCaptures,
    openBlockers,
    activeClients,
    recentClientNotes,
    recentCorrections,
    currentAnchor,
    todoist,
    dailyHealthYesterday,
    dailyHealthToday,
    dailyHealthTrend,
  ] = await Promise.all([
    safeRows(query(`
      SELECT ci.id,
             LEFT(COALESCE(NULLIF(ci.transcript, ''), ci.raw_text, ''), 150) AS content_preview,
             ci.classification, ci.status, ci.received_at,
             ci.capture_type, ci.source, ci.linked_entity_type, ci.linked_entity_id,
             ci.linked_capture_id, ci.captured_at
      FROM capture_inbox ci
      LEFT JOIN clients direct_client ON direct_client.id = ci.client_id
      LEFT JOIN prospects linked_prospect
        ON ci.linked_entity_type = 'prospect'
       AND linked_prospect.id::text = ci.linked_entity_id
      LEFT JOIN clients linked_client ON linked_client.id = linked_prospect.client_id
      WHERE COALESCE(ci.archived, false) = false
        AND (ci.client_id IS NULL OR direct_client.active = true)
        AND (
          linked_prospect.id IS NULL
          OR (linked_client.active = true AND COALESCE(linked_prospect.mira_archived, false) = false)
        )
      ORDER BY ci.received_at DESC
      LIMIT 10
    `)),
    safeRows(query(`
      SELECT id, content, blocking,
             GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (NOW() - created_at)) / 86400))::int AS days_open
      FROM blockers
      WHERE resolved = false
      ORDER BY created_at ASC
    `)),
    safeRows(query(`SELECT id, name FROM clients WHERE active = true ORDER BY name ASC`)),
    safeRows(query(`
      SELECT cn.created_at,
             c.name AS client_name,
             LEFT(COALESCE(cn.content, ''), 150) AS content_preview
      FROM client_notes cn
      JOIN clients c ON c.id = cn.client_id AND c.active = true
      WHERE COALESCE(cn.archived, false) = false
      ORDER BY cn.created_at DESC
      LIMIT 10
    `)),
    safeRows(query(`
      SELECT mc.original_class, mc.corrected_class, mc.created_at
      FROM mira_corrections mc
      JOIN capture_inbox ci ON ci.id = mc.capture_id
      WHERE COALESCE(mc.archived, false) = false
        AND COALESCE(ci.archived, false) = false
        AND mc.original_class <> mc.corrected_class
      ORDER BY mc.created_at DESC
      LIMIT 5
    `)),
    getCurrentAnchor({ query }).catch(err => {
      console.error('[mira_context] anchor failed:', err.message);
      return null;
    }),
    includeCrossClient ? getTodoistSnapshot().catch(err => {
      console.error('[mira_context] todoist failed:', err.message);
      return { configured: false, open_tasks_count: 0, stale_tasks: [], error: err.message };
    }) : Promise.resolve({ configured: false, open_tasks_count: 0, active_tasks: [], stale_tasks: [] }),
    safeRows(query(`
      SELECT * FROM daily_health_log
      WHERE log_date = (NOW() AT TIME ZONE 'America/New_York')::date - 1
    `)),
    safeRows(query(`
      SELECT * FROM daily_health_log
      WHERE log_date = (NOW() AT TIME ZONE 'America/New_York')::date
    `)),
    safeRows(query(`
      SELECT log_date, send_count_today, bounce_count_today, reply_count_today,
             warm_signals_fired_today, health_flags
      FROM daily_health_log
      WHERE log_date >= (NOW() AT TIME ZONE 'America/New_York')::date - 6
        AND log_date <= (NOW() AT TIME ZONE 'America/New_York')::date
      ORDER BY log_date DESC
    `)),
  ]);

  return {
    now: new Date().toISOString(),
    live_workstreams: LIVE_WORKSTREAMS,
    recent_captures: recentCaptures,
    current_anchor: currentAnchor,
    open_tasks_count: todoist.open_tasks_count,
    active_tasks: todoist.active_tasks || [],
    stale_tasks: todoist.stale_tasks,
    open_blockers: openBlockers,
    active_clients: activeClients,
    recent_client_notes: recentClientNotes,
    recent_corrections: recentCorrections,
    daily_health_yesterday: dailyHealthYesterday[0] || null,
    daily_health_today: dailyHealthToday[0] || null,
    daily_health_trend_7d: dailyHealthTrend,
  };
}

module.exports = {
  getTodoistSnapshot,
  getCurrentAnchor,
  buildMiraContext,
  contentSafeHealth,
  buildActivitySummaries,
  contentSafeAnchor,
};
