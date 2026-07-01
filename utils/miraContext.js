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
const { isActiveTodoistContextItem } = require('./miraWorld');

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

module.exports = { getTodoistSnapshot, getCurrentAnchor };
