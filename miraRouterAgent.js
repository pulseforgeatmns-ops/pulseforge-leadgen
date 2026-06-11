require('dotenv').config();

const pool = require('./db');

const AGENT_NAME = 'mira_router';
const DEFAULT_LIMIT = 10;
const WORKER_INTERVAL_MS = 15 * 60 * 1000;
const ADVISORY_LOCK_KEY = 91720260604;
const TODOIST_API_BASE = 'https://api.todoist.com/api/v1';

let intervalHandle = null;
let intervalRunning = false;
let todoistProjectMap = null;

function truncateError(value, max = 500) {
  const text = value === undefined || value === null ? '' : String(value);
  return text.length > max ? text.slice(0, max) : text;
}

function normalizeName(value) {
  return String(value || '').trim().toLowerCase();
}

function parseMetadata(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return {};
  }
}

function getSuggestedRouting(metadata) {
  return metadata.suggested_routing
    || metadata.suggestedRouting
    || metadata.classification?.suggested_routing
    || metadata.classifier?.suggested_routing
    || {};
}

function getSuggestedRoutingForRow(row) {
  const metadata = parseMetadata(row.raw_metadata);
  const routing = getSuggestedRouting(metadata);
  if (Object.keys(routing).length) return routing;
  return getSuggestedRouting(parseMetadata(row.classifier_notes));
}

function getCaptureContent(row) {
  const metadata = parseMetadata(row.raw_metadata);
  const candidates = [
    row.transcript,
    row.raw_text,
    metadata.content,
    metadata.text,
    row.link_url,
    row.photo_url,
  ];

  return candidates.find(value => typeof value === 'string' && value.trim())?.trim() || '';
}

async function logRouterEvent(row, action, status, payload = {}, errorMsg = null, durationMs = null) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, error_msg, duration_ms, ran_at, client_id)
    VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)
  `, [
    AGENT_NAME,
    action,
    JSON.stringify({ capture_id: row?.id || null, classification: row?.classification || null, ...payload }),
    status,
    errorMsg ? truncateError(errorMsg) : null,
    durationMs,
    row?.client_id || null,
  ]);
}

async function getTodoistProjectMap() {
  if (todoistProjectMap) return todoistProjectMap;
  const token = process.env.TODOIST_API_TOKEN || process.env.TODOIST_TOKEN;
  if (!token) {
    throw new Error('TODOIST_API_TOKEN not set');
  }

  const map = new Map();
  let cursor = null;

  do {
    const url = new URL(`${TODOIST_API_BASE}/projects`);
    url.searchParams.set('limit', '200');
    if (cursor) url.searchParams.set('cursor', cursor);

    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    const bodyText = await response.text();
    let body;
    try {
      body = bodyText ? JSON.parse(bodyText) : {};
    } catch (_) {
      body = { raw: bodyText };
    }

    if (!response.ok) {
      const message = body?.error || body?.raw || `Todoist projects failed with HTTP ${response.status}`;
      throw new Error(message);
    }

    for (const project of body.results || []) {
      if (project?.name && project?.id) {
        map.set(normalizeName(project.name), String(project.id));
      }
    }
    cursor = body.next_cursor || null;
  } while (cursor);

  todoistProjectMap = map;
  return todoistProjectMap;
}

async function createTodoistTask(row, content, suggestedRouting) {
  const token = process.env.TODOIST_API_TOKEN || process.env.TODOIST_TOKEN;
  if (!token) {
    throw new Error('TODOIST_API_TOKEN not set');
  }

  const projectMap = await getTodoistProjectMap();
  const requestedProject = suggestedRouting.todoist_project;
  const projectId = projectMap.get(normalizeName(requestedProject));
  const payload = { content };
  if (projectId) payload.project_id = projectId;

  const response = await fetch(`${TODOIST_API_BASE}/tasks`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const bodyText = await response.text();
  let body;
  try {
    body = bodyText ? JSON.parse(bodyText) : {};
  } catch (_) {
    body = { raw: bodyText };
  }

  if (!response.ok) {
    const message = body?.error || body?.raw || `Todoist task create failed with HTTP ${response.status}`;
    throw new Error(message);
  }
  if (!body?.id) {
    throw new Error('Todoist task create response did not include id');
  }

  if (requestedProject && !projectId) {
    console.warn(`[mira_router] capture_id=${row.id} Todoist project "${requestedProject}" not found; routed to Inbox`);
  }

  return String(body.id);
}

async function insertRoute(db, tableName, columns, values) {
  const placeholders = values.map((_, index) => `$${index + 1}`).join(', ');
  const query = `
    INSERT INTO ${tableName} (${columns.join(', ')})
    VALUES (${placeholders})
    RETURNING id
  `;
  const result = await db.query(query, values);
  return result.rows[0]?.id;
}

async function markRouted(db, row, routedToTable, routedToId) {
  await db.query(`
    UPDATE capture_inbox
    SET status = 'routed',
        processed_at = NOW(),
        routed_to_table = $1,
        routed_to_id = $2
    WHERE id = $3
  `, [routedToTable, routedToId, row.id]);
}

async function routeDatabaseCapture(db, row, content, suggestedRouting) {
  const clientId = row.client_id || null;

  if (row.classification === 'client_note') {
    return {
      routedToTable: 'client_notes',
      routedToId: await insertRoute(db, 'client_notes',
        ['client_id', 'capture_id', 'content', 'source'],
        [clientId, row.id, content, 'mira']),
    };
  }

  if (row.classification === 'idea') {
    return {
      routedToTable: 'ideas',
      routedToId: await insertRoute(db, 'ideas',
        ['capture_id', 'content'],
        [row.id, content]),
    };
  }

  if (row.classification === 'content_seed') {
    return {
      routedToTable: 'content_seeds',
      routedToId: await insertRoute(db, 'content_seeds',
        ['capture_id', 'content', 'brand'],
        [row.id, content, suggestedRouting.brand || null]),
    };
  }

  if (row.classification === 'blocker') {
    return {
      routedToTable: 'blockers',
      routedToId: await insertRoute(db, 'blockers',
        ['capture_id', 'client_id', 'content', 'blocking'],
        [row.id, clientId, content, suggestedRouting.blocking || null]),
    };
  }

  if (row.classification === 'reference') {
    return {
      routedToTable: 'refs',
      routedToId: await insertRoute(db, 'refs',
        ['capture_id', 'content'],
        [row.id, content]),
    };
  }

  if (row.classification === 'reminder') {
    return {
      routedToTable: 'reminders',
      routedToId: await insertRoute(db, 'reminders',
        ['capture_id', 'content', 'remind_at'],
        [row.id, content, suggestedRouting.remind_at || null]),
    };
  }

  throw new Error(`Unsupported classification: ${row.classification}`);
}

async function routeCapture(row) {
  const startedAt = Date.now();
  const suggestedRouting = getSuggestedRoutingForRow(row);
  const content = getCaptureContent(row);

  if (!content) {
    throw new Error('Capture content is empty');
  }

  if (row.classification === 'decision_needed') {
    return { id: row.id, status: 'skipped', reason: 'decision_needed' };
  }

  if (row.classification === 'task') {
    const taskId = await createTodoistTask(row, content, suggestedRouting);
    await markRouted(pool, row, 'todoist', taskId);
    await logRouterEvent(row, 'route_capture', 'success', { routed_to_table: 'todoist', routed_to_id: taskId }, null, Date.now() - startedAt)
      .catch(err => console.error('[mira_router] agent_log write failed:', err.message));
    console.log(`[mira_router] capture_id=${row.id} routed to todoist:${taskId}`);
    return { id: row.id, status: 'routed', routed_to_table: 'todoist', routed_to_id: taskId };
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await routeDatabaseCapture(client, row, content, suggestedRouting);
    await markRouted(client, row, result.routedToTable, result.routedToId);
    await client.query('COMMIT');
    await logRouterEvent(row, 'route_capture', 'success', {
      routed_to_table: result.routedToTable,
      routed_to_id: result.routedToId,
    }, null, Date.now() - startedAt)
      .catch(err => console.error('[mira_router] agent_log write failed:', err.message));
    console.log(`[mira_router] capture_id=${row.id} routed to ${result.routedToTable}:${result.routedToId}`);
    return {
      id: row.id,
      status: 'routed',
      routed_to_table: result.routedToTable,
      routed_to_id: result.routedToId,
    };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function getRoutableCaptures(limit = DEFAULT_LIMIT) {
  const { rows } = await pool.query(`
    SELECT id, raw_text, transcript, link_url, photo_url, classification, client_id, raw_metadata, classifier_notes
    FROM capture_inbox
    WHERE status = 'classified'
      AND routed_to_table IS NULL
      AND classification <> 'review_needed'
      AND classification <> 'decision_needed'
    ORDER BY received_at ASC
    LIMIT $1
  `, [limit]);

  return rows;
}

async function withWorkerLock(fn) {
  const lock = await pool.query('SELECT pg_try_advisory_lock($1) AS locked', [ADVISORY_LOCK_KEY]);
  if (!lock.rows[0]?.locked) {
    return { skipped: true, reason: 'worker_already_running' };
  }

  try {
    return await fn();
  } finally {
    await pool.query('SELECT pg_advisory_unlock($1)', [ADVISORY_LOCK_KEY]).catch(err => {
      console.error('[mira_router] advisory unlock failed:', err.message);
    });
  }
}

async function run(params = {}) {
  const limit = Math.max(1, Number(params.limit || DEFAULT_LIMIT));

  return withWorkerLock(async () => {
    const rows = await getRoutableCaptures(limit);
    if (!rows.length) {
      return { scanned: 0, routed: 0, skipped: 0, failed: 0 };
    }

    let routed = 0;
    let skipped = 0;
    let failed = 0;
    const results = [];

    for (const row of rows) {
      try {
        const result = await routeCapture(row);
        results.push(result);
        if (result.status === 'routed') routed++;
        if (result.status === 'skipped') skipped++;
      } catch (err) {
        failed++;
        console.error(`[mira_router] capture_id=${row.id} failed:`, err.message);
        await logRouterEvent(row, 'route_capture', 'failed', {}, err.message).catch(logErr => {
          console.error('[mira_router] agent_log write failed:', logErr.message);
        });
        results.push({ id: row.id, status: 'failed', error: truncateError(err.message) });
      }
    }

    return { scanned: rows.length, routed, skipped, failed, results };
  });
}

function startMiraRouterWorker(options = {}) {
  if (intervalHandle) return intervalHandle;
  const intervalMs = Math.max(60_000, Number(options.intervalMs || process.env.MIRA_ROUTER_INTERVAL_MS || WORKER_INTERVAL_MS));

  intervalHandle = setInterval(() => {
    if (intervalRunning) return;
    intervalRunning = true;
    run()
      .catch(err => console.error('[mira_router] worker error:', err.message))
      .finally(() => {
        intervalRunning = false;
      });
  }, intervalMs);

  intervalHandle.unref?.();
  console.log(`[mira_router] worker started interval=${intervalMs}ms`);
  return intervalHandle;
}

module.exports = {
  run,
  startMiraRouterWorker,
};

if (require.main === module) {
  run()
    .then(result => {
      console.log(JSON.stringify(result, null, 2));
      return pool.end();
    })
    .catch(async err => {
      console.error('[mira_router] fatal:', err.message);
      await pool.end().catch(() => {});
      process.exit(1);
    });
}
