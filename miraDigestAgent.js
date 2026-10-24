require('dotenv').config();

const pool = require('./db');
const { ensureMiraSchema } = require('./utils/miraSchema');
const { sendMiraTelegramMessage, truncate } = require('./utils/miraCorrections');
const { buildAnchorAppendix } = require('./utils/miraAnchor');

const AGENT_NAME = 'mira_digest';
const DIGEST_TZ = process.env.MIRA_TIMEZONE || 'America/New_York';
const DIGEST_HOUR_ET = 7;
const DIGEST_WINDOW_MINUTES = 15;
const DIGEST_SCHEDULER_INTERVAL_MS = 30_000;
const DIGEST_ADVISORY_LOCK_KEY = 91720260700;

let schedulerHandle = null;
let scheduledRunInFlight = false;

function formatDateForDigest(date = new Date()) {
  return new Intl.DateTimeFormat('en-US', {
    timeZone: DIGEST_TZ,
    weekday: 'long',
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  }).format(date);
}

function getCaptureContent(row = {}) {
  return row.transcript || row.raw_text || row.link_url || row.photo_url || '';
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

function getSuggestedRouting(row = {}) {
  const metadata = parseMetadata(row.raw_metadata);
  return metadata.suggested_routing
    || metadata.suggestedRouting
    || metadata.classification?.suggested_routing
    || metadata.classifier?.suggested_routing
    || {};
}

function byRoute(rows, classification, routedToTable) {
  return rows.filter(row => row.classification === classification && row.routed_to_table === routedToTable);
}

function failedReason(row = {}) {
  if (row.classifier_notes) return row.classifier_notes;
  const metadata = parseMetadata(row.raw_metadata);
  return metadata.error || metadata.reason || 'No reason recorded';
}

function appendPreviewLines(lines, rows, formatter, max = 8) {
  for (const row of rows.slice(0, max)) {
    lines.push(`  • ${formatter(row)}`);
  }
  if (rows.length > max) {
    lines.push(`  • …and ${rows.length - max} more`);
  }
}

function buildInlineKeyboard(reviewRows) {
  if (!reviewRows.length) return undefined;
  return {
    inline_keyboard: reviewRows.slice(0, 8).map(row => ([{
      text: `✏️ Fix #${row.id}`,
      callback_data: `mira_fix:${row.id}`,
    }])),
  };
}

function buildDigestMessage(rows) {
  const tasks = byRoute(rows, 'task', 'todoist');
  const clientNotes = byRoute(rows, 'client_note', 'client_notes');
  const blockers = byRoute(rows, 'blocker', 'blockers');
  const ideas = byRoute(rows, 'idea', 'ideas');
  const contentSeeds = byRoute(rows, 'content_seed', 'content_seeds');
  const references = byRoute(rows, 'reference', 'refs');
  const reminders = byRoute(rows, 'reminder', 'reminders');
  const review = rows.filter(row => row.status === 'review_needed' || row.classification === 'decision_needed');
  const failed = rows.filter(row => row.status === 'failed');

  const lines = [
    '🪞 Mira morning digest',
    formatDateForDigest(),
    '',
    `📥 Captured in the last 24h: ${rows.length}`,
    '',
    `✅ Tasks routed to Todoist: ${tasks.length}`,
  ];

  appendPreviewLines(lines, tasks, row => {
    const project = getSuggestedRouting(row).todoist_project || 'Inbox';
    return `${truncate(getCaptureContent(row), 90)} → ${project}`;
  });

  lines.push('', `💬 Client notes filed: ${clientNotes.length}`);
  appendPreviewLines(lines, clientNotes, row => {
    const clientName = row.client_name || row.business_name || row.client_id || 'Unknown client';
    return `${clientName}: ${truncate(getCaptureContent(row), 90)}`;
  });

  lines.push('', `🚧 Blockers logged: ${blockers.length}`);
  appendPreviewLines(lines, blockers, row => {
    const blocking = getSuggestedRouting(row).blocking || row.blocking || 'unspecified';
    return `${truncate(getCaptureContent(row), 90)} (blocking ${blocking})`;
  });

  lines.push('', `💡 Ideas captured: ${ideas.length}`);
  appendPreviewLines(lines, ideas, row => truncate(getCaptureContent(row), 100));

  lines.push('', `✍️ Content seeds: ${contentSeeds.length}`);
  appendPreviewLines(lines, contentSeeds, row => truncate(getCaptureContent(row), 100));

  lines.push('', `📚 References saved: ${references.length}`);
  lines.push(`⏰ Reminders set: ${reminders.length}`);

  lines.push('', `❓ Needs your review: ${review.length}`);
  appendPreviewLines(lines, review, row => {
    const reasoning = row.classifier_notes ? ` ${truncate(row.classifier_notes, 80)}` : '';
    return `${truncate(getCaptureContent(row), 80)}${reasoning} [✏️ Fix #${row.id}]`;
  });

  lines.push('', `❌ Failed: ${failed.length}`);
  appendPreviewLines(lines, failed, row => `${truncate(getCaptureContent(row), 80)} ${truncate(failedReason(row), 80)}`);

  return lines.join('\n');
}

async function fetchDigestRows() {
  const result = await pool.query(`
    SELECT
      ci.*,
      c.name AS client_name,
      c.business_name,
      b.blocking,
      r.remind_at
    FROM capture_inbox ci
    LEFT JOIN clients c ON c.id = ci.client_id
    LEFT JOIN prospects linked_prospect
      ON ci.linked_entity_type = 'prospect'
     AND linked_prospect.id::text = ci.linked_entity_id
    LEFT JOIN clients linked_client ON linked_client.id = linked_prospect.client_id
    LEFT JOIN blockers b ON b.capture_id = ci.id
    LEFT JOIN reminders r ON r.capture_id = ci.id
    WHERE ci.received_at >= NOW() - INTERVAL '24 hours'
      AND ci.received_at <= NOW()
      AND COALESCE(ci.archived, false) = false
      AND (ci.client_id IS NULL OR c.active = true)
      AND (
        linked_prospect.id IS NULL
        OR (linked_client.active = true AND COALESCE(linked_prospect.mira_archived, false) = false)
      )
    ORDER BY ci.received_at ASC
  `);
  return result.rows;
}

async function logDigest(status, payload = {}, errorMsg = null) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
    VALUES ($1, $2, $3, $4, $5, NOW(), $6)
  `, [
    AGENT_NAME,
    'daily_digest',
    JSON.stringify(payload),
    status,
    errorMsg ? truncate(errorMsg, 500) : null,
    null,
  ]);
}

function digestLocalParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: DIGEST_TZ,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false,
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map(part => [part.type, part.value]));
  return {
    date: `${values.year}-${values.month}-${values.day}`,
    hour: Number(values.hour) % 24,
    minute: Number(values.minute),
  };
}

function isDigestWindow(date = new Date()) {
  const local = digestLocalParts(date);
  return local.hour === DIGEST_HOUR_ET && local.minute < DIGEST_WINDOW_MINUTES;
}

async function digestAlreadySentToday(date = new Date()) {
  const local = digestLocalParts(date);
  const { rows } = await pool.query(`
    SELECT EXISTS (
      SELECT 1
      FROM agent_log
      WHERE agent_name = $1
        AND action = 'daily_digest'
        AND status = 'success'
        AND (ran_at AT TIME ZONE $2)::date = $3::date
    ) AS sent
  `, [AGENT_NAME, DIGEST_TZ, local.date]);
  return rows[0]?.sent === true;
}

async function withDigestLock(fn) {
  const client = await pool.connect();
  try {
    const lock = await client.query('SELECT pg_try_advisory_lock($1) AS locked', [DIGEST_ADVISORY_LOCK_KEY]);
    if (!lock.rows[0]?.locked) return { skipped: true, reason: 'digest_already_running' };
    return await fn();
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [DIGEST_ADVISORY_LOCK_KEY]).catch(err => {
      console.error('[mira_digest] advisory unlock failed:', err.message);
    });
    client.release();
  }
}

async function run(options = {}) {
  await ensureMiraSchema();
  if (options.scheduled && !isDigestWindow()) {
    return { sent: false, skipped: true, reason: 'outside_7am_et_window' };
  }

  return withDigestLock(async () => {
    if (options.scheduled && await digestAlreadySentToday()) {
      return { sent: false, skipped: true, reason: 'already_sent_today' };
    }

    const rows = await fetchDigestRows();
    const digestText = buildDigestMessage(rows);
    const anchorSection = await buildAnchorAppendix();
    const text = anchorSection ? `${digestText}\n${anchorSection}` : digestText;
    const reviewRows = rows.filter(row => row.status === 'review_needed' || row.classification === 'decision_needed');
    const replyMarkup = buildInlineKeyboard(reviewRows);

    await sendMiraTelegramMessage(text, replyMarkup ? { reply_markup: replyMarkup } : {});
    await logDigest('success', { captured: rows.length, review_needed: reviewRows.length, scheduled: Boolean(options.scheduled) });

    return { sent: true, captured: rows.length, review_needed: reviewRows.length };
  });
}

function startMiraDigestScheduler() {
  if (schedulerHandle) return schedulerHandle;

  const tick = () => {
    if (scheduledRunInFlight || !isDigestWindow()) return;
    scheduledRunInFlight = true;
    run({ scheduled: true })
      .catch(err => console.error('[mira_digest] scheduled run failed:', err.message))
      .finally(() => { scheduledRunInFlight = false; });
  };

  schedulerHandle = setInterval(tick, DIGEST_SCHEDULER_INTERVAL_MS);
  schedulerHandle.unref?.();
  tick();
  console.log(`[mira_digest] scheduler started for ${DIGEST_HOUR_ET}:00 ${DIGEST_TZ}`);
  return schedulerHandle;
}

module.exports = {
  run,
  buildDigestMessage,
  fetchDigestRows,
  startMiraDigestScheduler,
  digestLocalParts,
  isDigestWindow,
};

if (require.main === module) {
  run()
    .then(result => {
      console.log(JSON.stringify(result, null, 2));
      return pool.end();
    })
    .catch(async err => {
      console.error('[mira_digest] fatal:', err.message);
      await logDigest('failed', { error: err.message }, err.message).catch(() => {});
      await pool.end().catch(() => {});
      process.exit(1);
    });
}
