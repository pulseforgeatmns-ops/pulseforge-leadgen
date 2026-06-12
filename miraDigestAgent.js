require('dotenv').config();

const pool = require('./db');
const { ensureMiraSchema } = require('./utils/miraSchema');
const { sendMiraTelegramMessage, truncate } = require('./utils/miraCorrections');
const { buildAnchorAppendix } = require('./utils/miraAnchor');

const AGENT_NAME = 'mira_digest';
const DIGEST_TZ = process.env.MIRA_TIMEZONE || 'America/New_York';

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
    LEFT JOIN blockers b ON b.capture_id = ci.id
    LEFT JOIN reminders r ON r.capture_id = ci.id
    WHERE ci.received_at >= NOW() - INTERVAL '24 hours'
      AND ci.received_at <= NOW()
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

async function run() {
  await ensureMiraSchema();
  const rows = await fetchDigestRows();
  const digestText = buildDigestMessage(rows);
  const anchorSection = await buildAnchorAppendix();
  const text = anchorSection ? `${digestText}\n${anchorSection}` : digestText;
  const reviewRows = rows.filter(row => row.status === 'review_needed' || row.classification === 'decision_needed');
  const replyMarkup = buildInlineKeyboard(reviewRows);

  await sendMiraTelegramMessage(text, replyMarkup ? { reply_markup: replyMarkup } : {});
  await logDigest('success', { captured: rows.length, review_needed: reviewRows.length });

  return { sent: true, captured: rows.length, review_needed: reviewRows.length };
}

module.exports = {
  run,
  buildDigestMessage,
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
