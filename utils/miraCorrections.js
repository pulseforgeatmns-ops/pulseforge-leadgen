const axios = require('axios');
const pool = require('../db');

const VALID_MIRA_CATEGORIES = [
  'task',
  'client_note',
  'blocker',
  'idea',
  'content_seed',
  'decision_needed',
  'reference',
  'reminder',
];

const ROUTE_TABLES = new Set([
  'client_notes',
  'ideas',
  'content_seeds',
  'blockers',
  'refs',
  'reminders',
]);

function truncate(value, max = 120) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function getCaptureContent(row = {}) {
  return row.transcript || row.raw_text || row.link_url || row.photo_url || '';
}

function normalizeCategory(value) {
  return String(value || '').trim().toLowerCase();
}

async function logCorrectionEvent(captureId, status, payload = {}, errorMsg = null) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
    VALUES ($1, $2, $3, $4, $5, NOW(), $6)
  `, [
    'mira_correction',
    'correct_capture',
    JSON.stringify({ capture_id: captureId, ...payload }),
    status,
    errorMsg ? truncate(errorMsg, 500) : null,
    null,
  ]);
}

async function deleteTodoistTask(taskId) {
  const token = process.env.TODOIST_API_TOKEN || process.env.TODOIST_TOKEN;
  if (!token || !taskId) {
    throw new Error('Todoist token or task id missing');
  }

  const response = await fetch(`https://api.todoist.com/api/v1/tasks/${taskId}`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${token}` },
  });

  if (!response.ok && response.status !== 404) {
    const body = await response.text();
    throw new Error(body || `Todoist delete failed with HTTP ${response.status}`);
  }
}

async function undoRoute(client, row) {
  const routedToTable = row.routed_to_table;
  const routedToId = row.routed_to_id;
  if (!routedToTable || !routedToId) return { undone: false, reason: 'not_routed' };

  if (ROUTE_TABLES.has(routedToTable)) {
    const deleted = await client.query(`DELETE FROM ${routedToTable} WHERE id = $1 AND capture_id = $2`, [routedToId, row.id]);
    if (!deleted.rowCount) {
      return { undone: false, reason: `No ${routedToTable} row found for id ${routedToId}` };
    }
    return { undone: true, destination: `${routedToTable}:${routedToId}` };
  }

  if (routedToTable === 'todoist') {
    await deleteTodoistTask(routedToId);
    return { undone: true, destination: `todoist:${routedToId}` };
  }

  throw new Error(`Unknown routed_to_table: ${routedToTable}`);
}

async function correctMiraCapture(captureId, newCategory) {
  const correctedClass = normalizeCategory(newCategory);
  if (!VALID_MIRA_CATEGORIES.includes(correctedClass)) {
    throw new Error(`Invalid Mira category: ${newCategory}`);
  }

  const client = await pool.connect();
  let row;
  let undoResult = null;

  try {
    await client.query('BEGIN');
    const found = await client.query(`
      SELECT id, raw_text, transcript, link_url, photo_url, classification, routed_to_table, routed_to_id
      FROM capture_inbox
      WHERE id = $1
      FOR UPDATE
    `, [captureId]);

    row = found.rows[0];
    if (!row) throw new Error(`Capture ${captureId} not found`);

    const originalClass = row.classification || 'unclassified';
    const originalRoutedTo = row.routed_to_table
      ? `${row.routed_to_table}:${row.routed_to_id || ''}`
      : null;

    try {
      undoResult = await undoRoute(client, row);
    } catch (err) {
      undoResult = { undone: false, reason: err.message };
      await logCorrectionEvent(row.id, 'failed', {
        original_class: originalClass,
        corrected_class: correctedClass,
        original_routed_to: originalRoutedTo,
        undo_error: err.message,
      }, err.message).catch(logErr => {
        console.error('[mira_correction] agent_log write failed:', logErr.message);
      });
    }

    await client.query(`
      INSERT INTO mira_corrections
        (capture_id, original_class, corrected_class, original_routed_to, corrected_routed_to, note)
      VALUES
        ($1, $2, $3, $4, $5, $6)
    `, [
      row.id,
      originalClass,
      correctedClass,
      originalRoutedTo,
      null,
      undoResult?.reason && undoResult.reason !== 'not_routed'
        ? `Undo route issue: ${undoResult.reason}`
        : null,
    ]);

    await client.query(`
      UPDATE capture_inbox
      SET classification = $1,
          status = 'classified',
          routed_to_table = NULL,
          routed_to_id = NULL,
          processed_at = NULL
      WHERE id = $2
    `, [correctedClass, row.id]);

    await client.query('COMMIT');

    await logCorrectionEvent(row.id, 'success', {
      original_class: originalClass,
      corrected_class: correctedClass,
      original_routed_to: originalRoutedTo,
      undo: undoResult,
    }).catch(err => {
      console.error('[mira_correction] agent_log write failed:', err.message);
    });

    return {
      capture_id: row.id,
      content: getCaptureContent(row),
      snippet: truncate(getCaptureContent(row), 90),
      original_class: originalClass,
      corrected_class: correctedClass,
      undo: undoResult,
    };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function sendMiraTelegramMessage(text, extra = {}) {
  const botToken = process.env.MIRA_TELEGRAM_BOT_TOKEN;
  const chatId = process.env.JACOB_TELEGRAM_CHAT_ID;
  if (!botToken || !chatId) {
    console.warn('[mira] Telegram env missing; message skipped');
    return null;
  }

  return axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    chat_id: chatId,
    text,
    ...extra,
  }, { timeout: 5000 });
}

module.exports = {
  VALID_MIRA_CATEGORIES,
  correctMiraCapture,
  sendMiraTelegramMessage,
  truncate,
};
