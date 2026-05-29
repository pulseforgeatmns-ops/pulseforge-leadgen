const pool = require('../db');

// Maps inbound event names (Brevo + internal action_types) to the counter
// column they increment. Whitelisted — recordEvent interpolates the column name
// into SQL, so it must never come from arbitrary input.
const EVENT_COLUMN = {
  open: 'opens',
  opened: 'opens',
  email_opened: 'opens',
  click: 'clicks',
  clicked: 'clicks',
  email_clicked: 'clicks',
  reply: 'replies',
  replied: 'replies',
  email_reply: 'replies',
  bounce: 'bounces',
  bounced: 'bounces',
  email_bounced: 'bounces',
  soft_bounce: 'bounces',
  email_soft_bounce: 'bounces',
};

// Key columns are normalized to non-null values so a NULL never silently
// splits a combination across two rows (NULL <> NULL in a unique index).
function normText(value) {
  return value === undefined || value === null ? '' : String(value);
}

function normInt(value) {
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : 0;
}

// Startup migration. Idempotent — safe to call on every boot.
async function ensureEmailPerformanceTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS email_performance (
      id SERIAL PRIMARY KEY,
      client_id INTEGER,
      vertical TEXT,
      sequence TEXT,
      step INTEGER,
      subject_line TEXT,
      sends INTEGER DEFAULT 0,
      opens INTEGER DEFAULT 0,
      clicks INTEGER DEFAULT 0,
      replies INTEGER DEFAULT 0,
      bounces INTEGER DEFAULT 0,
      open_rate NUMERIC(5,2) DEFAULT 0,
      reply_rate NUMERIC(5,2) DEFAULT 0,
      last_updated TIMESTAMP DEFAULT NOW()
    )
  `);
  // Backs the ON CONFLICT upsert in recordSend / recordEvent. The helpers
  // normalize every key column to a non-null value before insert.
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS email_performance_combo_idx
      ON email_performance (client_id, vertical, sequence, step, subject_line)
  `);
}

// Increment sends for one subject/sequence/step/vertical combination, inserting
// the row if it does not exist yet. Recomputes the rates against the new
// denominator so they stay consistent between events.
async function recordSend(client_id, vertical, sequence, step, subject) {
  const cid = Number(client_id) || 0;
  const v = normText(vertical);
  const seq = normText(sequence);
  const st = normInt(step);
  const subj = normText(subject);

  await pool.query(`
    INSERT INTO email_performance
      (client_id, vertical, sequence, step, subject_line, sends, last_updated)
    VALUES ($1, $2, $3, $4, $5, 1, NOW())
    ON CONFLICT (client_id, vertical, sequence, step, subject_line)
    DO UPDATE SET
      sends = email_performance.sends + 1,
      open_rate  = ROUND((email_performance.opens::numeric   / NULLIF(email_performance.sends + 1, 0)) * 100, 2),
      reply_rate = ROUND((email_performance.replies::numeric / NULLIF(email_performance.sends + 1, 0)) * 100, 2),
      last_updated = NOW()
  `, [cid, v, seq, st, subj]);
}

// Increment opens / clicks / replies / bounces for a combination and recompute
// open_rate and reply_rate. Events do not carry the subject line, so this keys
// on client_id + vertical + sequence + step and updates the matching row(s).
async function recordEvent(client_id, vertical, sequence, step, event_type) {
  const col = EVENT_COLUMN[String(event_type || '').toLowerCase()];
  if (!col) return; // unknown event — nothing to record

  const cid = Number(client_id) || 0;
  const v = normText(vertical);
  const seq = normText(sequence);
  const st = normInt(step);

  // Only one counter changes per call; the others keep their current values
  // when the rates are recomputed.
  const newOpens = col === 'opens' ? 'opens + 1' : 'opens';
  const newReplies = col === 'replies' ? 'replies + 1' : 'replies';

  const res = await pool.query(`
    UPDATE email_performance
    SET ${col} = ${col} + 1,
        open_rate  = ROUND(((${newOpens})::numeric   / NULLIF(sends, 0)) * 100, 2),
        reply_rate = ROUND(((${newReplies})::numeric / NULLIF(sends, 0)) * 100, 2),
        last_updated = NOW()
    WHERE client_id = $1 AND vertical = $2 AND sequence = $3 AND step = $4
  `, [cid, v, seq, st]);

  // Event arrived before any recordSend (e.g. a legacy send) — persist a
  // placeholder row with an empty subject so the signal is not lost.
  if (res.rowCount === 0) {
    await pool.query(`
      INSERT INTO email_performance
        (client_id, vertical, sequence, step, subject_line, ${col}, last_updated)
      VALUES ($1, $2, $3, $4, '', 1, NOW())
      ON CONFLICT (client_id, vertical, sequence, step, subject_line)
      DO UPDATE SET ${col} = email_performance.${col} + 1, last_updated = NOW()
    `, [cid, v, seq, st]);
  }
}

module.exports = { ensureEmailPerformanceTable, recordSend, recordEvent };
