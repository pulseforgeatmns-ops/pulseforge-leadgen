'use strict';

const DISPOSITION_VALUES = Object.freeze([
  'voicemail',
  'answered_interested',
  'answered_not_interested',
  'answered_callback',
  'no_answer',
  'wrong_number',
  'disconnected',
  'gatekeeper_relayed',
  'gatekeeper_blocked',
  'incumbent_all_set',
]);

const DISPOSITION_SET = new Set(DISPOSITION_VALUES);

async function ensureCallDispositionSchema(db) {
  await db.query(`
    CREATE TABLE IF NOT EXISTS call_dispositions (
      id SERIAL PRIMARY KEY,
      prospect_id UUID REFERENCES prospects(id),
      client_id INTEGER,
      call_duration_seconds INTEGER,
      disposition TEXT,
      notes TEXT,
      cal_queue_id INTEGER,
      setter_id INTEGER,
      source TEXT NOT NULL DEFAULT 'cal',
      callback_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await db.query(`
    ALTER TABLE call_dispositions
      ADD COLUMN IF NOT EXISTS setter_id INTEGER,
      ADD COLUMN IF NOT EXISTS source TEXT,
      ADD COLUMN IF NOT EXISTS callback_at TIMESTAMPTZ
  `);
  await db.query(`
    UPDATE call_dispositions
    SET source = 'cal'
    WHERE source IS NULL
  `);
  await db.query(`
    ALTER TABLE call_dispositions
      ALTER COLUMN source SET DEFAULT 'cal',
      ALTER COLUMN source SET NOT NULL
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS call_dispositions_client_created_idx
      ON call_dispositions (client_id, created_at DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS call_dispositions_prospect_idx
      ON call_dispositions (prospect_id, created_at DESC)
  `);
}

function nextBusinessDayTen(now = new Date()) {
  const date = new Date(now);
  date.setDate(date.getDate() + 1);
  date.setHours(10, 0, 0, 0);
  while ([0, 6].includes(date.getDay())) date.setDate(date.getDate() + 1);
  return date;
}

function nurtureCallback(now = new Date()) {
  const date = new Date(now);
  date.setDate(date.getDate() + 90);
  date.setHours(10, 0, 0, 0);
  return date;
}

function resolveCallbackAt(disposition, requestedCallback, now = new Date()) {
  if (requestedCallback) return new Date(requestedCallback);
  if (disposition === 'answered_callback' || disposition === 'gatekeeper_relayed') {
    return nextBusinessDayTen(now);
  }
  if (disposition === 'incumbent_all_set') return nurtureCallback(now);
  return null;
}

async function applyProspectDisposition(db, {
  prospectId,
  clientId,
  disposition,
  callbackAt = null,
}) {
  if (!DISPOSITION_SET.has(disposition)) throw new Error(`Invalid call disposition: ${disposition}`);

  let changes;
  if (['voicemail', 'no_answer'].includes(disposition)) {
    changes = `setter_status = 'contacted', callback_at = $3`;
  } else if (['gatekeeper_relayed', 'answered_callback', 'gatekeeper_blocked'].includes(disposition)) {
    changes = `setter_status = 'follow_up', callback_at = $3`;
  } else if (disposition === 'answered_interested') {
    changes = `status = 'warm', setter_status = 'follow_up', is_hot = true, callback_at = $3`;
  } else if (disposition === 'incumbent_all_set') {
    changes = `status = 'cold', setter_status = 'follow_up', is_hot = false, callback_at = $3`;
  } else if (disposition === 'answered_not_interested') {
    changes = `status = 'dead', setter_status = 'dead', callback_at = NULL`;
  } else {
    // A wrong or disconnected number closes only the phone path. Other channels
    // remain eligible because global status and DNC are intentionally unchanged.
    changes = `phone = NULL, setter_status = 'dead', callback_at = NULL`;
  }
  const params = changes.includes('$3')
    ? [prospectId, clientId, callbackAt]
    : [prospectId, clientId];

  const result = await db.query(`
    UPDATE prospects
    SET ${changes},
        setter_updated_at = NOW(),
        last_contacted_at = NOW(),
        updated_at = NOW()
    WHERE id = $1 AND client_id = $2
    RETURNING *
  `, params);
  return result.rows[0] || null;
}

module.exports = {
  DISPOSITION_SET,
  DISPOSITION_VALUES,
  applyProspectDisposition,
  ensureCallDispositionSchema,
  nextBusinessDayTen,
  nurtureCallback,
  resolveCallbackAt,
};
