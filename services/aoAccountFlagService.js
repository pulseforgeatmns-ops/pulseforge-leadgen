'use strict';

const pool = require('../db');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');
const { insertActivity } = require('./aoCrmService');
const { AO_ACCOUNT_FLAG_REASONS, recommendJakeActionForFlag } = require('../utils/aoAccountFlagTypes');

async function assertAccountAccess({ clientId, prospectId, aoUserId, db = pool }) {
  const params = [prospectId, clientId];
  let ownerClause = '';
  if (aoUserId) {
    params.push(aoUserId);
    ownerClause = `AND p.assigned_ao_id = $${params.length}`;
  }
  const { rows } = await db.query(`
    SELECT p.id, c.name AS company_name
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.id = $1::uuid AND p.client_id = $2
      ${ownerClause}
    LIMIT 1
  `, params);
  return rows[0] || null;
}

async function createAccountFlag({
  clientId,
  aoUserId,
  prospectId,
  reason,
  note = null,
  db = pool,
}) {
  const normalizedReason = String(reason || '').trim();
  if (!AO_ACCOUNT_FLAG_REASONS.some(r => r.value === normalizedReason)) {
    return { status: 400, error: 'Invalid flag reason', reasons: AO_ACCOUNT_FLAG_REASONS };
  }

  const account = await assertAccountAccess({ clientId, prospectId, aoUserId, db });
  if (!account) {
    return { status: 404, error: 'Account not found for this AO' };
  }

  const label = AO_ACCOUNT_FLAG_REASONS.find(r => r.value === normalizedReason)?.label || normalizedReason;
  const trimmedNote = note && String(note).trim() ? String(note).trim() : null;

  const { rows } = await db.query(`
    INSERT INTO ao_account_flags (client_id, account_id, ao_id, reason, note, status)
    VALUES ($1, $2::uuid, $3, $4, $5, 'open')
    RETURNING *
  `, [clientId, prospectId, aoUserId, normalizedReason, trimmedNote]);

  const flag = rows[0];
  const helpReason = trimmedNote ? `${label}: ${trimmedNote}` : label;

  await db.query(`
    UPDATE prospects SET
      help_requested = true,
      help_reason = COALESCE($3, help_reason),
      help_requested_at = COALESCE(help_requested_at, NOW()),
      updated_at = NOW()
    WHERE id = $1::uuid AND client_id = $2
  `, [prospectId, clientId, helpReason]);

  await insertActivity({
    clientId,
    prospectId,
    aoUserId,
    activityType: 'flag_for_jake',
    outcome: normalizedReason,
    notes: trimmedNote || `Flagged for Jake — ${label}`,
    metadata: { flag_id: flag.id },
    db,
  });

  await logAoAuditEvent({
    event: 'AO_ACCOUNT_FLAGGED_FOR_JAKE',
    clientId,
    aoUserId,
    prospectId,
    payload: {
      flag_id: flag.id,
      reason: normalizedReason,
      note: trimmedNote,
    },
  });

  return {
    ok: true,
    flag: mapFlagRow(flag, { company_name: account.company_name }),
  };
}

function mapFlagRow(row, extras = {}) {
  const reasonMeta = AO_ACCOUNT_FLAG_REASONS.find(r => r.value === row.reason);
  return {
    id: row.id,
    account_id: row.account_id,
    ao_id: row.ao_id,
    reason: row.reason,
    reason_label: reasonMeta?.label || row.reason,
    note: row.note,
    status: row.status,
    created_at: row.created_at,
    resolved_at: row.resolved_at,
    company_name: extras.company_name || null,
    ao_name: extras.ao_name || null,
    recommended_action: recommendJakeActionForFlag(row.reason, extras.company_name),
  };
}

async function listOpenAccountFlags(clientId, { limit = 25, db = pool } = {}) {
  const { rows } = await db.query(`
    SELECT
      f.*,
      c.name AS company_name,
      u.name AS ao_name
    FROM ao_account_flags f
    LEFT JOIN prospects p ON p.id = f.account_id AND p.client_id = f.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN users u ON u.id = f.ao_id
    WHERE f.client_id = $1 AND f.status = 'open'
    ORDER BY f.created_at DESC
    LIMIT $2
  `, [clientId, limit]);

  return rows.map(row => mapFlagRow(row, {
    company_name: row.company_name,
    ao_name: row.ao_name,
  }));
}

module.exports = {
  createAccountFlag,
  listOpenAccountFlags,
  AO_ACCOUNT_FLAG_REASONS,
  recommendJakeActionForFlag,
};
