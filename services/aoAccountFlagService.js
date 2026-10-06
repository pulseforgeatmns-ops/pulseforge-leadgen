'use strict';

const pool = require('../db');
const { insertActivity } = require('./aoCrmService');
const { AO_ACCOUNT_FLAG_REASONS, recommendJakeActionForFlag } = require('../utils/aoAccountFlagTypes');
const { inc } = require('../utils/aoFlagMetrics');
const { runAoFlagBackfill } = require('./aoFlagBackfill');
const {
  buildIdempotencyKey,
  mapFlagRow,
  createAoEscalation,
} = require('./aoEscalationService');

const OPEN_STATUSES = ['open', 'reviewed'];

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
  creatorRole = 'ao',
  conversationId = null,
  sourceContext = {},
  db = pool,
}) {
  await runAoFlagBackfill(clientId, db).catch(err => {
    console.error('[ao-flag] backfill skipped:', err.message);
  });

  const account = await assertAccountAccess({ clientId, prospectId, aoUserId, db });
  if (!account) {
    return { status: 404, error: 'Account not found for this AO' };
  }

  const trimmedNote = note && String(note).trim() ? String(note).trim() : null;
  const escalation = await createAoEscalation({
    clientId,
    createdByUserId: aoUserId,
    createdByAoId: aoUserId,
    creatorRole,
    sourceType: 'crm_account',
    sourceId: prospectId,
    conversationId,
    prospectId,
    reason,
    note: trimmedNote,
    sourceContext,
    companyName: account.company_name,
    db,
  });

  if (escalation.status) return escalation;
  if (escalation.duplicate) return escalation;

  const flag = escalation.flag;
  const label = AO_ACCOUNT_FLAG_REASONS.find(r => r.value === String(reason).trim())?.label || reason;
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
    outcome: String(reason).trim(),
    notes: trimmedNote || `Flagged for Jake — ${label}`,
    metadata: { flag_id: flag.id, conversation_id: conversationId || null },
    db,
  });

  return {
    ok: true,
    flag,
    notification_skipped: escalation.notification_skipped,
  };
}

async function listFlagsForAssignee({
  clientId,
  assigneeUserId,
  status = 'open',
  limit = 50,
  db = pool,
}) {
  const statusMap = {
    open: ['open'],
    reviewed: ['reviewed'],
    resolved: ['resolved'],
    all: ['open', 'reviewed', 'resolved'],
  };
  const statuses = statusMap[status] || statusMap.open;
  const params = [clientId, assigneeUserId, statuses];
  const statusClause = `AND f.status = ANY($3::text[])`;

  const { rows } = await db.query(`
    SELECT
      f.*,
      c.name AS company_name,
      u.name AS ao_creator_name
    FROM ao_account_flags f
    LEFT JOIN prospects p ON p.id = f.account_id AND p.client_id = f.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN users u ON u.id = f.ao_id
    WHERE f.client_id = $1
      AND f.assigned_to_user_id = $2
      ${statusClause}
    ORDER BY f.unread DESC, f.created_at DESC
    LIMIT ${Math.min(Math.max(Number(limit) || 50, 1), 200)}
  `, params);

  return rows.map(row => mapFlagRow(row));
}

async function listFlagsForCreator({
  clientId,
  aoUserId,
  prospectId = null,
  limit = 20,
  db = pool,
}) {
  const params = [clientId, aoUserId];
  let prospectClause = '';
  if (prospectId) {
    params.push(prospectId);
    prospectClause = `AND f.account_id = $${params.length}::uuid`;
  }
  const { rows } = await db.query(`
    SELECT f.*, c.name AS company_name
    FROM ao_account_flags f
    LEFT JOIN prospects p ON p.id = f.account_id AND p.client_id = f.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE f.client_id = $1 AND f.ao_id = $2
      ${prospectClause}
    ORDER BY f.created_at DESC
    LIMIT ${Math.min(Math.max(Number(limit) || 20, 1), 100)}
  `, params);
  return rows.map(row => mapFlagRow(row));
}

async function countUnreadFlags({ clientId, assigneeUserId, db = pool }) {
  const { rows } = await db.query(`
    SELECT COUNT(*)::int AS count
    FROM ao_account_flags
    WHERE client_id = $1
      AND assigned_to_user_id = $2
      AND unread = true
      AND status = ANY($3::text[])
  `, [clientId, assigneeUserId, OPEN_STATUSES]);
  return rows[0]?.count || 0;
}

async function getFlagById(flagId, { clientId, viewerUserId, viewerRole, db = pool }) {
  const { rows } = await db.query(`
    SELECT
      f.*,
      c.name AS company_name,
      u.name AS ao_creator_name
    FROM ao_account_flags f
    LEFT JOIN prospects p ON p.id = f.account_id AND p.client_id = f.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN users u ON u.id = f.ao_id
    WHERE f.id = $1::uuid AND f.client_id = $2
    LIMIT 1
  `, [flagId, clientId]);
  const row = rows[0];
  if (!row) return null;

  const isAssignee = Number(row.assigned_to_user_id) === Number(viewerUserId);
  const isCreator = Number(row.ao_id) === Number(viewerUserId);
  const isAdmin = ['admin', 'manager'].includes(String(viewerRole || '').toLowerCase());
  if (!isAssignee && !isAdmin && !isCreator) return { forbidden: true };

  return mapFlagRow(row);
}

async function updateFlagStatus({
  flagId,
  clientId,
  actorUserId,
  status,
  resolutionNote = null,
  noteAppend = null,
  allowTenantAdmin = false,
  db = pool,
}) {
  const allowed = ['reviewed', 'resolved', 'open'];
  if (!allowed.includes(status)) {
    return { status: 400, error: 'invalid status' };
  }

  const accessClause = allowTenantAdmin
    ? 'AND (assigned_to_user_id = $3 OR $4 = true)'
    : 'AND assigned_to_user_id = $3';
  const accessParams = allowTenantAdmin
    ? [flagId, clientId, actorUserId, true]
    : [flagId, clientId, actorUserId];

  const { rows: existingRows } = await db.query(`
    SELECT * FROM ao_account_flags
    WHERE id = $1::uuid AND client_id = $2
      ${accessClause}
    LIMIT 1
  `, accessParams);
  const existing = existingRows[0];
  if (!existing) return { status: 404, error: 'Flag not found' };

  const params = [flagId, clientId];
  const sets = [];
  params.push(status);
  sets.push(`status = $${params.length}`);
  if (status === 'open') {
    sets.push('unread = true');
  } else {
    sets.push('unread = false');
  }
  if (status === 'reviewed') {
    sets.push('reviewed_at = COALESCE(reviewed_at, NOW())');
    inc('ao_flag_reviewed_count');
  }
  if (status === 'resolved') {
    sets.push('resolved_at = COALESCE(resolved_at, NOW())');
    inc('ao_flag_resolved_count');
  }
  if (status === 'open') {
    sets.push('resolved_at = NULL', 'reviewed_at = NULL');
  }
  if (resolutionNote != null && String(resolutionNote).trim()) {
    params.push(String(resolutionNote).trim());
    sets.push(`resolution_note = $${params.length}`);
  }
  if (noteAppend != null && String(noteAppend).trim()) {
    params.push(String(noteAppend).trim());
    sets.push(`note = CASE WHEN note IS NULL OR note = '' THEN $${params.length} ELSE note || E'\\n' || $${params.length} END`);
  }

  const { rows } = await db.query(`
    UPDATE ao_account_flags
    SET ${sets.join(', ')}
    WHERE id = $1::uuid AND client_id = $2
    RETURNING *
  `, params);

  await db.query(`
    UPDATE ao_flag_notifications
    SET read_at = COALESCE(read_at, NOW())
    WHERE flag_id = $1::uuid AND user_id = $2
  `, [flagId, existing.assigned_to_user_id]);

  return { ok: true, flag: mapFlagRow(rows[0]) };
}

async function listOpenAccountFlags(clientId, { limit = 25, db = pool } = {}) {
  const { resolveFlagAssigneeUserId } = require('../utils/aoFlagAssignee');
  const assignee = await resolveFlagAssigneeUserId(clientId, db);
  if (!assignee) {
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

  return listFlagsForAssignee({
    clientId,
    assigneeUserId: assignee.id,
    status: 'open',
    limit,
    db,
  });
}

async function listFlagNotifications({ clientId, userId, unreadOnly = true, limit = 30, db = pool }) {
  const params = [clientId, userId];
  const unreadClause = unreadOnly ? 'AND n.read_at IS NULL' : '';
  const { rows } = await db.query(`
    SELECT n.*
    FROM ao_flag_notifications n
    WHERE n.client_id = $1 AND n.user_id = $2
      ${unreadClause}
    ORDER BY n.created_at DESC
    LIMIT ${Math.min(Math.max(Number(limit) || 30, 1), 100)}
  `, params);
  return rows;
}

module.exports = {
  createAccountFlag,
  listOpenAccountFlags,
  listFlagsForAssignee,
  listFlagsForCreator,
  countUnreadFlags,
  getFlagById,
  updateFlagStatus,
  listFlagNotifications,
  buildIdempotencyKey,
  AO_ACCOUNT_FLAG_REASONS,
  recommendJakeActionForFlag,
  runAoFlagBackfill,
};
