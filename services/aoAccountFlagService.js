'use strict';

const crypto = require('node:crypto');
const pool = require('../db');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');
const { insertActivity } = require('./aoCrmService');
const { AO_ACCOUNT_FLAG_REASONS, recommendJakeActionForFlag } = require('../utils/aoAccountFlagTypes');
const { resolveFlagAssigneeUserId, isSelfFlag } = require('../utils/aoFlagAssignee');
const { inc } = require('../utils/aoFlagMetrics');
const { runAoFlagBackfill } = require('./aoFlagBackfill');

const OPEN_STATUSES = ['open', 'reviewed'];
const ACTIVE_STATUSES = ['open', 'reviewed'];

function buildIdempotencyKey({
  clientId,
  sourceType,
  sourceId,
  createdByUserId,
  reason,
  note,
  conversationId,
}) {
  const raw = [
    clientId,
    sourceType,
    sourceId || '',
    conversationId || '',
    createdByUserId,
    reason,
    (note || '').slice(0, 500),
  ].join('|');
  return crypto.createHash('sha256').update(raw).digest('hex');
}

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

async function findDuplicateOpenFlag({
  clientId,
  idempotencyKey,
  db = pool,
}) {
  const { rows } = await db.query(`
    SELECT * FROM ao_account_flags
    WHERE client_id = $1
      AND idempotency_key = $2
      AND status = ANY($3::text[])
    ORDER BY created_at DESC
    LIMIT 1
  `, [clientId, idempotencyKey, ACTIVE_STATUSES]);
  return rows[0] || null;
}

async function createFlagNotification({
  clientId,
  userId,
  flagId,
  title,
  body,
  payload,
  db = pool,
}) {
  try {
    const { rows } = await db.query(`
      INSERT INTO ao_flag_notifications (
        client_id, user_id, flag_id, title, body, payload
      ) VALUES ($1, $2, $3::uuid, $4, $5, $6::jsonb)
      RETURNING *
    `, [clientId, userId, flagId, title, body, JSON.stringify(payload || {})]);
    inc('ao_flag_notification_created_count');
    return rows[0];
  } catch (err) {
    inc('ao_flag_notification_failure_count');
    console.error('[ao-flag] notification insert failed:', err.message);
    return null;
  }
}

async function emitAoFlagCreated({
  clientId,
  flag,
  createdByUserId,
  createdByAoId,
  assigneeUserId,
  prospectId,
  db = pool,
}) {
  await logAoAuditEvent({
    event: 'ao_flag.created',
    clientId,
    aoUserId: createdByAoId || createdByUserId,
    prospectId,
    db,
    payload: {
      flagId: flag.id,
      tenantId: clientId,
      assignedToUserId: assigneeUserId,
      createdByUserId,
      createdByAoId,
      sourceType: flag.source_type,
      sourceId: flag.source_id,
      conversationId: flag.conversation_id,
      prospectId: flag.account_id || prospectId,
      reason: flag.reason,
      createdAt: flag.created_at,
    },
  });
}

function mapFlagRow(row, extras = {}) {
  const reasonMeta = AO_ACCOUNT_FLAG_REASONS.find(r => r.value === row.reason);
  const companyName = extras.company_name || row.company_name || null;
  const aoName = extras.ao_name || row.ao_creator_name || row.ao_name || null;
  const sourceMissing = !row.source_id && !row.account_id && !row.conversation_id;
  return {
    id: row.id,
    tenant_id: row.client_id,
    account_id: row.account_id,
    prospect_id: row.account_id,
    source_type: row.source_type || 'crm_account',
    source_id: row.source_id || row.account_id,
    conversation_id: row.conversation_id || null,
    ao_id: row.ao_id,
    created_by_user_id: row.created_by_user_id || row.ao_id,
    created_by_ao_id: row.ao_id,
    created_by_role: row.created_by_role || null,
    assigned_to_user_id: row.assigned_to_user_id || null,
    reason: row.reason,
    reason_label: reasonMeta?.label || row.reason,
    note: row.note,
    status: row.status === 'dismissed' ? 'resolved' : row.status,
    unread: Boolean(row.unread),
    created_at: row.created_at,
    reviewed_at: row.reviewed_at || null,
    resolved_at: row.resolved_at || null,
    resolution_note: row.resolution_note || null,
    source_context: row.source_context || {},
    legacy_partial: Boolean(row.legacy_partial),
    company_name: companyName,
    business_name: companyName,
    ao_name: aoName,
    source_unavailable: sourceMissing && row.legacy_partial,
    recommended_action: recommendJakeActionForFlag(row.reason, companyName),
    href: row.conversation_id
      ? `/ao/field#conversation=${row.conversation_id}`
      : (row.account_id ? `/ao/crm/manager#${row.account_id}` : null),
  };
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

  const normalizedReason = String(reason || '').trim();
  if (!AO_ACCOUNT_FLAG_REASONS.some(r => r.value === normalizedReason)) {
    return { status: 400, error: 'Invalid flag reason', reasons: AO_ACCOUNT_FLAG_REASONS };
  }

  const account = await assertAccountAccess({ clientId, prospectId, aoUserId, db });
  if (!account) {
    return { status: 404, error: 'Account not found for this AO' };
  }

  const assignee = await resolveFlagAssigneeUserId(clientId, db);
  if (!assignee) {
    return { status: 503, error: 'No operator assignee configured for this tenant' };
  }

  const label = AO_ACCOUNT_FLAG_REASONS.find(r => r.value === normalizedReason)?.label || normalizedReason;
  const trimmedNote = note && String(note).trim() ? String(note).trim() : null;
  const idempotencyKey = buildIdempotencyKey({
    clientId,
    sourceType: 'crm_account',
    sourceId: prospectId,
    createdByUserId: aoUserId,
    reason: normalizedReason,
    note: trimmedNote,
    conversationId,
  });

  const duplicate = await findDuplicateOpenFlag({ clientId, idempotencyKey, db });
  if (duplicate) {
    inc('ao_flag_duplicate_suppressed_count', { source_type: 'crm_account' });
    return {
      ok: true,
      duplicate: true,
      flag: mapFlagRow(duplicate, { company_name: account.company_name }),
    };
  }

  let flag;
  try {
    const { rows } = await db.query(`
      INSERT INTO ao_account_flags (
        client_id, account_id, ao_id, reason, note, status,
        source_type, source_id, conversation_id,
        created_by_user_id, created_by_role, assigned_to_user_id,
        unread, idempotency_key, source_context
      )
      VALUES (
        $1, $2::uuid, $3, $4, $5, 'open',
        'crm_account', $2::uuid, $6::uuid,
        $3, $7, $8,
        true, $9, $10::jsonb
      )
      RETURNING *
    `, [
      clientId,
      prospectId,
      aoUserId,
      normalizedReason,
      trimmedNote,
      conversationId,
      creatorRole,
      assignee.id,
      idempotencyKey,
      JSON.stringify(sourceContext || {}),
    ]);
    flag = rows[0];
    inc('ao_flag_created_count', { source_type: 'crm_account', creator_role: creatorRole });
  } catch (err) {
    console.error('[ao-flag] insert failed:', err.message);
    return { status: 500, error: 'Could not save flag' };
  }

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

  await emitAoFlagCreated({
    clientId,
    flag,
    createdByUserId: aoUserId,
    createdByAoId: aoUserId,
    assigneeUserId: assignee.id,
    prospectId,
    db,
  });

  const creator = await db.query('SELECT name FROM users WHERE id = $1', [aoUserId]);
  const aoName = creator.rows[0]?.name || 'An AO';
  const self = isSelfFlag({
    createdByUserId: aoUserId,
    assigneeUserId: assignee.id,
    creatorRole,
  });

  if (!self) {
    const title = `${aoName} flagged ${account.company_name || 'an account'} for you`;
    const body = trimmedNote || label;
    await createFlagNotification({
      clientId,
      userId: assignee.id,
      flagId: flag.id,
      title,
      body,
      payload: {
        flag_id: flag.id,
        ao_name: aoName,
        business_name: account.company_name,
        reason: label,
        href: `/max-briefing#flags`,
        created_at: flag.created_at,
      },
      db,
    });
  }

  return {
    ok: true,
    flag: mapFlagRow(flag, { company_name: account.company_name, ao_name: aoName }),
    notification_skipped: self,
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
  let unreadClause = unreadOnly ? 'AND n.read_at IS NULL' : '';
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
