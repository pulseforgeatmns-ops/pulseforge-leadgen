'use strict';

const crypto = require('node:crypto');
const pool = require('../db');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');
const { AO_ACCOUNT_FLAG_REASONS, recommendJakeActionForFlag } = require('../utils/aoAccountFlagTypes');
const { resolveFlagAssigneeUserId, isSelfFlag } = require('../utils/aoFlagAssignee');
const { inc } = require('../utils/aoFlagMetrics');

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

function buildConversationDeepLink(conversationId, messageIndex = null) {
  if (!conversationId) return null;
  let href = `/ao/field?session=${encodeURIComponent(conversationId)}`;
  if (messageIndex != null && messageIndex >= 0) {
    href += `&message=${encodeURIComponent(String(messageIndex))}`;
  }
  return href;
}

function mapFlagRow(row, extras = {}) {
  const reasonMeta = AO_ACCOUNT_FLAG_REASONS.find(r => r.value === row.reason);
  const companyName = extras.company_name || row.company_name || null;
  const aoName = extras.ao_name || row.ao_creator_name || row.ao_name || null;
  const ctx = row.source_context || {};
  const conversationId = row.conversation_id || (row.source_type === 'conversation' ? row.source_id : null);
  const messageIndex = ctx.message_index ?? ctx.message_id ?? null;
  const sourceMissing = !row.source_id && !row.account_id && !conversationId;
  const sourceType = row.source_type || 'crm_account';
  return {
    id: row.id,
    tenant_id: row.client_id,
    account_id: row.account_id,
    prospect_id: row.account_id,
    report_id: ctx.report_id || extras.report_id || null,
    message_id: ctx.message_id || null,
    message_index: messageIndex,
    source_type: sourceType,
    source_type_label: sourceType === 'conversation' ? 'Conversation flag' : 'CRM flag',
    source_id: row.source_id || row.account_id || conversationId,
    conversation_id: conversationId,
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
    source_context: ctx,
    legacy_partial: Boolean(row.legacy_partial),
    company_name: companyName,
    business_name: companyName,
    ao_name: aoName,
    source_unavailable: sourceMissing && row.legacy_partial,
    recommended_action: recommendJakeActionForFlag(row.reason, companyName),
    href: conversationId
      ? buildConversationDeepLink(conversationId, messageIndex)
      : (row.account_id ? `/ao/crm/manager#${row.account_id}` : null),
  };
}

async function findDuplicateOpenFlag({ clientId, idempotencyKey, db = pool }) {
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

async function findCrossWorkflowDuplicate({
  clientId,
  conversationId,
  aoUserId,
  note,
  db = pool,
}) {
  if (!conversationId) return null;
  const trimmed = String(note || '').trim().toLowerCase();
  const { rows } = await db.query(`
    SELECT * FROM ao_account_flags
    WHERE client_id = $1
      AND conversation_id = $2::uuid
      AND ao_id = $3
      AND status = ANY($4::text[])
      AND created_at > NOW() - interval '48 hours'
    ORDER BY created_at DESC
    LIMIT 5
  `, [clientId, conversationId, aoUserId, ACTIVE_STATUSES]);

  for (const row of rows) {
    const rowNote = String(row.note || '').trim().toLowerCase();
    if (!trimmed && !rowNote) return row;
    if (trimmed && rowNote && (trimmed === rowNote || rowNote.includes(trimmed) || trimmed.includes(rowNote))) {
      return row;
    }
  }
  return null;
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
    inc('ao_flag_notification_created_count', { source_type: payload?.source_type || 'unknown' });
    if (payload?.source_type === 'conversation') {
      inc('ao_conversation_flag_notification_count');
    }
    return rows[0];
  } catch (err) {
    inc('ao_flag_notification_failure_count');
    if (payload?.source_type === 'conversation') {
      inc('ao_conversation_flag_notification_failure_count');
    }
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

async function createAoEscalation({
  clientId,
  createdByUserId,
  createdByAoId = null,
  creatorRole = 'ao',
  sourceType,
  sourceId,
  conversationId = null,
  prospectId = null,
  reason,
  note = null,
  sourceContext = {},
  idempotencyKey = null,
  legacyPartial = false,
  createdAt = null,
  companyName = null,
  aoName = null,
  skipNotification = false,
  db = pool,
}) {
  const aoUserId = createdByAoId || createdByUserId;
  const normalizedReason = String(reason || '').trim();
  if (!AO_ACCOUNT_FLAG_REASONS.some(r => r.value === normalizedReason)) {
    return { status: 400, error: 'Invalid flag reason', reasons: AO_ACCOUNT_FLAG_REASONS };
  }

  const assignee = await resolveFlagAssigneeUserId(clientId, db);
  if (!assignee) {
    return { status: 503, error: 'No operator assignee configured for this tenant' };
  }

  const label = AO_ACCOUNT_FLAG_REASONS.find(r => r.value === normalizedReason)?.label || normalizedReason;
  const trimmedNote = note && String(note).trim() ? String(note).trim() : null;
  const resolvedConversationId = conversationId || (sourceType === 'conversation' ? sourceId : null);
  const key = idempotencyKey || buildIdempotencyKey({
    clientId,
    sourceType,
    sourceId,
    createdByUserId: aoUserId,
    reason: normalizedReason,
    note: trimmedNote,
    conversationId: resolvedConversationId,
  });

  let duplicate = await findDuplicateOpenFlag({ clientId, idempotencyKey: key, db });
  if (!duplicate && resolvedConversationId) {
    duplicate = await findCrossWorkflowDuplicate({
      clientId,
      conversationId: resolvedConversationId,
      aoUserId,
      note: trimmedNote,
      db,
    });
  }
  if (duplicate) {
    inc('ao_flag_duplicate_suppressed_count', { source_type: sourceType });
    if (sourceType === 'conversation') {
      inc('ao_conversation_flag_duplicate_suppressed_count');
    }
    return {
      ok: true,
      duplicate: true,
      flag: mapFlagRow(duplicate, { company_name: companyName, ao_name: aoName }),
    };
  }

  let flag;
  try {
    const insertSql = createdAt
      ? `
      INSERT INTO ao_account_flags (
        client_id, account_id, ao_id, reason, note, status,
        source_type, source_id, conversation_id,
        created_by_user_id, created_by_role, assigned_to_user_id,
        unread, idempotency_key, source_context, legacy_partial, created_at
      )
      VALUES (
        $1, $2::uuid, $3, $4, $5, 'open',
        $6, $7::uuid, $8::uuid,
        $3, $9, $10,
        true, $11, $12::jsonb, $13, $14
      )
      RETURNING *`
      : `
      INSERT INTO ao_account_flags (
        client_id, account_id, ao_id, reason, note, status,
        source_type, source_id, conversation_id,
        created_by_user_id, created_by_role, assigned_to_user_id,
        unread, idempotency_key, source_context, legacy_partial
      )
      VALUES (
        $1, $2::uuid, $3, $4, $5, 'open',
        $6, $7::uuid, $8::uuid,
        $3, $9, $10,
        true, $11, $12::jsonb, $13
      )
      RETURNING *`;

    const params = createdAt
      ? [
        clientId,
        prospectId || null,
        aoUserId,
        normalizedReason,
        trimmedNote,
        sourceType,
        sourceId,
        resolvedConversationId,
        creatorRole,
        assignee.id,
        key,
        JSON.stringify(sourceContext || {}),
        legacyPartial,
        createdAt,
      ]
      : [
        clientId,
        prospectId || null,
        aoUserId,
        normalizedReason,
        trimmedNote,
        sourceType,
        sourceId,
        resolvedConversationId,
        creatorRole,
        assignee.id,
        key,
        JSON.stringify(sourceContext || {}),
        legacyPartial,
      ];

    const { rows } = await db.query(insertSql, params);
    flag = rows[0];
    inc('ao_flag_created_count', { source_type: sourceType, creator_role: creatorRole });
    if (sourceType === 'conversation') {
      inc('ao_conversation_flag_created_count');
      inc('ao_conversation_flag_canonicalized_count');
    }
  } catch (err) {
    console.error('[ao-flag] insert failed:', err.message);
    return { status: 500, error: 'Could not save flag' };
  }

  await emitAoFlagCreated({
    clientId,
    flag,
    createdByUserId: aoUserId,
    createdByAoId: aoUserId,
    assigneeUserId: assignee.id,
    prospectId,
    db,
  });

  if (!aoName) {
    const creator = await db.query('SELECT name FROM users WHERE id = $1', [aoUserId]);
    aoName = creator.rows[0]?.name || 'An AO';
  }

  const self = skipNotification || isSelfFlag({
    createdByUserId: aoUserId,
    assigneeUserId: assignee.id,
    creatorRole,
  });

  if (!self) {
    const accountLabel = companyName || (sourceType === 'conversation' ? 'a Max conversation' : 'an account');
    const title = sourceType === 'conversation'
      ? `${aoName} flagged ${companyName ? companyName : 'a conversation'} for you`
      : `${aoName} flagged ${accountLabel} for you`;
    const body = trimmedNote || label;
    const href = mapFlagRow(flag, { company_name: companyName }).href || '/max-briefing#flags';
    await createFlagNotification({
      clientId,
      userId: assignee.id,
      flagId: flag.id,
      title,
      body,
      payload: {
        flag_id: flag.id,
        ao_name: aoName,
        business_name: companyName,
        reason: label,
        source_type: sourceType,
        conversation_id: resolvedConversationId,
        href,
        created_at: flag.created_at,
      },
      db,
    });
  }

  return {
    ok: true,
    flag: mapFlagRow(flag, { company_name: companyName, ao_name: aoName }),
    notification_skipped: self,
  };
}

module.exports = {
  buildIdempotencyKey,
  buildConversationDeepLink,
  mapFlagRow,
  createAoEscalation,
  findDuplicateOpenFlag,
  findCrossWorkflowDuplicate,
  createFlagNotification,
  emitAoFlagCreated,
  ACTIVE_STATUSES,
};
