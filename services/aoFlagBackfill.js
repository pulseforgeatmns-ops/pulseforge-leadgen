'use strict';

const pool = require('../db');
const { resolveFlagAssigneeUserId } = require('../utils/aoFlagAssignee');
const { inc } = require('../utils/aoFlagMetrics');
const { CONVERSATION_ESCALATION_REASON } = require('../utils/aoAccountFlagTypes');
const { createAoEscalation } = require('./aoEscalationService');

const BACKFILL_ACTION = 'AO_FLAG_INBOX_001_BACKFILL_DONE';
const CONV_BACKFILL_ACTION = 'AO_FLAG_INBOX_002_CONV_BACKFILL_DONE';

async function backfillAlreadyDone(clientId, db = pool) {
  const { rows } = await db.query(`
    SELECT 1
    FROM agent_log
    WHERE client_id = $1
      AND agent_name = 'ao'
      AND action = $2
    LIMIT 1
  `, [clientId, BACKFILL_ACTION]);
  return rows.length > 0;
}

async function markBackfillDone(clientId, stats, db = pool) {
  await db.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ('ao', $1, $2::jsonb, 'success', NOW(), $3)
  `, [BACKFILL_ACTION, JSON.stringify(stats), clientId]);
}

async function normalizeExistingFlags(clientId, assigneeId, db = pool) {
  const { rowCount } = await db.query(`
    UPDATE ao_account_flags
    SET
      assigned_to_user_id = COALESCE(assigned_to_user_id, $2),
      created_by_user_id = COALESCE(created_by_user_id, ao_id),
      source_type = COALESCE(NULLIF(source_type, ''), 'crm_account'),
      source_id = COALESCE(source_id, account_id),
      unread = CASE
        WHEN status = 'open' THEN COALESCE(unread, true)
        ELSE false
      END
    WHERE client_id = $1
  `, [clientId, assigneeId]);
  return rowCount || 0;
}

async function backfillFromActivities(clientId, assigneeId, db = pool) {
  const { rows: activities } = await db.query(`
    SELECT a.id, a.prospect_id, a.ao_id, a.outcome, a.notes, a.metadata, a.created_at
    FROM ao_prospect_activity a
    WHERE a.tenant_id = $1
      AND a.activity_type = 'flag_for_jake'
      AND NOT EXISTS (
        SELECT 1 FROM ao_account_flags f
        WHERE f.client_id = a.tenant_id
          AND (
            (a.metadata ? 'flag_id' AND f.id = (a.metadata->>'flag_id')::uuid)
            OR (
              f.account_id = a.prospect_id
              AND f.ao_id = a.ao_id
              AND f.reason = COALESCE(a.outcome, 'other')
              AND f.created_at BETWEEN a.created_at - interval '2 minutes' AND a.created_at + interval '2 minutes'
            )
          )
      )
    ORDER BY a.created_at ASC
  `, [clientId]);

  let inserted = 0;
  for (const act of activities) {
    if (!act.prospect_id) continue;
    const reason = String(act.outcome || 'other').trim() || 'other';
    await db.query(`
      INSERT INTO ao_account_flags (
        client_id, account_id, ao_id, reason, note, status,
        source_type, source_id, created_by_user_id, assigned_to_user_id,
        unread, legacy_partial, created_at, source_context
      ) VALUES (
        $1, $2::uuid, $3, $4, $5, 'open',
        'crm_account', $2::uuid, $3, $6,
        true, true, $7, $8::jsonb
      )
    `, [
      clientId,
      act.prospect_id,
      act.ao_id,
      reason,
      act.notes,
      assigneeId,
      act.created_at,
      JSON.stringify({
        backfill_source: 'ao_prospect_activity',
        activity_id: act.id,
        partial: !act.notes,
      }),
    ]);
    inserted += 1;
    inc('ao_flag_backfilled_count', { source_type: 'crm_account' });
  }
  return inserted;
}

async function convBackfillAlreadyDone(clientId, db = pool) {
  const { rows } = await db.query(`
    SELECT 1 FROM agent_log
    WHERE client_id = $1 AND agent_name = 'ao' AND action = $2
    LIMIT 1
  `, [clientId, CONV_BACKFILL_ACTION]);
  return rows.length > 0;
}

async function backfillFromConversationReports(clientId, assigneeId, db = pool) {
  const { rows: reports } = await db.query(`
    SELECT
      r.*,
      s.prospect_id AS session_prospect_id,
      s.payload AS session_payload
    FROM ao_max_conversation_reports r
    LEFT JOIN ao_max_sessions s ON s.id = r.session_id
    WHERE r.client_id = $1
      AND r.canonical_flag_id IS NULL
    ORDER BY r.created_at ASC
  `, [clientId]);

  let newFlags = 0;
  let duplicatesSuppressed = 0;
  let partialLegacy = 0;

  for (const report of reports) {
    const ctx = report.context_json || {};
    const payload = report.session_payload || {};
    const prospectId = ctx.selected_account?.prospect_id
      || payload.prospect_id
      || report.session_prospect_id
      || null;
    const messageIndex = ctx.message_index ?? null;
    const sessionMissing = !report.session_id;

    const escalation = await createAoEscalation({
      clientId,
      createdByUserId: report.ao_owner_id,
      createdByAoId: report.ao_owner_id,
      creatorRole: 'ao',
      sourceType: 'conversation',
      sourceId: report.session_id,
      conversationId: report.session_id,
      prospectId,
      reason: CONVERSATION_ESCALATION_REASON,
      note: report.note || report.category || 'Conversation flag',
      sourceContext: {
        report_id: report.id,
        category: report.category,
        message_index: messageIndex,
        backfill_source: 'ao_max_conversation_reports',
        partial: sessionMissing || messageIndex == null,
      },
      legacyPartial: sessionMissing || messageIndex == null,
      createdAt: report.created_at,
      skipNotification: true,
      db,
    });

    if (escalation.status || !escalation.flag?.id) continue;
    if (escalation.duplicate) duplicatesSuppressed += 1;
    else {
      newFlags += 1;
      if (sessionMissing || messageIndex == null) partialLegacy += 1;
      inc('ao_conversation_flag_backfilled_count');
    }

    await db.query(`
      UPDATE ao_max_conversation_reports
      SET canonical_flag_id = $2::uuid
      WHERE id = $1::uuid
    `, [report.id, escalation.flag.id]);
  }

  return {
    conversation_reports_found: reports.length,
    new_flags_created: newFlags,
    duplicates_suppressed: duplicatesSuppressed,
    partial_legacy_records: partialLegacy,
  };
}

async function runConversationReportBackfill(clientId, db = pool) {
  if (!clientId) return { skipped: true };
  if (await convBackfillAlreadyDone(clientId, db)) {
    return { skipped: true };
  }
  const assignee = await resolveFlagAssigneeUserId(clientId, db);
  const stats = await backfillFromConversationReports(clientId, assignee?.id || null, db);
  await db.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ('ao', $1, $2::jsonb, 'success', NOW(), $3)
  `, [CONV_BACKFILL_ACTION, JSON.stringify(stats), clientId]);
  return stats;
}

async function runAoFlagBackfill(clientId, db = pool) {
  if (!clientId) {
    return {
      existing_flags_found: 0,
      backfilled_flags: 0,
      flags_missing_source_linkage: 0,
      flags_missing_reason: 0,
      duplicates_suppressed: 0,
      skipped: true,
    };
  }

  if (await backfillAlreadyDone(clientId, db)) {
    const { rows: [{ count: existing }] } = await db.query(`
      SELECT COUNT(*)::int AS count FROM ao_account_flags WHERE client_id = $1
    `, [clientId]);
    const conversation = await runConversationReportBackfill(clientId, db);
    return {
      existing_flags_found: existing,
      backfilled_flags: 0,
      flags_missing_source_linkage: 0,
      flags_missing_reason: 0,
      duplicates_suppressed: 0,
      skipped: true,
      conversation_backfill: conversation,
    };
  }

  const assignee = await resolveFlagAssigneeUserId(clientId, db);
  const assigneeId = assignee?.id || null;

  const { rows: [{ count: existingCount }] } = await db.query(`
    SELECT COUNT(*)::int AS count FROM ao_account_flags WHERE client_id = $1
  `, [clientId]);

  await normalizeExistingFlags(clientId, assigneeId, db);
  const backfilled = await backfillFromActivities(clientId, assigneeId, db);

  const { rows: [{ missing_linkage }] } = await db.query(`
    SELECT COUNT(*)::int AS missing_linkage
    FROM ao_account_flags
    WHERE client_id = $1
      AND source_id IS NULL
      AND account_id IS NULL
  `, [clientId]);

  const { rows: [{ missing_reason }] } = await db.query(`
    SELECT COUNT(*)::int AS missing_reason
    FROM ao_account_flags
    WHERE client_id = $1
      AND (reason IS NULL OR trim(reason) = '')
  `, [clientId]);

  const stats = {
    existing_flags_found: existingCount,
    backfilled_flags: backfilled,
    flags_missing_source_linkage: missing_linkage,
    flags_missing_reason: missing_reason,
    duplicates_suppressed: 0,
    assignee_user_id: assigneeId,
  };

  await markBackfillDone(clientId, stats, db);
  const conversation = await runConversationReportBackfill(clientId, db);
  return { ...stats, conversation_backfill: conversation };
}

module.exports = {
  runAoFlagBackfill,
  runConversationReportBackfill,
  BACKFILL_ACTION,
  CONV_BACKFILL_ACTION,
};
