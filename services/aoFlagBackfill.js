'use strict';

const pool = require('../db');
const { resolveFlagAssigneeUserId } = require('../utils/aoFlagAssignee');
const { inc } = require('../utils/aoFlagMetrics');

const BACKFILL_ACTION = 'AO_FLAG_INBOX_001_BACKFILL_DONE';

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
    return {
      existing_flags_found: existing,
      backfilled_flags: 0,
      flags_missing_source_linkage: 0,
      flags_missing_reason: 0,
      duplicates_suppressed: 0,
      skipped: true,
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
  return stats;
}

module.exports = {
  runAoFlagBackfill,
  BACKFILL_ACTION,
};
