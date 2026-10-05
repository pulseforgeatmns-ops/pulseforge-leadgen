'use strict';

const pool = require('../db');
const { ensureAoRosterSchema } = require('../utils/aoRosterSchema');
const {
  AO_REVIEW_NEEDS_REASSIGNMENT,
  AO_REASSIGNMENT_REASON_AO_INACTIVE,
  isAoEligibleForAssignment,
  normalizeAoOperationalStatus,
} = require('../utils/aoRosterOperational');
const { insertActivity } = require('./aoCrmService');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');
const { resolveJakeAoOwner } = require('./aoFieldService');

const REVIEW_DECISIONS = Object.freeze([
  'keep',
  'reassign_tony',
  'reassign_rory',
  'research_needed',
  'disqualify',
]);

async function findAoByNamePattern(clientId, namePattern, db = pool) {
  const { rows } = await db.query(`
    SELECT id, name, email, client_id, role, active, ao_operational_status
    FROM users
    WHERE client_id = $1
      AND role = 'ao'
      AND name ILIKE $2
    ORDER BY active DESC, id ASC
    LIMIT 1
  `, [clientId, namePattern]);
  return rows[0] || null;
}

async function pauseAoUser({ clientId, aoUserId, status = 'paused', db = pool }) {
  await ensureAoRosterSchema(db);
  const operational = normalizeAoOperationalStatus(status);
  if (operational === 'active') {
    throw Object.assign(new Error('pauseAoUser requires paused or inactive status'), { code: 'invalid_pause_status' });
  }
  const { rows } = await db.query(`
    UPDATE users SET
      active = false,
      ao_operational_status = $3
    WHERE id = $1 AND client_id = $2 AND role = 'ao'
    RETURNING id, name, email, active, ao_operational_status
  `, [aoUserId, clientId, operational]);
  if (!rows[0]) return null;
  await logAoAuditEvent({
    event: 'AO_ROSTER_PAUSED',
    clientId,
    aoUserId,
    payload: { ao_operational_status: operational },
  });
  return rows[0];
}

async function transferAccountsFromInactiveAo({
  clientId,
  fromAoUserId,
  toAoUserId,
  reason = AO_REASSIGNMENT_REASON_AO_INACTIVE,
  reviewBucket = AO_REVIEW_NEEDS_REASSIGNMENT,
  db = pool,
  dryRun = false,
}) {
  await ensureAoRosterSchema(db);

  const { rows: fromUserRows } = await db.query(`
    SELECT id, name, active, ao_operational_status FROM users
    WHERE id = $1 AND client_id = $2 AND role = 'ao'
  `, [fromAoUserId, clientId]);
  const fromUser = fromUserRows[0];
  if (!fromUser) {
    throw Object.assign(new Error('Source AO not found'), { code: 'source_ao_not_found' });
  }

  const { rows: toUserRows } = await db.query(`
    SELECT id, name, active, ao_operational_status FROM users
    WHERE id = $1 AND client_id = $2 AND role = 'ao'
  `, [toAoUserId, clientId]);
  const toUser = toUserRows[0];
  if (!toUser || !isAoEligibleForAssignment(toUser)) {
    throw Object.assign(new Error('Target AO must be active for assignment'), { code: 'target_ao_not_active' });
  }

  const { rows: prospectRows } = await db.query(`
    SELECT id, company_id, assigned_ao_id, ao_review_bucket
    FROM prospects
    WHERE client_id = $1 AND assigned_ao_id = $2
  `, [clientId, fromAoUserId]);

  if (dryRun) {
    return {
      dry_run: true,
      from_ao: fromUser,
      to_ao: toUser,
      prospect_count: prospectRows.length,
      prospect_ids: prospectRows.map(r => r.id),
    };
  }

  const client = await db.connect();
  try {
    await client.query('BEGIN');

    let transferred = 0;
    for (const row of prospectRows) {
      await client.query(`
        UPDATE prospects SET
          assigned_ao_id = $3,
          ao_review_bucket = $4,
          ao_reassignment_prior_ao_id = $5,
          ao_reassignment_at = NOW(),
          ao_reassignment_reason = $6,
          ao_paused = true,
          updated_at = NOW()
        WHERE id = $1 AND client_id = $2
      `, [
        row.id,
        clientId,
        toAoUserId,
        reviewBucket,
        fromAoUserId,
        reason,
      ]);

      await client.query(`
        UPDATE ao_prospect_tasks SET assigned_ao_id = $3
        WHERE client_id = $1 AND prospect_id = $2
          AND status IN ('open', 'in_progress')
      `, [clientId, row.id, toAoUserId]);

      await client.query(`
        UPDATE ao_leads SET ao_owner_id = $3
        WHERE client_id = $1 AND ao_owner_id = $2 AND crm_prospect_id = $4
      `, [clientId, fromAoUserId, toAoUserId, row.id]);

      await insertActivity({
        clientId,
        prospectId: row.id,
        aoUserId: toAoUserId,
        activityType: 'status_change',
        notes: `Account transferred from inactive AO ${fromUser.name} to ${toUser.name} for roster review.`,
        metadata: {
          priorAssignedAoId: fromAoUserId,
          reassignedToAoId: toAoUserId,
          reassignmentReason: reason,
          reassignedAt: new Date().toISOString(),
          ao_review_bucket: reviewBucket,
        },
        db: client,
      });
      transferred += 1;
    }

    await client.query('COMMIT');

    await logAoAuditEvent({
      event: 'AO_ROSTER_BULK_TRANSFER',
      clientId,
      aoUserId: toAoUserId,
      payload: {
        from_ao_id: fromAoUserId,
        to_ao_id: toAoUserId,
        reason,
        transferred_count: transferred,
      },
    });

    return {
      dry_run: false,
      from_ao: fromUser,
      to_ao: toUser,
      transferred_count: transferred,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function resolveAoByDecision(clientId, decision, db = pool) {
  if (decision === 'reassign_tony') {
    return findAoByNamePattern(clientId, 'Tony%', db);
  }
  if (decision === 'reassign_rory') {
    return findAoByNamePattern(clientId, 'Rory%', db);
  }
  return null;
}

async function submitTransferredAccountReview({
  clientId,
  prospectId,
  reviewerUserId,
  decision,
  note = null,
  db = pool,
}) {
  await ensureAoRosterSchema(db);
  if (!REVIEW_DECISIONS.includes(decision)) {
    return { error: 'Invalid review decision', status: 400, decisions: REVIEW_DECISIONS };
  }

  const { rows } = await db.query(`
    SELECT p.*, u.name AS assigned_ao_name
    FROM prospects p
    LEFT JOIN users u ON u.id = p.assigned_ao_id
    WHERE p.id = $1::uuid AND p.client_id = $2
    LIMIT 1
  `, [prospectId, clientId]);
  const prospect = rows[0];
  if (!prospect) return { error: 'Account not found', status: 404 };
  if (!prospect.ao_review_bucket) {
    return { error: 'Account is not in transferred review bucket', status: 409 };
  }

  const priorAoId = prospect.ao_reassignment_prior_ao_id;
  let newAssignedAoId = prospect.assigned_ao_id;
  let clearReview = true;
  let aoPaused = false;
  let patch = {};

  if (decision === 'keep') {
    aoPaused = false;
    patch = { ao_review_bucket: null };
  } else if (decision === 'research_needed') {
    aoPaused = true;
    clearReview = false;
    patch = {
      ao_next_action: 'research_contact',
      ao_current_status: prospect.ao_current_status || 'researching',
    };
  } else if (decision === 'disqualify') {
    aoPaused = true;
    patch = {
      ao_current_status: 'not_a_fit',
      ao_next_action: 'disqualify',
      ao_disqualification_reason: note || 'Disqualified during inactive-AO transfer review',
      opportunity_stage: 'disqualified',
    };
  } else {
    const target = await resolveAoByDecision(clientId, decision, db);
    if (!target || !isAoEligibleForAssignment(target)) {
      return { error: 'Target AO not available for reassignment', status: 400 };
    }
    newAssignedAoId = target.id;
    aoPaused = false;
    patch = { assigned_ao_id: target.id };
  }

  await db.query(`
    UPDATE prospects SET
      assigned_ao_id = COALESCE($3, assigned_ao_id),
      ao_review_bucket = CASE WHEN $4 THEN NULL ELSE ao_review_bucket END,
      ao_paused = $5,
      ao_current_status = COALESCE($6, ao_current_status),
      ao_next_action = COALESCE($7, ao_next_action),
      ao_disqualification_reason = COALESCE($8, ao_disqualification_reason),
      opportunity_stage = COALESCE($9, opportunity_stage),
      updated_at = NOW()
    WHERE id = $1::uuid AND client_id = $2
  `, [
    prospectId,
    clientId,
    patch.assigned_ao_id || null,
    clearReview,
    aoPaused,
    patch.ao_current_status || null,
    patch.ao_next_action || null,
    patch.ao_disqualification_reason || null,
    patch.opportunity_stage || null,
  ]);

  if (newAssignedAoId !== prospect.assigned_ao_id) {
    await db.query(`
      UPDATE ao_prospect_tasks SET assigned_ao_id = $3
      WHERE client_id = $1 AND prospect_id = $2::uuid
        AND status IN ('open', 'in_progress')
    `, [clientId, prospectId, newAssignedAoId]);
  }

  await insertActivity({
    clientId,
    prospectId,
    aoUserId: reviewerUserId,
    activityType: decision === 'disqualify' ? 'disqualified' : 'note',
    notes: note || `Transfer review: ${decision.replace(/_/g, ' ')}`,
    metadata: {
      review_decision: decision,
      priorAssignedAoId: priorAoId,
      reassignedToAoId: newAssignedAoId,
      reassignmentReason: decision.startsWith('reassign_') ? 'manager_review' : null,
    },
    db,
  });

  await logAoAuditEvent({
    event: 'AO_TRANSFER_REVIEW_DECISION',
    clientId,
    aoUserId: reviewerUserId,
    prospectId,
    payload: { decision, prior_ao_id: priorAoId, assigned_ao_id: newAssignedAoId },
  });

  return { ok: true, decision, assigned_ao_id: newAssignedAoId };
}

async function runZachToJakeTransfer({ clientId = 10, dryRun = true, db = pool } = {}) {
  await ensureAoRosterSchema(db);
  const zach = await findAoByNamePattern(clientId, 'Zach%');
  if (!zach) throw Object.assign(new Error('Zach AO user not found'), { code: 'zach_not_found' });
  const jake = await resolveJakeAoOwner(clientId);
  if (!jake) throw Object.assign(new Error('Jake AO user not found'), { code: 'jake_not_found' });

  const pausePreview = {
    ao_user_id: zach.id,
    name: zach.name,
    would_set: { active: false, ao_operational_status: 'paused' },
  };

  const transferPreview = await transferAccountsFromInactiveAo({
    clientId,
    fromAoUserId: zach.id,
    toAoUserId: jake.id,
    db,
    dryRun: true,
  });

  if (dryRun) {
    return { pause: pausePreview, transfer: transferPreview };
  }

  const paused = await pauseAoUser({ clientId, aoUserId: zach.id, status: 'paused', db });
  const transfer = await transferAccountsFromInactiveAo({
    clientId,
    fromAoUserId: zach.id,
    toAoUserId: jake.id,
    db,
    dryRun: false,
  });

  return { pause: paused, transfer };
}

module.exports = {
  REVIEW_DECISIONS,
  findAoByNamePattern,
  pauseAoUser,
  transferAccountsFromInactiveAo,
  submitTransferredAccountReview,
  runZachToJakeTransfer,
};
