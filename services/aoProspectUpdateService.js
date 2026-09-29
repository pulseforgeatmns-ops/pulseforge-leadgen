'use strict';

const { randomUUID } = require('node:crypto');
const pool = require('../db');
const { ensureAoProspectRoutingSchema } = require('../utils/aoProspectRoutingSchema');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const {
  isValidOutcomeType,
  OUTCOME_TO_NEXT_ACTION,
  OUTCOME_TO_ADVISORY_STAGE,
} = require('../utils/aoProspectUpdateTypes');
const { DEBRIEF_NEXT_ACTIONS } = require('../utils/aoProspectRoutingConstants');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');

async function ensureProspectAccess({ prospectId, clientId, aoUserId, db = pool }) {
  const { rows } = await db.query(`
    SELECT id, assigned_ao_id, client_id
    FROM prospects
    WHERE id = $1::uuid AND client_id = $2
    LIMIT 1
  `, [prospectId, clientId]);
  const prospect = rows[0];
  if (!prospect) return { error: 'Prospect not found', status: 404 };
  if (Number(prospect.assigned_ao_id) !== Number(aoUserId)) {
    return { error: 'Prospect is not assigned to this AO', status: 403 };
  }
  return { prospect };
}

async function logProspectUpdate({
  clientId,
  aoUserId,
  prospectId,
  outcomeType,
  notes = null,
  nextAction = null,
  nextActionDueAt = null,
  advisoryStage = null,
  source = 'command_center',
  db = pool,
}) {
  await ensureAoFieldSchema();
  await ensureAoProspectRoutingSchema(db);

  if (!isValidOutcomeType(outcomeType)) {
    return { error: 'Valid outcome_type required', status: 400 };
  }

  const access = await ensureProspectAccess({ prospectId, clientId, aoUserId, db });
  if (access.error) return access;

  const candidateNextAction = nextAction || OUTCOME_TO_NEXT_ACTION[outcomeType] || null;
  const resolvedNextAction = DEBRIEF_NEXT_ACTIONS.includes(candidateNextAction)
    ? candidateNextAction
    : OUTCOME_TO_NEXT_ACTION[outcomeType] || null;
  const resolvedStage = advisoryStage || OUTCOME_TO_ADVISORY_STAGE[outcomeType] || null;
  const storedNotes = [
    notes ? String(notes).trim() : null,
    nextAction && !DEBRIEF_NEXT_ACTIONS.includes(nextAction) ? `Next: ${String(nextAction).trim()}` : null,
  ].filter(Boolean).join('\n') || null;
  const id = randomUUID();

  await db.query(`
    INSERT INTO ao_prospect_updates (
      id, tenant_id, ao_user_id, prospect_id, outcome_type, notes,
      next_action, next_action_due_at, advisory_stage
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
  `, [
    id,
    String(clientId),
    String(aoUserId),
    String(prospectId),
    outcomeType,
    storedNotes,
    candidateNextAction,
    nextActionDueAt || null,
    resolvedStage,
  ]);

  const updates = ['last_debrief_status = $2', 'updated_at = NOW()'];
  const params = [prospectId, outcomeType];
  if (resolvedNextAction) {
    params.push(resolvedNextAction);
    updates.push(`next_action = $${params.length}`);
  }
  if (nextActionDueAt) {
    params.push(nextActionDueAt);
    updates.push(`next_action_due_at = $${params.length}`);
  }
  params.push(String(aoUserId));
  updates.push(`next_action_owner = $${params.length}`);
  if (resolvedStage) {
    params.push(resolvedStage);
    updates.push(`advisory_stage = $${params.length}`);
  }
  params.push(clientId);
  await db.query(`
    UPDATE prospects
    SET ${updates.join(', ')}
    WHERE id = $1::uuid AND client_id = $${params.length}
  `, params);

  await logAoAuditEvent({
    event: 'AO_PROSPECT_UPDATE_LOGGED',
    clientId,
    aoUserId,
    prospectId,
    payload: {
      source,
      outcome_type: outcomeType,
      update_id: id,
      next_action: resolvedNextAction,
      next_action_due_at: nextActionDueAt || null,
    },
  });

  return {
    ok: true,
    update: {
      id,
      prospect_id: prospectId,
      outcome_type: outcomeType,
      notes: storedNotes,
      next_action: candidateNextAction,
      next_action_due_at: nextActionDueAt || null,
      advisory_stage: resolvedStage,
    },
  };
}

module.exports = {
  logProspectUpdate,
  ensureProspectAccess,
};
