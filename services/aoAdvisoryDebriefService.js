'use strict';

const pool = require('../db');
const { evaluateDebrief } = require('./aoAdvisoryDebriefEvaluator');
const { ensureAoProspectRoutingSchema } = require('../utils/aoProspectRoutingSchema');
const { inTransaction } = require('./aoProspectTaskService');

function debriefError(code, statusCode = 409) {
  const error = new Error(code);
  error.code = code;
  error.statusCode = statusCode;
  return error;
}

function mapDebriefRow(row) {
  if (!row) return null;
  return {
    ...row,
    evaluation: row.evaluation || {},
  };
}

async function submitDebrief({
  clientId,
  prospectId,
  taskId = null,
  aoOwnerId,
  debrief,
  db = pool,
  ensureSchema = true,
}) {
  if (ensureSchema) await ensureAoProspectRoutingSchema(db);
  return inTransaction(db, async client => {
  const prospect = (await client.query(`
    SELECT id, client_id, assigned_ao_id, prospect_motion, do_not_contact
    FROM prospects
    WHERE id = $1 AND client_id = $2
    FOR UPDATE
  `, [prospectId, clientId])).rows[0];
  if (!prospect) throw debriefError('prospect_not_found', 404);
  if (Number(prospect.assigned_ao_id) !== Number(aoOwnerId)) {
    throw debriefError('prospect_not_owned_by_ao', 403);
  }
  if (prospect.do_not_contact || prospect.prospect_motion === 'SUPPRESS') {
    throw debriefError('prospect_suppressed', 409);
  }
  const owner = (await client.query(`
    SELECT id FROM users
    WHERE id = $1 AND client_id = $2 AND role = 'ao' AND active = true
  `, [aoOwnerId, clientId])).rows[0];
  if (!owner) throw debriefError('ao_owner_not_active_for_tenant', 403);
  if (taskId) {
    const task = (await client.query(`
      SELECT id FROM ao_prospect_tasks
      WHERE id = $1 AND client_id = $2 AND prospect_id = $3 AND assigned_ao_id = $4
        AND status IN ('open', 'in_progress')
      FOR UPDATE
    `, [taskId, clientId, prospectId, aoOwnerId])).rows[0];
    if (!task) throw debriefError('task_not_owned_or_mismatched', 403);
  }
  const evaluation = evaluateDebrief(debrief, { defaultOwnerId: aoOwnerId });

  const { rows } = await client.query(`
    INSERT INTO ao_advisory_debriefs (
      client_id, prospect_id, task_id, ao_owner_id,
      person_spoken_to, role, decision_maker, current_cleaning_solution, stated_context,
      problem_or_risk, opportunity_timing, opportunity_type, opportunity_strength, blocker,
      recommended_next_step, recommended_message, follow_up_due_at, next_owner,
      prescribed_before_diagnosing, real_reason_to_continue, specific_dated_next_step,
      next_action, coaching_feedback, debrief_quality, evaluation
    ) VALUES (
      $1, $2, $3, $4,
      $5, $6, $7, $8, $9,
      $10, $11, $12, $13, $14,
      $15, $16, $17, $18,
      $19, $20, $21,
      $22, $23, $24, $25::jsonb
    )
    RETURNING *
  `, [
    clientId,
    prospectId,
    taskId,
    aoOwnerId,
    debrief.person_spoken_to || null,
    debrief.role || null,
    debrief.decision_maker || null,
    debrief.current_cleaning_solution || null,
    debrief.stated_context || null,
    debrief.problem_or_risk || null,
    debrief.opportunity_timing || null,
    debrief.opportunity_type || null,
    debrief.opportunity_strength || null,
    debrief.blocker || null,
    debrief.recommended_next_step || null,
    debrief.recommended_message || null,
    evaluation.follow_up_due_at,
    evaluation.next_action_owner,
    debrief.prescribed_before_diagnosing ?? null,
    debrief.real_reason_to_continue ?? null,
    Boolean(evaluation.follow_up_due_at),
    evaluation.next_action,
    evaluation.coaching_feedback,
    evaluation.debrief_quality,
    JSON.stringify(evaluation),
  ]);

  const saved = mapDebriefRow(rows[0]);

  const prospectUpdate = await client.query(`
    UPDATE prospects SET
      last_debrief_status = $3,
      next_action = $4,
      next_action_owner = $5,
      next_action_due_at = $6,
      advisory_stage = CASE
        WHEN $4 = 'SUPPRESS' THEN 'closed'
        WHEN $7 = true THEN 'debrief_pending'
        ELSE 'debrief_complete'
      END,
      prospect_motion = CASE
        WHEN $4 IN ('NURTURE') THEN 'NURTURE'
        WHEN $4 = 'SUPPRESS' THEN 'SUPPRESS'
        ELSE prospect_motion
      END,
      updated_at = NOW()
    WHERE id = $1 AND client_id = $2 AND assigned_ao_id = $8
    RETURNING id
  `, [
    prospectId,
    clientId,
    evaluation.debrief_quality,
    evaluation.next_action,
    evaluation.next_action_owner,
    saved.follow_up_due_at,
    evaluation.incomplete,
    aoOwnerId,
  ]);
  if (!prospectUpdate.rows[0]) throw debriefError('prospect_ownership_changed', 409);

  if (taskId) {
    const taskUpdate = await client.query(`
      UPDATE ao_prospect_tasks SET
        status = CASE WHEN $3 = 'SUPPRESS' THEN 'cancelled' WHEN $4 = true THEN 'in_progress' ELSE 'completed' END,
        completed_at = CASE WHEN $4 = true AND $3 <> 'SUPPRESS' THEN NULL ELSE NOW() END
      WHERE id = $1 AND client_id = $2 AND prospect_id = $5 AND assigned_ao_id = $6
      RETURNING id
    `, [taskId, clientId, evaluation.next_action, evaluation.incomplete, prospectId, aoOwnerId]);
    if (!taskUpdate.rows[0]) throw debriefError('task_ownership_changed', 409);
  }
  if (evaluation.next_action === 'SUPPRESS') {
    await client.query(`
      UPDATE ao_prospect_tasks SET status = 'cancelled', completed_at = NOW()
      WHERE client_id = $1 AND prospect_id = $2 AND status IN ('open', 'in_progress')
    `, [clientId, prospectId]);
  }

  return {
    debrief: saved,
    evaluation,
    debrief_result: {
      next_action: evaluation.next_action,
      reason: evaluation.classification_reason,
      recommended_next_step: debrief.recommended_next_step || null,
      suggested_message: evaluation.suggested_message,
      coaching_feedback: evaluation.coaching_feedback,
      debrief_quality: evaluation.debrief_quality,
    },
  };
  });
}

async function getLatestDebrief(prospectId, clientId, db = pool) {
  const { rows } = await db.query(`
    SELECT *
    FROM ao_advisory_debriefs
    WHERE prospect_id = $1 AND client_id = $2
    ORDER BY created_at DESC
    LIMIT 1
  `, [prospectId, clientId]);
  return mapDebriefRow(rows[0]);
}

async function listDebriefs({ clientId, aoOwnerId = null, limit = 50, db = pool }) {
  const params = [clientId, Math.min(limit, 200)];
  let ownerClause = '';
  if (aoOwnerId) {
    params.push(aoOwnerId);
    ownerClause = `AND d.ao_owner_id = $${params.length}`;
  }
  const { rows } = await db.query(`
    SELECT d.*, p.first_name, p.last_name, c.name AS company_name
    FROM ao_advisory_debriefs d
    JOIN prospects p ON p.id = d.prospect_id AND p.client_id = d.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE d.client_id = $1
      ${ownerClause}
    ORDER BY d.created_at DESC
    LIMIT $2
  `, params);
  return rows.map(mapDebriefRow);
}

module.exports = {
  submitDebrief,
  getLatestDebrief,
  listDebriefs,
  evaluateDebrief,
  debriefError,
};
