'use strict';

const pool = require('../db');
const { ensureAoProspectRoutingSchema } = require('../utils/aoProspectRoutingSchema');
const { companyName, locationText } = require('./aoProspectTaskService');

async function prospectsToWorkToday({ clientId, aoOwnerId = null, db = pool }) {
  await ensureAoProspectRoutingSchema();
  const today = new Date().toISOString().slice(0, 10);
  const params = [clientId, today];
  let ownerClause = '';
  if (aoOwnerId) {
    params.push(aoOwnerId);
    ownerClause = `AND (t.assigned_ao_id = $${params.length} OR p.assigned_ao_id = $${params.length})`;
  }

  const { rows } = await db.query(`
    SELECT
      p.id AS prospect_id,
      p.prospect_motion,
      p.ao_fit_score,
      p.ao_assignment_category,
      p.assigned_ao_id,
      p.next_action,
      p.next_action_due_at,
      t.id AS task_id,
      t.deadline,
      t.priority,
      t.account_name,
      u.name AS assigned_ao_name
    FROM prospects p
    LEFT JOIN ao_prospect_tasks t
      ON t.prospect_id = p.id
      AND t.client_id = p.client_id
      AND t.status IN ('open', 'in_progress')
    LEFT JOIN users u ON u.id = COALESCE(t.assigned_ao_id, p.assigned_ao_id)
    WHERE p.client_id = $1
      AND COALESCE(p.prospect_motion, '') NOT IN ('SUPPRESS', 'EMAIL_LED')
      AND (
        t.deadline <= $2::date
        OR p.next_action_due_at::date <= $2::date
        OR (t.status IN ('open', 'in_progress') AND t.deadline IS NULL)
      )
      ${ownerClause}
    ORDER BY
      CASE t.priority WHEN 'warm' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
      COALESCE(t.deadline, p.next_action_due_at::date) ASC NULLS LAST
    LIMIT 50
  `, params);

  return rows.map(row => ({
    prospect_id: row.prospect_id,
    account: row.account_name,
    assigned_ao: row.assigned_ao_name,
    motion: row.prospect_motion,
    priority: row.priority || 'normal',
    task_id: row.task_id,
    due: row.deadline || row.next_action_due_at,
    next_action: row.next_action,
  }));
}

async function explainAssignment(prospectId, clientId, db = pool) {
  await ensureAoProspectRoutingSchema();
  const { rows } = await db.query(`
    SELECT
      p.*,
      c.name AS company_name,
      c.location AS company_location,
      u.name AS assigned_ao_name
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN users u ON u.id = p.assigned_ao_id
    WHERE p.id = $1 AND p.client_id = $2
    LIMIT 1
  `, [prospectId, clientId]);
  if (!rows[0]) return null;
  const p = rows[0];
  return {
    prospect_id: p.id,
    account: companyName(p, { name: p.company_name, location: p.company_location }),
    location: locationText(p, { location: p.company_location }),
    assigned_ao: p.assigned_ao_name,
    assigned_ao_id: p.assigned_ao_id,
    assignment_category: p.ao_assignment_category,
    assignment_reason: p.ao_assignment_reason,
    motion: p.prospect_motion,
    ao_fit_score: p.ao_fit_score,
    ao_fit_reason: p.ao_fit_reason,
    recommended_angle: p.recommended_angle,
    recommended_first_action: p.recommended_first_action,
  };
}

async function followUpRequired({ clientId, aoOwnerId = null, db = pool }) {
  const params = [clientId];
  let ownerClause = '';
  if (aoOwnerId) {
    params.push(aoOwnerId);
    ownerClause = `AND (p.assigned_ao_id = $${params.length} OR p.next_action_owner = $${params.length}::text)`;
  }
  const { rows } = await db.query(`
    SELECT
      p.id AS prospect_id,
      p.next_action,
      p.next_action_owner,
      p.next_action_due_at,
      p.advisory_stage,
      c.name AS company_name,
      u.name AS assigned_ao_name
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN users u ON u.id = p.assigned_ao_id
    WHERE p.client_id = $1
      AND p.next_action IN ('AO_FOLLOW_UP', 'SEND_INFO', 'NEEDS_RESEARCH')
      ${ownerClause}
    ORDER BY p.next_action_due_at ASC NULLS LAST
    LIMIT 100
  `, params);
  return rows;
}

async function accountsReadyForJake({ clientId, db = pool }) {
  const { rows } = await db.query(`
    SELECT
      p.id AS prospect_id,
      p.next_action,
      p.next_action_due_at,
      c.name AS company_name,
      d.opportunity_strength,
      d.problem_or_risk,
      d.recommended_next_step,
      u.name AS assigned_ao_name
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN users u ON u.id = p.assigned_ao_id
    LEFT JOIN LATERAL (
      SELECT *
      FROM ao_advisory_debriefs
      WHERE prospect_id = p.id AND client_id = p.client_id
      ORDER BY created_at DESC
      LIMIT 1
    ) d ON true
    WHERE p.client_id = $1
      AND p.next_action IN ('JAKE_REVIEW', 'BOOK_ASSESSMENT')
    ORDER BY p.next_action_due_at ASC NULLS LAST
    LIMIT 100
  `, [clientId]);
  return rows;
}

async function weakDebriefs({ clientId, db = pool }) {
  const { rows } = await db.query(`
    SELECT
      d.id,
      d.prospect_id,
      d.debrief_quality,
      d.coaching_feedback,
      d.prescribed_before_diagnosing,
      d.created_at,
      u.name AS ao_name,
      c.name AS company_name
    FROM ao_advisory_debriefs d
    JOIN prospects p ON p.id = d.prospect_id AND p.client_id = d.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    JOIN users u ON u.id = d.ao_owner_id
    WHERE d.client_id = $1
      AND (
        d.debrief_quality IN ('weak', 'incomplete')
        OR d.prescribed_before_diagnosing = true
      )
    ORDER BY d.created_at DESC
    LIMIT 100
  `, [clientId]);
  return rows;
}

async function prescribingBeforeDiagnosing({ clientId, db = pool }) {
  const { rows } = await db.query(`
    SELECT
      d.id,
      d.prospect_id,
      d.created_at,
      d.coaching_feedback,
      u.name AS ao_name,
      c.name AS company_name
    FROM ao_advisory_debriefs d
    JOIN prospects p ON p.id = d.prospect_id AND p.client_id = d.client_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    JOIN users u ON u.id = d.ao_owner_id
    WHERE d.client_id = $1
      AND d.prescribed_before_diagnosing = true
    ORDER BY d.created_at DESC
    LIMIT 100
  `, [clientId]);
  return rows;
}

async function nextBestAction(prospectId, clientId, db = pool) {
  const assignment = await explainAssignment(prospectId, clientId, db);
  if (!assignment) return null;
  const debrief = (await db.query(`
    SELECT next_action, recommended_next_step, recommended_message, follow_up_due_at, coaching_feedback
    FROM ao_advisory_debriefs
    WHERE prospect_id = $1 AND client_id = $2
    ORDER BY created_at DESC
    LIMIT 1
  `, [prospectId, clientId])).rows[0];

  return {
    ...assignment,
    next_best_action: debrief?.next_action || assignment.recommended_first_action,
    recommended_message: debrief?.recommended_message || null,
    follow_up_due_at: debrief?.follow_up_due_at || null,
    coaching_feedback: debrief?.coaching_feedback || null,
  };
}

async function answerInspectionQuestion(question, { clientId, aoOwnerId = null, prospectId = null, db = pool }) {
  const q = String(question || '').toLowerCase();
  if (/work today|contact today|should .* work/.test(q)) {
    return { intent: 'work_today', items: await prospectsToWorkToday({ clientId, aoOwnerId, db }) };
  }
  if (/why.*assign|why did .* get/.test(q) && prospectId) {
    return { intent: 'assignment_explanation', item: await explainAssignment(prospectId, clientId, db) };
  }
  if (/follow[- ]?up/.test(q)) {
    return { intent: 'follow_up_required', items: await followUpRequired({ clientId, aoOwnerId, db }) };
  }
  if (/jake|ready for jake|owner review/.test(q)) {
    return { intent: 'jake_review', items: await accountsReadyForJake({ clientId, db }) };
  }
  if (/weak debrief|incomplete debrief/.test(q)) {
    return { intent: 'weak_debriefs', items: await weakDebriefs({ clientId, db }) };
  }
  if (/prescrib.*before diagnos/.test(q)) {
    return { intent: 'prescribed_before_diagnosing', items: await prescribingBeforeDiagnosing({ clientId, db }) };
  }
  if (/next best action|next action for/.test(q) && prospectId) {
    return { intent: 'next_best_action', item: await nextBestAction(prospectId, clientId, db) };
  }
  return {
    intent: 'overview',
    work_today: await prospectsToWorkToday({ clientId, aoOwnerId, db }),
    follow_up_required: await followUpRequired({ clientId, aoOwnerId, db }),
    jake_review: await accountsReadyForJake({ clientId, db }),
    weak_debriefs: await weakDebriefs({ clientId, db }),
  };
}

module.exports = {
  prospectsToWorkToday,
  explainAssignment,
  followUpRequired,
  accountsReadyForJake,
  weakDebriefs,
  prescribingBeforeDiagnosing,
  nextBestAction,
  answerInspectionQuestion,
};
