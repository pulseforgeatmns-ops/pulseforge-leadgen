'use strict';

const pool = require('../db');
const {
  routeProspect,
  buildWeeklyListMix,
  companyName,
  locationText,
} = require('./aoProspectRoutingService');
const { ensureAoProspectRoutingSchema } = require('../utils/aoProspectRoutingSchema');
const { formatAoTask } = require('../utils/aoProspectTaskFormat');

async function fetchProspectBundle(prospectId, clientId, db = pool) {
  const { rows } = await db.query(`
    SELECT
      p.*,
      c.name AS company_name,
      c.location AS company_location,
      c.website AS company_website,
      c.industry AS company_industry
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.id = $1 AND p.client_id = $2
    LIMIT 1
  `, [prospectId, clientId]);
  if (!rows[0]) return null;

  const touchpoints = (await db.query(`
    SELECT action_type, channel, outcome, created_at
    FROM touchpoints
    WHERE prospect_id = $1 AND client_id = $2
    ORDER BY created_at DESC
    LIMIT 20
  `, [prospectId, clientId])).rows;

  const company = rows[0].company_id ? {
    name: rows[0].company_name,
    location: rows[0].company_location,
    website: rows[0].company_website,
    industry: rows[0].company_industry,
  } : null;

  return { prospect: rows[0], company, touchpoints };
}

async function fetchAvailableAos(clientId, db = pool) {
  const { rows } = await db.query(`
    SELECT
      u.id,
      u.name,
      u.email,
      u.territory,
      u.active,
      (
        SELECT COUNT(*)::int
        FROM ao_prospect_tasks t
        WHERE t.assigned_ao_id = u.id
          AND t.client_id = $1
          AND t.status IN ('open', 'in_progress')
      ) AS open_task_count
    FROM users u
    WHERE u.client_id = $1
      AND u.role = 'ao'
      AND u.active = true
    ORDER BY u.name ASC
  `, [clientId]);
  return rows;
}

async function persistRouting(prospectId, clientId, routing, db = pool) {
  await db.query(`
    UPDATE prospects SET
      prospect_motion = $3,
      ao_fit_score = $4,
      ao_fit_reason = $5,
      assigned_ao_id = $6,
      ao_assignment_reason = $7,
      ao_assignment_category = $8,
      recommended_angle = $9,
      recommended_first_action = $10,
      advisory_stage = CASE
        WHEN $3 = 'SUPPRESS' THEN 'closed'
        WHEN advisory_stage IN ('debrief_pending', 'debrief_complete') THEN advisory_stage
        ELSE 'routed'
      END,
      updated_at = NOW()
    WHERE id = $1 AND client_id = $2
  `, [
    prospectId,
    clientId,
    routing.recommended_motion,
    routing.ao_fit_score,
    routing.ao_fit_reason,
    routing.recommended_ao_id,
    routing.reasoning?.category_reason || null,
    routing.assignment_category,
    routing.recommended_angle,
    routing.recommended_first_action,
  ]);
}

async function createTaskFromRouting({
  clientId,
  prospectId,
  routing,
  deadline,
  db = pool,
}) {
  const formatted = formatAoTask(routing);
  const { rows } = await db.query(`
    INSERT INTO ao_prospect_tasks (
      client_id, prospect_id, assigned_ao_id, assignment_category, motion, priority,
      account_name, segment, location, why_account_matters, recommended_angle,
      first_action, discovery_objective, suggested_opener, desired_next_outcome,
      required_log_fields, deadline, required_debrief, routing_snapshot
    ) VALUES (
      $1, $2, $3, $4, $5, $6,
      $7, $8, $9, $10, $11,
      $12, $13, $14, $15,
      $16::jsonb, $17, $18, $19::jsonb
    )
    RETURNING *
  `, [
    clientId,
    prospectId,
    routing.recommended_ao_id,
    routing.assignment_category,
    routing.recommended_motion === 'SUPPRESS' ? 'NURTURE' : routing.recommended_motion,
    formatted.priority,
    formatted.account,
    formatted.segment,
    formatted.location,
    formatted.why_this_account_matters,
    formatted.recommended_angle,
    formatted.first_action,
    formatted.discovery_objective,
    formatted.suggested_opener,
    formatted.desired_next_outcome,
    JSON.stringify(formatted.what_to_log),
    deadline || null,
    routing.recommended_motion !== 'EMAIL_LED',
    JSON.stringify({ routing, formatted }),
  ]);

  await db.query(`
    UPDATE prospects SET advisory_stage = 'tasked', updated_at = NOW()
    WHERE id = $1 AND client_id = $2
  `, [prospectId, clientId]);

  return { task: rows[0], formatted };
}

async function routeAndPersistProspect({ clientId, prospectId, aoName, db = pool }) {
  await ensureAoProspectRoutingSchema();
  const bundle = await fetchProspectBundle(prospectId, clientId, db);
  if (!bundle) return null;

  const availableAos = await fetchAvailableAos(clientId, db);
  const routing = routeProspect({
    prospect: bundle.prospect,
    company: bundle.company,
    touchpoints: bundle.touchpoints,
    availableAos,
    aoName,
    existingAssignment: bundle.prospect,
  });

  await persistRouting(prospectId, clientId, routing, db);
  return {
    prospect_id: prospectId,
    routing,
    formatted_task: formatAoTask(routing),
  };
}

async function generateWeeklyAoTasks({ clientId, prospectIds = null, db = pool }) {
  await ensureAoProspectRoutingSchema();
  const availableAos = await fetchAvailableAos(clientId, db);

  let ids = prospectIds;
  if (!ids) {
    const { rows } = await db.query(`
      SELECT p.id
      FROM prospects p
      WHERE p.client_id = $1
        AND COALESCE(p.do_not_contact, false) = false
        AND COALESCE(p.icp_score, 0) >= 40
        AND p.prospect_motion IS DISTINCT FROM 'SUPPRESS'
      ORDER BY p.icp_score DESC NULLS LAST, p.created_at DESC
      LIMIT 200
    `, [clientId]);
    ids = rows.map(r => r.id);
  }

  const routed = [];
  for (const prospectId of ids) {
    const bundle = await fetchProspectBundle(prospectId, clientId, db);
    if (!bundle) continue;
    const routing = routeProspect({
      prospect: bundle.prospect,
      company: bundle.company,
      touchpoints: bundle.touchpoints,
      availableAos,
      existingAssignment: bundle.prospect,
    });
    if (routing.recommended_motion === 'SUPPRESS' || routing.recommended_motion === 'EMAIL_LED') {
      await persistRouting(prospectId, clientId, routing, db);
      continue;
    }
    if (!routing.recommended_ao_id) continue;
    routed.push({ prospectId, routing });
  }

  const weekly = buildWeeklyListMix(routed);
  const created = [];
  for (const entry of weekly) {
    const prospectId = entry.prospectId;
    if (!prospectId || !entry.recommended_ao_id) continue;
    const { prospectId: _ignored, deadline, required_debrief, ...routing } = entry;
    await persistRouting(prospectId, clientId, routing, db);
    const result = await createTaskFromRouting({
      clientId,
      prospectId,
      routing,
      deadline,
      db,
    });
    created.push(result);
  }

  return { created_count: created.length, tasks: created };
}

async function getTaskById(taskId, { clientId, db = pool }) {
  const { rows } = await db.query(`
    SELECT t.*, u.name AS assigned_ao_name
    FROM ao_prospect_tasks t
    JOIN users u ON u.id = t.assigned_ao_id
    WHERE t.id = $1 AND t.client_id = $2
    LIMIT 1
  `, [taskId, clientId]);
  if (!rows[0]) return null;
  const task = rows[0];
  return {
    ...task,
    formatted: {
      account: task.account_name,
      segment: task.segment,
      location: task.location,
      assigned_ao: task.assigned_ao_name,
      motion: task.motion,
      priority: task.priority,
      why_this_account_matters: task.why_account_matters,
      recommended_angle: task.recommended_angle,
      first_action: task.first_action,
      discovery_objective: task.discovery_objective,
      suggested_opener: task.suggested_opener,
      desired_next_outcome: task.desired_next_outcome,
      what_to_log: task.required_log_fields,
    },
  };
}

async function listOpenTasks({ clientId, aoOwnerId = null, db = pool }) {
  const params = [clientId];
  let ownerClause = '';
  if (aoOwnerId) {
    params.push(aoOwnerId);
    ownerClause = `AND t.assigned_ao_id = $${params.length}`;
  }
  const { rows } = await db.query(`
    SELECT t.*, u.name AS assigned_ao_name, p.prospect_motion, p.ao_fit_score
    FROM ao_prospect_tasks t
    JOIN users u ON u.id = t.assigned_ao_id
    JOIN prospects p ON p.id = t.prospect_id AND p.client_id = t.client_id
    WHERE t.client_id = $1
      AND t.status IN ('open', 'in_progress')
      ${ownerClause}
    ORDER BY
      CASE t.priority WHEN 'warm' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
      t.deadline ASC NULLS LAST,
      t.created_at ASC
  `, params);
  return rows;
}

module.exports = {
  formatAoTask,
  fetchProspectBundle,
  fetchAvailableAos,
  persistRouting,
  createTaskFromRouting,
  routeAndPersistProspect,
  generateWeeklyAoTasks,
  getTaskById,
  listOpenTasks,
  companyName,
  locationText,
};
