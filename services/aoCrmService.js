'use strict';

const pool = require('../db');
const { ensureAoCrmSchema } = require('../utils/aoCrmSchema');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const { formatDateInTz } = require('./aoCommandCenterService');
const { isOverdue, isSameDay, endOfDay } = require('../utils/aoCommandCenterRanking');
const { logProspectUpdate, ensureProspectAccess } = require('./aoProspectUpdateService');
const {
  AO_CRM_OUTCOMES,
  AO_CRM_NEXT_ACTIONS,
  OUTCOME_TO_STATUS,
  OUTCOME_TO_TOUCH_TYPE,
  CRM_NEXT_TO_LEGACY,
  CLOSED_STATUSES,
  deriveDefaultStatus,
  accountIsActive,
  isValidCrmOutcome,
  isValidCrmNextAction,
  isValidCrmStatus,
} = require('../utils/aoCrmTypes');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');
const { loadIdentityForAssignedAo } = require('../utils/aoCommunicationIdentity');
const { ensureAoCommunicationIdentitySchema } = require('../utils/aoCommunicationIdentitySchema');
const {
  isAoEligibleForAssignment,
  shouldExcludeFromTodayQueue,
  transferredReviewLabel,
  isTransferredReviewAccount,
} = require('../utils/aoRosterOperational');

const WARM_STATUSES = new Set(['warm', 'walkthrough_target', 'walkthrough_booked', 'proposal_needed', 'proposal_sent']);
const WARM_STAGES = new Set(['walkthrough', 'proposal', 'engaged']);

function normalizeDateOnly(value) {
  if (!value) return null;
  const str = String(value).trim();
  const match = str.match(/^(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : null;
}

function formatAccountRow(row, { dateStr, aoNameById = {} }) {
  const status = deriveDefaultStatus(row);
  const nextDue = row.next_action_due_at || row.ao_next_action_due || null;
  const overdue = nextDue && isOverdue(nextDue, dateStr);
  const dueToday = nextDue && isSameDay(nextDue, dateStr);
  return {
    prospect_id: row.prospect_id,
    company_name: row.company_name || row.account_name || 'Unknown account',
    current_status: status,
    opportunity_stage: row.opportunity_stage || null,
    priority: row.ao_account_priority || row.task_priority || 'normal',
    recommended_angle: row.recommended_angle || null,
    why_account_matters: row.ao_why_account_matters || row.why_account_matters || row.ao_assignment_reason || null,
    last_touch_at: row.ao_last_touch_at || row.last_touch_at || null,
    last_touch_type: row.ao_last_touch_type || null,
    last_outcome: row.ao_last_outcome || row.last_debrief_status || null,
    next_action: row.ao_next_action || row.next_action || null,
    next_action_date: normalizeDateOnly(nextDue),
    next_action_due_at: nextDue,
    assigned_ao_id: row.assigned_ao_id != null ? String(row.assigned_ao_id) : null,
    assigned_ao_name: aoNameById[row.assigned_ao_id] || null,
    help_requested: Boolean(row.help_requested),
    help_reason: row.help_reason || null,
    latest_note_preview: row.latest_note_preview || null,
    overdue,
    due_today: dueToday,
    ao_paused: Boolean(row.ao_paused),
    open_task_id: row.open_task_id || null,
    task_deadline: normalizeDateOnly(row.task_deadline),
    segment: row.segment || row.vertical || null,
    ao_review_bucket: row.ao_review_bucket || null,
    transferred_review_label: transferredReviewLabel(row.ao_review_bucket),
    ao_reassignment_prior_ao_id: row.ao_reassignment_prior_ao_id != null
      ? String(row.ao_reassignment_prior_ao_id)
      : null,
    ao_reassignment_prior_ao_name: row.ao_reassignment_prior_ao_name || null,
    ao_reassignment_at: row.ao_reassignment_at || null,
    ao_reassignment_reason: row.ao_reassignment_reason || null,
  };
}

async function fetchAccountRows({ clientId, aoUserId = null, db = pool }) {
  const params = [clientId];
  let ownerClause = '';
  if (aoUserId) {
    params.push(aoUserId);
    ownerClause = `AND p.assigned_ao_id = $${params.length}`;
  }

  const { rows } = await db.query(`
    SELECT
      p.id AS prospect_id,
      p.assigned_ao_id,
      p.ao_current_status,
      p.opportunity_stage,
      p.ao_account_priority,
      p.ao_why_account_matters,
      p.ao_next_action,
      p.ao_last_touch_at,
      p.ao_last_touch_type,
      p.ao_last_outcome,
      p.help_requested,
      p.help_reason,
      p.help_requested_at,
      p.ao_paused,
      p.ao_review_bucket,
      p.ao_reassignment_prior_ao_id,
      p.ao_reassignment_at,
      p.ao_reassignment_reason,
      prior_ao.name AS ao_reassignment_prior_ao_name,
      p.ao_disqualification_reason,
      p.recommended_angle,
      p.ao_assignment_reason,
      p.next_action,
      p.next_action_due_at,
      p.last_debrief_status,
      p.advisory_stage,
      p.vertical,
      c.name AS company_name,
      c.website AS company_website,
      c.location AS company_location,
      c.industry AS company_industry,
      t.account_name,
      t.segment,
      t.why_account_matters,
      t.priority AS task_priority,
      t.id AS open_task_id,
      t.deadline AS task_deadline,
      (
        SELECT notes FROM ao_prospect_activity a
        WHERE a.prospect_id = p.id AND a.tenant_id = p.client_id
        ORDER BY a.created_at DESC LIMIT 1
      ) AS latest_note_preview,
      (
        SELECT MAX(created_at) FROM ao_prospect_updates u
        WHERE u.prospect_id = p.id::text AND u.tenant_id = p.client_id::text
      ) AS last_update_at,
      GREATEST(
        p.updated_at,
        COALESCE((
          SELECT MAX(created_at) FROM ao_prospect_activity a
          WHERE a.prospect_id = p.id AND a.tenant_id = p.client_id
        ), p.updated_at)
      ) AS recently_updated_at
    FROM prospects p
    LEFT JOIN users prior_ao ON prior_ao.id = p.ao_reassignment_prior_ao_id
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN LATERAL (
      SELECT *
      FROM ao_prospect_tasks apt
      WHERE apt.prospect_id = p.id
        AND apt.client_id = p.client_id
        AND apt.status IN ('open', 'in_progress')
      ORDER BY apt.created_at DESC
      LIMIT 1
    ) t ON true
    WHERE p.client_id = $1
      ${ownerClause}
      AND p.assigned_ao_id IS NOT NULL
      AND COALESCE(p.do_not_contact, false) = false
      AND COALESCE(p.prospect_motion, '') NOT IN ('SUPPRESS', 'EMAIL_LED')
    ORDER BY p.updated_at DESC
  `, params);

  return rows.map(row => ({
    ...row,
    last_touch_at: row.ao_last_touch_at || row.last_update_at || null,
  }));
}

async function loadAoNames(clientId, db = pool) {
  const { rows } = await db.query(`
    SELECT id, name FROM users WHERE client_id = $1 AND role = 'ao'
  `, [clientId]);
  const map = {};
  for (const row of rows) map[row.id] = row.name;
  return map;
}

function buildTodayQueue(accounts, dateStr, { ownerOperationallyActive = true } = {}) {
  if (!ownerOperationallyActive) return [];
  return accounts.filter(a => {
    if (shouldExcludeFromTodayQueue(a)) return false;
    if (!accountIsActive({ ao_current_status: a.current_status, ao_paused: a.ao_paused })) return false;
    if (a.help_requested) return true;
    if (a.overdue || a.due_today) return true;
    if (a.open_task_id && a.task_deadline && a.task_deadline <= dateStr) return true;
    if (a.current_status === 'ready_to_call' && !a.last_touch_at) return true;
    return false;
  });
}

async function getAoCrmDashboard({ clientId, aoUserId, aoUserName, date = null, db = pool }) {
  await ensureAoFieldSchema();
  await ensureAoCrmSchema(db);

  const dateStr = date || formatDateInTz();
  const aoNameById = await loadAoNames(clientId, db);
  const { rows: ownerRows } = await db.query(`
    SELECT id, name, active, COALESCE(ao_operational_status, 'active') AS ao_operational_status
    FROM users WHERE id = $1 AND client_id = $2 LIMIT 1
  `, [aoUserId, clientId]);
  const ownerProfile = ownerRows[0] || null;
  const ownerOperationallyActive = isAoEligibleForAssignment(ownerProfile);

  const raw = await fetchAccountRows({ clientId, aoUserId, db });
  const accounts = raw.map(row => formatAccountRow(row, { dateStr, aoNameById }));

  const todayQueue = buildTodayQueue(
    raw.map(row => formatAccountRow(row, { dateStr, aoNameById })),
    dateStr,
    { ownerOperationallyActive },
  );
  const needsReassignment = accounts.filter(a => isTransferredReviewAccount(a));
  const overdue = accounts.filter(a => a.overdue && accountIsActive({ ao_current_status: a.current_status, ao_paused: a.ao_paused }));
  const needsHelp = accounts.filter(a => a.help_requested);
  const warm = accounts.filter(a =>
    WARM_STATUSES.has(a.current_status) || WARM_STAGES.has(a.opportunity_stage)
  );
  const recentlyUpdated = [...accounts]
    .sort((a, b) => new Date(b.last_touch_at || 0) - new Date(a.last_touch_at || 0))
    .slice(0, 25);

  await logAoAuditEvent({
    event: 'AO_CRM_DASHBOARD_VIEWED',
    clientId,
    aoUserId,
    payload: {
      date: dateStr,
      account_count: accounts.length,
      queue_count: todayQueue.length,
    },
  });

  return {
    date: dateStr,
    tenant_id: String(clientId),
    ao_user: { id: String(aoUserId), name: aoUserName },
    outcomes: AO_CRM_OUTCOMES,
    next_actions: AO_CRM_NEXT_ACTIONS,
    summary: {
      my_accounts: accounts.length,
      today_queue: todayQueue.length,
      overdue: overdue.length,
      needs_help: needsHelp.length,
      warm: warm.length,
      needs_reassignment: needsReassignment.length,
      ao_operational_status: ownerProfile?.ao_operational_status || 'active',
      ao_queue_eligible: ownerOperationallyActive,
    },
    sections: {
      today_queue: todayQueue,
      needs_reassignment: needsReassignment,
      my_accounts: accounts,
      overdue,
      needs_help: needsHelp,
      warm_active: warm,
      recently_updated: recentlyUpdated,
    },
  };
}

async function listManagerAccounts({
  clientId,
  filters = {},
  db = pool,
}) {
  await ensureAoCrmSchema(db);
  const dateStr = filters.date || formatDateInTz();
  const aoNameById = await loadAoNames(clientId, db);
  let accounts = (await fetchAccountRows({ clientId, aoUserId: filters.ao_owner_id || null, db }))
    .map(row => formatAccountRow(row, { dateStr, aoNameById }));

  if (filters.status) {
    accounts = accounts.filter(a => a.current_status === filters.status);
  }
  if (filters.priority) {
    accounts = accounts.filter(a => a.priority === filters.priority);
  }
  if (filters.help_requested === 'true') {
    accounts = accounts.filter(a => a.help_requested);
  }
  if (filters.overdue === 'true') {
    accounts = accounts.filter(a => a.overdue);
  }
  if (filters.warm === 'true') {
    accounts = accounts.filter(a =>
      WARM_STATUSES.has(a.current_status) || WARM_STAGES.has(a.opportunity_stage)
    );
  }
  if (filters.no_next_action === 'true') {
    accounts = accounts.filter(a => !a.next_action && !CLOSED_STATUSES.has(a.current_status) && !a.ao_paused);
  }
  if (filters.next_action_due === 'today') {
    accounts = accounts.filter(a => a.due_today);
  }
  if (filters.recently_updated === 'true') {
    const cutoff = new Date(`${dateStr}T00:00:00`);
    cutoff.setDate(cutoff.getDate() - 7);
    accounts = accounts.filter(a => a.last_touch_at && new Date(a.last_touch_at) >= cutoff);
  }

  const startOfToday = new Date(`${dateStr}T00:00:00`);
  const summary = {
    total: accounts.length,
    touched_today: accounts.filter(a => a.last_touch_at && new Date(a.last_touch_at) >= startOfToday).length,
    overdue: accounts.filter(a => a.overdue).length,
    help_requested: accounts.filter(a => a.help_requested).length,
    warm: accounts.filter(a =>
      WARM_STATUSES.has(a.current_status) || WARM_STAGES.has(a.opportunity_stage)
    ).length,
    no_next_action: accounts.filter(a =>
      !a.next_action && !CLOSED_STATUSES.has(a.current_status) && !a.ao_paused
    ).length,
  };

  return { date: dateStr, accounts, filters, summary };
}

async function getAccountDetail({ clientId, prospectId, aoUserId = null, db = pool }) {
  await ensureAoCrmSchema(db);
  const params = [prospectId, clientId];
  let ownerClause = '';
  if (aoUserId) {
    params.push(aoUserId);
    ownerClause = `AND p.assigned_ao_id = $${params.length}`;
  }

  const { rows } = await db.query(`
    SELECT
      p.*,
      c.name AS company_name,
      c.website AS company_website,
      c.location AS company_location,
      c.industry AS company_industry,
      u.name AS assigned_ao_name
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN users u ON u.id = p.assigned_ao_id
    WHERE p.id = $1::uuid AND p.client_id = $2
      ${ownerClause}
    LIMIT 1
  `, params);
  if (!rows[0]) return null;

  const prospect = rows[0];
  const activity = (await db.query(`
    SELECT *
    FROM ao_prospect_activity
    WHERE prospect_id = $1::uuid AND tenant_id = $2
    ORDER BY created_at DESC
    LIMIT 100
  `, [prospectId, clientId])).rows;

  const touchpoints = (await db.query(`
    SELECT channel, action_type, outcome, content_summary, created_at
    FROM touchpoints
    WHERE prospect_id = $1::uuid AND client_id = $2
    ORDER BY created_at DESC
    LIMIT 30
  `, [prospectId, clientId])).rows;

  const openTask = (await db.query(`
    SELECT id, first_action, suggested_opener, deadline, status, why_account_matters
    FROM ao_prospect_tasks
    WHERE prospect_id = $1 AND client_id = $2 AND status IN ('open', 'in_progress')
    ORDER BY created_at DESC LIMIT 1
  `, [prospectId, clientId])).rows[0] || null;

  const status = deriveDefaultStatus(prospect);
  await ensureAoCommunicationIdentitySchema(db);
  const { assignedAo } = await loadIdentityForAssignedAo(db, {
    assignedAoId: prospect.assigned_ao_id,
    tenantId: clientId,
  });

  return {
    prospect_id: prospect.id,
    assignedAo,
    summary: {
      company_name: prospect.company_name || 'Unknown account',
      website: prospect.company_website || null,
      address: prospect.company_location || null,
      icp_category: prospect.vertical || prospect.company_industry || null,
      ao_owner: prospect.assigned_ao_name || null,
      assigned_ao_id: prospect.assigned_ao_id,
      priority: prospect.ao_account_priority || 'normal',
      current_status: status,
      opportunity_stage: prospect.opportunity_stage || null,
      recommended_angle: prospect.recommended_angle || null,
      why_account_matters: prospect.ao_why_account_matters || openTask?.why_account_matters || prospect.ao_assignment_reason || null,
    },
    contact: {
      name: [prospect.first_name, prospect.last_name].filter(Boolean).join(' ') || null,
      role: prospect.job_title || null,
      phone: prospect.phone || null,
      email: prospect.email || null,
      linkedin_url: prospect.linkedin_url || null,
    },
    sales_state: {
      current_status: status,
      next_action: prospect.ao_next_action || prospect.next_action || null,
      next_action_date: normalizeDateOnly(prospect.next_action_due_at),
      next_action_due_at: prospect.next_action_due_at || null,
      last_touch_at: prospect.ao_last_touch_at || null,
      last_outcome: prospect.ao_last_outcome || prospect.last_debrief_status || null,
      help_requested: Boolean(prospect.help_requested),
      help_reason: prospect.help_reason || null,
      disqualification_reason: prospect.ao_disqualification_reason || null,
      ao_paused: Boolean(prospect.ao_paused),
    },
    open_task: openTask,
    history: [
      ...activity.map(a => ({
        kind: 'activity',
        activity_type: a.activity_type,
        outcome: a.outcome,
        notes: a.notes,
        previous_status: a.previous_status,
        new_status: a.new_status,
        created_at: a.created_at,
      })),
      ...touchpoints.map(t => ({
        kind: 'touchpoint',
        activity_type: t.action_type || t.channel,
        outcome: t.outcome,
        notes: t.content_summary,
        created_at: t.created_at,
      })),
    ].sort((a, b) => new Date(b.created_at) - new Date(a.created_at)).slice(0, 100),
  };
}

async function insertActivity({
  clientId,
  prospectId,
  aoUserId,
  activityType,
  outcome,
  notes,
  previousStatus,
  newStatus,
  previousNextAction,
  newNextAction,
  nextActionDate,
  metadata = {},
  db = pool,
}) {
  await db.query(`
    INSERT INTO ao_prospect_activity (
      prospect_id, tenant_id, ao_id, activity_type, outcome, notes,
      previous_status, new_status, previous_next_action, new_next_action,
      next_action_date, metadata
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
  `, [
    prospectId,
    clientId,
    aoUserId,
    activityType,
    outcome || null,
    notes || null,
    previousStatus || null,
    newStatus || null,
    previousNextAction || null,
    newNextAction || null,
    nextActionDate || null,
    JSON.stringify(metadata || {}),
  ]);
}

async function submitOutcome({
  clientId,
  aoUserId,
  prospectId,
  outcome,
  notes,
  nextAction,
  nextActionDate,
  helpNeeded = false,
  helpReason = null,
  statusOverride = null,
  taskId = null,
  followUpTaskId = null,
  contactPatch = {},
  source = 'ao_crm',
  db = pool,
}) {
  await ensureAoCrmSchema(db);

  if (!isValidCrmOutcome(outcome)) {
    return { error: 'Valid outcome required', status: 400, outcomes: AO_CRM_OUTCOMES };
  }
  if (!notes || !String(notes).trim()) {
    return { error: 'Notes required', status: 400 };
  }
  if (!nextAction && !helpNeeded && !['bad_fit', 'not_interested'].includes(outcome)) {
    return { error: 'Next action required unless closing or requesting help', status: 400 };
  }
  if (nextAction && !isValidCrmNextAction(nextAction)) {
    return { error: 'Invalid next action', status: 400, next_actions: AO_CRM_NEXT_ACTIONS };
  }
  if (statusOverride && !isValidCrmStatus(statusOverride)) {
    return { error: 'Invalid status', status: 400 };
  }

  const access = await ensureProspectAccess({ prospectId, clientId, aoUserId, db });
  if (access.error) return access;

  const { rows: beforeRows } = await db.query(`
    SELECT ao_current_status, ao_next_action, next_action, help_requested
    FROM prospects WHERE id = $1::uuid AND client_id = $2
  `, [prospectId, clientId]);
  const before = beforeRows[0] || {};

  const internalOutcome = require('../utils/aoCrmTypes').resolveInternalOutcome(outcome);
  const newStatus = statusOverride || OUTCOME_TO_STATUS[outcome] || deriveDefaultStatus(before);
  const touchType = OUTCOME_TO_TOUCH_TYPE[outcome] || 'note';
  const legacyNext = nextAction ? CRM_NEXT_TO_LEGACY[nextAction] : null;
  const dueIso = nextActionDate
    ? new Date(`${normalizeDateOnly(nextActionDate)}T17:00:00`).toISOString()
    : null;

  const help = helpNeeded || outcome === 'needs_jake';

  await logProspectUpdate({
    clientId,
    aoUserId,
    prospectId,
    outcomeType: internalOutcome,
    notes,
    nextAction: legacyNext || undefined,
    nextActionDueAt: dueIso,
    source,
    db,
  });

  const disqualReason = outcome === 'bad_fit' ? (notes || 'Bad fit') : null;
  const opportunityStage = outcome === 'booked_walkthrough'
    ? 'walkthrough'
    : outcome === 'interested'
      ? 'engaged'
      : outcome === 'bad_fit'
        ? 'disqualified'
        : undefined;

  let contactFirst = contactPatch.contact_name || null;
  if (contactFirst && contactFirst.includes(' ')) {
    const parts = contactFirst.trim().split(/\s+/);
    contactFirst = parts[0];
    if (!contactPatch.contact_last_name) contactPatch.contact_last_name = parts.slice(1).join(' ');
  }

  await db.query(`
    UPDATE prospects SET
      ao_current_status = $3,
      ao_next_action = $4,
      ao_last_touch_at = NOW(),
      ao_last_touch_type = $5,
      ao_last_outcome = $6,
      help_requested = CASE WHEN $7 THEN true ELSE help_requested END,
      help_reason = CASE WHEN $7 THEN COALESCE($8, help_reason) ELSE help_reason END,
      help_requested_at = CASE WHEN $7 AND help_requested_at IS NULL THEN NOW() ELSE help_requested_at END,
      help_resolved_at = CASE WHEN $7 THEN NULL ELSE help_resolved_at END,
      ao_disqualification_reason = COALESCE($9, ao_disqualification_reason),
      opportunity_stage = COALESCE($10, opportunity_stage),
      ao_paused = CASE WHEN $4 = 'no_action' THEN true ELSE false END,
      first_name = COALESCE($11, first_name),
      last_name = COALESCE($12, last_name),
      job_title = COALESCE($13, job_title),
      phone = COALESCE($14, phone),
      email = COALESCE($15, email),
      updated_at = NOW()
    WHERE id = $1::uuid AND client_id = $2
  `, [
    prospectId,
    clientId,
    newStatus,
    nextAction || (outcome === 'bad_fit' ? 'disqualify' : null),
    touchType,
    internalOutcome,
    help,
    helpReason || (outcome === 'needs_jake' ? notes : null),
    disqualReason,
    opportunityStage || null,
    contactFirst,
    contactPatch.contact_last_name || null,
    contactPatch.contact_role || null,
    contactPatch.phone || null,
    contactPatch.email || null,
  ]);

  await insertActivity({
    clientId,
    prospectId,
    aoUserId,
    activityType: outcome === 'bad_fit' ? 'disqualified' : (help ? 'help_requested' : 'outcome_logged'),
    outcome,
    notes,
    previousStatus: before.ao_current_status,
    newStatus,
    previousNextAction: before.ao_next_action || before.next_action,
    newNextAction: nextAction,
    nextActionDate: normalizeDateOnly(nextActionDate),
    metadata: { internal_outcome: internalOutcome, source },
    db,
  });

  if (taskId) {
    await db.query(`
      UPDATE ao_prospect_tasks
      SET status = 'completed', completed_at = NOW()
      WHERE id = $1 AND client_id = $2 AND prospect_id = $3::uuid
        AND assigned_ao_id = $4 AND status IN ('open', 'in_progress')
    `, [taskId, clientId, prospectId, aoUserId]);
  } else {
    await db.query(`
      UPDATE ao_prospect_tasks
      SET status = 'completed', completed_at = NOW()
      WHERE client_id = $1 AND prospect_id = $2::uuid
        AND assigned_ao_id = $3 AND status IN ('open', 'in_progress')
    `, [clientId, prospectId, aoUserId]);
  }

  if (followUpTaskId) {
    const aoField = require('./aoFieldService');
    await aoField.completeFollowUpTask(followUpTaskId, aoUserId, db);
  }

  if (help) {
    await insertActivity({
      clientId,
      prospectId,
      aoUserId,
      activityType: 'help_requested',
      outcome: 'needs_jake',
      notes: helpReason || notes,
      previousStatus: before.ao_current_status,
      newStatus,
      db,
    });
  }

  await logAoAuditEvent({
    event: 'AO_CRM_OUTCOME_SUBMITTED',
    clientId,
    aoUserId,
    prospectId,
    payload: { outcome, next_action: nextAction, next_action_date: nextActionDate, help },
  });

  const detail = await getAccountDetail({ clientId, prospectId, aoUserId, db });
  return { ok: true, account: detail };
}

async function resolveHelp({
  clientId,
  prospectId,
  managerNote = null,
  db = pool,
}) {
  await ensureAoCrmSchema(db);
  await db.query(`
    UPDATE prospects SET
      help_requested = false,
      help_resolved_at = NOW(),
      help_manager_note = COALESCE($3, help_manager_note),
      updated_at = NOW()
    WHERE id = $1::uuid AND client_id = $2
  `, [prospectId, clientId, managerNote]);

  await insertActivity({
    clientId,
    prospectId,
    aoUserId: null,
    activityType: 'note',
    notes: managerNote || 'Help request resolved by manager.',
    db,
  });

  return { ok: true };
}

module.exports = {
  getAoCrmDashboard,
  listManagerAccounts,
  getAccountDetail,
  submitOutcome,
  resolveHelp,
  insertActivity,
  formatAccountRow,
  buildTodayQueue,
};
