'use strict';

const pool = require('../db');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const { ensureAoProspectRoutingSchema } = require('../utils/aoProspectRoutingSchema');
const {
  rankProspectRows,
  isOverdue,
  isSameDay,
  endOfDay,
  greetingForHour,
} = require('../utils/aoCommandCenterRanking');
const { conversationPreview, prospectLabel } = require('./aoMaxConversation');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');

function formatDateInTz(date = new Date(), tz = 'America/New_York') {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: tz,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

function formatProspectItem(row) {
  return {
    prospect_id: row.prospect_id,
    lead_id: row.lead_id || null,
    company_name: row.company_name || row.account_name || 'Unknown account',
    segment: row.segment || row.vertical || null,
    prospect_motion: row.prospect_motion || null,
    ao_fit_score: row.ao_fit_score != null ? Number(row.ao_fit_score) / 100 : null,
    ao_fit_reason: row.ao_fit_reason || null,
    assigned_ao_id: row.assigned_ao_id != null ? String(row.assigned_ao_id) : null,
    ao_assignment_category: row.ao_assignment_category || null,
    recommended_angle: row.recommended_angle || null,
    recommended_first_action: row.recommended_first_action || row.first_action || null,
    advisory_stage: row.advisory_stage || null,
    last_debrief_status: row.last_debrief_status || null,
    next_action: row.next_action || null,
    next_action_owner: row.next_action_owner || null,
    next_action_due_at: row.next_action_due_at || null,
    latest_session_id: row.latest_session_id || null,
    conversation_status: row.conversation_status || null,
    last_touch_at: row.last_touch_at || null,
    brief_available: true,
    priority_score: row.priority_score,
    assignment_reason: row.ao_assignment_reason || row.why_account_matters || null,
  };
}

async function fetchAssignedProspects({ clientId, aoUserId, db = pool }) {
  const { rows } = await db.query(`
    SELECT
      p.id AS prospect_id,
      p.prospect_motion,
      p.ao_fit_score,
      p.ao_fit_reason,
      p.assigned_ao_id,
      p.ao_assignment_reason,
      p.ao_assignment_category,
      p.recommended_angle,
      p.recommended_first_action,
      p.advisory_stage,
      p.last_debrief_status,
      p.next_action,
      p.next_action_owner,
      p.next_action_due_at,
      p.vertical,
      p.created_at AS prospect_created_at,
      p.updated_at AS prospect_updated_at,
      c.name AS company_name,
      t.segment,
      t.account_name,
      t.first_action,
      t.why_account_matters,
      t.created_at AS task_created_at,
      al.id AS lead_id,
      sess.id AS latest_session_id,
      sess.status AS conversation_status,
      sess.updated_at AS session_updated_at,
      sess.payload AS session_payload,
      (
        SELECT MAX(created_at)
        FROM ao_prospect_updates u
        WHERE u.prospect_id = p.id::text AND u.tenant_id = p.client_id::text
      ) AS last_update_at,
      (
        SELECT MAX(created_at)
        FROM touchpoints tp
        WHERE tp.prospect_id = p.id AND tp.client_id = p.client_id
      ) AS last_touchpoint_at,
      EXISTS (
        SELECT 1 FROM ao_routing_issue_flags f
        WHERE f.prospect_id = p.id::text
          AND f.tenant_id = p.client_id::text
          AND f.ao_user_id = $2::text
          AND f.created_at >= NOW() - INTERVAL '14 days'
      ) AS has_open_flag
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    LEFT JOIN LATERAL (
      SELECT *
      FROM ao_prospect_tasks apt
      WHERE apt.prospect_id = p.id
        AND apt.client_id = p.client_id
        AND apt.assigned_ao_id = $2
        AND apt.status IN ('open', 'in_progress')
      ORDER BY apt.created_at DESC
      LIMIT 1
    ) t ON true
    LEFT JOIN LATERAL (
      SELECT id
      FROM ao_leads al0
      WHERE al0.crm_prospect_id = p.id
        AND al0.client_id = p.client_id
        AND al0.ao_owner_id = $2
      ORDER BY al0.updated_at DESC
      LIMIT 1
    ) al ON true
    LEFT JOIN LATERAL (
      SELECT id, status, updated_at, payload
      FROM ao_max_sessions s
      WHERE s.prospect_id = p.id::text
        AND s.client_id = p.client_id
        AND s.ao_owner_id = $2
        AND s.mode = 'conversation'
      ORDER BY s.updated_at DESC
      LIMIT 1
    ) sess ON true
    WHERE p.client_id = $1
      AND p.assigned_ao_id = $2
      AND COALESCE(p.do_not_contact, false) = false
      AND COALESCE(p.prospect_motion, '') NOT IN ('SUPPRESS', 'EMAIL_LED')
  `, [clientId, aoUserId]);

  return rows.map(row => ({
    ...row,
    assigned_at: row.task_created_at || row.prospect_updated_at,
    last_touch_at: row.last_update_at || row.last_touchpoint_at || null,
  }));
}

async function fetchConversations({ clientId, aoUserId, dateStr, db = pool }) {
  const { rows } = await db.query(`
    SELECT
      id,
      status,
      prospect_id,
      mission_id,
      payload,
      created_at,
      updated_at,
      closed_at,
      reopened_at
    FROM ao_max_sessions
    WHERE ao_owner_id = $1
      AND client_id = $2
      AND mode = 'conversation'
      AND (
        status IN ('active', 'reopened')
        OR (status = 'done' AND closed_at >= $3::timestamptz)
        OR (status = 'reopened' AND reopened_at >= $3::timestamptz)
      )
    ORDER BY updated_at DESC
    LIMIT 100
  `, [aoUserId, clientId, `${dateStr}T00:00:00-04:00`]);

  return rows.map(row => ({
    conversation_id: row.id,
    session_id: row.id,
    prospect_id: row.prospect_id,
    mission_id: row.mission_id,
    conversation_status: row.status,
    prospect_label: prospectLabel(row.payload || {}, row),
    last_message_preview: conversationPreview(row.payload || {}),
    last_updated: row.updated_at,
    closed_at: row.closed_at,
    reopened_at: row.reopened_at,
    can_reopen: row.status === 'done',
    brief_available: Boolean(row.prospect_id),
  }));
}

async function fetchRoutingIssues({ clientId, aoUserId, limit = 20, db = pool }) {
  const { rows } = await db.query(`
    SELECT
      f.*,
      c.name AS company_name
    FROM ao_routing_issue_flags f
    LEFT JOIN prospects p ON p.id::text = f.prospect_id AND p.client_id = $1
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE f.tenant_id = $2
      AND f.ao_user_id = $3
    ORDER BY f.created_at DESC
    LIMIT $4
  `, [clientId, String(clientId), String(aoUserId), Math.min(limit, 50)]);

  return rows.map(row => ({
    id: row.id,
    created_at: row.created_at,
    issue_type: row.issue_type,
    prospect_id: row.prospect_id,
    company_name: row.company_name || null,
    notes: row.notes,
    decision_id: row.decision_id,
    session_id: row.session_id || row.conversation_id,
    status: 'open',
  }));
}

function buildUpdatesNeeded(prospects, dateStr) {
  return prospects.filter(row => {
    if (row.advisory_stage === 'closed') return false;
    const doneWithoutOutcome = row.conversation_status === 'done' && !row.last_debrief_status;
    const missingNextAction = row.last_update_at && !row.next_action;
    const staleDebrief = row.advisory_stage === 'in_progress'
      && row.last_touch_at
      && !isSameDay(row.last_touch_at, dateStr);
    const activeNoUpdateToday = ['in_progress', 'debrief_pending', 'tasked', 'routed'].includes(row.advisory_stage)
      && (!row.last_touch_at || !isSameDay(row.last_touch_at, dateStr));
    return doneWithoutOutcome || missingNextAction || staleDebrief || activeNoUpdateToday;
  });
}

async function getCommandCenter({
  clientId,
  aoUserId,
  aoUserName = null,
  date = null,
  source = 'command_center',
  db = pool,
}) {
  await ensureAoFieldSchema();
  await ensureAoProspectRoutingSchema(db);

  const dateStr = date || formatDateInTz();
  const prospects = await fetchAssignedProspects({ clientId, aoUserId, db });
  const ranked = rankProspectRows(prospects, { dateStr });

  const followupsDue = ranked.filter(row => {
    if (!row.next_action_due_at) return false;
    const due = new Date(row.next_action_due_at) <= endOfDay(dateStr);
    const ownerMatch = !row.next_action_owner
      || row.next_action_owner === 'ao'
      || String(row.next_action_owner) === String(aoUserId);
    const notClosed = row.advisory_stage !== 'closed';
    return due && ownerMatch && notClosed;
  });

  const priorityAccounts = ranked.filter(row => {
    const overdueOrDue = row.next_action_due_at
      && (isOverdue(row.next_action_due_at, dateStr) || isSameDay(row.next_action_due_at, dateStr));
    const activeConv = ['active', 'reopened'].includes(row.conversation_status);
    const highValue = ['UNFAIR_ADVANTAGE', 'HIGH_VALUE_ICP'].includes(row.ao_assignment_category);
    const activeStage = ['in_progress', 'tasked', 'routed', 'debrief_pending'].includes(row.advisory_stage);
    return overdueOrDue || activeConv || highValue || activeStage;
  });

  const recentlyAssigned = ranked.filter(row => {
    const assignedAt = row.assigned_at ? new Date(row.assigned_at) : null;
    const sevenDaysAgo = new Date(`${dateStr}T00:00:00`);
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    const recent = assignedAt && assignedAt >= sevenDaysAgo;
    const untouched = !row.last_touch_at;
    return recent || untouched;
  });

  const conversations = await fetchConversations({ clientId, aoUserId, dateStr, db });
  const routingIssues = await fetchRoutingIssues({ clientId, aoUserId, db });
  const updatesNeeded = buildUpdatesNeeded(ranked, dateStr);

  const summary = {
    priority_accounts: priorityAccounts.length,
    followups_due: followupsDue.length,
    conversations_to_continue: conversations.filter(c => ['active', 'reopened'].includes(c.conversation_status)).length
      + conversations.filter(c => c.can_reopen).length,
    routing_flags_open: routingIssues.length,
    updates_needed: updatesNeeded.length,
  };

  await logAoAuditEvent({
    event: 'AO_COMMAND_CENTER_VIEWED',
    clientId,
    aoUserId,
    payload: { source, date: dateStr, summary },
  });

  const hour = Number(new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    hour: 'numeric',
    hour12: false,
  }).format(new Date()));

  return {
    date: dateStr,
    tenant_id: String(clientId),
    ao_user: {
      id: String(aoUserId),
      name: aoUserName,
      greeting: aoUserName ? `${greetingForHour(hour)}, ${aoUserName.split(' ')[0]}.` : null,
    },
    summary,
    sections: {
      priority_accounts: priorityAccounts.map(formatProspectItem),
      followups_due: followupsDue.map(formatProspectItem),
      conversations_to_continue: conversations,
      recently_assigned: recentlyAssigned.map(formatProspectItem),
      routing_issues: routingIssues,
      updates_needed: updatesNeeded.map(formatProspectItem),
    },
  };
}

module.exports = {
  getCommandCenter,
  formatDateInTz,
  formatProspectItem,
  fetchAssignedProspects,
};
