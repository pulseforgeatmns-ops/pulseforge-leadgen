'use strict';

const pool = require('../db');
const { classifyAoMaxIntent } = require('../utils/aoMaxIntent');
const {
  todayISO,
  mapAccountRow,
  comparePrioritizedAccounts,
  formatPrioritizationResponse,
  formatAccountBriefing,
  buildCoachingReply,
} = require('../utils/aoAccountPrioritization');
const { normalizeDueDate } = require('../utils/aoQueueFormat');

async function fetchAssignedAccountRows({ aoOwnerId, clientId }) {
  const { rows } = await pool.query(`
    SELECT
      t.id AS task_id,
      t.due_date,
      t.priority,
      t.next_action,
      t.suggested_message,
      t.waiting_on_jake,
      t.status AS task_status,
      t.last_interaction_summary,
      t.ao_owner_id,
      l.id AS lead_id,
      l.client_id,
      l.business_name,
      l.address,
      l.status AS lead_status,
      l.interest_level,
      l.attribution_source,
      l.campaign_name,
      l.original_visit_note,
      l.probe_answers,
      l.last_contact_date,
      c.contact_name,
      c.contact_title,
      c.phone AS contact_phone,
      c.is_decision_maker,
      e.id AS open_escalation_id,
      e.status AS open_escalation_status
    FROM ao_follow_up_tasks t
    JOIN ao_leads l ON l.id = t.lead_id
    LEFT JOIN ao_contacts c ON c.id = t.contact_id
    LEFT JOIN LATERAL (
      SELECT id, status
      FROM ao_escalations
      WHERE lead_id = l.id AND status NOT IN ('resolved', 'ignored')
      ORDER BY created_at DESC
      LIMIT 1
    ) e ON true
    WHERE t.ao_owner_id = $1
      AND l.client_id = $2
      AND t.status = 'open'
      AND l.status NOT IN ('not_a_fit', 'do_not_contact', 'closed_lost', 'converted_to_crm')
  `, [aoOwnerId, clientId]);

  return rows;
}

async function fetchAssignedLeadWithoutOpenTask({ aoOwnerId, clientId }) {
  const { rows } = await pool.query(`
    SELECT
      NULL::uuid AS task_id,
      l.next_follow_up_date AS due_date,
      'normal'::text AS priority,
      NULL::text AS next_action,
      NULL::text AS suggested_message,
      false AS waiting_on_jake,
      NULL::text AS task_status,
      NULL::text AS last_interaction_summary,
      l.ao_owner_id,
      l.id AS lead_id,
      l.client_id,
      l.business_name,
      l.address,
      l.status AS lead_status,
      l.interest_level,
      l.attribution_source,
      l.campaign_name,
      l.original_visit_note,
      l.probe_answers,
      l.last_contact_date,
      c.contact_name,
      c.contact_title,
      c.phone AS contact_phone,
      c.is_decision_maker,
      e.id AS open_escalation_id,
      e.status AS open_escalation_status
    FROM ao_leads l
    LEFT JOIN ao_contacts c ON c.lead_id = l.id
    LEFT JOIN LATERAL (
      SELECT id, status
      FROM ao_escalations
      WHERE lead_id = l.id AND status NOT IN ('resolved', 'ignored')
      ORDER BY created_at DESC
      LIMIT 1
    ) e ON true
    WHERE l.ao_owner_id = $1
      AND l.client_id = $2
      AND l.status NOT IN ('not_a_fit', 'do_not_contact', 'closed_lost', 'converted_to_crm')
      AND NOT EXISTS (
        SELECT 1 FROM ao_follow_up_tasks t
        WHERE t.lead_id = l.id AND t.status = 'open'
      )
    ORDER BY l.updated_at DESC
    LIMIT 10
  `, [aoOwnerId, clientId]);

  return rows;
}

async function listPrioritizedAccounts({ aoOwnerId, clientId, limit = 5 }) {
  const today = todayISO();
  const taskRows = await fetchAssignedAccountRows({ aoOwnerId, clientId });
  const leadOnlyRows = await fetchAssignedLeadWithoutOpenTask({ aoOwnerId, clientId });

  const seenLeadIds = new Set(taskRows.map(r => r.lead_id));
  const combined = [
    ...taskRows.map(row => mapAccountRow(row, today)),
    ...leadOnlyRows
      .filter(row => !seenLeadIds.has(row.lead_id))
      .map(row => mapAccountRow(row, today)),
  ];

  const actionable = combined
    .filter(row => !['disqualified', 'converted_to_crm'].includes(row.operational_state))
    .sort(comparePrioritizedAccounts);

  return actionable.slice(0, limit);
}

async function buildAccountPrioritizationReply({ aoOwnerId, clientId }) {
  const today = todayISO();
  const allRows = await fetchAssignedAccountRows({ aoOwnerId, clientId });
  const hasOverdue = allRows.some(row => {
    const due = normalizeDueDate(row.due_date);
    return due && due < today;
  });
  const accounts = await listPrioritizedAccounts({ aoOwnerId, clientId, limit: 5 });
  return {
    intent: 'account_prioritization',
    reply: formatPrioritizationResponse(accounts, { hasOverdue }),
    accounts,
  };
}

async function findAssignedLeadByName({ aoOwnerId, clientId, businessNameQuery }) {
  const query = String(businessNameQuery || '').trim();
  if (!query) return null;

  const { rows } = await pool.query(`
    SELECT
      l.*,
      c.contact_name, c.contact_title, c.phone AS contact_phone, c.email AS contact_email,
      c.is_decision_maker,
      t.id AS open_task_id,
      t.status AS open_task_status,
      t.next_action AS open_next_action,
      t.due_date AS open_task_due,
      t.suggested_message,
      t.waiting_on_jake,
      t.last_interaction_summary,
      e.id AS open_escalation_id,
      e.status AS open_escalation_status,
      e.reason AS open_escalation_reason,
      e.summary AS open_escalation_summary
    FROM ao_leads l
    LEFT JOIN LATERAL (
      SELECT * FROM ao_contacts
      WHERE lead_id = l.id
      ORDER BY is_decision_maker DESC, created_at ASC
      LIMIT 1
    ) c ON true
    LEFT JOIN LATERAL (
      SELECT * FROM ao_follow_up_tasks
      WHERE lead_id = l.id AND status = 'open'
      ORDER BY due_date ASC, created_at ASC
      LIMIT 1
    ) t ON true
    LEFT JOIN LATERAL (
      SELECT * FROM ao_escalations
      WHERE lead_id = l.id AND status NOT IN ('resolved', 'ignored')
      ORDER BY created_at DESC
      LIMIT 1
    ) e ON true
    WHERE l.ao_owner_id = $1
      AND l.client_id = $2
      AND l.business_name ILIKE $3
    ORDER BY
      CASE WHEN l.business_name ILIKE $4 THEN 0 ELSE 1 END,
      l.updated_at DESC
    LIMIT 1
  `, [aoOwnerId, clientId, `%${query}%`, query]);

  return rows[0] || null;
}

async function buildAccountBriefingReply({ aoOwnerId, clientId, businessNameQuery }) {
  const lead = await findAssignedLeadByName({ aoOwnerId, clientId, businessNameQuery });
  if (!lead) {
    return {
      intent: 'account_briefing',
      reply: `I couldn't find "${businessNameQuery}" in your assigned accounts. Double-check the name or open Queue to browse your list.`,
      account: null,
    };
  }

  return {
    intent: 'account_briefing',
    reply: formatAccountBriefing(lead),
    account: {
      lead_id: lead.id,
      business_name: lead.business_name,
    },
  };
}

async function handleAoMaxQuestion({ aoOwnerId, clientId, message }) {
  const { intent, briefingTarget } = classifyAoMaxIntent(message);

  if (intent === 'account_prioritization') {
    return buildAccountPrioritizationReply({ aoOwnerId, clientId });
  }

  if (intent === 'account_briefing') {
    return buildAccountBriefingReply({
      aoOwnerId,
      clientId,
      businessNameQuery: briefingTarget,
    });
  }

  return buildCoachingReply(message);
}

module.exports = {
  fetchAssignedAccountRows,
  findAssignedLeadByName,
  listPrioritizedAccounts,
  buildAccountPrioritizationReply,
  buildAccountBriefingReply,
  handleAoMaxQuestion,
};
