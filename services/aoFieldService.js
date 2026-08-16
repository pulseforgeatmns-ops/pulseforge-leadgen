const axios = require('axios');
const pool = require('../db');
const { suggestTemplate, renderTemplate } = require('../utils/aoMessageTemplates');

function mapLead(row) {
  return {
    id: row.id,
    business_name: row.business_name,
    address: row.address,
    business_type: row.business_type,
    status: row.status,
    interest_level: row.interest_level,
    ao_owner_id: row.ao_owner_id,
    first_contact_date: row.first_contact_date,
    last_contact_date: row.last_contact_date,
    next_follow_up_date: row.next_follow_up_date,
    next_follow_up_owner_id: row.next_follow_up_owner_id,
    attribution_source: row.attribution_source,
    commission_eligible: row.commission_eligible,
    original_visit_note: row.original_visit_note,
    created_at: row.created_at,
    updated_at: row.updated_at,
    contact_name: row.contact_name || null,
    contact_title: row.contact_title || null,
    contact_phone: row.contact_phone || null,
    contact_email: row.contact_email || null,
    contact_id: row.contact_id || null,
  };
}

function mapTask(row) {
  return {
    id: row.id,
    lead_id: row.lead_id,
    contact_id: row.contact_id,
    ao_owner_id: row.ao_owner_id,
    due_date: row.due_date,
    status: row.status,
    priority: row.priority,
    next_action: row.next_action,
    last_interaction_summary: row.last_interaction_summary,
    suggested_message: row.suggested_message,
    waiting_on_jake: row.waiting_on_jake,
    created_at: row.created_at,
    completed_at: row.completed_at,
    business_name: row.business_name,
    contact_name: row.contact_name,
    interest_level: row.interest_level,
  };
}

async function getAoProfile(userId) {
  const { rows } = await pool.query(`
    SELECT id, name, email, phone, role, territory, manager_id, daily_goal, weekly_goal, client_id, active
    FROM users WHERE id = $1 LIMIT 1
  `, [userId]);
  return rows[0] || null;
}

async function listQueue({ aoOwnerId, clientId, filter = 'today' }) {
  const params = [aoOwnerId, clientId];
  let where = `t.ao_owner_id = $1 AND l.client_id = $2 AND t.status = 'open'`;

  const today = new Date().toISOString().slice(0, 10);
  if (filter === 'today') {
    params.push(today);
    where += ` AND t.due_date = $${params.length}`;
  } else if (filter === 'overdue') {
    params.push(today);
    where += ` AND t.due_date < $${params.length}`;
  } else if (filter === 'week') {
    const weekEnd = new Date();
    weekEnd.setDate(weekEnd.getDate() + 7);
    params.push(today, weekEnd.toISOString().slice(0, 10));
    where += ` AND t.due_date BETWEEN $${params.length - 1} AND $${params.length}`;
  } else if (filter === 'high') {
    where += ` AND t.priority = 'high'`;
  } else if (filter === 'waiting') {
    where += ` AND t.waiting_on_jake = true`;
  }

  const { rows } = await pool.query(`
    SELECT t.*, l.business_name, l.interest_level, c.contact_name
    FROM ao_follow_up_tasks t
    JOIN ao_leads l ON l.id = t.lead_id
    LEFT JOIN ao_contacts c ON c.id = t.contact_id
    WHERE ${where}
    ORDER BY t.priority DESC, t.due_date ASC, t.created_at ASC
  `, params);
  return rows.map(mapTask);
}

async function getLeadDetail(leadId, aoOwnerId) {
  const { rows } = await pool.query(`
    SELECT l.*, c.id AS contact_id, c.contact_name, c.contact_title,
      c.phone AS contact_phone, c.email AS contact_email, c.is_decision_maker
    FROM ao_leads l
    LEFT JOIN ao_contacts c ON c.lead_id = l.id
    WHERE l.id = $1 AND l.ao_owner_id = $2
    ORDER BY c.is_decision_maker DESC, c.created_at ASC
    LIMIT 1
  `, [leadId, aoOwnerId]);
  if (!rows.length) return null;

  const lead = mapLead(rows[0]);
  const { rows: tasks } = await pool.query(`
    SELECT * FROM ao_follow_up_tasks
    WHERE lead_id = $1 AND status = 'open'
    ORDER BY due_date ASC
  `, [leadId]);
  lead.open_tasks = tasks.map(mapTask);
  return lead;
}

async function createVisitRecord({
  clientId,
  aoOwnerId,
  aoName,
  businessName,
  address,
  businessType,
  contactName,
  contactTitle,
  phone,
  email,
  isDecisionMaker,
  visitNote,
  interestLevel,
  nextAction,
  dueDate,
  status,
  escalate,
  escalationReason,
  escalationSummary,
}) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const leadStatus = status || (interestLevel === 'high' ? 'needs_follow_up' : 'new_visit');
    const { rows: leadRows } = await client.query(`
      INSERT INTO ao_leads (
        client_id, business_name, address, business_type, status, interest_level,
        ao_owner_id, original_visit_note, next_follow_up_date, next_follow_up_owner_id,
        last_contact_date
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$7,NOW())
      RETURNING *
    `, [
      clientId, businessName, address || null, businessType || null,
      leadStatus, interestLevel || 'medium', aoOwnerId, visitNote || null,
      dueDate || null,
    ]);
    const lead = leadRows[0];

    let contact = null;
    if (contactName) {
      const { rows: contactRows } = await client.query(`
        INSERT INTO ao_contacts (lead_id, contact_name, contact_title, phone, email, is_decision_maker)
        VALUES ($1,$2,$3,$4,$5,$6)
        RETURNING *
      `, [lead.id, contactName, contactTitle || null, phone || null, email || null, Boolean(isDecisionMaker)]);
      contact = contactRows[0];
    }

    const template = suggestTemplate({
      interestLevel: lead.interest_level,
      status: lead.status,
      nextAction,
    });
    const suggestedMessage = renderTemplate(template.id, {
      contact_name: contactName || 'there',
      business_name: businessName,
      ao_name: aoName || 'your Anchor rep',
    });

    const taskDue = dueDate || new Date().toISOString().slice(0, 10);
    const waitingOnJake = Boolean(escalate);
    const { rows: taskRows } = await client.query(`
      INSERT INTO ao_follow_up_tasks (
        lead_id, contact_id, ao_owner_id, due_date, priority, next_action,
        last_interaction_summary, suggested_message, waiting_on_jake
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      RETURNING *
    `, [
      lead.id,
      contact?.id || null,
      aoOwnerId,
      taskDue,
      interestLevel === 'high' ? 'high' : 'normal',
      nextAction || 'Follow up',
      visitNote || null,
      suggestedMessage,
      waitingOnJake,
    ]);
    const task = taskRows[0];

    let escalation = null;
    if (escalate) {
      const { rows: escRows } = await client.query(`
        INSERT INTO ao_escalations (lead_id, contact_id, ao_owner_id, reason, summary)
        VALUES ($1,$2,$3,$4,$5)
        RETURNING *
      `, [
        lead.id,
        contact?.id || null,
        aoOwnerId,
        escalationReason || 'high_interest',
        escalationSummary || visitNote || `Escalation for ${businessName}`,
      ]);
      escalation = escRows[0];
      await client.query(`
        UPDATE ao_follow_up_tasks SET status = 'escalated' WHERE id = $1
      `, [task.id]);
      task.status = 'escalated';
    }

    await client.query('COMMIT');
    return { lead: mapLead({ ...lead, ...contact }), contact, task: mapTask({ ...task, business_name: businessName, contact_name: contactName }), escalation };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function updateTask(taskId, aoOwnerId, updates) {
  const fields = [];
  const values = [];

  if (updates.status) {
    values.push(updates.status);
    fields.push(`status = $${values.length}`);
    if (updates.status === 'done') {
      values.push(new Date());
      fields.push(`completed_at = $${values.length}`);
    }
  }
  if (updates.due_date) {
    values.push(updates.due_date);
    fields.push(`due_date = $${values.length}`);
    if (updates.status !== 'escalated') {
      values.push('rescheduled');
      fields.push(`status = $${values.length}`);
    }
  }
  if (updates.next_action !== undefined) {
    values.push(updates.next_action);
    fields.push(`next_action = $${values.length}`);
  }
  if (!fields.length) return null;

  values.push(taskId, aoOwnerId);
  const { rows } = await pool.query(`
    UPDATE ao_follow_up_tasks
    SET ${fields.join(', ')}
    WHERE id = $${values.length - 1} AND ao_owner_id = $${values.length}
    RETURNING *
  `, values);
  return rows[0] ? mapTask(rows[0]) : null;
}

async function escalateTask(taskId, aoOwnerId, { reason, summary }) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows: taskRows } = await client.query(`
      SELECT t.*, l.business_name, c.contact_name
      FROM ao_follow_up_tasks t
      JOIN ao_leads l ON l.id = t.lead_id
      LEFT JOIN ao_contacts c ON c.id = t.contact_id
      WHERE t.id = $1 AND t.ao_owner_id = $2
      FOR UPDATE
    `, [taskId, aoOwnerId]);
    const task = taskRows[0];
    if (!task) {
      await client.query('ROLLBACK');
      return null;
    }

    const { rows: escRows } = await client.query(`
      INSERT INTO ao_escalations (lead_id, contact_id, ao_owner_id, reason, summary)
      VALUES ($1,$2,$3,$4,$5)
      RETURNING *
    `, [task.lead_id, task.contact_id, aoOwnerId, reason, summary]);

    await client.query(`
      UPDATE ao_follow_up_tasks
      SET status = 'escalated', waiting_on_jake = true
      WHERE id = $1
    `, [taskId]);

    await client.query(`
      UPDATE ao_leads SET status = 'walkthrough_requested', updated_at = NOW()
      WHERE id = $1 AND status NOT IN ('walkthrough_booked', 'closed_won', 'closed_lost')
    `, [task.lead_id]);

    await client.query('COMMIT');
    return { task: mapTask(task), escalation: escRows[0] };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

function mapAdminVisit(row) {
  return {
    id: row.id,
    business_name: row.business_name,
    address: row.address,
    business_type: row.business_type,
    status: row.status,
    interest_level: row.interest_level,
    ao_owner_id: row.ao_owner_id,
    ao_name: row.ao_name,
    attribution_source: row.attribution_source,
    commission_eligible: row.commission_eligible,
    original_visit_note: row.original_visit_note,
    first_contact_date: row.first_contact_date,
    last_contact_date: row.last_contact_date,
    next_follow_up_date: row.next_follow_up_date,
    created_at: row.created_at,
    contact_name: row.contact_name,
    contact_title: row.contact_title,
    contact_phone: row.contact_phone,
    contact_email: row.contact_email,
    is_decision_maker: row.is_decision_maker,
    task_id: row.task_id,
    task_status: row.task_status,
    task_priority: row.task_priority,
    next_action: row.next_action,
    suggested_message: row.suggested_message,
    waiting_on_jake: row.waiting_on_jake,
    task_due_date: row.task_due_date,
    escalation_id: row.escalation_id,
    escalation_reason: row.escalation_reason,
    escalation_status: row.escalation_status,
    escalation_summary: row.escalation_summary,
    escalated_at: row.escalated_at,
  };
}

async function listAdminVisits({ clientId, escalatedOnly = false }) {
  const params = [clientId];
  let where = 'l.client_id = $1';
  if (escalatedOnly) {
    where += ` AND e.id IS NOT NULL`;
  }
  const { rows } = await pool.query(`
    SELECT
      l.*,
      u.name AS ao_name,
      c.contact_name, c.contact_title, c.phone AS contact_phone, c.email AS contact_email,
      c.is_decision_maker,
      t.id AS task_id, t.status AS task_status, t.priority AS task_priority,
      t.next_action, t.suggested_message, t.waiting_on_jake, t.due_date AS task_due_date,
      e.id AS escalation_id, e.reason AS escalation_reason, e.status AS escalation_status,
      e.summary AS escalation_summary, e.created_at AS escalated_at
    FROM ao_leads l
    JOIN users u ON u.id = l.ao_owner_id
    LEFT JOIN LATERAL (
      SELECT * FROM ao_contacts
      WHERE lead_id = l.id
      ORDER BY is_decision_maker DESC, created_at ASC
      LIMIT 1
    ) c ON true
    LEFT JOIN LATERAL (
      SELECT * FROM ao_follow_up_tasks
      WHERE lead_id = l.id
      ORDER BY created_at DESC
      LIMIT 1
    ) t ON true
    LEFT JOIN LATERAL (
      SELECT * FROM ao_escalations
      WHERE lead_id = l.id
      ORDER BY created_at DESC
      LIMIT 1
    ) e ON true
    WHERE ${where}
    ORDER BY l.created_at DESC
    LIMIT 200
  `, params);
  return rows.map(mapAdminVisit);
}

async function getAdminVisit(leadId, clientId) {
  const { rows } = await pool.query(`
    SELECT
      l.*,
      u.name AS ao_name,
      c.contact_name, c.contact_title, c.phone AS contact_phone, c.email AS contact_email,
      c.is_decision_maker,
      t.id AS task_id, t.status AS task_status, t.priority AS task_priority,
      t.next_action, t.suggested_message, t.waiting_on_jake, t.due_date AS task_due_date,
      e.id AS escalation_id, e.reason AS escalation_reason, e.status AS escalation_status,
      e.summary AS escalation_summary, e.created_at AS escalated_at
    FROM ao_leads l
    JOIN users u ON u.id = l.ao_owner_id
    LEFT JOIN LATERAL (
      SELECT * FROM ao_contacts
      WHERE lead_id = l.id
      ORDER BY is_decision_maker DESC, created_at ASC
      LIMIT 1
    ) c ON true
    LEFT JOIN LATERAL (
      SELECT * FROM ao_follow_up_tasks
      WHERE lead_id = l.id
      ORDER BY created_at DESC
      LIMIT 1
    ) t ON true
    LEFT JOIN LATERAL (
      SELECT * FROM ao_escalations
      WHERE lead_id = l.id
      ORDER BY created_at DESC
      LIMIT 1
    ) e ON true
    WHERE l.id = $1 AND l.client_id = $2
    LIMIT 1
  `, [leadId, clientId]);
  if (!rows.length) return null;

  const visit = mapAdminVisit(rows[0]);
  const { rows: contacts } = await pool.query(`
    SELECT * FROM ao_contacts WHERE lead_id = $1 ORDER BY is_decision_maker DESC, created_at ASC
  `, [leadId]);
  const { rows: tasks } = await pool.query(`
    SELECT * FROM ao_follow_up_tasks WHERE lead_id = $1 ORDER BY created_at DESC
  `, [leadId]);
  const { rows: escalations } = await pool.query(`
    SELECT * FROM ao_escalations WHERE lead_id = $1 ORDER BY created_at DESC
  `, [leadId]);

  return {
    ...visit,
    contacts,
    tasks: tasks.map(mapTask),
    escalations,
  };
}

async function listEscalations({ clientId, status }) {
  const params = [clientId];
  let where = 'l.client_id = $1';
  if (status) {
    params.push(status);
    where += ` AND e.status = $${params.length}`;
  }
  const { rows } = await pool.query(`
    SELECT e.*, l.business_name, l.address, l.interest_level,
      c.contact_name, c.phone AS contact_phone, u.name AS ao_name
    FROM ao_escalations e
    JOIN ao_leads l ON l.id = e.lead_id
    LEFT JOIN ao_contacts c ON c.id = e.contact_id
    JOIN users u ON u.id = e.ao_owner_id
    WHERE ${where}
    ORDER BY e.created_at DESC
    LIMIT 100
  `, params);
  return rows;
}

async function updateEscalation(escalationId, { status }) {
  const resolvedAt = ['resolved'].includes(status) ? new Date() : null;
  const { rows } = await pool.query(`
    UPDATE ao_escalations
    SET status = COALESCE($2, status),
        resolved_at = CASE WHEN $2 = 'resolved' THEN NOW() ELSE resolved_at END
    WHERE id = $1
    RETURNING *
  `, [escalationId, status]);
  return rows[0] || null;
}

async function notifyJakeEscalation(escalation, lead, aoName) {
  if (!process.env.BREVO_API_KEY) return false;
  const toEmail = process.env.JAKE_EMAIL || process.env.ADMIN_EMAIL || 'jacob@gopulseforge.com';
  await axios.post('https://api.brevo.com/v3/smtp/email', {
    sender: { name: 'Max — Anchor Field', email: 'jacob@gopulseforge.com' },
    to: [{ email: toEmail, name: 'Jake' }],
    subject: `AO escalation — ${lead.business_name}`,
    textContent: [
      `${aoName || 'An AO'} escalated a field lead.`,
      '',
      `Business: ${lead.business_name}`,
      lead.address ? `Address: ${lead.address}` : null,
      `Reason: ${escalation.reason}`,
      '',
      escalation.summary,
      '',
      `Review field visits: ${process.env.APP_URL || 'https://pulseforge-leadgen-production.up.railway.app'}/admin/field-visits`,
    ].filter(Boolean).join('\n'),
  }, {
    headers: {
      'api-key': process.env.BREVO_API_KEY,
      'Content-Type': 'application/json',
    },
  });
  return true;
}

async function depositEscalationAction(escalation, lead, clientId) {
  await pool.query(`
    INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, client_id)
    VALUES ('ao_field', 'ao_escalation', $1, $2, $3, 'pending', $4)
  `, [
    `AO escalation — ${lead.business_name}`,
    escalation.summary,
    JSON.stringify({
      escalation_id: escalation.id,
      lead_id: lead.id,
      reason: escalation.reason,
      admin_url: '/admin/field-visits',
    }),
    clientId,
  ]);
}

module.exports = {
  mapLead,
  mapTask,
  getAoProfile,
  listQueue,
  getLeadDetail,
  createVisitRecord,
  updateTask,
  escalateTask,
  listEscalations,
  updateEscalation,
  notifyJakeEscalation,
  depositEscalationAction,
  listAdminVisits,
  getAdminVisit,
};
