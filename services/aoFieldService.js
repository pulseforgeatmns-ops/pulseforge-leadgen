const axios = require('axios');
const pool = require('../db');
const { buildSuggestedMessage, buildDirectMailOpening, normalizeNextAction, parseContactRole, formatDecisionMakerStatus } = require('../utils/aoMessageTemplates');
const { normalizeDueDate } = require('../utils/aoQueueFormat');

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
    campaign_name: row.campaign_name || null,
    commission_eligible: row.commission_eligible,
    original_visit_note: row.original_visit_note,
    probe_answers: row.probe_answers || null,
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
    due_date: normalizeDueDate(row.due_date),
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
    attribution_source: row.attribution_source || null,
    campaign_name: row.campaign_name || null,
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
    where += ` AND t.priority IN ('high', 'warm')`;
  } else if (filter === 'direct_mail') {
    where += ` AND l.attribution_source = 'direct_mail_campaign'`;
  } else if (filter === 'waiting') {
    where += ` AND t.waiting_on_jake = true`;
  }

  const { rows } = await pool.query(`
    SELECT t.*, l.business_name, l.interest_level, l.attribution_source, l.campaign_name, c.contact_name
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
  contactRole,
  isDecisionMaker,
  visitNote,
  interestLevel,
  nextAction,
  nextActionOwner,
  dueDate,
  status,
  escalate,
  escalationReason,
  escalationSummary,
  probeAnswers,
  skipTask = false,
}) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const leadStatus = status || (interestLevel === 'high' ? 'needs_follow_up' : 'new_visit');
    const ownerIsJake = nextActionOwner === 'jake' || nextActionOwner === 'walkthrough';
    const followUpOwnerId = ownerIsJake ? null : aoOwnerId;

    const { rows: leadRows } = await client.query(`
      INSERT INTO ao_leads (
        client_id, business_name, address, business_type, status, interest_level,
        ao_owner_id, original_visit_note, next_follow_up_date, next_follow_up_owner_id,
        last_contact_date, probe_answers
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),$11)
      RETURNING *
    `, [
      clientId, businessName, address || null, businessType || null,
      leadStatus, interestLevel || 'medium', aoOwnerId, visitNote || null,
      skipTask ? null : (dueDate || null),
      followUpOwnerId,
      probeAnswers && Object.keys(probeAnswers).length ? JSON.stringify(probeAnswers) : null,
    ]);
    const lead = leadRows[0];

    const role = contactRole || (isDecisionMaker ? 'decision_maker' : 'unknown');
    const normalizedNextAction = normalizeNextAction(nextAction);

    let contact = null;
    if (contactName) {
      const { rows: contactRows } = await client.query(`
        INSERT INTO ao_contacts (lead_id, contact_name, contact_title, phone, email, is_decision_maker)
        VALUES ($1,$2,$3,$4,$5,$6)
        RETURNING *
      `, [lead.id, contactName, contactTitle || null, phone || null, email || null, role === 'decision_maker']);
      contact = contactRows[0];
    }

    const suggestedMessage = buildSuggestedMessage({
      contactName,
      businessName,
      aoName,
      visitNote,
      contactTitle,
      contactRole: role,
      interestLevel: interestLevel || 'medium',
      nextAction: normalizedNextAction,
      status: leadStatus,
      escalationReason,
    });

    let task = null;
    if (!skipTask) {
      const taskDue = dueDate || new Date().toISOString().slice(0, 10);
      const waitingOnJake = Boolean(escalate) || ownerIsJake;
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
        interestLevel === 'high' || role === 'decision_maker' ? 'high' : 'normal',
        normalizedNextAction,
        visitNote || null,
        suggestedMessage,
        waitingOnJake,
      ]);
      task = taskRows[0];
    } else {
      task = {
        suggested_message: suggestedMessage,
        status: 'cancelled',
        waiting_on_jake: false,
      };
    }

    let escalation = null;
    if (escalate) {
      const { rows: escRows } = await client.query(`
        INSERT INTO ao_escalations (lead_id, contact_id, ao_owner_id, reason, summary, probe_answers)
        VALUES ($1,$2,$3,$4,$5,$6)
        RETURNING *
      `, [
        lead.id,
        contact?.id || null,
        aoOwnerId,
        escalationReason || 'high_interest',
        escalationSummary || visitNote || `Escalation for ${businessName}`,
        probeAnswers && Object.keys(probeAnswers).length ? JSON.stringify(probeAnswers) : null,
      ]);
      escalation = escRows[0];
      if (task?.id) {
        await client.query(`
          UPDATE ao_follow_up_tasks SET status = 'escalated' WHERE id = $1
        `, [task.id]);
        task.status = 'escalated';
      }
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

function endOfBusinessWeekISO(from = new Date()) {
  const d = new Date(from);
  const day = d.getDay();
  let add = 5 - day;
  if (add < 0) add += 7;
  d.setDate(d.getDate() + add);
  return d.toISOString().slice(0, 10);
}

async function findDirectMailLead(clientId, businessName) {
  const { rows } = await pool.query(`
    SELECT id, business_name FROM ao_leads
    WHERE client_id = $1
      AND attribution_source = 'direct_mail_campaign'
      AND lower(regexp_replace(business_name, '[^a-z0-9]', '', 'g'))
        = lower(regexp_replace($2, '[^a-z0-9]', '', 'g'))
    LIMIT 1
  `, [clientId, businessName]);
  return rows[0] || null;
}

async function getTaskForFollowUp(taskId, aoOwnerId) {
  const { rows } = await pool.query(`
    SELECT t.*, l.business_name, l.address, l.business_type, l.status AS lead_status,
      l.attribution_source, l.campaign_name, l.original_visit_note, l.interest_level,
      c.contact_name, c.contact_title, c.phone AS contact_phone, c.email AS contact_email
    FROM ao_follow_up_tasks t
    JOIN ao_leads l ON l.id = t.lead_id
    LEFT JOIN ao_contacts c ON c.id = t.contact_id
    WHERE t.id = $1 AND t.ao_owner_id = $2 AND t.status = 'open'
    LIMIT 1
  `, [taskId, aoOwnerId]);
  return rows[0] || null;
}

async function createDirectMailFollowUpLead({
  clientId,
  aoOwnerId,
  aoName,
  businessName,
  address = null,
  businessType = null,
  campaignName = 'Campaign 001',
  note = 'Received direct mail before AO visit',
  dueDate = null,
}) {
  const existing = await findDirectMailLead(clientId, businessName);
  if (existing) {
    return { skipped: true, reason: 'already_exists', lead: existing };
  }

  const taskDue = dueDate || endOfBusinessWeekISO();
  const opening = buildDirectMailOpening(aoName);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { rows: leadRows } = await client.query(`
      INSERT INTO ao_leads (
        client_id, business_name, address, business_type, status, interest_level,
        ao_owner_id, attribution_source, campaign_name, original_visit_note,
        next_follow_up_date, next_follow_up_owner_id, last_contact_date
      ) VALUES ($1,$2,$3,$4,'needs_follow_up','medium',$5,'direct_mail_campaign',$6,$7,$8,$5,NOW())
      RETURNING *
    `, [
      clientId, businessName, address, businessType,
      aoOwnerId, campaignName, note, taskDue,
    ]);
    const lead = leadRows[0];

    const { rows: taskRows } = await client.query(`
      INSERT INTO ao_follow_up_tasks (
        lead_id, ao_owner_id, due_date, priority, next_action,
        last_interaction_summary, suggested_message
      ) VALUES ($1,$2,$3,'warm','in_person_revisit',$4,$5)
      RETURNING *
    `, [lead.id, aoOwnerId, taskDue, note, opening]);

    await client.query('COMMIT');
    return {
      skipped: false,
      lead: mapLead(lead),
      task: mapTask({ ...taskRows[0], business_name: businessName, attribution_source: 'direct_mail_campaign', campaign_name: campaignName }),
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function completeDirectMailFollowUp(taskId, aoOwnerId, {
  clientId,
  aoName,
  answers = {},
  visitNote,
  nextAction,
  interestLevel,
  contactName,
  contactTitle,
  contactRole,
}) {
  const taskRow = await getTaskForFollowUp(taskId, aoOwnerId);
  if (!taskRow) return null;
  if (taskRow.attribution_source !== 'direct_mail_campaign') {
    throw new Error('Task is not a direct mail follow-up');
  }

  const normalizedNextAction = normalizeNextAction(nextAction || answers.next_step || 'Follow up');
  const probeAnswers = { ...answers };
  const summary = [
    visitNote || null,
    answers.mailer_remembered ? `Mailer remembered: ${answers.mailer_remembered}` : null,
    answers.cleaning_decision_maker ? `Cleaning decision-maker: ${answers.cleaning_decision_maker}` : null,
    answers.reached_decision_maker ? `Reached decision-maker: ${answers.reached_decision_maker}` : null,
    answers.outside_cleaner ? `Outside cleaner: ${answers.outside_cleaner}` : null,
    answers.cleaner_issues ? `Cleaner issues: ${answers.cleaner_issues}` : null,
    answers.walkthrough_interest ? `Walkthrough interest: ${answers.walkthrough_interest}` : null,
    answers.next_step ? `Next step: ${answers.next_step}` : null,
  ].filter(Boolean).join('\n');

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    let contactId = taskRow.contact_id;
    if (contactName) {
      const role = parseContactRole(contactRole);
      if (contactId) {
        await client.query(`
          UPDATE ao_contacts
          SET contact_name = $2, contact_title = COALESCE($3, contact_title),
              is_decision_maker = $4, updated_at = NOW()
          WHERE id = $1
        `, [contactId, contactName, contactTitle || null, role === 'decision_maker']);
      } else {
        const { rows: contactRows } = await client.query(`
          INSERT INTO ao_contacts (lead_id, contact_name, contact_title, is_decision_maker)
          VALUES ($1,$2,$3,$4)
          RETURNING id
        `, [taskRow.lead_id, contactName, contactTitle || null, role === 'decision_maker']);
        contactId = contactRows[0].id;
      }
    }

    const leadStatus = /not a fit|not a fit|do not contact|hard no/i.test(normalizedNextAction)
      ? 'not_a_fit'
      : /walkthrough|tour/i.test(normalizedNextAction) || /yes|interested|walkthrough/i.test(String(answers.walkthrough_interest || ''))
        ? 'walkthrough_requested'
        : 'needs_follow_up';

    await client.query(`
      UPDATE ao_leads
      SET status = $2,
          interest_level = COALESCE($3, interest_level),
          original_visit_note = COALESCE($4, original_visit_note),
          probe_answers = $5,
          last_contact_date = NOW(),
          updated_at = NOW()
      WHERE id = $1
    `, [
      taskRow.lead_id,
      leadStatus,
      interestLevel || null,
      summary || null,
      Object.keys(probeAnswers).length ? JSON.stringify(probeAnswers) : null,
    ]);

    await client.query(`
      UPDATE ao_follow_up_tasks
      SET status = 'done', completed_at = NOW(), last_interaction_summary = $2
      WHERE id = $1
    `, [taskId, summary || visitNote || 'Direct mail follow-up logged']);

    let escalation = null;
    const escalate = /jake|walkthrough|owner should|admin should/i.test(normalizedNextAction)
      || /yes|interested|walkthrough/i.test(String(answers.walkthrough_interest || ''));
    if (escalate) {
      const reason = /walkthrough|tour/i.test(normalizedNextAction) ? 'walkthrough_request' : 'high_interest';
      const { rows: escRows } = await client.query(`
        INSERT INTO ao_escalations (lead_id, contact_id, ao_owner_id, reason, summary, probe_answers)
        VALUES ($1,$2,$3,$4,$5,$6)
        RETURNING *
      `, [taskRow.lead_id, contactId, aoOwnerId, reason, summary, JSON.stringify(probeAnswers)]);
      escalation = escRows[0];
    }

    await client.query('COMMIT');
    return {
      task: mapTask({ ...taskRow, status: 'done' }),
      lead: mapLead({ ...taskRow, id: taskRow.lead_id, status: leadStatus }),
      escalation,
      summary,
      aoName,
      clientId,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function resolveAoOwnerByName(namePattern, clientId) {
  const { rows } = await pool.query(`
    SELECT id, name, email, client_id, role
    FROM users
    WHERE client_id = $1
      AND role = 'ao'
      AND active = true
      AND name ILIKE $2
    ORDER BY id ASC
    LIMIT 1
  `, [clientId, namePattern]);
  return rows[0] || null;
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
    campaign_name: row.campaign_name || null,
    commission_eligible: row.commission_eligible,
    original_visit_note: row.original_visit_note,
    probe_answers: row.probe_answers || null,
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
    escalation_probe_answers: row.escalation_probe_answers || null,
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
      e.summary AS escalation_summary, e.probe_answers AS escalation_probe_answers,
      e.created_at AS escalated_at
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
      e.summary AS escalation_summary, e.probe_answers AS escalation_probe_answers,
      e.created_at AS escalated_at
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

async function depositEscalationAction(escalation, lead, clientId, extras = {}) {
  const probeAnswers = extras.probeAnswers || escalation.probe_answers || null;
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
      business_name: lead.business_name,
      contact_name: lead.contact_name || null,
      visit_note: lead.original_visit_note || null,
      interest_level: lead.interest_level || null,
      ao_name: extras.aoName || null,
      next_action: extras.nextAction || null,
      decision_maker_status: extras.decisionMakerStatus
        || formatDecisionMakerStatus({
          contactRole: extras.contactRole,
          isDecisionMaker: lead.is_decision_maker,
          contactTitle: lead.contact_title,
        }),
      suggested_message: extras.suggestedMessage || null,
      probe_answers: probeAnswers,
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
  createDirectMailFollowUpLead,
  completeDirectMailFollowUp,
  getTaskForFollowUp,
  findDirectMailLead,
  resolveAoOwnerByName,
  endOfBusinessWeekISO,
  updateTask,
  escalateTask,
  listEscalations,
  updateEscalation,
  notifyJakeEscalation,
  depositEscalationAction,
  listAdminVisits,
  getAdminVisit,
};
