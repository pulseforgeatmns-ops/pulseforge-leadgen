'use strict';

const pool = require('../db');
const { ensureAoDispositionSchema } = require('../utils/aoDispositionSchema');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const { ensureAoCrmSchema } = require('../utils/aoCrmSchema');
const {
  validateMarkDeadInput,
  isActiveDisposition,
  AO_DEAD_REASONS,
} = require('../utils/aoDispositionTypes');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');

async function insertProspectDeadActivity({
  clientId,
  prospectId,
  aoUserId,
  reason,
  note,
  db,
}) {
  await db.query(`
    INSERT INTO ao_prospect_activity (
      prospect_id, tenant_id, ao_id, activity_type, outcome, notes,
      previous_status, new_status, metadata
    ) VALUES ($1, $2, $3, 'status_change', $4, $5, 'active', 'dead', $6::jsonb)
  `, [
    prospectId,
    clientId,
    aoUserId,
    reason,
    note,
    JSON.stringify({ event: 'ao_prospect_marked_dead', dead_reason: reason }),
  ]);
}

async function closeOpenFieldTasksForLead({ leadId, aoOwnerId, db }) {
  await db.query(`
    UPDATE ao_follow_up_tasks
    SET status = 'cancelled', completed_at = NOW()
    WHERE lead_id = $1 AND ao_owner_id = $2 AND status = 'open'
  `, [leadId, aoOwnerId]);

  await db.query(`
    UPDATE ao_route_stops rs
    SET status = 'skipped', completed_at = NOW()
    FROM ao_routes r, ao_follow_up_tasks t
    WHERE rs.route_id = r.id
      AND rs.task_id = t.id
      AND t.lead_id = $1
      AND r.ao_owner_id = $2
      AND r.status = 'active'
      AND rs.status = 'pending'
  `, [leadId, aoOwnerId]);
}

async function cancelOpenProspectTasks({ clientId, prospectId, db }) {
  await db.query(`
    UPDATE ao_prospect_tasks
    SET status = 'cancelled', completed_at = NOW()
    WHERE client_id = $1 AND prospect_id = $2::uuid
      AND status IN ('open', 'in_progress')
  `, [clientId, prospectId]);
}

async function applyProspectSuppression({ clientId, prospectId, db }) {
  await db.query(`
    UPDATE prospects SET
      do_not_contact = true,
      prospect_motion = 'SUPPRESS',
      next_action = 'SUPPRESS',
      ao_next_action = 'disqualify',
      updated_at = NOW()
    WHERE id = $1::uuid AND client_id = $2
  `, [prospectId, clientId]);
}

async function persistLeadDead({
  leadId,
  clientId,
  aoUserId,
  reason,
  note,
  db,
}) {
  await db.query(`
    UPDATE ao_leads SET
      disposition_status = 'dead',
      dead_reason = $3,
      dead_note = $4,
      dead_at = COALESCE(dead_at, NOW()),
      dead_by = COALESCE(dead_by, $5),
      status = CASE WHEN $3 = 'do_not_contact' THEN 'do_not_contact' ELSE status END,
      updated_at = NOW()
    WHERE id = $1 AND client_id = $2
  `, [leadId, clientId, reason, note, aoUserId]);
}

async function persistProspectDead({
  prospectId,
  clientId,
  aoUserId,
  reason,
  note,
  db,
}) {
  const { rows: before } = await db.query(`
    SELECT disposition_status, ao_current_status
    FROM prospects WHERE id = $1::uuid AND client_id = $2
  `, [prospectId, clientId]);
  if (!before.length) return { error: 'Prospect not found', status: 404 };

  if (!isActiveDisposition(before[0].disposition_status)) {
    return { already_dead: true, prospect_id: prospectId };
  }

  await db.query(`
    UPDATE prospects SET
      disposition_status = 'dead',
      dead_reason = $3,
      dead_note = $4,
      dead_at = NOW(),
      dead_by = $5,
      ao_current_status = 'dead',
      ao_next_action = 'no_action',
      ao_paused = true,
      updated_at = NOW()
    WHERE id = $1::uuid AND client_id = $2
  `, [prospectId, clientId, reason, note, aoUserId]);

  if (reason === 'do_not_contact') {
    await applyProspectSuppression({ clientId, prospectId, db });
  }

  await cancelOpenProspectTasks({ clientId, prospectId, db });

  await insertProspectDeadActivity({
    clientId,
    prospectId,
    aoUserId,
    reason,
    note,
    db,
  });

  return { prospect_id: prospectId, previous_status: before[0].ao_current_status };
}

async function markFollowUpTaskDead({
  taskId,
  clientId,
  aoUserId,
  reason,
  note,
  db = pool,
}) {
  await ensureAoFieldSchema();
  await ensureAoDispositionSchema(db);
  await ensureAoCrmSchema(db);

  const validation = validateMarkDeadInput({ reason, note });
  if (validation.error) return validation;

  const { rows } = await db.query(`
    SELECT t.id AS task_id, t.lead_id, t.ao_owner_id, t.status AS task_status,
      l.client_id, l.crm_prospect_id, l.disposition_status AS lead_disposition,
      l.business_name
    FROM ao_follow_up_tasks t
    JOIN ao_leads l ON l.id = t.lead_id
    WHERE t.id = $1 AND t.ao_owner_id = $2 AND l.client_id = $3
    LIMIT 1
  `, [taskId, aoUserId, clientId]);

  const row = rows[0];
  if (!row) return { error: 'Task not found', status: 404 };

  if (row.task_status !== 'open' && !isActiveDisposition(row.lead_disposition)) {
    return { already_dead: true, queue_item_id: taskId, lead_id: row.lead_id };
  }

  const client = await db.connect();
  try {
    await client.query('BEGIN');

    await persistLeadDead({
      leadId: row.lead_id,
      clientId,
      aoUserId,
      reason: validation.reason,
      note: validation.note,
      db: client,
    });

    await closeOpenFieldTasksForLead({
      leadId: row.lead_id,
      aoOwnerId: aoUserId,
      db: client,
    });

    let prospectResult = null;
    if (row.crm_prospect_id) {
      prospectResult = await persistProspectDead({
        prospectId: row.crm_prospect_id,
        clientId,
        aoUserId,
        reason: validation.reason,
        note: validation.note,
        db: client,
      });
      if (prospectResult?.error) {
        await client.query('ROLLBACK');
        return prospectResult;
      }

      await client.query(`
        UPDATE ao_follow_up_tasks t
        SET status = 'cancelled', completed_at = NOW()
        FROM ao_leads l
        WHERE l.crm_prospect_id = $1::uuid
          AND l.client_id = $2
          AND t.lead_id = l.id
          AND t.status = 'open'
          AND t.ao_owner_id = $3
      `, [row.crm_prospect_id, clientId, aoUserId]);
    }

    await logAoAuditEvent({
      event: 'ao_prospect_marked_dead',
      clientId,
      aoUserId,
      prospectId: row.crm_prospect_id || null,
      payload: {
        prospect_id: row.crm_prospect_id || null,
        queue_item_id: taskId,
        crm_account_id: row.crm_prospect_id || null,
        actor_id: aoUserId,
        reason: validation.reason,
        note: validation.note,
        lead_id: row.lead_id,
        occurred_at: new Date().toISOString(),
      },
    });

    await client.query('COMMIT');

    return {
      ok: true,
      queue_item_id: taskId,
      lead_id: row.lead_id,
      prospect_id: row.crm_prospect_id || null,
      already_dead: Boolean(prospectResult?.already_dead),
      disposition_status: 'dead',
      reason: validation.reason,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function markProspectDead({
  prospectId,
  clientId,
  aoUserId,
  reason,
  note,
  queueItemId = null,
  db = pool,
}) {
  await ensureAoDispositionSchema(db);
  await ensureAoCrmSchema(db);
  await ensureAoFieldSchema();

  const validation = validateMarkDeadInput({ reason, note });
  if (validation.error) return validation;

  const { rows: access } = await db.query(`
    SELECT id, assigned_ao_id, disposition_status
    FROM prospects
    WHERE id = $1::uuid AND client_id = $2
  `, [prospectId, clientId]);
  if (!access.length) return { error: 'Prospect not found', status: 404 };
  if (Number(access[0].assigned_ao_id) !== Number(aoUserId)) {
    return { error: 'Prospect is not assigned to this AO', status: 403 };
  }

  if (!isActiveDisposition(access[0].disposition_status)) {
    return {
      ok: true,
      already_dead: true,
      prospect_id: prospectId,
      queue_item_id: queueItemId,
    };
  }

  const client = await db.connect();
  try {
    await client.query('BEGIN');

    const prospectResult = await persistProspectDead({
      prospectId,
      clientId,
      aoUserId,
      reason: validation.reason,
      note: validation.note,
      db: client,
    });
    if (prospectResult?.error) {
      await client.query('ROLLBACK');
      return prospectResult;
    }

    const { rows: linkedLeads } = await client.query(`
      SELECT id FROM ao_leads
      WHERE crm_prospect_id = $1::uuid AND client_id = $2 AND ao_owner_id = $3
    `, [prospectId, clientId, aoUserId]);

    for (const lead of linkedLeads) {
      await persistLeadDead({
        leadId: lead.id,
        clientId,
        aoUserId,
        reason: validation.reason,
        note: validation.note,
        db: client,
      });
      await closeOpenFieldTasksForLead({
        leadId: lead.id,
        aoOwnerId: aoUserId,
        db: client,
      });
    }

    if (queueItemId) {
      await client.query(`
        UPDATE ao_follow_up_tasks
        SET status = 'cancelled', completed_at = NOW()
        WHERE id = $1 AND ao_owner_id = $2 AND status = 'open'
      `, [queueItemId, aoUserId]);
    }

    await logAoAuditEvent({
      event: 'ao_prospect_marked_dead',
      clientId,
      aoUserId,
      prospectId,
      payload: {
        prospect_id: prospectId,
        queue_item_id: queueItemId,
        crm_account_id: prospectId,
        actor_id: aoUserId,
        reason: validation.reason,
        note: validation.note,
        occurred_at: new Date().toISOString(),
      },
    });

    await client.query('COMMIT');

    return {
      ok: true,
      prospect_id: prospectId,
      queue_item_id: queueItemId,
      disposition_status: 'dead',
      reason: validation.reason,
      already_dead: Boolean(prospectResult.already_dead),
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

function listDeadReasons() {
  return AO_DEAD_REASONS;
}

module.exports = {
  markFollowUpTaskDead,
  markProspectDead,
  listDeadReasons,
  validateMarkDeadInput,
  isActiveDisposition,
};
