'use strict';

const { randomUUID } = require('node:crypto');
const pool = require('../db');
const { isValidRoutingIssueType } = require('../utils/aoRoutingIssueTypes');

async function findLatestDecisionId({ sessionId, tenantId }) {
  if (!sessionId) return null;
  try {
    const { rows } = await pool.query(`
      SELECT decision_id::text
      FROM decision_shadow_events
      WHERE session_id = $1
        AND ($2::text IS NULL OR tenant_id = $2::text)
      ORDER BY timestamp DESC
      LIMIT 1
    `, [String(sessionId), tenantId != null ? String(tenantId) : null]);
    return rows[0]?.decision_id || null;
  } catch {
    return null;
  }
}

async function createRoutingIssueFlag({
  tenantId,
  aoUserId,
  sessionId = null,
  conversationId = null,
  messageId = null,
  prospectId = null,
  missionId = null,
  routeObserved = null,
  routeExpected = null,
  issueType,
  notes = null,
  decisionId = null,
}) {
  if (!isValidRoutingIssueType(issueType)) {
    return { error: 'Valid issue_type required', status: 400 };
  }

  const linkedDecisionId = decisionId || await findLatestDecisionId({
    sessionId: sessionId || conversationId,
    tenantId,
  });

  const id = randomUUID();
  const { rows } = await pool.query(`
    INSERT INTO ao_routing_issue_flags (
      id, tenant_id, ao_user_id, session_id, conversation_id, message_id,
      prospect_id, mission_id, route_observed, route_expected, issue_type, notes, decision_id
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12, $13)
    RETURNING *
  `, [
    id,
    String(tenantId),
    aoUserId != null ? String(aoUserId) : null,
    sessionId || null,
    conversationId || sessionId || null,
    messageId || null,
    prospectId || null,
    missionId || null,
    routeObserved ? JSON.stringify(routeObserved) : null,
    routeExpected || null,
    issueType,
    notes ? String(notes).trim() : null,
    linkedDecisionId,
  ]);

  return { ok: true, flag: rows[0] };
}

async function listRoutingIssueFlags({ tenantId = null, limit = 50 } = {}) {
  const params = [];
  let where = '';
  if (tenantId != null) {
    params.push(String(tenantId));
    where = `WHERE tenant_id = $${params.length}`;
  }
  params.push(Math.min(Number(limit) || 50, 200));

  const { rows } = await pool.query(`
    SELECT *
    FROM ao_routing_issue_flags
    ${where}
    ORDER BY created_at DESC
    LIMIT $${params.length}
  `, params);

  return rows;
}

module.exports = {
  createRoutingIssueFlag,
  listRoutingIssueFlags,
  findLatestDecisionId,
};
