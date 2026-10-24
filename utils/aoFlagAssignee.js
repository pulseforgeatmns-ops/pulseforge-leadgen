'use strict';

const pool = require('../db');

const OPERATOR_ROLES = ['admin', 'manager'];

async function resolveFlagAssigneeUserId(clientId, db = pool) {
  const emailCandidates = [
    process.env.JAKE_EMAIL,
    process.env.ADMIN_EMAIL,
    'jacob@gopulseforge.com',
  ].filter(Boolean);

  for (const email of emailCandidates) {
    const { rows } = await db.query(`
      SELECT id, name, email, role
      FROM users
      WHERE active = true
        AND lower(trim(email)) = lower(trim($1))
        AND role = ANY($2::text[])
      ORDER BY CASE WHEN client_id = $3 THEN 0 WHEN client_id IS NULL THEN 1 ELSE 2 END, id ASC
      LIMIT 1
    `, [email, OPERATOR_ROLES, clientId]);
    if (rows[0]) return rows[0];
  }

  const { rows } = await db.query(`
    SELECT id, name, email, role
    FROM users
    WHERE active = true
      AND role = ANY($1::text[])
      AND (client_id = $2 OR client_id IS NULL)
    ORDER BY CASE WHEN client_id = $2 THEN 0 ELSE 1 END, id ASC
    LIMIT 1
  `, [OPERATOR_ROLES, clientId]);
  return rows[0] || null;
}

function isSelfFlag({ createdByUserId, assigneeUserId, creatorRole }) {
  if (!createdByUserId || !assigneeUserId) return false;
  if (Number(createdByUserId) === Number(assigneeUserId)) return true;
  return OPERATOR_ROLES.includes(String(creatorRole || '').toLowerCase())
    && Number(createdByUserId) === Number(assigneeUserId);
}

module.exports = {
  resolveFlagAssigneeUserId,
  isSelfFlag,
  OPERATOR_ROLES,
};
