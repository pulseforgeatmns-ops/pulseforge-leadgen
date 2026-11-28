'use strict';

// Package A's narrow admission boundary.  This is deliberately an admission
// decision only: it grants no specialist, provider, action, or execution right.
async function authorizeObjectiveAdmission(pool, actor, tenantId) {
  if (!pool || typeof pool.query !== 'function' || !actor || !/^[1-9][0-9]*$/.test(String(actor.id || ''))) return { authorized: false };
  const r = await pool.query('SELECT id, client_id, role, active FROM users WHERE id=$1', [Number(actor.id)]);
  const row = r.rows[0];
  if (!row || row.active !== true || !['admin', 'manager'].includes(row.role)) return { authorized: false };
  // Operator admins are tenant-scoped by their trusted client membership.
  if (String(row.client_id) !== String(tenantId)) return { authorized: false };
  return { authorized: true, actorId: String(row.id), tenantId: String(tenantId), policy: 'objective_admission_v1' };
}
module.exports = { authorizeObjectiveAdmission };
