'use strict';
const { tenant, failure } = require('../packages/max/clarity/common');
const { getAuthenticatedActor, isImpersonating } = require('../utils/requestIdentity');
// Session identity identifies the login, never its current authority. Recheck
// active/role/tenant on every protected evidence read and decision request.
async function authorizeClarity(req, { pool, write = false } = {}) {
  if (isImpersonating(req)) throw failure('clarity_impersonation_forbidden',403);
  const sessionActor = getAuthenticatedActor(req);
  const actorId = tenant(sessionActor?.id);
  const actor = (await pool.query('SELECT id,role,client_id,active FROM users WHERE id=$1',[actorId])).rows[0];
  if (!actor || actor.active !== true || !['admin','manager','viewer','client'].includes(actor.role)) throw failure('clarity_unauthorized',403);
  const selected = req.query?.client_id ?? req.body?.client_id ?? req.session?.active_client_id;
  const cid = tenant(actor.role === 'admin' ? selected : actor.client_id);
  if (selected != null && tenant(selected) !== cid) throw failure('clarity_tenant_mismatch',403);
  if (cid !== 10) throw failure('clarity_not_found',404);
  const canDecide = ['admin','manager'].includes(actor.role);
  if (write && !canDecide) throw failure('clarity_approval_forbidden',403);
  return { clientId: cid, actorId: actor.id, role: actor.role, canDecide };
}
module.exports = { authorizeClarity };
