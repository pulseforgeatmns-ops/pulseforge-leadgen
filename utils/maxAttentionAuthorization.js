'use strict';

const { getAuthenticatedActor, getEffectiveActor } = require('./requestIdentity');

function positiveId(value) {
  if (!['string', 'number'].includes(typeof value) || !/^[1-9][0-9]*$/.test(String(value))) return null;
  const id = Number(value);
  return Number.isSafeInteger(id) ? id : null;
}
function rejected(code, statusCode = 403) {
  return Object.assign(new Error(code), { code, statusCode });
}

async function resolveAttentionScope(req, db) {
  const authenticatedId = positiveId(getAuthenticatedActor(req)?.id);
  const effectiveId = positiveId(getEffectiveActor(req)?.id);
  if (!authenticatedId || !effectiveId) throw rejected('authenticated_actor_required');
  // Do not trust a stale role, tenant binding or active flag in the session.
  const { rows } = await db.query('SELECT id, role, client_id, active FROM users WHERE id = ANY($1::int[])',
    [[authenticatedId, effectiveId]]);
  const auth = rows.find(row => positiveId(row.id) === authenticatedId);
  const actor = rows.find(row => positiveId(row.id) === effectiveId);
  if (!auth || !actor || auth.active !== true || actor.active !== true) throw rejected('actor_access_revoked');
  if (![auth.role, actor.role].every(role => ['admin', 'manager'].includes(role))) {
    throw rejected('attention_access_denied');
  }
  if (authenticatedId !== effectiveId && auth.role !== 'admin') throw rejected('impersonation_not_authorized');
  const clientId = actor.role === 'admin'
    ? positiveId(req.session?.active_client_id ?? actor.client_id)
    : positiveId(actor.client_id);
  if (!clientId) throw rejected('authenticated_tenant_required');
  const selectors = [req.body?.client_id, req.body?.clientId, req.query?.client_id, req.query?.clientId]
    .filter(value => value !== undefined);
  if (selectors.some(value => positiveId(value) === null)) throw rejected('invalid_client_id', 400);
  if (selectors.some(value => positiveId(value) !== clientId)) throw rejected('tenant_scope_mismatch');
  const client = await db.query('SELECT id FROM clients WHERE id = $1 AND active = true', [clientId]);
  if (!client.rows.length) throw rejected('tenant_access_revoked');
  return clientId;
}

module.exports = { resolveAttentionScope };
