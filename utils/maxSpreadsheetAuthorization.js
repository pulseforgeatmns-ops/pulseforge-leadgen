'use strict';

const { getAuthenticatedActor, getEffectiveActor } = require('./requestIdentity');

function rejected(code, statusCode = 403) {
  return Object.assign(new Error(code), { code, statusCode });
}

function positiveId(value) {
  const id = Number(value);
  return Number.isSafeInteger(id) && id > 0 ? id : null;
}

// This must be a deliberately configured authenticated principal, never a name,
// workbook author, effective/impersonated AO or client-supplied approval flag.
function canApproveSpreadsheet(authenticated, approverId = process.env.MAX_SPREADSHEET_APPROVER_USER_ID) {
  const expected = positiveId(approverId);
  return expected != null && positiveId(authenticated?.id) === expected
    && authenticated?.active !== false && authenticated?.role === 'admin';
}

async function resolveSpreadsheetScope(req, db, { approverId, allowMissingAo = false } = {}) {
  const authenticated = getAuthenticatedActor(req);
  const effective = getEffectiveActor(req);
  if (!positiveId(authenticated?.id) || !positiveId(effective?.id)) throw rejected('authenticated_actor_required');
  // Reload both principals: a stale browser/session cannot retain revoked access.
  const { rows } = await db.query('SELECT id, role, client_id, active FROM users WHERE id = ANY($1::int[])',
    [[positiveId(authenticated.id), positiveId(effective.id)]]);
  const auth = rows.find(r => String(r.id) === String(authenticated.id));
  const actor = rows.find(r => String(r.id) === String(effective.id));
  if (!auth || !actor || auth.active === false || actor.active === false) throw rejected('actor_access_revoked');
  if (!['admin', 'manager', 'ao'].includes(auth.role) || !['admin', 'manager', 'ao'].includes(actor.role)) {
    throw rejected('spreadsheet_access_denied');
  }
  if (auth.id !== actor.id && auth.role !== 'admin') throw rejected('impersonation_not_authorized');
  // Only a current admin may choose a tenant. A stale session after reassignment
  // or demotion cannot override the current non-admin user's tenant binding.
  const clientId = actor.role === 'admin'
    ? positiveId(req.session?.active_client_id || actor.client_id)
    : positiveId(actor.client_id);
  if (!clientId) throw rejected('authenticated_tenant_required');
  const requestedClient = req.body?.client_id ?? req.body?.clientId ?? req.query?.client_id;
  if (requestedClient != null && positiveId(requestedClient) !== clientId) throw rejected('tenant_scope_mismatch');
  const requestedAo = req.body?.ao_id ?? req.body?.ao_owner_id ?? req.query?.ao_id;
  const aoId = actor.role === 'ao' ? positiveId(actor.id) : positiveId(requestedAo);
  if (!aoId && !allowMissingAo) throw rejected('explicit_ao_scope_required', 400);
  if (actor.role === 'ao' && requestedAo != null && positiveId(requestedAo) !== aoId) throw rejected('ao_scope_mismatch');
  if (aoId) {
    const ao = await db.query("SELECT id FROM users WHERE id = $1 AND client_id = $2 AND role = 'ao' AND active IS DISTINCT FROM FALSE", [aoId, clientId]);
    if (!ao.rows.length) throw rejected('ao_scope_not_authorized');
  }
  return Object.freeze({ clientId, tenantId: clientId, aoId, actorId: positiveId(actor.id),
    authenticatedUserId: positiveId(auth.id), canApprove: canApproveSpreadsheet(auth, approverId) });
}

function deniesSave(text = '') {
  return /\b(do\s+not|don['’]?t|never|without|no\s+sav|not\s+yet|not\s+now|cancel|stop|hold|preview|before\s+(?:you\s+)?sav)\b/i.test(String(text));
}

function explicitlyApproves(text = '') {
  return !deniesSave(text) && /^(?:save|approve|confirm|apply)(?:\s+(?:the\s+)?(?:selected|safe|these|those|all|approved))?(?:\s+(?:changes|updates|operations))?[.!]?$/i.test(String(text).trim());
}

module.exports = { resolveSpreadsheetScope, canApproveSpreadsheet, deniesSave, explicitlyApproves, rejected };
