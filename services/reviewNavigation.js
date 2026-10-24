'use strict';
const { parseReviewNavigation, resolveReviewNavigation } = require('../packages/max/workspace/ReviewNavigation');
const { resolveSpreadsheetScope } = require('../utils/maxSpreadsheetAuthorization');
async function reviewNavigationForRequest(req, { db = require('../db'), explicit = req.body?.reviewNavigation } = {}) {
  const intent = parseReviewNavigation(req.body?.question, explicit);
  if (!intent) return null;
  if (intent.kind === 'blocked') return { handled: true, ...resolveReviewNavigation(intent) };
  const scopeRequest = { ...req, body: { ...req.body, ...(intent.ownerId != null ? { ao_id: intent.ownerId } : {}) } };
  const scope = await resolveSpreadsheetScope(scopeRequest, db, { allowMissingAo: true });
  const requestedTenant = req.body?.context?.tenantId ?? req.body?.tenantId;
  if (requestedTenant != null && String(requestedTenant) !== String(scope.clientId)) throw Object.assign(new Error('tenant_mismatch'), { statusCode: 403 });
  if (intent.kind === 'content_review' && !['admin', 'manager'].includes(scope.actorRole)) throw Object.assign(new Error('review_access_denied'), { statusCode: 403 });
  let aos;
  if (intent.kind === 'account_review') {
    ({ rows: aos } = await db.query("SELECT id, name FROM users WHERE client_id = $1 AND role = 'ao' AND active IS DISTINCT FROM FALSE AND ($2::int IS NULL OR id = $2) ORDER BY name, id", [scope.clientId, scope.aoId]));
  }
  return { handled: true, ...resolveReviewNavigation(intent, { tenantId: scope.clientId, aos, defaultAoId: scope.aoId }), tenantId: String(scope.clientId) };
}
module.exports = { reviewNavigationForRequest };
