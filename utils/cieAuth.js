'use strict';

/**
 * SPEC-096 — CIE authorization helpers.
 * For client-role users, authenticated session client_id is authoritative.
 * Opaque interview/blueprint IDs are not authorization.
 */

const {
  getRequestClientId,
  normalizeClientId,
} = require('./clientContext');
const {
  ClientIntelligenceError,
} = require('../services/clientIntelligenceInterview');

function userFrom(req) {
  return req?.user || req?.session?.user || null;
}

function isClientRole(req) {
  return userFrom(req)?.role === 'client';
}

function isInternalOperator(req) {
  const role = userFrom(req)?.role;
  return role === 'admin' || role === 'manager';
}

/**
 * Resolve the CIE client scope for this request.
 * @param {object} req
 * @param {string|number|null} [requestedId] - route/query/body hint (ignored for client role)
 */
function resolveCieClientId(req, requestedId) {
  if (isClientRole(req)) {
    const raw = userFrom(req)?.client_id;
    const id = parseInt(raw, 10);
    if (!Number.isFinite(id) || id <= 0) {
      throw new ClientIntelligenceError(
        'client_scope_required',
        'Client role requires an assigned client',
        403
      );
    }
    return id;
  }

  if (requestedId != null && String(requestedId).trim() !== '') {
    return normalizeClientId(requestedId);
  }

  return getRequestClientId(req);
}

/**
 * Fail closed when a client-role user targets another client's resource.
 * Admin/manager retain cross-client access for intentional operator workflows.
 */
function assertCieClientAccess(req, resourceClientId) {
  if (!isClientRole(req)) return;
  const allowed = resolveCieClientId(req);
  const resource = Number(resourceClientId);
  if (!Number.isFinite(resource) || resource !== Number(allowed)) {
    throw new ClientIntelligenceError(
      'forbidden_client_scope',
      'CIE resource does not belong to the authenticated client',
      403
    );
  }
}

/**
 * When client role supplies a conflicting :id / body / query client id, reject.
 */
function assertRequestedClientMatches(req, requestedId) {
  if (!isClientRole(req)) return;
  if (requestedId == null || String(requestedId).trim() === '') return;
  const allowed = resolveCieClientId(req);
  const requested = Number(requestedId);
  if (!Number.isFinite(requested) || requested !== Number(allowed)) {
    throw new ClientIntelligenceError(
      'forbidden_client_scope',
      'Client cannot override CIE client scope',
      403
    );
  }
}

module.exports = {
  isClientRole,
  isInternalOperator,
  resolveCieClientId,
  assertCieClientAccess,
  assertRequestedClientMatches,
};
