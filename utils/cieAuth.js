'use strict';

/**
 * SPEC-096 — CIE authorization helpers.
 * For client-role users, authenticated session client_id is authoritative.
 * For admin/manager operators, session.active_client_id is authoritative.
 * Opaque interview/blueprint IDs are not authorization.
 */

const {
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
 * Resolve the canonical CIE tenant for this request.
 * Session/active-client context is authoritative — CIE never defaults to client 1.
 * @param {object} req
 */
function resolveCieCanonicalClientId(req) {
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

  const user = userFrom(req);
  if (user?.client_id != null && String(user.client_id).trim() !== '') {
    const bound = parseInt(user.client_id, 10);
    if (Number.isFinite(bound) && bound > 0) {
      return bound;
    }
  }

  const sessionRaw = req?.session?.active_client_id;
  if (sessionRaw != null && String(sessionRaw).trim() !== '') {
    const sessionId = parseInt(sessionRaw, 10);
    if (Number.isFinite(sessionId) && sessionId > 0) {
      return sessionId;
    }
  }

  throw new ClientIntelligenceError(
    'tenant_context_required',
    'Active client context is required for CIE operations',
    400
  );
}

/**
 * Resolve the CIE client scope for this request.
 * @param {object} req
 * @param {string|number|null} [requestedId] - route/query/body hint (must match canonical)
 */
function resolveCieClientId(req, requestedId) {
  const canonical = resolveCieCanonicalClientId(req);

  // Client-role override rejection is handled by assertRequestedClientMatches.
  if (isClientRole(req)) {
    return canonical;
  }

  if (requestedId != null && String(requestedId).trim() !== '') {
    const requested = normalizeClientId(requestedId);
    if (Number(requested) !== Number(canonical)) {
      throw new ClientIntelligenceError(
        'tenant_mismatch',
        'Requested client does not match active tenant context',
        403
      );
    }
  }

  return canonical;
}

/**
 * Fail closed when a resource belongs to a different tenant than the request context.
 */
function assertCieClientAccess(req, resourceClientId) {
  const allowed = resolveCieCanonicalClientId(req);
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
  const allowed = resolveCieCanonicalClientId(req);
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
  resolveCieCanonicalClientId,
  resolveCieClientId,
  assertCieClientAccess,
  assertRequestedClientMatches,
};
