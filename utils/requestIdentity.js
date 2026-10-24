'use strict';

/**
 * Canonical request identity: authenticated actor (session) vs effective actor (AO impersonation).
 */

function snapshotUser(user) {
  if (!user) return null;
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role,
    client_id: user.client_id ?? null,
    active: user.active !== false,
  };
}

function getSessionImpersonation(session) {
  if (!session?.impersonation?.active) return null;
  return session.impersonation;
}

function getAuthenticatedActor(req) {
  return req.authenticatedUser
    || req.session?.user
    || req.user
    || null;
}

function getEffectiveActor(req) {
  if (req.effectiveUser) return req.effectiveUser;
  const imp = getSessionImpersonation(req.session);
  if (imp?.effectiveUser) return imp.effectiveUser;
  return getAuthenticatedActor(req);
}

function isImpersonating(req) {
  return Boolean(getSessionImpersonation(req.session));
}

function impersonationProvenance(req) {
  if (!isImpersonating(req)) return null;
  const auth = getAuthenticatedActor(req);
  const effective = getEffectiveActor(req);
  const imp = getSessionImpersonation(req.session);
  return {
    impersonated: true,
    authenticated_user_id: auth?.id ?? imp?.authenticatedUserId ?? null,
    effective_user_id: effective?.id ?? imp?.effectiveUserId ?? null,
    tenant_id: imp?.tenantId ?? null,
    started_at: imp?.startedAt ?? null,
    started_by_role: imp?.startedByRole ?? null,
  };
}

/**
 * Attach req.authenticatedUser, req.effectiveUser, req.impersonation from session.
 * Does not mutate session.user (always the authenticated login).
 */
function bindRequestIdentity(req) {
  const authenticated = snapshotUser(req.session?.user || req.user);
  req.authenticatedUser = authenticated;

  const imp = getSessionImpersonation(req.session);
  if (imp?.active && imp.effectiveUser) {
    req.impersonation = {
      active: true,
      authenticatedUserId: imp.authenticatedUserId,
      effectiveUserId: imp.effectiveUserId,
      tenantId: imp.tenantId,
      startedAt: imp.startedAt,
      startedByRole: imp.startedByRole,
    };
    req.effectiveUser = snapshotUser(imp.effectiveUser);
  } else {
    req.impersonation = null;
    req.effectiveUser = authenticated;
  }
}

module.exports = {
  snapshotUser,
  bindRequestIdentity,
  getAuthenticatedActor,
  getEffectiveActor,
  isImpersonating,
  impersonationProvenance,
  getSessionImpersonation,
};
