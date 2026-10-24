'use strict';

const { normalizeClientId } = require('./clientContext');

/**
 * Whether the signed-in user may select targetClientId as active tenant.
 * Admin retains cross-tenant access; other roles honor users.client_id when set.
 */
function assertAuthorizedClientSwitch(user, targetClientId) {
  if (!user) {
    return { ok: false, status: 401, error: 'unauthenticated' };
  }
  if (user.role === 'admin') {
    return { ok: true };
  }
  const bound =
    user.client_id != null && String(user.client_id).trim() !== ''
      ? normalizeClientId(user.client_id)
      : null;
  if (bound != null && Number(normalizeClientId(targetClientId)) !== Number(bound)) {
    return {
      ok: false,
      status: 403,
      error: 'forbidden_client_scope',
      message: 'You are not authorized to switch to that client',
    };
  }
  return { ok: true };
}

function filterClientsForUser(clients, user) {
  if (!user || user.role === 'admin') return clients;
  const bound =
    user.client_id != null && String(user.client_id).trim() !== ''
      ? normalizeClientId(user.client_id)
      : null;
  if (bound == null) return clients;
  return (clients || []).filter(c => Number(c.id) === Number(bound));
}

/** Session active_client_id must reference an operator-switchable tenant when possible. */
function reconcileOperatorActiveClient(session, clients) {
  const list = Array.isArray(clients) ? clients : [];
  if (!list.length) {
    return normalizeClientId(session?.active_client_id || 1);
  }
  let activeId = normalizeClientId(session?.active_client_id || list[0].id);
  const allowed = list.some((c) => Number(c.id) === Number(activeId));
  if (!allowed) {
    activeId = Number(list[0].id);
    if (session) session.active_client_id = activeId;
  }
  return activeId;
}

module.exports = {
  assertAuthorizedClientSwitch,
  filterClientsForUser,
  reconcileOperatorActiveClient,
};
