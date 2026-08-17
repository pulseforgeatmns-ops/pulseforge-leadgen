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

module.exports = {
  assertAuthorizedClientSwitch,
  filterClientsForUser,
};
