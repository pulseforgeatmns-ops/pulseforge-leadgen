'use strict';

const { normalizeClientId } = require('./clientContext');
const { getEffectiveActor } = require('./requestIdentity');

function effectiveAoOwnerId(req) {
  const effective = getEffectiveActor(req);
  if (effective?.role === 'ao' && effective.id != null) {
    return effective.id;
  }
  const override = Number(
    req.query?.ao_owner_id
    || req.query?.ao_user_id
    || req.body?.ao_owner_id
    || req.body?.ao_user_id,
  );
  if (Number.isInteger(override) && override > 0) return override;
  if (effective?.role === 'ao') return effective.id;
  return null;
}

function effectiveRoleIsAo(req) {
  return getEffectiveActor(req)?.role === 'ao';
}

function aoClientIdForRequest(req) {
  const effective = getEffectiveActor(req);
  if (effective?.role === 'ao') {
    const assigned = Number(effective.client_id);
    return Number.isInteger(assigned) && assigned > 0 ? assigned : null;
  }
  return normalizeClientId(req.session?.active_client_id || effective?.client_id) || 10;
}

module.exports = {
  effectiveAoOwnerId,
  aoClientIdForRequest,
  effectiveRoleIsAo,
};
