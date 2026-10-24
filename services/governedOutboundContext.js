'use strict';

const {
  assertGovernedOutboundTenantId,
  createGovernedOutboundTenantContext,
} = require('./governedOutboundTenant');

function firstPresent(...values) {
  for (const value of values) {
    if (value == null) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return null;
}

function failGovernedOutboundTenantRequired() {
  throw Object.assign(new Error('governed_outbound_tenant_required'), {
    code: 'governed_outbound_tenant_required',
  });
}

function assertGovernedOutboundTenantRequired(tenantId) {
  const raw = firstPresent(tenantId);
  if (!raw) failGovernedOutboundTenantRequired();
  return assertGovernedOutboundTenantId(raw);
}

/**
 * Governed outbound authorization identity — resolved once at the boundary.
 * Precedence: governedContext.tenantId → explicit tenantId → program.tenant_id.
 * Mission/source/client/store/infrastructure must not override program authorization.
 */
function resolveGovernedAuthorizationTenantId(input = {}) {
  const raw = firstPresent(
    input.governedContext && input.governedContext.tenantId,
    input.tenantId,
    input.explicitTenantId,
    input.program && input.program.tenant_id,
    input.program && input.program.tenantId,
  );
  if (!raw) failGovernedOutboundTenantRequired();
  return assertGovernedOutboundTenantId(raw);
}

function createGovernedOutboundContext(input = {}) {
  const tenantId = resolveGovernedAuthorizationTenantId(input);
  const transport = createGovernedOutboundTenantContext(tenantId);
  return Object.freeze({
    tenantId,
    clientId: transport.clientId,
    programId: input.programId != null ? String(input.programId) : (
      input.program && input.program.id != null ? String(input.program.id) : null
    ),
    sourceMissionId: input.sourceMissionId != null ? String(input.sourceMissionId) : (
      input.program && input.program.source_mission_id != null
        ? String(input.program.source_mission_id)
        : null
    ),
    mode: input.mode != null ? String(input.mode) : (
      input.program && input.program.mode != null ? String(input.program.mode) : null
    ),
    transport,
  });
}

/** @deprecated name — use resolveGovernedAuthorizationTenantId via this wrapper */
function resolveReplenishmentTenantContext(input = {}) {
  const tenantId = resolveGovernedAuthorizationTenantId(input);
  return Object.freeze({
    tenantId,
    clientId: Number(tenantId),
    governedContext: input.governedContext || null,
  });
}

module.exports = {
  assertGovernedOutboundTenantRequired,
  resolveGovernedAuthorizationTenantId,
  createGovernedOutboundContext,
  resolveReplenishmentTenantContext,
};
