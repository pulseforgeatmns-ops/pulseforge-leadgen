'use strict';

const ALLOWED_GOVERNED_OUTBOUND_TENANTS = Object.freeze(['10', '13']);
// Send/tick serialization. Distinct from the Max control-cycle lock so Scout
// replenishment cannot block an eligible Emmett send.
const GOVERNED_OUTBOUND_SEND_LOCK_NAMESPACE = 261018;
// Per-tenant Max outbound control cycle. Held for the duration of one cycle so
// a second tick cannot overlap Scout, schedules, or grant decisions.
const GOVERNED_OUTBOUND_CONTROL_LOCK_NAMESPACE = 261020;

function assertGovernedOutboundTenantId(tenantId) {
  const tid = String(tenantId ?? '').trim();
  if (!ALLOWED_GOVERNED_OUTBOUND_TENANTS.includes(tid)) {
    throw Object.assign(new Error(`unsupported_governed_outbound_tenant:${tid}`), { code: 'unsupported_governed_outbound_tenant' });
  }
  return tid;
}

function createGovernedOutboundTenantContext(tenantId) {
  const tid = assertGovernedOutboundTenantId(tenantId);
  const clientId = Number(tid);
  return Object.freeze({
    tenantId: tid,
    clientId,
    advisoryLockNamespace: GOVERNED_OUTBOUND_SEND_LOCK_NAMESPACE,
    controlLockNamespace: GOVERNED_OUTBOUND_CONTROL_LOCK_NAMESPACE,
    advisoryLockKey: clientId,
    attentionTitle: tid === '13' ? 'Babrun outbound needs attention' : 'Anchor outbound needs attention',
    usesBrevoTransport: tid === '10',
    usesTenantMailboxTransport: tid === '13',
    requiresAoOwners: tid === '10',
    requiresLegacyEmailTelemetry: tid === '10',
  });
}

function parseGovernedOutboundTenantIds(raw = process.env.GOVERNED_OUTBOUND_TENANT_IDS) {
  const source = String(raw || '10,13').trim();
  const ids = source.split(',').map((part) => part.trim()).filter(Boolean);
  const unique = [...new Set(ids.map(assertGovernedOutboundTenantId))];
  return unique.length ? unique : ['10'];
}

function governedOutboundEnabledForTenant(tenantId) {
  const tid = assertGovernedOutboundTenantId(tenantId);
  if (tid === '10') {
    return process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED === 'true';
  }
  if (tid === '13') {
    return process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED === 'true'
      || process.env.GOVERNED_OUTBOUND_TENANT_13_ENABLED === 'true';
  }
  return false;
}

function governedOutboundSendingDisabledForTenant(tenantId) {
  const tid = assertGovernedOutboundTenantId(tenantId);
  if (tid === '10') return process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED === 'false';
  if (tid === '13') return process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED === 'false';
  return true;
}

module.exports = {
  ALLOWED_GOVERNED_OUTBOUND_TENANTS,
  GOVERNED_OUTBOUND_SEND_LOCK_NAMESPACE,
  GOVERNED_OUTBOUND_CONTROL_LOCK_NAMESPACE,
  assertGovernedOutboundTenantId,
  createGovernedOutboundTenantContext,
  parseGovernedOutboundTenantIds,
  governedOutboundEnabledForTenant,
  governedOutboundSendingDisabledForTenant,
};
