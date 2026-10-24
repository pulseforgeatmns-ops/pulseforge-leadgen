'use strict';

const { createGovernedOutboundContext } = require('../../services/governedOutboundContext');
const { hash, missionScope } = require('../../packages/acquisition-mission/DailyOutboundPolicy');

function anchorGovernedContext(overrides = {}) {
  return createGovernedOutboundContext({ tenantId: '10', ...overrides });
}

function babrunGovernedContext(overrides = {}) {
  return createGovernedOutboundContext({ tenantId: '13', ...overrides });
}

function governedProgram({ tenantId, sourceMissionId = 'mission_source', mode = 'active', policy = null } = {}) {
  if (!tenantId) {
    throw Object.assign(new Error('governed_outbound_tenant_required'), { code: 'governed_outbound_tenant_required' });
  }
  const ctx = createGovernedOutboundContext({ tenantId, programId: `outbound_${tenantId}`, sourceMissionId, mode });
  const p = policy || {
    tenantId: ctx.tenantId,
    sourceMissionId,
    senderEmail: ctx.tenantId === '13' ? 'hello@babrun.com' : 'sender@anchor.example',
    inboxIntegrationId: 'mailbox',
    sendingIdentityId: 'identity',
    dailyCap: 2,
    totalCap: 10,
    spacingMinutes: 60,
  };
  return {
    id: ctx.programId,
    tenant_id: ctx.tenantId,
    source_mission_id: sourceMissionId,
    mode,
    policy: p,
    policy_hash: hash(p),
    scope_hash: hash(missionScope({ tenantId: ctx.tenantId, structuredMission: { immutable: true } })),
  };
}

function governedAdaptersFixture({ tenantId, infrastructure = null, tenant = null, loadMission = null } = {}) {
  const governedContext = createGovernedOutboundContext({ tenantId });
  return {
    governedContext,
    tenantId: governedContext.tenantId,
    infrastructure: infrastructure || (async () => ({
      cap: 2,
      snapshot: { sentToday: 0 },
      assessed: { governor: { outcome: 'proceed' }, health: { score: 80 } },
      operating: { planningDailyCapacity: 2, dispatchCapacityNow: 2 },
    })),
    tenant: tenant || (async () => ({ sender: { senderEmail: governedContext.tenantId === '13' ? 'hello@babrun.com' : 'sender@anchor.example' } })),
    loadMission,
  };
}

function governedStoreFixture({ context, candidateOwnership = async () => null, event = async () => {} } = {}) {
  const governedContext = context || anchorGovernedContext();
  return {
    tenantId: governedContext.tenantId,
    clientId: governedContext.clientId,
    governedContext,
    candidateOwnership,
    event,
  };
}

function createGovernedOutboundTestContext(options = {}) {
  return createGovernedOutboundContext(options);
}

function createGovernedOutboundTestStore({ context, ...rest } = {}) {
  return governedStoreFixture({ context, ...rest });
}

module.exports = {
  anchorGovernedContext,
  babrunGovernedContext,
  governedProgram,
  governedAdaptersFixture,
  governedStoreFixture,
  createGovernedOutboundTestContext,
  createGovernedOutboundTestStore,
};
