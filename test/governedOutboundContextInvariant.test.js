'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  assertGovernedOutboundTenantRequired,
  resolveGovernedAuthorizationTenantId,
  createGovernedOutboundContext,
  resolveReplenishmentTenantContext,
} = require('../services/governedOutboundContext');
const {
  anchorGovernedContext,
  babrunGovernedContext,
  governedProgram,
} = require('./helpers/governedOutboundFixtures');

test('Anchor 10 and Babrun 13 are allowed governed authorization tenants', () => {
  assert.equal(assertGovernedOutboundTenantRequired('10'), '10');
  assert.equal(assertGovernedOutboundTenantRequired('13'), '13');
  assert.equal(anchorGovernedContext().tenantId, '10');
  assert.equal(babrunGovernedContext().tenantId, '13');
});

test('unsupported governed tenant fails closed', () => {
  assert.throws(() => assertGovernedOutboundTenantRequired('99'), { code: 'unsupported_governed_outbound_tenant' });
  assert.throws(() => resolveGovernedAuthorizationTenantId({ tenantId: '1' }), { code: 'unsupported_governed_outbound_tenant' });
});

test('missing governed tenant fails at boundary', () => {
  assert.throws(() => assertGovernedOutboundTenantRequired(''), { code: 'governed_outbound_tenant_required' });
  assert.throws(() => assertGovernedOutboundTenantRequired(null), { code: 'governed_outbound_tenant_required' });
  assert.throws(() => resolveGovernedAuthorizationTenantId({}), { code: 'governed_outbound_tenant_required' });
  assert.throws(() => createGovernedOutboundContext({}), { code: 'governed_outbound_tenant_required' });
});

test('program tenant wins over mission/source metadata', () => {
  const program = governedProgram({ tenantId: '10', sourceMissionId: 'mission_anchor' });
  const tenantId = resolveGovernedAuthorizationTenantId({
    program,
    source: { tenantId: '1', tenant_id: '1' },
  });
  assert.equal(tenantId, '10');
  const ctx = createGovernedOutboundContext({ program, sourceMissionId: 'mission_other' });
  assert.equal(ctx.tenantId, '10');
});

test('clientId alone cannot establish governed authorization', () => {
  assert.throws(
    () => resolveReplenishmentTenantContext({ clientId: 10 }),
    { code: 'governed_outbound_tenant_required' },
  );
});

test('infrastructure tenant hint without program/context does not authorize', () => {
  assert.throws(
    () => resolveReplenishmentTenantContext({
      scoutContext: { tenantId: '10' },
    }),
    { code: 'governed_outbound_tenant_required' },
  );
});

test('sender-only fixtures without program tenant fail closed', () => {
  assert.throws(
    () => resolveReplenishmentTenantContext({
      store: { tenantId: '10', clientId: 10 },
    }),
    { code: 'governed_outbound_tenant_required' },
  );
});
