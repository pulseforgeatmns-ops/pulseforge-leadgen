'use strict';
require('./helpers/denyExternalProviders.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const { sourceScope, scoutSearchFingerprint, _test: { scoutInput } } = require('../services/maxOutboundControlLoop');
const { buildBoundedScoutContext } = require('../packages/max/scoutAcquisition/BoundedContext');
const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');

const source = { id: 'mission_anchor_weekend_20261003_approved_market', payload: { structuredMission: {
  market: { segment: 'commercial_cleaning_buyers', industry: 'property_and_office_operations',
    eligibleSubsegments: ['property_manager', 'str_manager', 'realtor', 'commercial_office'] },
  geography: { region: 'Greater Manchester', scope: null,
    cities: ['Manchester', 'Hooksett', 'Bedford', 'Auburn', 'Goffstown', 'Londonderry'] },
} } };
const program = { id: 'fixture-program', tenant_id: '10', source_mission_id: source.id };
const plan = { deficit: 5, targetDays: 3, targetInventory: 15, cleanInventory: 2 };

test('the actual legacy buyer segment preserves commercial cleaning as the service through the normal delegation', () => {
  const before = JSON.stringify(source);
  const input = scoutInput(program, source, plan, { tenantId: '10' });
  const bounded = buildBoundedScoutContext(input);
  const definition = buildAcquisitionSearchDefinition({ ...bounded, authorizedTenantId: '10' });
  assert.equal(sourceScope(source).commercialCapability, 'commercial_cleaning');
  assert.equal(input.targetContext.businessType, 'commercial_cleaning');
  assert.equal(definition.businessNeed, 'commercial_cleaning');
  assert.deepEqual(definition.segments, source.payload.structuredMission.market.eligibleSubsegments);
  assert.deepEqual(definition.geography.cities, source.payload.structuredMission.geography.cities);
  assert.equal(scoutSearchFingerprint(program, source), '22d50dc07f997a3a811368a7e8b1de5a3211df5bd555260c3d42c4e780a960b7');
  assert.equal(JSON.stringify(source), before);
});

test('explicit capabilities and business types keep precedence; unrelated buyer segments gain no cleaning inference', () => {
  const explicit = structuredClone(source);
  explicit.payload.structuredMission.market.capability = 'explicit_service';
  explicit.payload.structuredMission.market.businessType = 'explicit_business';
  assert.equal(sourceScope(explicit).commercialCapability, 'explicit_service');
  assert.equal(scoutInput(program, explicit, plan, { tenantId: '10' }).targetContext.businessType, 'explicit_business');
  const unrelated = structuredClone(source);
  unrelated.payload.structuredMission.market.segment = 'unrecognized_buyers';
  assert.equal(sourceScope(unrelated).commercialCapability, null);
});
