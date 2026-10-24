'use strict';

/**
 * SPEC-178 / ADR-093 — Canonical Market Definition acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  resolveCanonicalSegmentKey,
  buildSemanticMarketDefinition,
} = require('../packages/scout/intelligence/MarketDefinition');
const { buildMarketDefinition } = require('../packages/scout/intelligence/MarketUnderstanding');
const {
  buildAcquisitionSearchDefinition,
  buildSearchDefinitionFromMarketDefinition,
} = require('../packages/max/scoutAcquisition/SearchDefinition');
const { buildDelegationFromMission } = require('../packages/scout/Discovery.helpers');

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.';

function strMission(overrides = {}) {
  return {
    id: 'mission-str-1',
    tenantId: '1',
    objectiveText: STR_OBJECTIVE,
    constraints: {
      vertical: 'short_term_rental',
      locationHint: 'Greater Manchester',
    },
    ...overrides,
  };
}

describe('SPEC-178 / ADR-093 — Canonical Market Definition', () => {
  it('AUDIT-058: operator objective beats conflicting mission.constraints.vertical', () => {
    const mission = strMission({
      constraints: { vertical: 'property_management', locationHint: 'Greater Manchester' },
    });
    const resolved = resolveCanonicalSegmentKey({ mission });
    assert.equal(resolved.segmentKey, 'short_term_rental');
    assert.equal(resolved.source, 'operator_objective');
  });

  it('AUDIT-058: buildMarketDefinition stays STR when constraints.vertical conflicts', () => {
    const mission = strMission({
      constraints: { vertical: 'property_management', locationHint: 'Greater Manchester' },
    });
    const market = buildMarketDefinition({ mission });
    assert.equal(market.segmentKey, 'short_term_rental');
    assert.equal(market.segmentResolutionSource, 'operator_objective');
    assert.equal(market.searchDefinition.segments[0], 'short_term_rental');
    assert.equal(market.searchDefinition.projectedFromMarketDefinition, true);
    assert.equal(market.searchDefinition.marketDefinitionSegmentKey, 'short_term_rental');
  });

  it('locked mission plan segment wins over constraints when objective is silent', () => {
    const resolved = resolveCanonicalSegmentKey({
      mission: {
        structuredMission: { market: { segment: 'law_firm' } },
        constraints: { vertical: 'property_management' },
      },
    });
    assert.equal(resolved.segmentKey, 'law_firm');
    assert.equal(resolved.source, 'mission_objective');
  });

  it('constraints.vertical applies only when objective and plan are silent', () => {
    const resolved = resolveCanonicalSegmentKey({
      mission: { constraints: { vertical: 'accounting' } },
    });
    assert.equal(resolved.segmentKey, 'accounting');
    assert.equal(resolved.source, 'mission_constraints');
  });

  it('same mission input always resolves to the same segment', () => {
    const input = { mission: strMission({ constraints: { vertical: 'property_management' } }) };
    const first = resolveCanonicalSegmentKey(input);
    const second = resolveCanonicalSegmentKey(input);
    assert.deepEqual(first, second);
  });

  it('SearchDefinition is a projection — segments cannot diverge from MarketDefinition', () => {
    const semantic = buildSemanticMarketDefinition({
      mission: strMission(),
      segmentKey: 'short_term_rental',
      segmentSource: 'operator_objective',
      geography: 'Greater Manchester',
    });
    const projected = buildSearchDefinitionFromMarketDefinition(semantic, {
      tenantId: '1',
      targetContext: { geography: 'Greater Manchester' },
      businessContext: { serviceGeography: 'Greater Manchester', commercialCapability: 'commercial_cleaning' },
    });
    assert.equal(projected.projectedFromMarketDefinition, true);
    assert.deepEqual(projected.segments, semantic.segments);
    assert.equal(projected.marketDefinitionSegmentKey, 'short_term_rental');
  });

  it('SearchDefinition defaults cannot override a canonical MarketDefinition ancestor', () => {
    const semantic = buildSemanticMarketDefinition({
      mission: strMission(),
      segmentKey: 'short_term_rental',
      geography: 'Greater Manchester',
    });
    const projected = buildSearchDefinitionFromMarketDefinition(semantic, {
      tenantId: '1',
      businessContext: { commercialCapability: 'commercial_cleaning' },
    });
    assert.equal(projected.segments[0], 'short_term_rental');
    assert.notDeepEqual(projected.segments, ['law_firm', 'accounting']);
  });

  it('buildAcquisitionSearchDefinition projects when marketDefinition is supplied', () => {
    const semantic = buildSemanticMarketDefinition({
      mission: strMission(),
      segmentKey: 'short_term_rental',
      geography: 'Greater Manchester',
    });
    const def = buildAcquisitionSearchDefinition({
      marketDefinition: semantic,
      tenantId: '1',
      businessContext: { commercialCapability: 'commercial_cleaning', serviceGeography: 'Greater Manchester' },
    });
    assert.equal(def.projectedFromMarketDefinition, true);
    assert.equal(def.segments[0], 'short_term_rental');
  });

  it('buildDelegationFromMission derives segment from objective, not constraints.vertical alone', () => {
    const delegation = buildDelegationFromMission(
      strMission({ constraints: { vertical: 'property_management', locationHint: 'Greater Manchester' } })
    );
    assert.deepEqual(delegation.targetContext.segments, ['short_term_rental']);
  });

  it('MarketUnderstanding builds semantic model before search projection', () => {
    const market = buildMarketDefinition({
      mission: strMission({ constraints: { vertical: 'property_management' } }),
    });
    assert.equal(market.segmentKey, 'short_term_rental');
    assert.equal(market.market, 'Short-Term Rental Operations');
    assert.ok(market.searchDefinition);
    assert.equal(market.searchDefinition.segments[0], market.segmentKey);
  });
});
