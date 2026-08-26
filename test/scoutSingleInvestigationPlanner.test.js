'use strict';

/**
 * SPEC-180 — Single Investigation Planner acceptance tests.
 * ADR-095: one canonical planner; InvestigationPlanBuilder is a compatibility adapter.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { createHypothesisInvestigationPlan } = require('../packages/scout/coverage/HypothesisInvestigationPlanner');
const {
  createInvestigationPlan,
  isCanonicalPlan,
  projectProviderSequenceFromCanonical,
} = require('../packages/scout/investigation/InvestigationPlanBuilder');
const { runHypothesisDrivenDiscovery } = require('../packages/scout/coverage/HypothesisDrivenDiscoveryEngine');
const { buildMarketDefinition } = require('../packages/scout/intelligence/MarketUnderstanding');
const { createInjectedDiscoverAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');

function sampleMission() {
  return {
    id: 'mission-spec180',
    tenantId: '10',
    objectiveText: 'Find property managers in Manchester NH',
    constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
  };
}

describe('SPEC-180 — Single Investigation Planner', () => {
  it('canonical planner emits version SPEC-180 with tasks[]', () => {
    const mission = sampleMission();
    const marketDefinition = buildMarketDefinition({ mission });
    const plan = createHypothesisInvestigationPlan({ mission, marketDefinition });

    assert.equal(plan.version, 'SPEC-180');
    assert.ok(Array.isArray(plan.tasks));
    assert.ok(plan.tasks.length >= 1);
    assert.ok(Array.isArray(plan.hypotheses));
    assert.ok(Array.isArray(plan.evidenceRequirements));
    assert.ok(Array.isArray(plan.assignedProviders));
    assert.match(plan.rationale, /SPEC-180/);
  });

  it('InvestigationPlanBuilder delegates without independent hypothesis generation', () => {
    const mission = sampleMission();
    const marketDefinition = buildMarketDefinition({ mission });

    const canonical = createHypothesisInvestigationPlan({ mission, marketDefinition });
    const adapted = createInvestigationPlan({ mission, marketDefinition });

    assert.equal(adapted.version, 'SPEC-180');
    assert.ok(isCanonicalPlan(adapted));
    assert.deepEqual(
      adapted.tasks.map((t) => t.evidenceType),
      canonical.tasks.map((t) => t.evidenceType)
    );
    assert.ok(Array.isArray(adapted.providerSequence));
    assert.ok(adapted.providerSequence.length >= 1);
  });

  it('HypothesisDrivenDiscoveryEngine reuses pre-built plan (no duplicate planning)', async () => {
    const mission = sampleMission();
    const marketDefinition = buildMarketDefinition({ mission });
    const prebuiltPlan = createHypothesisInvestigationPlan({ mission, marketDefinition });

    const adapter = createInjectedDiscoverAdapter(async () => [
      {
        id: 'biz-1',
        name: 'Test PM Co',
        website: 'https://example.com',
        address: '100 Main St, Manchester NH',
      },
    ]);

    const result = await runHypothesisDrivenDiscovery({
      mission,
      marketDefinition,
      searchDefinition: { cities: ['Manchester NH'], concepts: ['property management'] },
      adapters: [adapter],
      investigationPlan: prebuiltPlan,
      opts: { maxIterations: 1 },
    });

    assert.equal(result.investigationPlan.createdAt, prebuiltPlan.createdAt);
    assert.equal(result.investigationPlan.version, 'SPEC-180');
    assert.deepEqual(
      result.investigationPlan.tasks.map((t) => t.id),
      prebuiltPlan.tasks.map((t) => t.id)
    );
  });

  it('projectProviderSequenceFromCanonical maps tasks to legacy providerSequence', () => {
    const mission = sampleMission();
    const marketDefinition = buildMarketDefinition({ mission });
    const plan = createHypothesisInvestigationPlan({ mission, marketDefinition });
    const sequence = projectProviderSequenceFromCanonical(plan);

    assert.ok(sequence.length >= 1);
    for (const entry of sequence) {
      assert.ok(entry.providerId);
      assert.ok(Array.isArray(entry.evidenceExpected));
      assert.ok(entry.gap);
    }
  });
});
