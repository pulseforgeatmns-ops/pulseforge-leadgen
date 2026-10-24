'use strict';

/**
 * SPEC-161 — Market Memory acceptance tests.
 * ADR-081 — Markets Are Living Systems.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildBusinessMemory,
  buildMarketMemoryRecord,
  appendMarketSnapshot,
  detectMarketChanges,
  detectEntityChanges,
  reviseBusinessUnderstanding,
  mergeMarketMemory,
  extractMarketMemoryFromDiscovery,
  recallMarketMemoryForInvestigation,
  buildMarketChangesSection,
} = require('../packages/scout/memory/MarketMemory');
const {
  createMemoryIntelligenceStore,
  persistDiscoveryKnowledge,
  prepareInvestigationWithMemory,
  setDefaultStore,
  marketEntityKey,
} = require('../packages/scout/memory');
const { runDiscoveryPipeline } = require('../packages/scout/DiscoveryPipeline');
const { buildSemanticMarketDefinition } = require('../packages/scout/intelligence/MarketDefinition');
const { createInjectedDiscoverAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');

function sampleMission(overrides = {}) {
  return {
    id: 'mission-spec161-1',
    tenantId: '10',
    clientId: 10,
    objectiveText: 'Find property management operators in Manchester NH',
    constraints: {
      vertical: 'property_management',
      locationHint: 'Manchester NH',
    },
    ...overrides,
  };
}

function abcBusiness(overrides = {}) {
  return buildBusinessMemory({
    entityId: 'abc_property_management',
    name: 'ABC Property Management',
    currentUnderstanding: {
      assertions: ['Manages 18 properties'],
      confidence: 0.72,
      summary: 'Small STR operator',
    },
    buyingSignalHistory: [{ type: 'portfolio_growth', label: '18 properties', observedAt: '2026-01-15' }],
    confidenceHistory: [{ at: '2026-01-15', confidence: 0.72 }],
    ...overrides,
  });
}

function priorMarketMemory() {
  return buildMarketMemoryRecord({
    tenantId: '10',
    entityKey: marketEntityKey('Manchester NH', 'property_management'),
    geography: 'Manchester NH',
    segment: 'property_management',
    confidence: 0.82,
    entities: [abcBusiness(), buildBusinessMemory({ entityId: 'xyz_pm', name: 'XYZ PM' })],
    marketUnderstanding: {
      dominantTerminology: ['Vacation Rental Management'],
      outstandingUnknowns: ['Vendor relationships for ABC'],
    },
    coverage: { investigated: 43, qualified: 12 },
    investigationCount: 1,
    historicalSnapshots: [
      { at: '2026-01-15', confidence: 0.82, entityCount: 43 },
    ],
  });
}

describe('SPEC-161 — Market Memory', () => {
  let store;

  beforeEach(() => {
    store = createMemoryIntelligenceStore();
    setDefaultStore(store);
  });

  it('Scenario 1: revisiting a market loads existing Market Memory', async () => {
    const prior = priorMarketMemory();
    await store.persistKnowledge('10', { market: prior, counts: { companies: 0, claims: 0 } });

    const recall = recallMarketMemoryForInvestigation({ priorMarketMemory: prior });
    assert.equal(recall.loaded, true);
    assert.equal(recall.investigationStartsFromMemory, true);
    assert.equal(recall.entities.length, 2);

    const prep = await prepareInvestigationWithMemory({
      tenantId: '10',
      marketDefinition: { geography: 'Manchester NH', segment: 'property_management' },
      opts: { store },
    });
    assert.equal(prep.hasPriorKnowledge, true);
    assert.ok(prep.memory.market);
  });

  it('Scenario 2: known business changes update timeline and detect difference', () => {
    const prior = abcBusiness();
    const current = abcBusiness({
      currentUnderstanding: {
        assertions: ['Manages 31 properties', 'Hiring cleaners'],
        confidence: 0.88,
        summary: 'Growing STR operator',
      },
      buyingSignalHistory: [
        { type: 'portfolio_growth', label: '31 properties', observedAt: '2026-06-01' },
        { type: 'hiring', label: 'Hiring cleaners', observedAt: '2026-06-01' },
      ],
    });

    const revised = reviseBusinessUnderstanding(prior, current, {
      reason: 'Portfolio grew from 18 to 31 properties',
      missionId: 'mission-2',
    });

    assert.equal(revised.historicalUnderstandings.length, 1);
    assert.equal(revised.historicalUnderstandings[0].archived, true);
    assert.equal(revised.currentUnderstanding.assertions[0], 'Manages 31 properties');
    assert.ok(revised.confidenceHistory.length >= 2);

    const changes = detectEntityChanges([prior], [current]);
    assert.equal(changes.understandingRevisions.length, 1);
    assert.equal(changes.buyingSignalsIncreased.length, 1);
    assert.ok(changes.observations.some((o) => o.kind === 'understanding_revised'));
  });

  it('Scenario 3: no meaningful changes increases confidence and avoids duplicate work', () => {
    const prior = priorMarketMemory();
    const currentEntities = prior.entities.map((e) => ({ ...e }));

    const changes = detectMarketChanges(prior, {
      entities: currentEntities,
      confidence: prior.confidence,
      marketUnderstanding: prior.marketUnderstanding,
      coverage: prior.coverage,
    });

    assert.equal(changes.duplicateInvestigationAvoided, true);
    assert.equal(changes.confidenceChange.direction, 'increased');
    assert.ok(changes.confidenceChange.current > prior.confidence);
    assert.ok(changes.observations.some((o) => o.kind === 'confidence_reinforced'));
  });

  it('Scenario 4: contradictory evidence revises understanding while retaining history', () => {
    const prior = abcBusiness({
      currentUnderstanding: {
        assertions: ['Vacation rental operator'],
        confidence: 0.75,
      },
    });
    const current = abcBusiness({
      currentUnderstanding: {
        assertions: ['Corporate housing operator'],
        confidence: 0.81,
        contradictoryEvidence: [{ observation: 'Website now lists corporate housing' }],
      },
    });

    const revised = reviseBusinessUnderstanding(prior, current, {
      reason: 'Market drift: vacation rental → corporate housing',
    });

    assert.equal(revised.historicalUnderstandings.length, 1);
    assert.equal(revised.historicalUnderstandings[0].understanding.assertions[0], 'Vacation rental operator');
    assert.equal(revised.currentUnderstanding.assertions[0], 'Corporate housing operator');
    assert.equal(revised.status, 'conflict');

    const marketChanges = detectMarketChanges(
      buildMarketMemoryRecord({
        entities: [prior],
        marketUnderstanding: { dominantTerminology: ['Vacation Rental'] },
        confidence: 0.75,
      }),
      {
        entities: [current],
        confidence: 0.81,
        marketUnderstanding: { dominantTerminology: ['Corporate Housing'] },
      }
    );

    assert.ok(marketChanges.marketDrift);
    assert.equal(marketChanges.marketDrift.prior, 'Vacation Rental');
    assert.equal(marketChanges.marketDrift.current, 'Corporate Housing');
  });

  it('Scenario 5: Mission Intelligence Report includes market changes section', () => {
    const prior = priorMarketMemory();
    const currentEntities = [
      abcBusiness({
        currentUnderstanding: {
          assertions: ['Manages 31 properties'],
          confidence: 0.9,
        },
        buyingSignalHistory: [
          { type: 'portfolio_growth', label: '31 properties' },
          { type: 'hiring', label: 'Hiring cleaners' },
        ],
      }),
      buildBusinessMemory({ entityId: 'new_op', name: 'New Operator LLC' }),
    ];

    const changes = detectMarketChanges(prior, {
      entities: currentEntities,
      confidence: 0.86,
      coverage: { investigated: 48 },
      marketUnderstanding: prior.marketUnderstanding,
    });

    const section = buildMarketChangesSection(changes, {
      outstandingUnknowns: ['Vendor relationships for ABC'],
    });

    assert.equal(section.spec, 'SPEC-161');
    assert.equal(section.hasPriorMemory, true);
    assert.ok(section.marketChangesSinceLastInvestigation.newOperators.length >= 1);
    assert.ok(section.marketChangesSinceLastInvestigation.marketGrowth);
    assert.equal(section.marketChangesSinceLastInvestigation.marketGrowth.delta, 5);
    assert.ok(section.marketChangesSinceLastInvestigation.confidenceChange);
    assert.ok(section.marketChangesSinceLastInvestigation.outstandingUnknowns.length >= 1);
  });

  it('persists market memory snapshots across discovery missions', async () => {
    const prior = priorMarketMemory();
    await store.persistKnowledge('10', { market: prior, counts: {} });

    const pipelineResult = {
      marketDefinition: {
        geography: 'Manchester NH',
        segment: 'property_management',
        terminology: ['Vacation Rental Management'],
        customerTypes: ['property_management'],
      },
      missionIntelligenceReport: {
        currentConfidence: 0.86,
        remainingUnknowns: ['Vendor relationships'],
        businessUnderstanding: {
          items: [
            {
              entity: 'ABC Property Management',
              assertions: ['Manages 31 properties'],
              confidence: 0.9,
            },
          ],
        },
      },
      investigationState: { confidence: 0.86 },
      universeEstimate: { expected: 48 },
      qualifiedCount: 14,
      confidence: 0.86,
      stages: [{ stage: 'execute' }],
    };

    const extracted = extractMarketMemoryFromDiscovery(pipelineResult, {
      tenantId: '10',
      missionId: 'mission-spec161-2',
      priorMarketMemory: prior,
    });

    assert.ok(extracted.marketMemory);
    assert.equal(extracted.marketMemory.historicalSnapshots.length, 2);
    assert.ok(extracted.changes.hasPriorMemory);

    const persistResult = await persistDiscoveryKnowledge(pipelineResult, {
      tenantId: '10',
      missionId: 'mission-spec161-2',
      priorMarketMemory: prior,
      store,
    });

    assert.equal(persistResult.persisted, true);
    assert.ok(persistResult.marketMemory);

    const reloaded = await store.loadForMarket('10', 'Manchester NH', 'property_management');
    assert.ok(reloaded.market);
    assert.ok((reloaded.market.historicalSnapshots || []).length >= 1);
  });

  it('appendMarketSnapshot records confidence evolution', () => {
    let memory = buildMarketMemoryRecord({ confidence: 0.54 });
    memory = appendMarketSnapshot(memory, { at: '2026-01-01', confidence: 0.54, missionId: 'm1' });
    memory = appendMarketSnapshot(memory, { at: '2026-03-01', confidence: 0.68, missionId: 'm2' });
    memory = appendMarketSnapshot(memory, { at: '2026-06-01', confidence: 0.83, missionId: 'm3' });

    assert.equal(memory.historicalSnapshots.length, 3);
    assert.equal(memory.confidenceHistory.length, 3);
    assert.equal(memory.confidence, 0.83);
    assert.equal(memory.investigationCount, 3);
  });

  it('mergeMarketMemory preserves prior entities and merges relationships', () => {
    const existing = buildMarketMemoryRecord({
      entities: [abcBusiness()],
      relationships: [{ from: 'owner_a', to: 'abc', relation: 'operates' }],
      confidence: 0.7,
    });
    const incoming = buildMarketMemoryRecord({
      entities: [buildBusinessMemory({ entityId: 'new_op', name: 'New Operator LLC' })],
      relationships: [{ from: 'owner_b', to: 'new_op', relation: 'operates' }],
      confidence: 0.8,
    });

    const merged = mergeMarketMemory(existing, incoming, { missionId: 'm2' });
    assert.equal(merged.entities.length, 2);
    assert.equal(merged.relationships.length, 2);
  });

  it('discovery pipeline loads and persists market memory on second run', async () => {
    const mission = sampleMission();
    const market = buildSemanticMarketDefinition({
      mission,
      segments: ['property_management'],
      geography: 'Manchester NH',
    });
    const candidates = [
      {
        id: 'c1',
        name: 'ABC Property Management',
        industry: 'property_management',
        location: 'Manchester, NH',
        icpScore: 85,
        signals: [{ type: 'portfolio_growth', label: '18 properties' }],
      },
    ];
    const discover = createInjectedDiscoverAdapter(candidates);
    const memoryStore = createMemoryIntelligenceStore();
    const sharedOpts = {
      discover,
      companies: candidates,
      enablePlaces: false,
      useInvestigativeReasoningLoop: true,
      memoryStore,
      loadCompanies: async () => candidates,
    };

    const first = await runDiscoveryPipeline({
      mission,
      scoutPayload: { objective: mission.objectiveText },
      opts: sharedOpts,
    });
    assert.ok(first.marketMemoryPersist?.persisted || first.marketMemoryPersist?.marketMemory);

    const second = await runDiscoveryPipeline({
      mission: { ...mission, id: 'mission-spec161-2' },
      scoutPayload: { objective: mission.objectiveText },
      opts: sharedOpts,
    });

    assert.equal(second.memoryLoaded, true);
    assert.ok(second.marketMemoryRecall?.loaded);
    if (second.missionIntelligenceReport?.marketChangesSinceLastInvestigation) {
      assert.equal(second.missionIntelligenceReport.spec, 'SPEC-161');
      assert.ok(second.missionIntelligenceReport.hasPriorMemory === true);
    }
  });
});
