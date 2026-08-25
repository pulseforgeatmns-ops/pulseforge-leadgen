'use strict';

/**
 * SPEC-158 — Market Definition & Hypothesis Engine acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildSemanticMarketDefinition,
  reviseMarketDefinition,
  conceptsFromMarketDefinition,
} = require('../packages/scout/intelligence/MarketDefinition');
const { buildMarketDefinition } = require('../packages/scout/intelligence/MarketUnderstanding');
const { expandConcepts } = require('../packages/scout/coverage/ConceptLibrary');
const {
  generateInitialSearchHypotheses,
  evaluateHypothesisBranch,
  generateFollowUpHypotheses,
  inferTerminologyRevision,
} = require('../packages/scout/investigation/SearchHypothesisEngine');
const {
  createInvestigationTree,
  addHypothesisBranch,
  recordBranchEvidence,
  setFinalUnderstanding,
  serializeInvestigationTree,
} = require('../packages/scout/investigation/InvestigationTree');
const { executeHypothesisDrivenCoverage } = require('../packages/scout/coverage/HypothesisDrivenDiscovery');
const {
  recordTerminologyPerformance,
  rankTerminologyForGeography,
  applyTerminologyLearning,
} = require('../packages/scout/memory/TerminologyLearning');
const { buildDiscoveryReport } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
const { createInjectedDiscoverAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');

function strMission(overrides = {}) {
  return {
    id: 'mission-str-1',
    tenantId: '1',
    objectiveText:
      'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.',
    constraints: {
      vertical: 'short_term_rental',
      locationHint: 'Greater Manchester',
    },
    ...overrides,
  };
}

describe('SPEC-158 — Market Definition & Hypothesis Engine', () => {
  it('Scenario 1: short-term rental expands into complete Market Definition', () => {
    const mission = strMission();
    const market = buildMarketDefinition({ mission });

    assert.equal(market.valid, true);
    assert.equal(market.market, 'Short-Term Rental Operations');
    assert.equal(market.geography, 'Greater Manchester');
    assert.ok(market.customerTypes.includes('Airbnb Hosts'));
    assert.ok(market.customerTypes.includes('Vacation Rental Managers'));
    assert.ok(market.decisionMakers.includes('Operations Manager'));
    assert.ok(market.terminology.includes('Vacation Rental'));
    assert.ok(market.terminology.includes('Airbnb'));
    assert.ok(market.adjacentMarkets.includes('Property Management'));
    assert.ok(market.exclusions.includes('Hotels'));
    assert.ok(market.buyingSignals.includes('Hiring cleaners'));
    assert.ok(market.expectedEvidence.includes('listing_count'));
    assert.ok(market.searchConcepts.length >= 6);
  });

  it('Scenario 1b: semantic builder from operator segment alone', () => {
    const semantic = buildSemanticMarketDefinition({
      mission: strMission(),
      segments: ['short_term_rental'],
      geography: 'New Hampshire',
      operatorSegment: 'Short-term rental operator',
    });

    assert.equal(semantic.market, 'Short-Term Rental Operations');
    assert.ok(semantic.terminology.includes('Short-term rental operator'));
    assert.ok(semantic.customerTypes.includes('Executive Housing'));
  });

  it('Scenario 2: no results spawns follow-up hypotheses and investigation continues', async () => {
    const mission = strMission();
    const market = buildMarketDefinition({ mission });
    const searchDefinition = market.searchDefinition;

    const adapter = createInjectedDiscoverAdapter(async ({ searchDefinition: scoped }) => {
      const concept = (scoped.segments || [])[0] || '';
      if (/Vacation Rental/i.test(concept)) {
        return [
          { id: 'vr-1', name: 'Granite Vacation Rentals', location: 'Manchester, NH' },
          { id: 'vr-2', name: 'Lakes Region STR', location: 'Bedford, NH' },
        ];
      }
      return [];
    });

    const result = await executeHypothesisDrivenCoverage({
      marketDefinition: market,
      searchDefinition,
      adapters: [adapter],
      opts: { resultThreshold: 8 },
    });

    assert.ok(result.searchHypotheses.length >= 2);
    assert.ok(result.candidates.length >= 2);
    assert.ok(result.investigationReport);
    assert.ok(result.investigationReport.hypotheses.some((h) => h.status === 'confirmed' || h.confidence > 0));
    assert.equal(result.discoveryPlan.hypothesisDriven, true);
  });

  it('Scenario 2b: follow-up hypotheses generated when branch underperforms', () => {
    const market = buildSemanticMarketDefinition({
      mission: strMission(),
      segments: ['short_term_rental'],
      geography: 'Manchester NH',
    });
    const initial = generateInitialSearchHypotheses(market);
    assert.ok(initial.length >= 3);

    const evaluated = initial.slice(0, 1).map((h) =>
      evaluateHypothesisBranch(h, { resultCount: 0, uniqueCandidates: 0 }, { resultThreshold: 8 })
    );
    assert.equal(evaluated[0].status, 'rejected');

    const followUps = generateFollowUpHypotheses(market, evaluated);
    assert.ok(followUps.length >= 1);
    assert.ok(followUps[0].spawnedFrom);
  });

  it('Scenario 3: evidence contradicting terminology revises Market Definition', () => {
    const market = buildSemanticMarketDefinition({
      mission: strMission(),
      segments: ['short_term_rental'],
      geography: 'New Hampshire',
    });

    const hypotheses = [
      evaluateHypothesisBranch(
        { id: 'h1', text: 'Operators use Vacation Rental terminology.', searchTerms: ['Vacation Rental Management'] },
        { resultCount: 27, uniqueCandidates: 27, dominantConcept: 'Vacation Rental Management' },
        { resultThreshold: 8 }
      ),
      evaluateHypothesisBranch(
        { id: 'h2', text: 'Operators use short-term rental terminology.', searchTerms: ['Short-term rental'] },
        { resultCount: 2, uniqueCandidates: 2, dominantConcept: 'Short-term rental' },
        { resultThreshold: 8 }
      ),
    ];

    const revision = inferTerminologyRevision(hypotheses);
    assert.ok(revision);
    assert.equal(revision.dominantTerminology, 'Vacation Rental Management');

    const revised = reviseMarketDefinition(market, revision);
    assert.equal(revised.revised, true);
    assert.ok(revised.terminology[0].includes('Vacation Rental'));
    assert.ok(revised.revisionHistory.length >= 1);
    assert.ok(revised.customerTypes.includes('Vacation Rental Management'));
  });

  it('Scenario 4: mission report contains market definition, hypotheses, evidence, final understanding', () => {
    const market = buildMarketDefinition({ mission: strMission() });
    const tree = createInvestigationTree(market, generateInitialSearchHypotheses(market));
    const branch = addHypothesisBranch(tree, tree.hypotheses[0]);
    recordBranchEvidence(tree, branch.id, {
      resultCount: 27,
      uniqueCandidates: 27,
      confidence: 0.84,
      hypothesisStatus: 'confirmed',
    });
    setFinalUnderstanding(tree, {
      dominantTerminology: 'Vacation Rental Management',
      totalCandidates: 27,
      summary: 'Market best described through vacation rental terminology.',
    });

    const serialized = serializeInvestigationTree(tree);
    const report = buildDiscoveryReport({
      coverage: { complete: true, cities: { searched: 1, planned: 1 }, concepts: { searched: 4, planned: 4 }, sources: { searched: 1, planned: 1 }, searches: { addressed: 4, planned: 4 }, warnings: [] },
      candidateUniverse: [{ candidate_id: '1' }],
      qualifiedCount: 3,
      marketDefinition: market,
      investigationReport: serialized,
      revisedMarketDefinition: market,
    });

    assert.ok(report.marketDefinition);
    assert.equal(report.marketDefinition.market, 'Short-Term Rental Operations');
    assert.ok(report.investigationHypotheses.length >= 1);
    assert.equal(report.investigationHypotheses[0].confidence, 0.84);
    assert.ok(report.finalUnderstanding);
    assert.match(report.finalUnderstanding.summary, /vacation rental/i);
  });

  it('terminology learning ranks better-performing terms for geography', () => {
    let store = new Map();
    store = recordTerminologyPerformance(store, {
      geography: 'New Hampshire',
      terminology: 'Vacation Rental Management',
      resultCount: 27,
      confidence: 0.84,
    });
    store = recordTerminologyPerformance(store, {
      geography: 'New Hampshire',
      terminology: 'Short-term rental',
      resultCount: 2,
      confidence: 0.2,
    });
    store = recordTerminologyPerformance(store, {
      geography: 'New Hampshire',
      terminology: 'Vacation Rental Management',
      resultCount: 30,
      confidence: 0.88,
    });

    const ranked = rankTerminologyForGeography(store, 'New Hampshire');
    assert.equal(ranked[0].terminology, 'Vacation Rental Management');
    assert.ok(ranked[0].performance > ranked[1].performance);

    const market = buildSemanticMarketDefinition({
      segments: ['short_term_rental'],
      geography: 'New Hampshire',
    });
    const withLearning = applyTerminologyLearning(market, store);
    assert.equal(withLearning.terminology[0], 'Vacation Rental Management');
    assert.equal(withLearning.terminologyLearningApplied, true);
  });

  it('expandConcepts prefers semantic market definition terminology over static map', () => {
    const definition = buildAcquisitionSearchDefinition({
      tenantId: '1',
      targetContext: { geography: 'Manchester NH', segments: ['short_term_rental'] },
      businessContext: { serviceGeography: 'Manchester NH' },
    });
    const market = buildMarketDefinition({
      mission: strMission(),
    });

    const fromSemantic = expandConcepts(definition, market);
    assert.ok(fromSemantic.includes('Guest Accommodation'));
    assert.ok(fromSemantic.includes('Executive Stay'));
    assert.ok(fromSemantic.length >= 8);
  });

  it('investigation tree records branch lineage', () => {
    const market = buildSemanticMarketDefinition({
      segments: ['short_term_rental'],
      geography: 'Manchester NH',
    });
    const tree = createInvestigationTree(market, []);
    const parent = addHypothesisBranch(tree, {
      id: 'h1',
      text: 'Primary hypothesis',
      searchTerms: ['STR'],
    });
    const child = addHypothesisBranch(
      tree,
      { id: 'h2', text: 'Follow-up hypothesis', searchTerms: ['Vacation Rental'] },
      { parentBranchId: parent.id }
    );
    assert.equal(child.parentBranchId, parent.id);
    assert.equal(tree.branches.length, 2);
  });
});
