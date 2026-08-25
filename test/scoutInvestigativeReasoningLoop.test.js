'use strict';

/**
 * SPEC-159 — Investigative Reasoning Loop acceptance tests.
 * ADR-079 — Understanding Before Recommendation.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  createInvestigationState,
  applyMarketDefinitionRevision,
  applyUniverseEstimateRevision,
  seedFromPriorMemory,
} = require('../packages/scout/investigation/InvestigationState');
const {
  HYPOTHESIS_LIFECYCLE,
  applySearchHypothesisEvaluation,
  generateReplacementHypotheses,
  archiveHypothesis,
  summarizeHypothesisHistory,
} = require('../packages/scout/investigation/HypothesisLifecycle');
const {
  runInvestigativeReasoningLoop,
  runReasoningCycle,
  shouldStopInvestigation,
} = require('../packages/scout/investigation/InvestigativeReasoningLoop');
const {
  buildMissionIntelligenceReport,
  buildRecommendationFromUnderstanding,
} = require('../packages/scout/investigation/MissionIntelligenceReport');
const { buildSemanticMarketDefinition, reviseMarketDefinition } = require('../packages/scout/intelligence/MarketDefinition');
const { evaluateHypothesisBranch } = require('../packages/scout/investigation/SearchHypothesisEngine');
const { estimateCandidateUniverse } = require('../packages/scout/universe/CandidateUniverseEstimate');
const { runDiscoveryPipeline } = require('../packages/scout/DiscoveryPipeline');
const { createInjectedDiscoverAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');

function strMission() {
  return {
    id: 'mission-spec159',
    tenantId: '1',
    clientId: 1,
    objectiveText: 'Find short-term rental operators in Greater Manchester.',
    constraints: { vertical: 'short_term_rental', locationHint: 'Greater Manchester' },
  };
}

function strMarket() {
  return buildSemanticMarketDefinition({
    mission: strMission(),
    segments: ['short_term_rental'],
    geography: 'Greater Manchester',
  });
}

describe('SPEC-159 — Investigative Reasoning Loop', () => {
  it('InvestigationState contains understanding fields, not search terms', () => {
    const state = createInvestigationState({
      mission: strMission(),
      marketDefinition: strMarket(),
      universeEstimate: estimateCandidateUniverse({ marketDefinition: strMarket() }),
    });

    assert.ok(state.marketDefinition);
    assert.ok(state.universeEstimate);
    assert.ok(Array.isArray(state.activeHypotheses));
    assert.ok(Array.isArray(state.rejectedHypotheses));
    assert.ok(state.evidenceGraph);
    assert.ok(state.uncertainty);
    assert.ok(state.confidence != null);
    assert.ok(Array.isArray(state.nextQuestions));
    assert.equal(state.searchTerms, undefined);
  });

  it('Scenario 1: evidence contradicting market definition revises understanding', async () => {
    const market = strMarket();
    const hypotheses = [
      evaluateHypothesisBranch(
        {
          id: 'h-vr',
          text: 'Operators use Vacation Rental terminology.',
          searchTerms: ['Vacation Rental Management'],
        },
        { resultCount: 27, uniqueCandidates: 27, dominantConcept: 'Vacation Property Management' },
        { resultThreshold: 8 }
      ),
      evaluateHypothesisBranch(
        {
          id: 'h-str',
          text: 'Operators call themselves STR companies.',
          searchTerms: ['Short-term rental'],
        },
        { resultCount: 2, uniqueCandidates: 2 },
        { resultThreshold: 8 }
      ),
    ];

    const result = await runInvestigativeReasoningLoop({
      mission: strMission(),
      marketDefinition: market,
      universeEstimate: estimateCandidateUniverse({ marketDefinition: market }),
      coverageResult: {
        candidates: [],
        searchHypotheses: hypotheses,
        coverage: { complete: true },
        revisedMarketDefinition: reviseMarketDefinition(market, {
          dominantTerminology: 'Vacation Property Management',
          reason: '80% of operators call themselves Vacation Property Management, not STR.',
        }),
      },
    });

    assert.ok(result.state.marketDefinition.revised || result.state.understandingRevisions.length > 0);
    assert.ok(
      (result.state.marketDefinition.terminology || []).some((t) =>
        /Vacation Property Management|Vacation Rental/i.test(t)
      )
    );
    assert.ok(result.report.finalMarketDefinition.revised || result.state.understandingRevisions.length > 0);
    assert.equal(result.understandingFirst, true);
  });

  it('Scenario 2: coverage increase changes universe estimate with recorded reason', () => {
    const universe = estimateCandidateUniverse({ marketDefinition: strMarket() });
    let state = createInvestigationState({
      mission: strMission(),
      marketDefinition: strMarket(),
      universeEstimate: universe,
    });

    const before = state.universeEstimate.expected;
    state = applyUniverseEstimateRevision(state, {
      investigated: 45,
      discovered: 45,
      coverageComplete: true,
      reason: 'Coverage increased after Places evidence batch',
    });

    assert.ok(state.understandingRevisions.length >= 1);
    const revision = state.understandingRevisions.find((r) => r.kind === 'universe_estimate');
    assert.ok(revision);
    assert.ok(revision.reason.includes('Coverage increased'));
    assert.notEqual(state.universeEstimate.expected, before);
  });

  it('Scenario 3: failed hypothesis is archived and replacement generated', () => {
    const market = strMarket();
    const failed = applySearchHypothesisEvaluation(
      {
        id: 'h-fail',
        text: 'Operators call themselves STR companies.',
        searchTerms: ['STR company'],
      },
      { status: 'rejected', resultCount: 0, reason: 'Zero results — terminology rejected' }
    );

    assert.equal(failed.lifecycle, HYPOTHESIS_LIFECYCLE.REJECTED);
    assert.ok(failed.archiveReason);

    const replacements = generateReplacementHypotheses([failed], market, { generateFollowUp: true });
    assert.ok(replacements.length >= 1);
    assert.equal(replacements[0].lifecycle, HYPOTHESIS_LIFECYCLE.GENERATED);
    assert.ok(replacements[0].spawnedFrom === failed.id || replacements[0].parentId === failed.id);

    const archived = archiveHypothesis(failed, 'Moved to archive after rejection');
    assert.equal(archived.lifecycle, HYPOTHESIS_LIFECYCLE.ARCHIVED);
    assert.ok(archived.lifecycleHistory.length >= 2);
  });

  it('Scenario 4: investigation report includes remaining unknowns', async () => {
    const market = strMarket();
    const result = await runInvestigativeReasoningLoop({
      mission: strMission(),
      marketDefinition: market,
      universeEstimate: estimateCandidateUniverse({ marketDefinition: market }),
      coverageResult: {
        candidates: [
          {
            id: 'c1',
            name: 'Granite Vacation Rentals',
            signals: [{ type: 'expansion', label: 'Added 3 properties', source: 'website' }],
          },
        ],
        searchHypotheses: [],
        coverage: { complete: true, searches: { ratio: 1, executed: 10, planned: 10 } },
      },
      opts: { forceComplete: true },
    });

    assert.ok(Array.isArray(result.report.remainingUnknowns));
    assert.ok(result.report.confidenceEvolution.length >= 1);
    assert.ok(result.report.hypothesisHistory);
    assert.ok(result.report.evidenceGraphSummary);
    assert.ok(result.report.recommendation.basedOnUnderstanding === true);
    assert.ok(result.report.suggestedNextInvestigation);
    assert.ok(result.report.summary);
  });

  it('Scenario 5: prior investigation memory seeds starting understanding', () => {
    const market = strMarket();
    const memory = {
      loaded: true,
      market: {
        knownTerminology: ['Vacation Property Management'],
        geography: 'Greater Manchester',
      },
      investigation: {
        overallConfidence: 0.67,
        remainingGaps: ['contact_path', 'buying_signals'],
      },
      claims: [{ id: 'c1', text: 'Market uses vacation rental terminology', missingEvidence: ['listing_count'] }],
    };

    const state = seedFromPriorMemory(
      createInvestigationState({ mission: strMission(), marketDefinition: market }),
      memory
    );

    assert.equal(state.seededFromMemory, true);
    assert.ok(state.priorUnderstanding);
    assert.ok(state.confidence >= 0.67);
    assert.ok(
      (state.marketDefinition.terminology || []).includes('Vacation Property Management')
    );
    assert.ok(state.nextQuestions.length >= 1);
  });

  it('ADR-079: recommendations derive from understanding, not raw evidence', () => {
    const state = createInvestigationState({
      mission: strMission(),
      marketDefinition: strMarket(),
      hypotheses: [
        {
          id: 'h1',
          text: 'Vacation Rental terminology dominates.',
          searchTerms: ['Vacation Rental'],
          lifecycle: HYPOTHESIS_LIFECYCLE.SUPPORTED,
          confidence: 0.82,
        },
      ],
    });
    state.confidence = 0.74;
    state.activeHypotheses = state.activeHypotheses.length
      ? state.activeHypotheses
      : [
          {
            lifecycle: HYPOTHESIS_LIFECYCLE.SUPPORTED,
            searchTerms: ['Vacation Property Management'],
            confidence: 0.82,
          },
        ];

    const rec = buildRecommendationFromUnderstanding(state);
    assert.equal(rec.basedOnUnderstanding, true);
    assert.equal(rec.notDirectFromEvidence, true);
    assert.ok(/Vacation Property Management|confidence 0.74/i.test(rec.summary));
  });

  it('reasoning cycle updates confidence evolution as evidence arrives', () => {
    let state = createInvestigationState({
      mission: strMission(),
      marketDefinition: strMarket(),
      initialConfidence: 0.35,
    });

    const cycle = runReasoningCycle(
      state,
      [
        {
          id: 'ev1',
          source: 'places',
          label: 'Found via Vacation Rental search',
          relatedTo: 'candidate:c1',
        },
      ],
      {
        coverageMetrics: { complete: false },
        investigatedCount: 5,
        existingIntelligence: { companyCount: 3 },
        confidenceSource: 'places_evidence',
      }
    );

    assert.ok(cycle.state.confidence > 0.35);
    assert.ok(cycle.state.confidenceEvolution.length >= 2);
    assert.ok(cycle.understandingChanged);
  });

  it('DiscoveryPipeline attaches investigation state and mission intelligence report', async () => {
    const mission = {
      id: 'amo-spec159',
      tenantId: '10',
      clientId: 10,
      objective: 'Find law firms in Greater Manchester for commercial cleaning.',
      structuredMission: {
        objective: 'Find law firms in Greater Manchester for commercial cleaning.',
        market: { segment: 'law_firm', industry: 'legal', label: 'Law firms' },
        geography: { region: 'Greater Manchester', cities: ['Manchester', 'Bedford'] },
      },
      constraints: { vertical: 'law_firm', locationHint: 'Greater Manchester' },
    };
    const delegation = {
      tenantId: '10',
      targetContext: { geography: 'Greater Manchester', segments: ['law_firm'], businessType: 'law_firm' },
      businessContext: {
        serviceGeography: 'Greater Manchester',
        preferredSegments: ['law_firm'],
        commercialCapability: 'commercial_cleaning',
      },
    };

    const adapter = createInjectedDiscoverAdapter(async ({ searchDefinition }) => {
      const workload = searchDefinition._coverageWorkload || {};
      const city = workload.city || 'Manchester NH';
      return [{ id: 'lf-1', name: `${city} Legal Group`, location: city, industry: 'law_firm' }];
    });

    const result = await runDiscoveryPipeline({
      mission,
      delegation,
      opts: {
        discoveryAdapters: [adapter],
        enablePlaces: false,
        companies: [],
        people: [],
        useInvestigativeReasoningLoop: true,
      },
    });

    assert.ok(result.investigationState, `expected investigationState, got outcome=${result.outcome} block=${result.blockReason}`);
    assert.ok(result.missionIntelligenceReport);
    assert.ok(result.intelligenceReport.understandingFirst === true);
    assert.ok(Array.isArray(result.missionIntelligenceReport.remainingUnknowns));
    assert.ok(result.investigationState.confidence != null);
  });

  it('hypothesis history preserves lifecycle transitions', () => {
    const state = {
      activeHypotheses: [
        applySearchHypothesisEvaluation(
          { id: 'h1', text: 'Confirmed hypothesis' },
          { status: 'confirmed', confidence: 0.9 }
        ),
      ],
      rejectedHypotheses: [
        applySearchHypothesisEvaluation(
          { id: 'h2', text: 'Rejected hypothesis' },
          { status: 'rejected', resultCount: 0 }
        ),
      ],
      archivedHypotheses: [],
    };

    const history = summarizeHypothesisHistory(state);
    assert.equal(history.length, 2);
    assert.ok(history.some((h) => h.lifecycle === HYPOTHESIS_LIFECYCLE.SUPPORTED));
    assert.ok(history.some((h) => h.lifecycle === HYPOTHESIS_LIFECYCLE.REJECTED));
    assert.ok(history.every((h) => Array.isArray(h.lifecycleHistory)));
  });

  it('stop condition requires coverage, low uncertainty, and no higher-value branch', () => {
    const openState = createInvestigationState({
      mission: strMission(),
      marketDefinition: strMarket(),
      initialUnknowns: ['Do Airbnb hosts advertise separately?'],
    });
    openState.coverage = { complete: true };
    openState.nextQuestions = [
      { question: 'Test hypothesis: adjacent terminology', priority: 'high', source: 'hypothesis' },
    ];

    const openStop = shouldStopInvestigation(openState);
    assert.equal(openStop.stop, false);

    const closedState = { ...openState, nextQuestions: [], uncertainty: { open: [], persistent: ['Facebook groups unknown'] } };
    closedState.confidence = 0.9;
    const closedStop = shouldStopInvestigation(closedState, { forceComplete: true });
    assert.equal(closedStop.stop, true);
  });
});
