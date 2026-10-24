'use strict';

/**
 * SPEC-163 — Investigative Strategy Engine acceptance tests.
 * ADR-083 — Investigate what reduces uncertainty most.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { buildEvidence, buildUnderstanding } = require('../packages/scout/synthesis/types');
const { buildSemanticMarketDefinition } = require('../packages/scout/intelligence/MarketDefinition');
const {
  createInvestigationState,
  applyBusinessJudgment,
  applyBusinessUnderstandingSynthesis,
} = require('../packages/scout/investigation/InvestigationState');
const { activateHeuristics } = require('../packages/scout/heuristics/BusinessHeuristicsEngine');
const { synthesizeFromCandidates } = require('../packages/scout/synthesis/EvidenceSynthesisEngine');
const { runInvestigativeReasoningLoop } = require('../packages/scout/investigation/InvestigativeReasoningLoop');
const { buildMissionIntelligenceReport } = require('../packages/scout/investigation/MissionIntelligenceReport');
const {
  buildInvestigationOption,
  buildInvestigationStrategy,
  buildInvestigativeStrategy,
  recalculateStrategyAfterResolution,
  applyInvestigativeStrategy,
  explainInvestigationChoice,
  buildInvestigativeStrategyReport,
  evaluateStrategyStoppingConditions,
  extractUnknownsFromState,
} = require('../packages/scout/investigation/InvestigativeStrategyEngine');
const { createInvestigationBoard } = require('../packages/scout/investigation/InvestigationBoard');

function vendorInstabilityUnderstanding() {
  return buildUnderstanding({
    entity: 'ABC Property Management',
    entityId: 'abc-pm',
    kind: 'service_need',
    assertions: ['Vendor relationships may be weakening', 'Operations leadership changed recently'],
    supportingEvidence: [
      buildEvidence({ source: 'google_reviews', observation: 'Recent negative cleanliness reviews on Google' }),
      buildEvidence({ source: 'linkedin', observation: 'New Operations Manager hired last month' }),
      buildEvidence({ source: 'indeed', observation: 'Hiring facilities staff for property maintenance' }),
    ],
    confidence: 0.74,
  });
}

function baseState() {
  return createInvestigationState({
    missionId: 'spec163',
    marketDefinition: buildSemanticMarketDefinition({
      market: 'Property Management',
      geography: 'Manchester NH',
    }),
    uncertainty: {
      open: [
        'Decision maker unknown',
        'Current cleaning vendor unknown',
        'Portfolio size unknown',
        'Portfolio growth trajectory unknown',
        'Buying window signals unknown',
      ],
      persistent: [],
      resolved: [],
    },
  });
}

describe('SPEC-163 — Investigative Strategy Engine', () => {
  it('InvestigationStrategy model includes required fields', () => {
    const strategy = buildInvestigationStrategy({
      knowns: [{ label: 'Company name known' }],
      unknowns: [{ gap: 'current_vendor', label: 'Current cleaning vendor' }],
      candidateInvestigations: [
        buildInvestigationOption({
          source: 'google_reviews',
          sourceLabel: 'Google Reviews',
          gap: 'current_vendor',
          expectedInformationGain: 0.23,
        }),
      ],
      selectedInvestigation: buildInvestigationOption({
        source: 'google_reviews',
        expectedInformationGain: 0.23,
      }),
      expectedInformationGain: 0.23,
      reasoning: 'Highest expected information gain',
    });

    assert.equal(strategy.kind, 'investigation_strategy');
    assert.equal(strategy.spec, 'SPEC-163');
    assert.ok(Array.isArray(strategy.knowns));
    assert.ok(Array.isArray(strategy.unknowns));
    assert.ok(Array.isArray(strategy.candidateInvestigations));
    assert.equal(strategy.expectedInformationGain, 0.23);
    assert.ok(strategy.reasoning);
  });

  it('Scenario 1: five unknowns rank investigations by expected information gain', () => {
    const state = baseState();
    const unknowns = extractUnknownsFromState(state);
    assert.ok(unknowns.length >= 5);

    const strategy = buildInvestigativeStrategy({ state });
    assert.ok(strategy.unknowns.length >= 5);
    assert.ok(strategy.candidateInvestigations.length >= 5);
    assert.ok(strategy.investigationQueue.length >= 5);

    const gains = strategy.investigationQueue.map((q) => q.expectedGain);
    const sorted = [...gains].sort((a, b) => b - a);
    assert.deepEqual(gains, sorted);

    assert.ok(strategy.selectedInvestigation);
    assert.ok(strategy.selectedInvestigation.expectedInformationGain > 0);
    assert.equal(strategy.selectedInvestigation.documentedGain, true);

    for (const candidate of strategy.candidateInvestigations) {
      assert.equal(candidate.documentedGain, true);
      assert.ok(candidate.expectedInformationGain >= 0);
      assert.ok(candidate.reasoning);
    }
  });

  it('Scenario 2: resolving highest-priority unknown recalculates strategy', () => {
    const state = baseState();
    const before = buildInvestigativeStrategy({ state });
    const topGap = before.investigationQueue[0].gap;

    const { state: nextState, strategy: after } = recalculateStrategyAfterResolution({
      state,
      resolvedGap: topGap,
      resolvedLabel: before.investigationQueue[0].unknown,
    });

    assert.ok(after.recalculatedAt);
    assert.ok(after.reasoning.includes('Resolved'));
    assert.ok(
      !after.investigationQueue.some((q) => q.gap === topGap),
      `expected ${topGap} to be removed from queue after resolution`
    );
    assert.ok(after.investigationQueue.length < before.investigationQueue.length);
  });

  it('Scenario 3: activated heuristic shifts investigation priorities', () => {
    const synthesisResult = synthesizeFromCandidates({
      candidates: [
        {
          id: 'abc-pm',
          name: 'ABC Property Management',
          signals: [
            { source: 'google_reviews', label: 'Recent negative cleanliness reviews' },
            { source: 'linkedin', label: 'New Operations Manager hired' },
          ],
        },
      ],
    });

    let state = baseState();
    state = applyBusinessUnderstandingSynthesis(state, synthesisResult);
    const judgment = activateHeuristics({
      businessUnderstandings: [vendorInstabilityUnderstanding(), ...(state.businessUnderstandings || [])],
    });
    state = applyBusinessJudgment(state, judgment);

    const withoutHeuristics = buildInvestigativeStrategy({ state: baseState() });
    const withHeuristics = buildInvestigativeStrategy({ state, judgmentResult: judgment });

    assert.ok(withHeuristics.activatedHeuristics.some((h) => h.id === 'vendor_instability'));

    const vendorQueue = withHeuristics.investigationQueue.find(
      (q) => q.gap === 'current_vendor' || q.gap === 'cleaning_responsibility'
    );
    assert.ok(vendorQueue);
    assert.ok(/vendor|clean/i.test(vendorQueue.reason));

    const vendorTopGain = withHeuristics.candidateInvestigations
      .filter((c) => c.gap === 'current_vendor')
      .reduce((max, c) => Math.max(max, c.expectedInformationGain), 0);
    const baselineVendorGain = withoutHeuristics.candidateInvestigations
      .filter((c) => c.gap === 'current_vendor')
      .reduce((max, c) => Math.max(max, c.expectedInformationGain), 0);
    assert.ok(vendorTopGain >= baselineVendorGain);
  });

  it('Scenario 4: explainInvestigationChoice answers why Google Reviews first', () => {
    const state = baseState();
    const strategy = buildInvestigativeStrategy({ state });

    const googleReviews = strategy.candidateInvestigations.find(
      (c) => c.source === 'google_reviews' && c.gap === 'current_vendor'
    );
    assert.ok(googleReviews, 'expected google_reviews candidate for current_vendor');

    const explanation = explainInvestigationChoice(strategy, 'google_reviews');
    assert.ok(explanation.currentUncertainty);
    assert.ok(explanation.expectedGain > 0);
    assert.ok(explanation.explanation.includes('Expected gain'));
    assert.equal(explanation.spec, 'SPEC-163');
    assert.equal(explanation.adr, 'ADR-083');

    const topForVendor = strategy.candidateInvestigations
      .filter((c) => c.gap === 'current_vendor')
      .sort((a, b) => b.expectedInformationGain - a.expectedInformationGain)[0];
    assert.equal(topForVendor.source, 'google_reviews');
    assert.ok(topForVendor.expectedInformationGain > 0.15);
  });

  it('Scenario 5: coverage threshold explains why investigation stopped', () => {
    const state = {
      ...baseState(),
      coverage: { complete: true, searches: { ratio: 0.95 } },
      uncertainty: { open: [], persistent: [], resolved: ['All key gaps resolved'] },
      confidence: 0.9,
    };

    const strategy = buildInvestigativeStrategy({
      state,
      opts: { minExpectedGain: 0.05, coverageThreshold: 0.8 },
    });

    const stop = evaluateStrategyStoppingConditions(strategy, state, {
      minExpectedGain: 0.05,
      coverageThreshold: 0.8,
    });

    assert.equal(stop.stop, true);
    assert.ok(stop.explanation);
    assert.match(stop.explanation, /minimal value|Coverage threshold|negligible/i);
  });

  it('Scenario 6: Mission Intelligence Report includes Investigation Strategy section', () => {
    const synthesisResult = synthesizeFromCandidates({
      candidates: [
        {
          id: 'abc-pm',
          name: 'ABC Property Management',
          signals: [
            { source: 'linkedin', label: 'Operations Manager hired' },
            { source: 'google_reviews', label: 'Recent negative cleanliness reviews' },
          ],
        },
      ],
    });

    let state = baseState();
    state = applyBusinessUnderstandingSynthesis(state, synthesisResult);
    const judgment = activateHeuristics({
      businessUnderstandings: [vendorInstabilityUnderstanding()],
    });
    state = applyBusinessJudgment(state, judgment);

    const strategy = buildInvestigativeStrategy({ state, judgmentResult: judgment });
    state = applyInvestigativeStrategy(state, strategy);

    const report = buildMissionIntelligenceReport({
      state,
      synthesisResult,
      judgmentResult: judgment,
      investigativeStrategy: strategy,
      stop: strategy.stoppingCondition,
    });

    assert.equal(report.strategySpec, 'SPEC-163');
    assert.equal(report.strategyAdr, 'ADR-083');
    assert.ok(report.investigativeStrategy);
    assert.ok(Array.isArray(report.investigativeStrategy.knowns));
    assert.ok(Array.isArray(report.investigativeStrategy.remainingUnknowns));
    assert.ok(Array.isArray(report.investigativeStrategy.investigationQueue));
    assert.ok(Array.isArray(report.investigativeStrategy.informationGainHistory));
    assert.ok(report.investigativeStrategy.recommendedNextInvestigation);
    assert.equal(report.investigativeStrategy.everyInvestigationDocumented, true);
    assert.equal(report.strategyDrivenInvestigation, true);
    assert.equal(report.suggestedNextInvestigation.documentedGain, true);
    assert.ok(report.suggestedNextInvestigation.expectedInformationGain != null);
  });

  it('integrates with investigative reasoning loop end-to-end', async () => {
    const marketDefinition = buildSemanticMarketDefinition({
      market: 'Property Management',
      geography: 'Manchester NH',
    });

    const result = await runInvestigativeReasoningLoop({
      mission: { id: 'spec163-loop' },
      marketDefinition,
      candidates: [
        {
          id: 'abc-pm',
          name: 'ABC Property Management',
          signals: [
            { source: 'linkedin', label: 'Operations Manager hired' },
            { source: 'google_reviews', label: 'Recent negative cleanliness reviews on Google' },
            { source: 'indeed', label: 'Hiring facilities staff' },
          ],
        },
      ],
      coverageMetrics: { complete: true, investigated: 1, qualified: 1 },
    });

    assert.ok(result.investigativeStrategy);
    assert.ok(result.report.investigativeStrategy);
    assert.equal(result.report.strategySpec, 'SPEC-163');
    if (result.investigativeStrategy.stoppingCondition?.stop) {
      assert.ok(result.investigativeStrategy.stoppingCondition.explanation);
    } else {
      assert.ok(result.state.nextQuestions.length >= 1);
      assert.equal(result.state.nextQuestions[0].documentedGain, true);
      assert.ok(result.state.nextQuestions[0].expectedInformationGain != null);
    }
  });

  it('buildInvestigativeStrategyReport formats operator-facing strategy section', () => {
    const strategy = buildInvestigativeStrategy({ state: baseState() });
    const section = buildInvestigativeStrategyReport(strategy, {
      stop: true,
      explanation: 'Coverage threshold reached.',
    });

    assert.equal(section.spec, 'SPEC-163');
    assert.ok(section.investigationQueue.length >= 1);
    assert.ok(section.recommendedNextInvestigation.action === 'investigate');
    assert.equal(section.whyInvestigationStopped, 'Coverage threshold reached.');
    assert.equal(section.everyInvestigationDocumented, true);
  });

  it('uses investigation board unknowns when provided', () => {
    const board = createInvestigationBoard({
      unknown: ['decision_maker', 'current_vendor', 'portfolio_size', 'expansion_plans', 'buying_signals'],
    });
    const strategy = buildInvestigativeStrategy({
      state: baseState(),
      board,
    });

    assert.ok(strategy.investigationQueue.length >= 5);
    const gaps = new Set(strategy.investigationQueue.map((q) => q.gap));
    assert.ok(gaps.has('decision_maker'));
    assert.ok(gaps.has('current_vendor'));
  });
});
