'use strict';

/**
 * SPEC-145 — Adaptive Investigation Planning tests.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { Scout, investigation } = require('../packages/scout');

const {
  createInvestigationBoard,
  summarizeBoard,
  getTopPriorityUnknown,
  updateBoardAfterStep,
  computeExpectedValue,
  computeCoverage,
  createProviderLearningStore,
  estimateInformationGain,
  selectNextInvestigation,
  explainStepSelection,
  providersForGap,
  planInvestigationChain,
  runInvestigationEngine,
  COMPLETION_REASONS,
  renderJournalTrail,
  createInvestigationJournal,
  recordJournalStep,
  serializeJournal,
} = investigation;

describe('SPEC-145 — Adaptive Investigation Planning', () => {
  it('scores unknowns by value of information (impact × ease)', () => {
    const decisionMaker = computeExpectedValue(0.95, 0.3);
    const officeHours = computeExpectedValue(0.1, 0.05);
    assert.ok(decisionMaker > officeHours);
    assert.ok(decisionMaker > 0.6);
    assert.ok(officeHours < 0.1);
  });

  it('maintains a live investigation board with known and unknown buckets', () => {
    const board = createInvestigationBoard({
      known: [{ gap: 'geographic_fit', label: 'Manchester office', confidence: 0.9 }],
      missing: ['decision_maker', 'portfolio_size', 'office_hours'],
    });
    const summary = summarizeBoard(board);

    assert.equal(summary.knownCount, 1);
    assert.equal(summary.unknownCount, 3);
    assert.equal(summary.topPriorityUnknown.gap, 'decision_maker');
    assert.match(summary.topPriorityUnknown.whyHighestPriority, /Impact/);
  });

  it('chooses provider by question, not fixed pipeline order', () => {
    const dmProviders = providersForGap('decision_maker');
    const portfolioProviders = providersForGap('portfolio_size');

    assert.ok(dmProviders.includes('linkedin'));
    assert.ok(portfolioProviders.includes('county_records') || portfolioProviders.includes('website'));

    const dmStep = selectNextInvestigation({
      missing: ['decision_maker', 'office_hours'],
      attempted: [],
      resolvedGaps: [],
      board: createInvestigationBoard({ missing: ['decision_maker', 'office_hours'] }),
    });

    assert.equal(dmStep.gap, 'decision_maker');
    assert.ok(dmStep.expectedInformationGain > 0);
    assert.ok(dmStep.rationale.includes('expected gain'));
  });

  it('prefers county records over linkedin for portfolio_size when adaptive', () => {
    const chain = planInvestigationChain('portfolio_size');
    const top = chain[0];
    assert.ok(['county_records', 'website', 'existing_pf'].includes(top.providerId));
    assert.ok(top.expectedInformationGain >= (chain[chain.length - 1].expectedInformationGain || 0));
  });

  it('marks persistent unknowns after three provider failures', () => {
    let board = createInvestigationBoard({ missing: ['current_vendor'] });

    board = updateBoardAfterStep(board, { gap: 'current_vendor', providerId: 'website', failed: true, collected: [] });
    board = updateBoardAfterStep(board, { gap: 'current_vendor', providerId: 'linkedin', failed: true, collected: [] });
    board = updateBoardAfterStep(board, { gap: 'current_vendor', providerId: 'news', failed: true, collected: [] });

    const summary = summarizeBoard(board);
    assert.equal(summary.unknownCount, 0);
    assert.equal(summary.persistentCount, 1);
    assert.equal(summary.persistent[0].gap, 'current_vendor');
    assert.match(summary.persistent[0].reason, /3 providers failed/);
  });

  it('records investigation journal with reasoning trail', () => {
    const journal = createInvestigationJournal('mission-1');
    recordJournalStep(journal, {
      question: 'Need property count',
      rationale: 'Website insufficient',
      selectedProvider: 'county_records',
      providerLabel: 'County Records',
      outcome: 'resolved',
      resolvedGaps: ['portfolio_size'],
      nextQuestion: 'Need decision maker',
    });

    const serialized = serializeJournal(journal);
    const trail = renderJournalTrail(serialized);
    assert.ok(trail.some((line) => line.includes('property count') || line.includes('Need property count')));
    assert.ok(trail.some((line) => line.includes('Website insufficient') || line.includes('county_records')));
  });

  it('answers acceptance criteria: most important unknown, why, provider, stop reason', () => {
    const board = createInvestigationBoard({ missing: ['decision_maker', 'office_hours'] });
    const step = selectNextInvestigation({
      missing: ['decision_maker', 'office_hours'],
      attempted: [],
      resolvedGaps: [],
      board,
    });
    const explanation = explainStepSelection(step, board);

    assert.equal(explanation.mostImportantUnknown, 'decision_maker');
    assert.ok(explanation.whyHighestPriority);
    assert.ok(explanation.chosenProvider);
    assert.ok(explanation.whyThisProvider);
    assert.ok(explanation.expectedInformationGain > 0);
  });

  it('stops on diminishing returns when expected gain is below threshold', () => {
    const step = selectNextInvestigation({
      missing: ['office_hours', 'geographic_fit'],
      attempted: [],
      resolvedGaps: [],
      minExpectedGain: 0.5,
      board: createInvestigationBoard({ missing: ['office_hours', 'geographic_fit'] }),
    });

    assert.ok(step);
    assert.ok(step.belowGainThreshold || (step.expectedInformationGain || 0) < 0.5);
    if (step.belowGainThreshold) {
      assert.equal(step.stopRecommendation.reason, 'diminishing_returns');
    }
  });

  it('learns provider effectiveness from outcomes', () => {
    const learning = createProviderLearningStore();
    const before = learning.getEffectiveness('linkedin', 'decision_maker');
    learning.recordOutcome('linkedin', 'decision_maker', { resolved: true });
    const after = learning.getEffectiveness('linkedin', 'decision_maker');
    assert.ok(after >= before);

    const summary = learning.summarize();
    assert.ok(summary.patterns.some((p) => p.provider === 'linkedin' && p.gap === 'decision_maker'));
  });

  it('estimates information gain for provider comparison', () => {
    const linkedinGain = estimateInformationGain({
      gapImpact: 0.9,
      providerEffectiveness: 0.35,
      providerCoverage: 0.5,
      providerReliability: 0.75,
    });
    const countyGain = estimateInformationGain({
      gapImpact: 0.9,
      providerEffectiveness: 0.85,
      providerCoverage: 0.35,
      providerReliability: 0.6,
    });
    assert.ok(countyGain > linkedinGain);
  });

  it('runs adaptive investigation loop with board, journal, and stop explanation', async () => {
    const mission = {
      id: 'mission-spec145-1',
      tenantId: '10',
      objectiveText: 'Acquire commercial cleaning clients in Manchester NH',
      constraints: {
        vertical: 'property_management',
        locationHint: 'Manchester NH',
      },
    };

    const candidates = [
      {
        id: 'c1',
        name: 'Granite State PM',
        industry: 'property_management',
        location: 'Manchester, NH',
        website: 'https://granitepm.example',
        email: 'info@granitepm.example',
        people: [{ name: 'Jane Owner', jobTitle: 'Owner', email: 'jane@granitepm.example' }],
        signals: [{ type: 'portfolio_growth', source: 'website', label: '43 STRs listed' }],
      },
    ];

    const result = await runInvestigationEngine({
      mission,
      opts: {
        discover: async () => candidates,
        companies: candidates,
        estimatedMarket: 4,
        maxIterations: 8,
        confidenceThreshold: 0.5,
        persistMemory: false,
      },
    });

    assert.ok(result.investigationBoard);
    assert.ok(result.investigationJournal);
    assert.ok(result.investigationJournal.entries.length >= 1);
    assert.ok(result.stopExplanation || result.completionReason);
    assert.ok(result.iterations.some((i) => i.stepSelection || i.nextStep));
    assert.ok(result.investigationBoard.topPriorityUnknown || result.investigationBoard.unknownCount === 0);

    const topUnknown = getTopPriorityUnknown(
      createInvestigationBoard({ missing: result.missingEvidence?.missing || [] })
    );
    if (topUnknown) {
      assert.ok(topUnknown.expectedValue > 0);
    }
  });

  it('Scout.investigate includes adaptive planning artifacts', async () => {
    const result = await Scout.investigate({
      mission: {
        id: 'mission-spec145-2',
        tenantId: '1',
        objectiveText: 'Find property managers in Manchester NH',
        constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
      },
      opts: {
        discover: async () => [
          {
            id: 'c1',
            name: 'Test PM',
            website: 'https://test.example',
            people: [{ name: 'Owner', jobTitle: 'Owner' }],
          },
        ],
        maxIterations: 4,
        confidenceThreshold: 0.4,
        persistMemory: false,
      },
    });

    assert.ok(result.investigationBoard || result.investigationReport?.investigationBoard);
    assert.ok(result.investigationJournal || result.investigationReport?.investigationJournal);
  });

  it('computes coverage percentage for key investigation gaps', () => {
    const board = createInvestigationBoard({
      known: [
        { gap: 'decision_maker' },
        { gap: 'buying_signals' },
        { gap: 'portfolio_size' },
      ],
      missing: ['current_vendor'],
    });
    const coverage = computeCoverage(board);
    assert.ok(coverage >= 0.5);
  });
});
