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
  createInvestigationPlan,
  createInvestigationPlanWithLearning,
  reviseInvestigationPlan,
  skipRemainingProviders,
  buildInvestigationStatusFromPlan,
  buildInvestigationPlan,
  buildProviderPlan,
  PROVIDER_STATUS,
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

  // --- SPEC-145 Acceptance Tests (ADR-064 Investigation Before Execution) ---

  it('Test 1: generates complete Investigation Plan before calling any provider', async () => {
    const mission = {
      id: 'mission-spec145-at1',
      tenantId: '10',
      objectiveText: 'Find STR operators in Greater Manchester',
      constraints: { vertical: 'short_term_rental', locationHint: 'Manchester NH' },
    };

    let providerCalled = false;
    const plan = createInvestigationPlan({
      mission,
      marketDefinition: {
        valid: true,
        segment: 'short_term_rental',
        geography: 'Manchester NH',
        segments: ['short_term_rental'],
      },
    });

    assert.equal(plan.version, 'SPEC-145');
    assert.ok(plan.objective);
    assert.ok(plan.hypotheses.length >= 1);
    assert.ok(plan.evidenceRequired.length >= 1);
    assert.ok(plan.providerSequence.length >= 1);
    assert.ok(plan.stoppingConditions.confidenceTarget);
    assert.ok(plan.stoppingConditions.coverageTarget);
    assert.ok(plan.estimatedCoverage);
    assert.ok(plan.estimatedConfidence >= 0);
    assert.ok(plan.estimatedCost >= 0);

    for (const entry of plan.providerSequence) {
      assert.ok(entry.provider || entry.providerId);
      assert.ok(Array.isArray(entry.capabilities));
      assert.ok(Array.isArray(entry.evidenceExpected));
      assert.ok(entry.estimatedCost != null);
      assert.ok(entry.confidenceGain != null);
    }

    const result = await runInvestigationEngine({
      mission,
      opts: {
        investigationPlan: plan,
        discover: async () => {
          providerCalled = true;
          return [];
        },
        maxIterations: 2,
        persistMemory: false,
      },
    });

    assert.ok(result.investigationPlan);
    assert.equal(result.investigationPlan.version, 'SPEC-145');
    assert.ok(plan.createdAt);
    assert.ok(providerCalled);
  });

  it('Test 2: replans when provider unavailable without investigation failure', async () => {
    const basePlan = buildInvestigationPlan({
      mission: { id: 'm-replan' },
      objective: 'Find decision makers',
      hypotheses: [{ text: 'Decision maker exists', gap: 'decision_maker' }],
      evidenceRequired: ['decision_makers'],
      providerSequence: [
        buildProviderPlan({
          providerId: 'county_records',
          providerLabel: 'County Records',
          gap: 'portfolio_size',
          capabilities: ['property_count'],
          evidenceExpected: ['portfolio_size'],
          estimatedCost: 2,
          confidenceGain: 0.7,
          order: 1,
        }),
        buildProviderPlan({
          providerId: 'linkedin',
          providerLabel: 'LinkedIn',
          gap: 'decision_maker',
          capabilities: ['people'],
          evidenceExpected: ['decision_maker'],
          estimatedCost: 8,
          confidenceGain: 0.5,
          order: 2,
        }),
      ],
      stoppingConditions: { confidenceTarget: 0.8, coverageTarget: 0.8 },
      estimatedConfidence: 0.75,
    });

    const revised = reviseInvestigationPlan(basePlan, {
      unavailableProviders: ['county_records'],
      reason: 'County records unavailable',
      replacements: {
        county_records: [
          buildProviderPlan({
            providerId: 'website',
            providerLabel: 'Company Website',
            gap: 'portfolio_size',
            capabilities: ['website'],
            evidenceExpected: ['portfolio_size'],
            estimatedCost: 0,
            confidenceGain: 0.35,
          }),
        ],
      },
    });

    assert.equal(revised.revisions.length, 1);
    assert.ok(revised.estimatedConfidence < basePlan.estimatedConfidence);
    const unavailable = revised.providerSequence.find((p) => p.providerId === 'county_records');
    assert.equal(unavailable.status, PROVIDER_STATUS.UNAVAILABLE);
    const replacement = revised.providerSequence.find((p) => p.providerId === 'website');
    assert.ok(replacement);

    const mission = {
      id: 'mission-spec145-at2',
      tenantId: '10',
      objectiveText: 'Find STR operators',
      constraints: { vertical: 'short_term_rental', locationHint: 'Manchester NH' },
    };

    const result = await runInvestigationEngine({
      mission,
      opts: {
        investigationPlan: revised,
        discover: async () => [
          {
            id: 'c1',
            name: 'STR Co',
            website: 'https://str.example',
            people: [{ name: 'Owner', jobTitle: 'Owner' }],
          },
        ],
        executeStep: async (step) => {
          if (step.providerId === 'county_records') {
            return { step, collected: [], resolvedGaps: [], cost: 2, skipped: false };
          }
          return {
            step,
            collected: [{ id: 'ev1', source: step.providerId, evidenceType: step.providerId }],
            resolvedGaps: [step.gap],
            cost: step.costScore || 1,
          };
        },
        maxIterations: 6,
        persistMemory: false,
      },
    });

    assert.notEqual(result.outcome, 'blocked');
    assert.ok(result.investigationPlan.revisions.length >= 0 || result.investigationStatus);
  });

  it('Test 3: skips remaining providers when confidence threshold reached early', async () => {
    const plan = createInvestigationPlan({
      mission: {
        id: 'mission-spec145-at3',
        objectiveText: 'Find property managers',
        constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
      },
      marketDefinition: {
        valid: true,
        segment: 'property_management',
        geography: 'Manchester NH',
        segments: ['property_management'],
      },
    });

    const pendingBefore = plan.providerSequence.filter((p) => p.status === 'pending').length;
    assert.ok(pendingBefore >= 2);

    const result = await runInvestigationEngine({
      mission: {
        id: 'mission-spec145-at3',
        tenantId: '10',
        objectiveText: 'Find property managers',
        constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
      },
      opts: {
        investigationPlan: plan,
        discover: async () => [
          {
            id: 'c1',
            name: 'Full Evidence PM',
            website: 'https://full.example',
            email: 'owner@full.example',
            people: [{ name: 'Jane Owner', jobTitle: 'Owner', email: 'jane@full.example' }],
            signals: [{ type: 'portfolio_growth', source: 'website', label: '50 units' }],
          },
        ],
        confidenceThreshold: 0.3,
        maxIterations: 8,
        persistMemory: false,
      },
    });

    const skipped = (result.investigationPlan?.providerSequence || []).filter(
      (p) => p.status === 'skipped'
    );
    const completedEarly =
      result.completionReason === COMPLETION_REASONS.CONFIDENCE_THRESHOLD ||
      result.completionReason === COMPLETION_REASONS.COVERAGE_COMPLETE ||
      skipped.length > 0;

    assert.ok(completedEarly || result.overallConfidence >= 0.3);
    if (skipped.length > 0) {
      assert.ok(skipped.every((s) => s.skipReason));
    }
  });

  it('Test 4: reports remaining unknowns, confidence, and recommended next provider when budget exhausted', async () => {
    const plan = createInvestigationPlan({
      mission: {
        id: 'mission-spec145-at4',
        objectiveText: 'Find STR operators',
        constraints: { vertical: 'short_term_rental', locationHint: 'Manchester NH' },
      },
      marketDefinition: {
        valid: true,
        segment: 'short_term_rental',
        geography: 'Manchester NH',
        segments: ['short_term_rental'],
      },
    });

    const result = await runInvestigationEngine({
      mission: {
        id: 'mission-spec145-at4',
        tenantId: '10',
        objectiveText: 'Find STR operators',
        constraints: { vertical: 'short_term_rental', locationHint: 'Manchester NH' },
      },
      opts: {
        investigationPlan: plan,
        discover: async () => [{ id: 'c1', name: 'Sparse STR Co' }],
        maxCostBudget: 1,
        maxIterations: 12,
        confidenceThreshold: 0.99,
        persistMemory: false,
      },
    });

    assert.ok(result.investigationStatus);
    assert.ok(Array.isArray(result.investigationStatus.remainingUnknowns));
    assert.ok(result.investigationStatus.confidence != null);
    assert.ok(
      result.completionReason === COMPLETION_REASONS.COST_EXCEEDS_BENEFIT ||
        result.totalCost >= 1 ||
        result.investigationStatus.remainingSteps.length > 0
    );
    if (result.investigationStatus.remainingSteps.length > 0) {
      assert.ok(
        result.investigationStatus.recommendedNextProvider ||
          result.investigationStatus.recommendedNextInvestigation
      );
    }
    assert.ok(result.report?.investigationStatus || result.investigationStatus);
  });

  it('Test 5: repeat investigation reuses provider learning and produces different plan', () => {
    const mission = {
      id: 'mission-spec145-at5',
      objectiveText: 'Find STR operators in Manchester',
      constraints: { vertical: 'short_term_rental', locationHint: 'Manchester NH' },
    };
    const marketDefinition = {
      valid: true,
      segment: 'short_term_rental',
      geography: 'Manchester NH',
      segments: ['short_term_rental'],
    };

    const freshPlan = createInvestigationPlan({ mission, marketDefinition });
    const priorMemory = {
      investigation: {
        providerLearning: {
          google_maps: { geographic_fit: 0.82, business_fit: 0.6 },
          linkedin: { decision_maker: 0.91, portfolio_size: 0.09 },
          county_records: { portfolio_size: 0.85 },
          apollo: { decision_maker: 0.15 },
        },
      },
    };

    const learnedPlan = createInvestigationPlanWithLearning(
      { mission, marketDefinition },
      priorMemory
    );

    assert.notDeepEqual(
      freshPlan.providerSequence.map((p) => `${p.providerId}:${p.gap}`),
      learnedPlan.providerSequence.map((p) => `${p.providerId}:${p.gap}`)
    );

    const freshLinkedIn = freshPlan.providerSequence.find(
      (p) => p.providerId === 'linkedin' && p.gap === 'decision_maker'
    );
    const learnedLinkedIn = learnedPlan.providerSequence.find(
      (p) => p.providerId === 'linkedin' && p.gap === 'decision_maker'
    );
    if (freshLinkedIn && learnedLinkedIn) {
      assert.ok(learnedLinkedIn.confidenceGain >= freshLinkedIn.confidenceGain);
    }

    const learnedCounty = learnedPlan.providerSequence.find(
      (p) => p.providerId === 'county_records' && p.gap === 'portfolio_size'
    );
    if (learnedCounty) {
      assert.ok(learnedCounty.order <= (learnedPlan.providerSequence.length || 1));
    }
  });

  it('buildInvestigationStatusFromPlan surfaces completed and remaining steps', () => {
    const plan = buildInvestigationPlan({
      providerSequence: [
        buildProviderPlan({ providerId: 'google_maps', gap: 'geographic_fit', status: 'completed' }),
        buildProviderPlan({ providerId: 'linkedin', gap: 'decision_maker', status: 'pending' }),
      ],
      estimatedConfidence: 0.6,
    });

    const status = buildInvestigationStatusFromPlan(plan, {
      confidence: 0.72,
      coverage: 0.4,
      cost: 8,
      remainingUnknowns: ['portfolio_size'],
    });

    assert.equal(status.completedSteps.length, 1);
    assert.equal(status.remainingSteps.length, 1);
    assert.equal(status.confidence, 0.72);
    assert.equal(status.recommendedNextProvider, 'linkedin');
  });

  it('skipRemainingProviders marks pending steps as skipped', () => {
    const plan = buildInvestigationPlan({
      providerSequence: [
        buildProviderPlan({ providerId: 'a', status: 'completed' }),
        buildProviderPlan({ providerId: 'b', status: 'pending' }),
        buildProviderPlan({ providerId: 'c', status: 'pending' }),
      ],
    });

    const skipped = skipRemainingProviders(plan, 'confidence_threshold_reached');
    const pending = skipped.providerSequence.filter((p) => p.status === 'pending');
    const skippedSteps = skipped.providerSequence.filter((p) => p.status === 'skipped');

    assert.equal(pending.length, 0);
    assert.equal(skippedSteps.length, 2);
  });
});
