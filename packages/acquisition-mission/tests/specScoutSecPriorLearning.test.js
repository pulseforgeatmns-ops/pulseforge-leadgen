'use strict';

/**
 * SPEC-077 — Canonical Scout SEC execution & prior learning consumption.
 * Repairs AUDIT-077 first divergence.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  createAcquisitionMissionEngine,
  buildExecutionInput,
  executeSpecialist,
  SPECIALISTS,
  LEARNING_OBJECT_KINDS,
} = amo;
const {
  runScoutDiscovery,
  runScoutForAmoMission,
  buildScoutDiscoverOpts,
} = require('../../max/workspace/ScoutDiscoveryExecutor');
const {
  mapSecPriorLearningToOutcomeLearnings,
  evaluatePriorLearningInfluence,
} = require('../../scout/investigation/PriorLearningInfluence');
const { runInvestigativeReasoningLoop } = require('../../scout/investigation/InvestigativeReasoningLoop');
const { INITIAL_HEURISTICS, cloneHeuristicLibrary } = require('../../scout/heuristics/HeuristicLibrary');
const { buildSemanticMarketDefinition } = require('../../scout/intelligence/MarketDefinition');
const { estimateCandidateUniverse } = require('../../scout/universe/CandidateUniverseEstimate');

function fixtureScoutDiscoverResponse(overrides = {}) {
  return {
    status: 'completed',
    confidence: 0.74,
    intelligenceResult: {
      status: 'completed',
      payload: {
        companies: [{ id: 'co-harbor', name: 'Harbor Law Group' }],
        prospects: [{ id: 'p-1', name: 'Alex Morgan', title: 'Office Manager' }],
        opportunities: [{
          companyId: 'co-harbor',
          name: 'Harbor Law Group',
          fit: 0.78,
          confidence: 0.74,
          signals: [{ type: 'hiring', label: 'Hiring operations manager', source: 'job_board' }],
          evidenceRefs: [{
            id: 'ev-1',
            label: 'Operations manager job posting',
            snapshot: { source: 'job_board', companyName: 'Harbor Law Group' },
          }],
        }],
        qualifiedCount: 1,
        evidence: [{
          label: 'Operations manager job posting',
          source: 'job_board',
          confidence: 0.74,
          timestamp: '2026-08-29T12:00:00.000Z',
          provenance: { kind: 'observed', source: 'job_board' },
        }],
        confidence: 0.74,
        buyingSignals: [{ label: 'Hiring operations manager' }],
        missionIntelligenceReport: {
          outcomeReview: { lessons: [] },
          recommendation: { summary: 'Prioritize Harbor Law Group.' },
        },
      },
    },
    pipeline: { learningInfluence: overrides.learningInfluence || [] },
    learningInfluence: overrides.learningInfluence || [],
  };
}

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function missionWithPlan(engine, overrides = {}) {
  return engine.create({
    tenantId: '10',
    objective: OBJECTIVE,
    targetSegment: 'law_firm',
    missionType: 'acquisition',
    planApproved: true,
    ...overrides,
  });
}

function seedOutcomeLearning(store, row) {
  return store.addOutcomeLearning({
    id: row.id || `olearn_${Math.random().toString(36).slice(2, 8)}`,
    spec: 'SPEC-166',
    tenantId: row.tenantId || '10',
    missionId: row.missionId,
    evaluationId: row.evaluationId || 'eval_test_1',
    kind: row.kind || LEARNING_OBJECT_KINDS.HEURISTIC,
    subject: row.subject || 'Operations hiring',
    statement: row.statement || 'Law firms with active office-manager hiring signals previously converted more reliably.',
    direction: row.direction || 'strengthened',
    accuracy: row.accuracy || 'correct',
    autoApplied: false,
    at: row.at || '2026-08-20T12:00:00.000Z',
    ...row,
  });
}

function hiringCandidates() {
  return [
    {
      id: 'c-harbor',
      name: 'Harbor Law Group',
      signals: [{ type: 'hiring', label: 'Hiring operations manager', source: 'job_board' }],
    },
    {
      id: 'c-granite',
      name: 'Granite Legal Partners',
      signals: [{ type: 'hiring', label: 'Hiring office coordinator', source: 'linkedin' }],
    },
    {
      id: 'c-quiet',
      name: 'Quiet Legal LLC',
      signals: [{ type: 'decision_maker', label: 'Named partner only', source: 'website' }],
    },
  ];
}

describe('SPEC-077 Scout SEC execution & prior learning', () => {
  it('SEC execution: discovery stage uses buildExecutionInput → executeSpecialist → Scout.discover', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = missionWithPlan(engine);
    let secInputSeen = null;
    let discoverCalled = false;

    const executionResult = await executeSpecialist({
      mission,
      specialist: SPECIALISTS.SCOUT,
      store: engine.store,
      transactionId: 'sec_tx_scout_1',
      run: async (secInput) => {
        secInputSeen = secInput;
        assert.equal(secInput.spec, 'SPEC-132');
        assert.equal(secInput.specialist, SPECIALISTS.SCOUT);
        assert.ok(secInput.missionPlan || secInput.specialistInput);
        assert.ok(Array.isArray(secInput.memoryContext.priorLearning));

        return runScoutDiscovery(
          { ...secInput, mission },
          {
            persistMemory: false,
            discoverImpl: async (input) => {
              discoverCalled = true;
              assert.ok(Array.isArray(input.opts.priorOutcomeLearnings));
              return fixtureScoutDiscoverResponse();
            },
          }
        );
      },
    });

    assert.ok(secInputSeen);
    assert.equal(discoverCalled, true);
    assert.equal(executionResult.spec, 'SPEC-132');
    assert.equal(executionResult.status, 'SUCCESS');
    assert.ok(executionResult.contributions);
  });

  it('prior learning delivered: Mission A OutcomeLearning reaches investigative loop on Mission B', async () => {
    const engine = createAcquisitionMissionEngine();
    const missionA = missionWithPlan(engine);
    seedOutcomeLearning(engine.store, {
      missionId: missionA.id,
      kind: LEARNING_OBJECT_KINDS.HEURISTIC,
      statement: 'Law firms with active office-manager hiring signals previously converted more reliably.',
    });

    const missionB = missionWithPlan(engine);
    const input = buildExecutionInput({
      mission: missionB,
      specialist: SPECIALISTS.SCOUT,
      store: engine.store,
    });

    const priorOutcomeLearnings = mapSecPriorLearningToOutcomeLearnings(
      input.memoryContext.priorLearning
    );
    assert.ok(priorOutcomeLearnings.length >= 1);
    assert.equal(priorOutcomeLearnings[0].autoApplied, false);

    const market = buildSemanticMarketDefinition({
      mission: missionB,
      segments: ['law_firm'],
      geography: 'Manchester NH',
    });

    const result = await runInvestigativeReasoningLoop({
      mission: missionB,
      marketDefinition: market,
      universeEstimate: estimateCandidateUniverse({ marketDefinition: market }),
      priorOutcomeLearnings,
      coverageResult: {
        candidates: hiringCandidates(),
        searchHypotheses: [],
        coverage: { complete: true },
      },
    });

    assert.ok(Array.isArray(result.priorOutcomeLearnings));
    assert.ok(result.priorOutcomeLearnings.length >= 1);
    assert.ok(result.report.outcomeReview);
  });

  it('relevant business_heuristic learning produces learningInfluence when current evidence matches', async () => {
    const priorOutcomeLearnings = [{
      id: 'learn_ops_hiring',
      kind: LEARNING_OBJECT_KINDS.HEURISTIC,
      sourceMissionId: 'm_a',
      evaluationId: 'eval_a',
      statement: 'Law firms with active office-manager hiring signals previously converted more reliably.',
      autoApplied: false,
    }];

    const influence = evaluatePriorLearningInfluence({
      priorOutcomeLearnings,
      candidates: hiringCandidates(),
    });

    assert.equal(influence.learningInfluence.length, 1);
    assert.equal(influence.learningInfluence[0].learningId, 'learn_ops_hiring');
    assert.equal(influence.learningInfluence[0].autoApplied, false);
    assert.match(influence.learningInfluence[0].reasonUsed, /operations-hiring signals/i);
    assert.ok(influence.learningInfluence[0].currentEvidenceCount >= 2);
  });

  it('irrelevant messaging learning does not affect Scout investigation influence', () => {
    const influence = evaluatePriorLearningInfluence({
      priorOutcomeLearnings: [{
        id: 'learn_msg',
        kind: LEARNING_OBJECT_KINDS.MESSAGING,
        sourceMissionId: 'm_a',
        evaluationId: 'eval_msg',
        statement: 'Subject line with question mark improved open rates.',
        autoApplied: false,
      }],
      candidates: hiringCandidates(),
    });

    assert.deepEqual(influence.learningInfluence, []);
    assert.deepEqual(influence.strategyAdjustments, []);
  });

  it('irrelevant learning without matching current evidence produces empty learningInfluence', async () => {
    const priorOutcomeLearnings = [{
      id: 'learn_vendor',
      kind: LEARNING_OBJECT_KINDS.HEURISTIC,
      sourceMissionId: 'm_a',
      evaluationId: 'eval_vendor',
      statement: 'Vendor instability heuristic strengthened after correct prediction.',
      autoApplied: false,
    }];

    const influence = evaluatePriorLearningInfluence({
      priorOutcomeLearnings,
      candidates: [{
        id: 'c-plain',
        signals: [{ type: 'decision_maker', label: 'Named partner', source: 'website' }],
      }],
    });

    assert.deepEqual(influence.learningInfluence, []);
  });

  it('advisory safety: autoApplied=false and no heuristic library mutation from priorLearning', async () => {
    const libraryBefore = cloneHeuristicLibrary(INITIAL_HEURISTICS);
    const priorOutcomeLearnings = [{
      id: 'learn_ops',
      kind: LEARNING_OBJECT_KINDS.HEURISTIC,
      sourceMissionId: 'm_a',
      evaluationId: 'eval_a',
      statement: 'Law firms with active office-manager hiring signals previously converted more reliably.',
      autoApplied: false,
    }];

    await runInvestigativeReasoningLoop({
      mission: { id: 'm_b', tenantId: '10' },
      marketDefinition: buildSemanticMarketDefinition({
        mission: { id: 'm_b', constraints: { vertical: 'law_firm' } },
        segments: ['law_firm'],
        geography: 'Manchester NH',
      }),
      priorOutcomeLearnings,
      coverageResult: {
        candidates: hiringCandidates(),
        coverage: { complete: true },
      },
    });

    const libraryAfter = cloneHeuristicLibrary(INITIAL_HEURISTICS);
    assert.deepEqual(libraryAfter, libraryBefore);
    for (const row of priorOutcomeLearnings) {
      assert.equal(row.autoApplied, false);
    }
  });

  it('current evidence authority: historical learning alone does not produce influence without matching signals', () => {
    const influence = evaluatePriorLearningInfluence({
      priorOutcomeLearnings: [{
        id: 'learn_ops',
        kind: LEARNING_OBJECT_KINDS.HEURISTIC,
        sourceMissionId: 'm_a',
        evaluationId: 'eval_a',
        statement: 'Law firms with active office-manager hiring signals previously converted more reliably.',
        autoApplied: false,
      }],
      candidates: [],
    });

    assert.deepEqual(influence.learningInfluence, []);
  });

  it('MIR includes prior OutcomeLearning review when relevant learning is supplied', async () => {
    const priorOutcomeLearnings = [{
      id: 'learn_ops',
      kind: LEARNING_OBJECT_KINDS.HEURISTIC,
      sourceMissionId: 'm_a',
      evaluationId: 'eval_a',
      statement: 'Law firms with active office-manager hiring signals previously converted more reliably.',
      autoApplied: false,
    }];

    const result = await runInvestigativeReasoningLoop({
      mission: { id: 'm_b', tenantId: '10', objectiveText: OBJECTIVE },
      marketDefinition: buildSemanticMarketDefinition({
        mission: { id: 'm_b', constraints: { vertical: 'law_firm' } },
        segments: ['law_firm'],
        geography: 'Manchester NH',
      }),
      priorOutcomeLearnings,
      coverageResult: {
        candidates: hiringCandidates(),
        coverage: { complete: true },
      },
    });

    assert.ok(result.report.outcomeReview);
    assert.ok(Array.isArray(result.report.outcomeReview.lessons));
    assert.ok(result.report.outcomeReview.lessons.length >= 1);
    assert.ok(Array.isArray(result.learningInfluence));
  });

  it('runScoutForAmoMission attaches SEC executionResult with learningInfluence field', async () => {
    const engine = createAcquisitionMissionEngine();
    const missionA = missionWithPlan(engine);
    seedOutcomeLearning(engine.store, {
      missionId: missionA.id,
      statement: 'Law firms with active office-manager hiring signals previously converted more reliably.',
    });
    const missionB = missionWithPlan(engine);

    const scoutResult = await runScoutForAmoMission(missionB, {
      engine,
      transactionId: 'sec_tx_scout_2',
      persistMemory: false,
      discoverImpl: async (input) => {
        assert.ok(input.opts.priorOutcomeLearnings.length >= 1);
        return fixtureScoutDiscoverResponse({
          learningInfluence: [{
            learningId: 'learn_ops',
            sourceMissionId: missionA.id,
            evaluationId: 'eval_test_1',
            kind: LEARNING_OBJECT_KINDS.HEURISTIC,
            reasonUsed: 'Used to prioritize verification of operations-hiring signals.',
            autoApplied: false,
          }],
        });
      },
    });

    assert.ok(scoutResult.executionResult);
    assert.equal(scoutResult.executionResult.spec, 'SPEC-132');
    assert.ok(Array.isArray(scoutResult.executionResult.explainability.learningInfluence));
  });

  it('failure behavior: prior-learning retrieval failure continues with priorLearning=[]', async () => {
    const mission = missionWithPlan(createAcquisitionMissionEngine(), { id: 'm_fail_retrieval' });
    const brokenStore = {
      listOutcomeLearnings() {
        throw new Error('store unavailable');
      },
    };

    const input = buildExecutionInput({
      mission,
      specialist: SPECIALISTS.SCOUT,
      store: brokenStore,
    });

    assert.deepEqual(input.memoryContext.priorLearning, []);
    assert.match(input.memoryContext.priorLearningRetrievalWarning, /store unavailable|failed/i);

    const opts = buildScoutDiscoverOpts(mission, input, { persistMemory: false });
    assert.deepEqual(opts.priorOutcomeLearnings, []);
  });

  it('Market Memory remains separate: priorOutcomeLearnings does not replace opts.memory', () => {
    const engine = createAcquisitionMissionEngine();
    const mission = missionWithPlan(engine);
    const input = buildExecutionInput({
      mission,
      specialist: SPECIALISTS.SCOUT,
      store: engine.store,
      memoryContext: {
        observations: [],
        priorLearning: [{
          id: 'learn_1',
          kind: LEARNING_OBJECT_KINDS.HEURISTIC,
          sourceMissionId: 'm_old',
          evaluationId: 'eval_old',
          statement: 'Office manager hiring predicts conversion.',
          autoApplied: false,
          relevance: { score: 0.8, reasons: ['same_segment'] },
          evidence: [],
        }],
      },
    });

    const marketMemory = { market: { entities: [{ id: 'ent_1' }] }, terminology: ['law firm'] };
    const opts = buildScoutDiscoverOpts(mission, input, { memory: marketMemory, persistMemory: false });

    assert.equal(opts.memory, marketMemory);
    assert.ok(Array.isArray(opts.priorOutcomeLearnings));
    assert.equal(opts.priorOutcomeLearnings[0].source, 'outcome_learning');
    assert.notEqual(opts.priorOutcomeLearnings[0].id, 'ent_1');
  });
});
