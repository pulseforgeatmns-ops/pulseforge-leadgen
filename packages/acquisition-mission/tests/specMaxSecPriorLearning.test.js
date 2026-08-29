'use strict';

/**
 * AUDIT-078 — Canonical Max SEC execution & prior learning influence.
 * Repairs first divergence: executionInput.memoryContext.priorLearning → runMaxPrioritization().
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
  EXECUTION_STATUSES,
} = amo;
const {
  runMaxPrioritization,
  buildPrioritizationPayload,
} = require('../../max/workspace/MaxPrioritizationExecutor');
const {
  evaluateMaxPriorLearningInfluence,
  applyMaxPriorLearningAdjustments,
  maxAllowsLearningKind,
} = require('../../max/workspace/MaxPriorLearningInfluence');
const { INITIAL_HEURISTICS, cloneHeuristicLibrary } = require('../../scout/heuristics/HeuristicLibrary');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function fixtureDiscovery(overrides = {}) {
  return {
    rankedProspects: overrides.rankedProspects || [{
      id: 'co-harbor',
      companyId: 'co-harbor',
      name: 'Harbor Law Group',
      fit: 0.82,
      timing: 0.74,
      confidence: 0.78,
      rationale: 'Strong law firm fit with verified hiring signal.',
      signals: [{ type: 'hiring', label: 'Hiring operations manager', source: 'job_board' }],
      evidenceRefs: [{
        id: 'ev-harbor-1',
        label: 'Operations manager job posting',
      }],
    }, {
      id: 'co-granite',
      companyId: 'co-granite',
      name: 'Granite Legal Partners',
      fit: 0.71,
      confidence: 0.68,
      signals: [{ type: 'hiring', label: 'Hiring office coordinator', source: 'linkedin' }],
      evidenceRefs: [{ id: 'ev-granite-1', label: 'Office coordinator opening' }],
    }],
    opportunities: overrides.opportunities || [],
    evidence: overrides.evidence || [{
      label: 'Operations manager job posting',
      source: 'job_board',
      confidence: 0.74,
    }],
    buyingSignals: overrides.buyingSignals || [{ label: 'Hiring operations manager' }],
    confidence: 0.76,
    missionIntelligenceReport: {
      recommendation: { summary: 'Prioritize Harbor Law Group for first outreach wave.' },
    },
    ...overrides,
  };
}

function fixturePlan() {
  return {
    objective: OBJECTIVE,
    market: { segment: 'law_firm', label: 'Law Firms' },
    successMetric: { metric: 'walkthroughs', target: 3 },
    constraints: [{ label: 'Operator voice' }],
  };
}

function opsHiringLearning(overrides = {}) {
  return {
    id: overrides.id || 'learn_ops_hiring',
    kind: LEARNING_OBJECT_KINDS.HEURISTIC,
    sourceMissionId: overrides.sourceMissionId || 'm_a',
    evaluationId: overrides.evaluationId || 'eval_a',
    statement: overrides.statement
      || 'Law firms with active office-manager hiring signals previously converted more reliably.',
    direction: overrides.direction || 'strengthened',
    autoApplied: false,
    ...overrides,
  };
}

function maxExecutionInput(overrides = {}) {
  return {
    transactionId: 'sec_tx_max_1',
    specialistInput: {
      structuredMission: fixturePlan(),
      discovery: fixtureDiscovery(overrides.discoveryOverrides || {}),
    },
    memoryContext: {
      priorLearning: overrides.priorLearning || [],
      ...(overrides.priorLearningRetrievalWarning
        ? { priorLearningRetrievalWarning: overrides.priorLearningRetrievalWarning }
        : {}),
    },
    mission: { id: 'm_b', tenantId: '10', objective: OBJECTIVE },
    ...overrides,
  };
}

describe('AUDIT-078 Max SEC prior learning influence', () => {
  it('SEC execution: runMaxPrioritization reads memoryContext.priorLearning', async () => {
    const priorLearning = [opsHiringLearning()];
    const result = await runMaxPrioritization(maxExecutionInput({ priorLearning }));

    assert.equal(result.status, EXECUTION_STATUSES.SUCCESS);
    assert.ok(Array.isArray(result.explainability.learningInfluence));
    assert.equal(result.explainability.learningInfluence.length, 1);
    assert.equal(result.explainability.learningInfluence[0].learningId, 'learn_ops_hiring');
    assert.equal(result.explainability.learningInfluence[0].autoApplied, false);
    assert.equal(result.explainability.learningInfluence[0].advisoryOnly, true);
  });

  it('relevant prior learning + matching Scout evidence influences rationale and emits learningInfluence', async () => {
    const evaluation = evaluateMaxPriorLearningInfluence({
      priorLearning: [opsHiringLearning()],
      discovery: fixtureDiscovery(),
      plan: fixturePlan(),
    });

    assert.equal(evaluation.learningInfluence.length, 1);
    assert.match(evaluation.learningInfluence[0].reasonUsed, /operations-hiring signals/i);
    assert.ok(Array.isArray(evaluation.learningInfluence[0].currentEvidenceRefs));
    assert.ok(evaluation.learningInfluence[0].currentEvidenceRefs.length >= 1);

    const base = buildPrioritizationPayload(
      { objective: OBJECTIVE },
      fixtureDiscovery(),
      fixturePlan()
    );
    const adjusted = applyMaxPriorLearningAdjustments(base, evaluation);

    assert.ok(
      adjusted.recommendations.some((row) => /strategic confidence/i.test(row)),
      'expected recommendation influenced by prior learning'
    );
    const harbor = adjusted.priorities.find((row) => row.name === 'Harbor Law Group');
    assert.ok(harbor);
    assert.ok(harbor.confidence > 0.78, 'expected confidence boost on evidence-matched prospect');
    assert.match(harbor.rationale, /Prior OutcomeLearning supports/i);
  });

  it('relevant historical learning without matching current evidence produces no material change', async () => {
    const evaluation = evaluateMaxPriorLearningInfluence({
      priorLearning: [opsHiringLearning()],
      discovery: fixtureDiscovery({
        rankedProspects: [{
          id: 'co-quiet',
          name: 'Quiet Legal LLC',
          fit: 0.55,
          confidence: 0.5,
          signals: [{ type: 'decision_maker', label: 'Named partner only' }],
        }],
        buyingSignals: [],
        evidence: [],
      }),
    });

    assert.deepEqual(evaluation.learningInfluence, []);

    const result = await runMaxPrioritization(maxExecutionInput({
      priorLearning: [opsHiringLearning()],
      discoveryOverrides: {
        rankedProspects: [{
          id: 'co-quiet',
          name: 'Quiet Legal LLC',
          fit: 0.55,
          confidence: 0.5,
          signals: [{ type: 'decision_maker', label: 'Named partner only' }],
        }],
        buyingSignals: [],
        evidence: [],
      },
    }));

    assert.deepEqual(result.explainability.learningInfluence, []);
  });

  it('evidence conflict: historical negative learning + strong current Scout evidence keeps priority with caveat', async () => {
    const negativeLearning = opsHiringLearning({
      id: 'learn_law_poor',
      statement: 'Law firms performed poorly in prior Manchester outreach missions.',
      direction: 'weakened',
    });

    const evaluation = evaluateMaxPriorLearningInfluence({
      priorLearning: [negativeLearning],
      discovery: fixtureDiscovery(),
    });

    assert.equal(evaluation.learningInfluence.length, 1);
    assert.match(evaluation.learningInfluence[0].reasonUsed, /remains authoritative/i);

    const base = buildPrioritizationPayload(
      { objective: OBJECTIVE },
      fixtureDiscovery(),
      fixturePlan()
    );
    const adjusted = applyMaxPriorLearningAdjustments(base, evaluation);

    const harborRank = adjusted.priorities.findIndex((row) => row.name === 'Harbor Law Group');
    assert.equal(harborRank, 0, 'Harbor Law should remain top priority');
    assert.ok(
      adjusted.constraints.some((row) => /Historical risk/i.test(row)),
      'expected historical risk constraint'
    );
    assert.ok(
      adjusted.recommendations.some((row) => /Keep Harbor Law Group prioritized/i.test(row)),
      'expected conflict caveat recommendation'
    );
  });

  it('explainability retains required lineage fields', async () => {
    const result = await runMaxPrioritization(maxExecutionInput({
      priorLearning: [opsHiringLearning()],
    }));

    const row = result.explainability.learningInfluence[0];
    assert.equal(row.learningId, 'learn_ops_hiring');
    assert.equal(row.sourceMissionId, 'm_a');
    assert.equal(row.evaluationId, 'eval_a');
    assert.ok(row.reasonUsed);
    assert.equal(row.advisoryOnly, true);
    assert.equal(row.autoApplied, false);
    assert.ok(Array.isArray(row.currentEvidenceRefs));
  });

  it('contribution integrity: Max emits canonical PRIORITIZATION fields only', async () => {
    const result = await runMaxPrioritization(maxExecutionInput({
      priorLearning: [opsHiringLearning()],
    }));

    const payload = result.contributions;
    assert.ok(Array.isArray(payload.priorities));
    assert.ok(Array.isArray(payload.objectives));
    assert.ok(payload.objectiveReason);
    assert.ok(payload.timing);
    assert.ok(Array.isArray(payload.recommendations));
    assert.ok(Array.isArray(payload.constraints));
    assert.deepEqual(payload.delegation, { paige: 'variants', emmett: 'capacity' });
    assert.doesNotThrow(() => amo.assertContract(SPECIALISTS.MAX, payload));
    assert.ok(!Object.prototype.hasOwnProperty.call(result.contributions, 'learningInfluence'));
  });

  it('Paige-only messaging learning is defensively rejected for Max influence', () => {
    const evaluation = evaluateMaxPriorLearningInfluence({
      priorLearning: [{
        id: 'learn_msg',
        kind: LEARNING_OBJECT_KINDS.MESSAGING,
        sourceMissionId: 'm_a',
        evaluationId: 'eval_msg',
        statement: 'Subject line with question mark improved open rates.',
        autoApplied: false,
      }],
      discovery: fixtureDiscovery(),
    });

    assert.deepEqual(evaluation.learningInfluence, []);
    assert.equal(maxAllowsLearningKind(LEARNING_OBJECT_KINDS.MESSAGING), false);
  });

  it('no mutation: prior learning does not mutate heuristic libraries or learning autoApplied flags', async () => {
    const libraryBefore = cloneHeuristicLibrary(INITIAL_HEURISTICS);
    const priorLearning = [opsHiringLearning()];

    await runMaxPrioritization(maxExecutionInput({ priorLearning }));

    const libraryAfter = cloneHeuristicLibrary(INITIAL_HEURISTICS);
    assert.deepEqual(libraryAfter, libraryBefore);
    assert.equal(priorLearning[0].autoApplied, false);
  });

  it('available but unused prior learning yields learningInfluence = []', async () => {
    const result = await runMaxPrioritization(maxExecutionInput({
      priorLearning: [opsHiringLearning({
        statement: 'Vendor instability heuristic strengthened after correct prediction.',
      })],
    }));

    assert.deepEqual(result.explainability.learningInfluence, []);
  });

  it('executeSpecialist Max path attaches learningInfluence on SEC executionResult', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'law_firm',
      planApproved: true,
    });

    const maxResult = await executeSpecialist({
      mission,
      specialist: SPECIALISTS.MAX,
      store: engine.store,
      transactionId: 'sec_tx_max_amo',
      run: (secInput) => runMaxPrioritization({
        ...secInput,
        specialistInput: {
          structuredMission: fixturePlan(),
          discovery: fixtureDiscovery(),
        },
        memoryContext: {
          priorLearning: [opsHiringLearning()],
        },
        mission,
      }),
    });

    assert.equal(maxResult.status, EXECUTION_STATUSES.SUCCESS);
    assert.ok(Array.isArray(maxResult.explainability.learningInfluence));
    assert.equal(maxResult.explainability.learningInfluence.length, 1);
  });

  it('buildExecutionInput delivers priorLearning to Max without moving it into specialistInput', async () => {
    const engine = createAcquisitionMissionEngine();
    const missionA = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'law_firm',
      planApproved: true,
    });

    engine.store.addOutcomeLearning({
      id: 'olearn_max_1',
      tenantId: '10',
      missionId: missionA.id,
      evaluationId: 'eval_max_1',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      subject: 'Operations hiring',
      statement: 'Law firms with active office-manager hiring signals previously converted more reliably.',
      direction: 'strengthened',
      accuracy: 'correct',
      autoApplied: false,
      at: '2026-08-20T12:00:00.000Z',
    });

    const missionB = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'law_firm',
      planApproved: true,
    });

    const input = buildExecutionInput({
      mission: missionB,
      specialist: SPECIALISTS.MAX,
      store: engine.store,
      discovery: fixtureDiscovery(),
    });

    assert.ok(Array.isArray(input.memoryContext.priorLearning));
    assert.ok(input.memoryContext.priorLearning.length >= 1);
    assert.ok(!input.specialistInput.priorLearning);
  });
});
