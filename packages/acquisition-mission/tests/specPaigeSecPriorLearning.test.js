'use strict';

/**
 * AUDIT-079 — Canonical Paige SEC execution & prior learning influence.
 * Repairs first divergence: executionInput.memoryContext.priorLearning → buildPaigeVariantsPayload().
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
  runPaigeVariants,
  buildPaigeVariantsPayload,
  buildBasePaigeVariantsPayload,
} = require('../../max/workspace/PaigeVariantsExecutor');
const {
  evaluatePaigePriorLearningInfluence,
  applyPaigePriorLearningAdjustments,
  paigeAllowsLearningKind,
} = require('../../max/workspace/PaigePriorLearningInfluence');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function fixturePlan() {
  return {
    objective: OBJECTIVE,
    market: { segment: 'law_firm', label: 'Law Firms' },
    successMetric: { metric: 'walkthroughs', target: 3 },
    constraints: [{ label: 'Operator voice' }],
  };
}

function fixtureMax(overrides = {}) {
  return {
    priorities: [{
      rank: 1,
      name: 'Harbor Law Group',
      companyId: 'co-harbor',
      confidence: 0.78,
      rationale: 'Strong law firm fit with verified hiring signal.',
    }],
    objectives: [{ text: OBJECTIVE }],
    objectiveReason: 'Prioritize Harbor Law Group for first outreach wave.',
    recommendations: ['Prioritize Harbor Law Group in the first outreach wave.'],
    constraints: ['Operator voice'],
    ...overrides,
  };
}

function fixtureScout(overrides = {}) {
  return {
    buyingSignals: [{ label: 'Hiring operations manager' }],
    rankedProspects: [{
      id: 'co-harbor',
      name: 'Harbor Law Group',
      signals: [{ type: 'hiring', label: 'Hiring operations manager' }],
    }],
    ...overrides,
  };
}

function genericSubjectLearning(overrides = {}) {
  return {
    id: overrides.id || 'learn_generic_subject',
    kind: LEARNING_OBJECT_KINDS.MESSAGING,
    sourceMissionId: overrides.sourceMissionId || 'm_a',
    evaluationId: overrides.evaluationId || 'eval_a',
    statement: overrides.statement
      || 'Generic subject lines underperformed with law firms.',
    direction: overrides.direction || 'needs_review',
    autoApplied: false,
    ...overrides,
  };
}

function validatedSpecificSubjectLearning(overrides = {}) {
  return genericSubjectLearning({
    id: 'learn_specific_subject',
    statement: 'Company-specific subject lines performed well with law firms.',
    direction: 'validated',
    ...overrides,
  });
}

function paigeExecutionInput(overrides = {}) {
  return {
    transactionId: 'sec_tx_paige_1',
    specialistInput: {
      structuredMission: fixturePlan(),
      maxPrioritization: fixtureMax(overrides.maxOverrides),
      scoutDiscovery: fixtureScout(overrides.scoutOverrides),
    },
    workspaceContext: {
      max: fixtureMax(overrides.maxOverrides),
      scout: fixtureScout(overrides.scoutOverrides),
    },
    missionPlan: fixturePlan(),
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

describe('AUDIT-079 — Canonical Paige SEC prior learning influence', () => {
  it('SEC execution: runPaigeVariants reads memoryContext.priorLearning', async () => {
    const priorLearning = [genericSubjectLearning()];
    const result = await runPaigeVariants(paigeExecutionInput({ priorLearning }));

    assert.equal(result.status, EXECUTION_STATUSES.SUCCESS);
    assert.ok(Array.isArray(result.explainability.learningInfluence));
    assert.equal(result.explainability.learningInfluence.length, 1);
    assert.equal(result.explainability.learningInfluence[0].learningId, 'learn_generic_subject');
    assert.equal(result.explainability.learningInfluence[0].autoApplied, false);
    assert.equal(result.explainability.learningInfluence[0].advisoryOnly, true);
  });

  it('relevant messaging learning + matching context influences strategy and emits learningInfluence', async () => {
    const evaluation = evaluatePaigePriorLearningInfluence({
      priorLearning: [genericSubjectLearning()],
      max: fixtureMax(),
      scout: fixtureScout(),
      plan: fixturePlan(),
    });

    assert.equal(evaluation.learningInfluence.length, 1);
    assert.match(evaluation.learningInfluence[0].reasonUsed, /subject_specificity/i);
    assert.ok(Array.isArray(evaluation.learningInfluence[0].currentEvidenceRefs));
    assert.ok(evaluation.learningInfluence[0].currentEvidenceRefs.length >= 1);

    const base = buildBasePaigeVariantsPayload({
      max: fixtureMax(),
      scout: fixtureScout(),
      plan: fixturePlan(),
    });
    const adjusted = applyPaigePriorLearningAdjustments(base, evaluation, fixturePlan());

    assert.notEqual(adjusted.variants[0].subject, base.variants[0].subject);
    assert.ok(
      adjusted.hypotheses.some((row) => /company-specific subject/i.test(row)),
      'expected hypothesis influenced by prior learning'
    );
    assert.ok(
      adjusted.experiments.some((row) => row.name === 'subject_specificity'),
      'expected subject specificity experiment'
    );
  });

  it('needs_review direction avoids prior weak approach and creates experiment', async () => {
    const evaluation = evaluatePaigePriorLearningInfluence({
      priorLearning: [genericSubjectLearning({ direction: 'needs_review' })],
      max: fixtureMax(),
      scout: fixtureScout(),
      plan: fixturePlan(),
    });

    assert.equal(evaluation.learningInfluence.length, 1);
    assert.equal(evaluation.learningInfluence[0].direction, 'needs_review');

    const base = buildBasePaigeVariantsPayload({
      max: fixtureMax(),
      scout: fixtureScout(),
      plan: fixturePlan(),
    });
    const adjusted = applyPaigePriorLearningAdjustments(base, evaluation, fixturePlan());

    assert.ok(
      adjusted.experiments.some((row) => row.name === 'subject_specificity'),
      'expected experiment against generic subject approach'
    );
    assert.ok(
      adjusted.hypotheses.some((row) => /generic office-cleaning templates/i.test(row)),
      'expected caution hypothesis'
    );
  });

  it('validated learning + supporting context reinforces relevant hypothesis', async () => {
    const evaluation = evaluatePaigePriorLearningInfluence({
      priorLearning: [validatedSpecificSubjectLearning()],
      max: fixtureMax(),
      scout: fixtureScout(),
      plan: fixturePlan(),
    });

    assert.equal(evaluation.learningInfluence.length, 1);
    assert.equal(evaluation.learningInfluence[0].direction, 'validated');

    const base = buildBasePaigeVariantsPayload({
      max: fixtureMax(),
      scout: fixtureScout(),
      plan: fixturePlan(),
    });
    const adjusted = applyPaigePriorLearningAdjustments(base, evaluation, fixturePlan());

    assert.ok(
      adjusted.hypotheses.some((row) => /personalized subject lines/i.test(row)),
      'expected reinforced hypothesis'
    );
  });

  it('prior learning without contextual match produces no material change and learningInfluence = []', async () => {
    const restaurantPlan = { ...fixturePlan(), market: { segment: 'restaurant', label: 'Restaurants' } };
    const evaluation = evaluatePaigePriorLearningInfluence({
      priorLearning: [genericSubjectLearning({
        statement: 'Generic subject lines underperformed with restaurants.',
      })],
      max: fixtureMax(),
      scout: fixtureScout({ buyingSignals: [] }),
      plan: restaurantPlan,
    });

    assert.deepEqual(evaluation.learningInfluence, []);

    const result = await runPaigeVariants(paigeExecutionInput({
      priorLearning: [genericSubjectLearning({
        statement: 'Generic subject lines underperformed with restaurants.',
      })],
      scoutOverrides: { buyingSignals: [] },
      missionPlan: restaurantPlan,
      specialistInput: {
        structuredMission: restaurantPlan,
        maxPrioritization: fixtureMax(),
        scoutDiscovery: fixtureScout({ buyingSignals: [] }),
      },
      workspaceContext: {
        max: fixtureMax(),
        scout: fixtureScout({ buyingSignals: [] }),
      },
    }));

    assert.deepEqual(result.explainability.learningInfluence, []);
  });

  it('Max authority: historical learning cannot override Max objectives/priorities', async () => {
    const evaluation = evaluatePaigePriorLearningInfluence({
      priorLearning: [genericSubjectLearning({
        statement: 'Generic subject lines performed well with law firms.',
        direction: 'validated',
      })],
      max: fixtureMax({
        recommendations: ['Prioritize Harbor Law Group with company-specific personalization.'],
        objectives: [{ text: 'Book walkthroughs using personalized company-specific outreach.' }],
      }),
      scout: fixtureScout(),
      plan: fixturePlan(),
    });

    if (evaluation.learningInfluence.length) {
      assert.match(
        evaluation.learningInfluence[0].reasonUsed,
        /Max strategy remains authoritative|grounded anchor/i
      );
    }

    const result = await runPaigeVariants(paigeExecutionInput({
      priorLearning: [genericSubjectLearning({
        statement: 'Generic subject lines performed well with law firms.',
        direction: 'validated',
      })],
      maxOverrides: {
        recommendations: ['Prioritize Harbor Law Group with company-specific personalization.'],
        objectives: [{ text: 'Book walkthroughs using personalized company-specific outreach.' }],
      },
    }));

    assert.ok(result.contributions.variants[0].subject.includes('Harbor Law Group'));
  });

  it('no copying: prior learning statements are not copied verbatim into outreach', async () => {
    const statement = 'Generic subject lines underperformed with law firms.';
    const result = await runPaigeVariants(paigeExecutionInput({
      priorLearning: [genericSubjectLearning({ statement })],
    }));

    const payload = result.contributions;
    assert.notEqual(payload.variants[0].subject, statement);
    assert.notEqual(payload.variants[0].body, statement);
    assert.notEqual(payload.messaging, statement);
    assert.ok(!payload.hypotheses.includes(statement));
  });

  it('explainability retains required learningInfluence fields', async () => {
    const result = await runPaigeVariants(paigeExecutionInput({
      priorLearning: [genericSubjectLearning()],
    }));

    const row = result.explainability.learningInfluence[0];
    assert.equal(row.learningId, 'learn_generic_subject');
    assert.equal(row.sourceMissionId, 'm_a');
    assert.equal(row.evaluationId, 'eval_a');
    assert.ok(row.direction);
    assert.ok(row.reasonUsed);
    assert.equal(row.advisoryOnly, true);
    assert.equal(row.autoApplied, false);
    assert.ok(Array.isArray(row.currentEvidenceRefs));
  });

  it('contribution integrity: Paige emits canonical VARIANTS fields only', async () => {
    const result = await runPaigeVariants(paigeExecutionInput({
      priorLearning: [genericSubjectLearning()],
    }));

    const payload = result.contributions;
    assert.ok(Array.isArray(payload.variants));
    assert.ok(Array.isArray(payload.subjects));
    assert.ok(payload.cta);
    assert.ok(Array.isArray(payload.hypotheses));
    assert.ok(Array.isArray(payload.experiments));
    assert.ok(payload.messaging);
    assert.doesNotThrow(() => amo.assertContract(SPECIALISTS.PAIGE, payload));
    assert.ok(!Object.prototype.hasOwnProperty.call(result.contributions, 'learningInfluence'));
  });

  it('defensively rejects non-messaging learning kinds for Paige influence', () => {
    assert.equal(paigeAllowsLearningKind(LEARNING_OBJECT_KINDS.HEURISTIC), false);
    assert.equal(paigeAllowsLearningKind(LEARNING_OBJECT_KINDS.MESSAGING), true);
    assert.equal(
      paigeAllowsLearningKind(LEARNING_OBJECT_KINDS.STRATEGY, {
        statement: 'Geography expansion timing was wrong.',
      }),
      false
    );
    assert.equal(
      paigeAllowsLearningKind(LEARNING_OBJECT_KINDS.STRATEGY, {
        statement: 'Adjust messaging tone for law firm owners.',
      }),
      true
    );

    const evaluation = evaluatePaigePriorLearningInfluence({
      priorLearning: [{
        id: 'learn_heuristic',
        kind: LEARNING_OBJECT_KINDS.HEURISTIC,
        sourceMissionId: 'm_a',
        statement: 'Law firms with hiring signals convert more reliably.',
        autoApplied: false,
      }],
      max: fixtureMax(),
      scout: fixtureScout(),
      plan: fixturePlan(),
    });

    assert.deepEqual(evaluation.learningInfluence, []);
  });

  it('deliverability learning is rejected for Paige influence', () => {
    assert.equal(
      paigeAllowsLearningKind(LEARNING_OBJECT_KINDS.MESSAGING, {
        statement: 'High bounce rate requires inbox warmup before outreach.',
      }),
      false
    );
  });

  it('no mutation: prior learning does not auto-apply or mutate learning flags', async () => {
    const priorLearning = [genericSubjectLearning()];
    await runPaigeVariants(paigeExecutionInput({ priorLearning }));
    assert.equal(priorLearning[0].autoApplied, false);
  });

  it('buildExecutionInput delivers priorLearning to Paige without moving it into specialistInput', async () => {
    const engine = createAcquisitionMissionEngine();
    const missionA = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'law_firm',
      planApproved: true,
    });

    engine.store.addOutcomeLearning({
      id: 'olearn_paige_1',
      tenantId: '10',
      missionId: missionA.id,
      evaluationId: 'eval_paige_1',
      kind: LEARNING_OBJECT_KINDS.MESSAGING,
      subject: 'Subject specificity',
      statement: 'Generic subject lines underperformed with law firms.',
      direction: 'needs_review',
      accuracy: 'incorrect',
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
      specialist: SPECIALISTS.PAIGE,
      store: engine.store,
    });

    assert.ok(Array.isArray(input.memoryContext.priorLearning));
    assert.ok(input.memoryContext.priorLearning.length >= 1);
    assert.ok(!input.specialistInput.priorLearning);
  });

  it('executeSpecialist Paige path attaches learningInfluence on SEC executionResult', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'law_firm',
      planApproved: true,
    });

    const result = await executeSpecialist({
      mission,
      specialist: SPECIALISTS.PAIGE,
      store: engine.store,
      transactionId: 'sec_tx_paige_amo',
      run: (secInput) => runPaigeVariants({
        ...secInput,
        specialistInput: {
          structuredMission: fixturePlan(),
          maxPrioritization: fixtureMax(),
          scoutDiscovery: fixtureScout(),
        },
        workspaceContext: {
          max: fixtureMax(),
          scout: fixtureScout(),
        },
        missionPlan: fixturePlan(),
        memoryContext: {
          priorLearning: [genericSubjectLearning()],
        },
        mission,
      }),
    });

    assert.equal(result.status, EXECUTION_STATUSES.SUCCESS);
    assert.ok(Array.isArray(result.explainability.learningInfluence));
    assert.equal(result.explainability.learningInfluence.length, 1);
  });

  it('buildPaigeVariantsPayload returns payload separate from learningInfluence lineage', () => {
    const { payload, learningInfluence } = buildPaigeVariantsPayload(
      paigeExecutionInput({ priorLearning: [genericSubjectLearning()] })
    );

    assert.ok(payload.variants);
    assert.equal(learningInfluence.length, 1);
    assert.ok(!Object.prototype.hasOwnProperty.call(payload, 'learningInfluence'));
  });

  it('content_learnings remains separate — Paige consumes SEC priorLearning only', () => {
    assert.ok(
      !paigeAllowsLearningKind('content_learning'),
      'content_learnings store is not consumed on canonical AMO Paige path'
    );
  });
});
