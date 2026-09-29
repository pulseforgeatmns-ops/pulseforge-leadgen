'use strict';

/**
 * Canonical cross-mission OutcomeLearning retrieval — SEC memoryContext injection.
 * Repairs AUDIT-076 first divergence: OutcomeLearning → SEC priorLearning.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  createAcquisitionMissionEngine,
  createMemoryAmoStore,
  buildExecutionInput,
  executeSpecialist,
  SPECIALISTS,
  LEARNING_OBJECT_KINDS,
  retrieveRelevantOutcomeLearning,
  buildMemoryContextWithPriorLearning,
  specialistAllowsKind,
} = require('../index');

const OBJECTIVE_A =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function missionWithPlan(engine, overrides = {}) {
  return engine.create({
    tenantId: '10',
    objective: OBJECTIVE_A,
    targetSegment: 'law_firm',
    missionType: 'acquisition',
    planApproved: true,
    ...overrides,
  });
}

function secondMission(engine, overrides = {}) {
  return missionWithPlan(engine, overrides);
}

function seedOutcomeLearning(store, row) {
  return store.addOutcomeLearning({
    id: row.id || `olearn_${Math.random().toString(36).slice(2, 8)}`,
    spec: 'SPEC-166',
    tenantId: row.tenantId || '10',
    missionId: row.missionId,
    evaluationId: row.evaluationId || 'eval_test_1',
    kind: row.kind || LEARNING_OBJECT_KINDS.HEURISTIC,
    subject: row.subject || 'Vendor instability',
    statement: row.statement || 'Heuristic strengthened after correct prediction.',
    direction: row.direction || 'strengthened',
    accuracy: row.accuracy || 'correct',
    autoApplied: false,
    at: row.at || '2026-08-20T12:00:00.000Z',
    ...row,
  });
}

function completeMissionWithLearning(engine, mission, learningOverrides = {}) {
  engine.capturePrediction(mission.id, {
    recommendation: {
      summary: 'Call Harbor Law today.',
      confidence: 0.72,
      kind: 'outreach',
    },
    expectedOutcome: {
      kind: 'walkthrough',
      label: 'Walkthrough booked',
      probability: 0.72,
    },
    opportunityId: 'opp-harbor',
  });

  engine.recordOutcome(mission.id, {
    type: 'walkthrough_booked',
    at: '2026-08-25T14:00:00.000Z',
  });

  const learnings = engine.store.listOutcomeLearnings('10', mission.id);
  if (!learnings.length) {
    seedOutcomeLearning(engine.store, {
      missionId: mission.id,
      tenantId: mission.tenantId,
      ...learningOverrides,
    });
  } else if (Object.keys(learningOverrides).length) {
    seedOutcomeLearning(engine.store, {
      missionId: mission.id,
      tenantId: mission.tenantId,
      ...learningOverrides,
    });
  }

  return engine.store.listOutcomeLearnings('10').filter((row) => row.missionId === mission.id);
}

describe('Canonical OutcomeLearning retrieval — SEC memoryContext', () => {
  it('cross-mission: Mission A learning appears in Mission B SEC memoryContext', () => {
    const engine = createAcquisitionMissionEngine();
    const missionA = missionWithPlan(engine, { objective: OBJECTIVE_A });
    completeMissionWithLearning(engine, missionA, {
      kind: LEARNING_OBJECT_KINDS.HEURISTIC,
      statement: 'Law firm vendor instability heuristic strengthened.',
    });

    const missionB = secondMission(engine);

    const input = buildExecutionInput({
      mission: missionB,
      specialist: SPECIALISTS.SCOUT,
      store: engine.store,
    });

    assert.ok(Array.isArray(input.memoryContext.priorLearning));
    assert.ok(input.memoryContext.priorLearning.length >= 1);
    const item = input.memoryContext.priorLearning.find(
      (row) => row.sourceMissionId === missionA.id
    );
    assert.ok(item, 'Mission A learning should be present for Mission B');
    assert.equal(item.autoApplied, false);
    assert.ok(item.relevance.score >= 0.4);
    assert.ok(item.relevance.reasons.includes('specialist_relevant_kind'));
  });

  it('current-mission exclusion: Mission B own learning is not in priorLearning', () => {
    const engine = createAcquisitionMissionEngine();
    const missionB = secondMission(engine);
    completeMissionWithLearning(engine, missionB, {
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Strategy lesson from current mission only.',
    });

    const input = buildExecutionInput({
      mission: missionB,
      specialist: SPECIALISTS.MAX,
      store: engine.store,
    });

    const fromCurrent = input.memoryContext.priorLearning.filter(
      (row) => row.sourceMissionId === missionB.id
    );
    assert.equal(fromCurrent.length, 0);
  });

  it('tenant isolation: Tenant A learning never appears in Tenant B SEC context', () => {
    const engine = createAcquisitionMissionEngine();
    const missionA = missionWithPlan(engine, { tenantId: '10', objective: OBJECTIVE_A });
    completeMissionWithLearning(engine, missionA);

    const missionB = secondMission(engine, { tenantId: '20', targetSegment: 'law_firm' });

    seedOutcomeLearning(engine.store, {
      tenantId: '20',
      missionId: 'mission_other_tenant',
      kind: LEARNING_OBJECT_KINDS.HEURISTIC,
      statement: 'Tenant 20 only heuristic.',
    });

    const input = buildExecutionInput({
      mission: missionB,
      specialist: SPECIALISTS.MAX,
      store: engine.store,
    });

    for (const row of input.memoryContext.priorLearning) {
      assert.notEqual(String(row.sourceMissionId), missionA.id);
    }
    assert.ok(
      input.memoryContext.priorLearning.every((row) => {
        const source = engine.store.getMission(row.sourceMissionId);
        return !source || String(source.tenantId) === '20';
      })
    );
  });

  it('specialist filtering: Scout receives only allowed kinds', () => {
    const store = createMemoryAmoStore();
    const mission = {
      id: 'm_scout_filter',
      tenantId: '10',
      targetSegment: 'law_firm',
      structuredMission: { market: { segment: 'law_firm' }, geography: { primary: 'Manchester NH' } },
    };
    store.putMission(mission);

    seedOutcomeLearning(store, {
      missionId: 'm_prior',
      kind: LEARNING_OBJECT_KINDS.HEURISTIC,
      statement: 'Scout-relevant heuristic.',
    });
    seedOutcomeLearning(store, {
      missionId: 'm_prior',
      kind: LEARNING_OBJECT_KINDS.MESSAGING,
      statement: 'Messaging-only lesson.',
    });
    seedOutcomeLearning(store, {
      missionId: 'm_prior',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Generic strategy lesson.',
    });

    const result = retrieveRelevantOutcomeLearning({
      tenantId: '10',
      mission,
      specialist: SPECIALISTS.SCOUT,
      store,
    });

    assert.ok(result.items.length >= 1);
    for (const item of result.items) {
      assert.ok(
        [
          LEARNING_OBJECT_KINDS.HEURISTIC,
          LEARNING_OBJECT_KINDS.MARKET_UNDERSTANDING,
          LEARNING_OBJECT_KINDS.OPPORTUNITY_RULE,
        ].includes(item.kind),
        `Scout received disallowed kind: ${item.kind}`
      );
    }
    assert.ok(!result.items.some((row) => row.kind === LEARNING_OBJECT_KINDS.MESSAGING));
  });

  it('specialist filtering: Max receives strategy and organizational kinds', () => {
    const store = createMemoryAmoStore();
    const mission = {
      id: 'm_max_filter',
      tenantId: '10',
      targetSegment: 'law_firm',
      structuredMission: { market: { segment: 'law_firm' } },
    };
    store.putMission(mission);

    seedOutcomeLearning(store, {
      missionId: 'm_prior_max',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Max strategy lesson.',
    });
    seedOutcomeLearning(store, {
      missionId: 'm_prior_max',
      kind: LEARNING_OBJECT_KINDS.ORGANIZATIONAL,
      statement: 'Org-wide lesson.',
    });
    seedOutcomeLearning(store, {
      missionId: 'm_prior_max',
      kind: LEARNING_OBJECT_KINDS.MESSAGING,
      statement: 'Paige messaging lesson.',
    });

    const result = retrieveRelevantOutcomeLearning({
      tenantId: '10',
      mission,
      specialist: SPECIALISTS.MAX,
      store,
    });

    const kinds = result.items.map((row) => row.kind);
    assert.ok(kinds.includes(LEARNING_OBJECT_KINDS.STRATEGY));
    assert.ok(kinds.includes(LEARNING_OBJECT_KINDS.ORGANIZATIONAL));
    assert.ok(!kinds.includes(LEARNING_OBJECT_KINDS.MESSAGING));
  });

  it('specialist filtering: Paige receives messaging and communication strategy', () => {
    const store = createMemoryAmoStore();
    const mission = {
      id: 'm_paige_filter',
      tenantId: '10',
      structuredMission: { market: { segment: 'law_firm' } },
    };
    store.putMission(mission);

    seedOutcomeLearning(store, {
      missionId: 'm_prior_paige',
      kind: LEARNING_OBJECT_KINDS.MESSAGING,
      statement: 'Subject line hook underperformed.',
    });
    seedOutcomeLearning(store, {
      missionId: 'm_prior_paige',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Adjust messaging tone for law firm owners.',
      primaryCause: 'Messaging mismatch',
    });
    seedOutcomeLearning(store, {
      missionId: 'm_prior_paige',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Geography expansion timing was wrong.',
      primaryCause: 'Market timing misread',
    });

    const result = retrieveRelevantOutcomeLearning({
      tenantId: '10',
      mission,
      specialist: SPECIALISTS.PAIGE,
      store,
    });

    const kinds = result.items.map((row) => row.kind);
    assert.ok(kinds.includes(LEARNING_OBJECT_KINDS.MESSAGING));
    assert.ok(result.items.some((row) => /messaging/i.test(row.statement)));
    assert.ok(!result.items.some((row) => /Geography expansion timing/i.test(row.statement)));
  });

  it('Emmett empty case: no deliverability learning yields priorLearning = []', () => {
    const store = createMemoryAmoStore();
    const mission = {
      id: 'm_emmett_empty',
      tenantId: '10',
      structuredMission: { market: { segment: 'law_firm' } },
    };
    store.putMission(mission);

    seedOutcomeLearning(store, {
      missionId: 'm_prior_emmett',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Strategy lesson unrelated to deliverability.',
    });
    seedOutcomeLearning(store, {
      missionId: 'm_prior_emmett',
      kind: LEARNING_OBJECT_KINDS.HEURISTIC,
      statement: 'Business heuristic unrelated to inbox health.',
    });

    const result = retrieveRelevantOutcomeLearning({
      tenantId: '10',
      mission,
      specialist: SPECIALISTS.EMMETT,
      store,
    });

    assert.deepEqual(result.items, []);

    const input = buildExecutionInput({
      mission,
      specialist: SPECIALISTS.EMMETT,
      store,
    });
    assert.deepEqual(input.memoryContext.priorLearning, []);
  });

  it('advisory semantics: every item retains autoApplied = false', () => {
    const engine = createAcquisitionMissionEngine();
    const missionA = missionWithPlan(engine);
    completeMissionWithLearning(engine, missionA);
    const missionB = secondMission(engine);

    const input = buildExecutionInput({
      mission: missionB,
      specialist: SPECIALISTS.MAX,
      store: engine.store,
    });

    for (const item of input.memoryContext.priorLearning) {
      assert.equal(item.autoApplied, false);
    }
  });

  it('provenance: learning ID and source mission/evaluation lineage survive into SEC', () => {
    const engine = createAcquisitionMissionEngine();
    const missionA = missionWithPlan(engine);
    const learnings = completeMissionWithLearning(engine, missionA, {
      kind: LEARNING_OBJECT_KINDS.OPPORTUNITY_RULE,
      statement: 'Opportunity rule improved.',
      evaluationId: 'eval_provenance_1',
    });
    const missionB = secondMission(engine);

    const input = buildExecutionInput({
      mission: missionB,
      specialist: SPECIALISTS.MAX,
      store: engine.store,
    });

    const sourceLearning = learnings[0] || engine.store.listOutcomeLearnings('10', missionA.id)[0];
    const injected = input.memoryContext.priorLearning.find(
      (row) => row.id === sourceLearning.id || row.sourceMissionId === missionA.id
    );
    assert.ok(injected, 'Expected injected prior learning row');
    assert.equal(injected.id, sourceLearning.id);
    assert.equal(injected.sourceMissionId, missionA.id);
    assert.equal(injected.evaluationId, sourceLearning.evaluationId);
    assert.ok(injected.statement);
    assert.ok(Array.isArray(injected.evidence));
    assert.ok(injected.evidence.some((ev) => ev.kind === 'evaluation'));
  });

  it('no learning: specialist execution remains valid with priorLearning = []', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = missionWithPlan(engine);

    const input = buildExecutionInput({
      mission,
      specialist: SPECIALISTS.SCOUT,
      store: engine.store,
    });

    assert.deepEqual(input.memoryContext.priorLearning, []);
    assert.ok(Array.isArray(input.memoryContext.observations));

    const result = await executeSpecialist({
      mission,
      specialist: SPECIALISTS.SCOUT,
      store: engine.store,
      run: (secInput) => {
        assert.deepEqual(secInput.memoryContext.priorLearning, []);
        return {
          spec: 'SPEC-132',
          status: 'SUCCESS',
          confidence: { overall: 0.7, evidence: 0.7, fit: 0.7, completeness: 0.7 },
          evidence: [{
            label: 'Test evidence',
            source: 'test',
            confidence: 0.7,
            timestamp: '2026-08-29T12:00:00.000Z',
            provenance: { kind: 'observed', source: 'test' },
          }],
          contributions: {
            companies: [{ id: 'c1', name: 'Test Co' }],
            prospects: [],
            buyingSignals: [],
            evidence: [],
            confidence: 0.7,
          },
        };
      },
    });

    assert.equal(result.status, 'SUCCESS');
  });

  it('failure behavior: retrieval failure does not fabricate prior learning', () => {
    const mission = {
      id: 'm_fail',
      tenantId: '10',
      targetSegment: 'law_firm',
    };
    const brokenStore = {
      listOutcomeLearnings() {
        throw new Error('store unavailable');
      },
    };

    const memoryContext = buildMemoryContextWithPriorLearning(
      { store: brokenStore },
      mission,
      SPECIALISTS.MAX
    );

    assert.deepEqual(memoryContext.priorLearning, []);
    assert.match(memoryContext.priorLearningRetrievalWarning, /store unavailable|failed/i);
  });

  it('preserves existing observations in memoryContext', () => {
    const engine = createAcquisitionMissionEngine();
    const missionA = missionWithPlan(engine);
    completeMissionWithLearning(engine, missionA);
    const missionB = secondMission(engine);

    const observations = [{
      id: 'obs_1',
      missionId: missionB.id,
      specialist: 'scout',
      observation: 'Operator noted competitor activity.',
      at: '2026-08-29T10:00:00.000Z',
    }];

    const input = buildExecutionInput({
      mission: missionB,
      specialist: SPECIALISTS.SCOUT,
      store: engine.store,
      observations,
    });

    assert.equal(input.memoryContext.observations.length, 1);
    assert.equal(input.memoryContext.observations[0].id, 'obs_1');
    assert.ok(Array.isArray(input.memoryContext.priorLearning));
  });

  it('explicit priorLearning passthrough is respected', () => {
    const mission = { id: 'm_explicit', tenantId: '10' };
    const explicit = [{
      id: 'learn_explicit',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      sourceMissionId: 'm_old',
      evaluationId: 'eval_old',
      statement: 'Explicit lesson.',
      direction: 'updated',
      relevance: { score: 1, reasons: ['manual'] },
      evidence: [],
      autoApplied: false,
    }];

    const memoryContext = buildMemoryContextWithPriorLearning(
      { memoryContext: { observations: [], priorLearning: explicit } },
      mission,
      SPECIALISTS.MAX
    );

    assert.equal(memoryContext.priorLearning.length, 1);
    assert.equal(memoryContext.priorLearning[0].id, 'learn_explicit');
  });
});

describe('specialistAllowsKind unit checks', () => {
  it('Scout excludes messaging-only lessons', () => {
    assert.equal(
      specialistAllowsKind(SPECIALISTS.SCOUT, { kind: LEARNING_OBJECT_KINDS.MESSAGING }),
      false
    );
    assert.equal(
      specialistAllowsKind(SPECIALISTS.SCOUT, { kind: LEARNING_OBJECT_KINDS.HEURISTIC }),
      true
    );
  });

  it('Emmett excludes generic strategy', () => {
    assert.equal(
      specialistAllowsKind(SPECIALISTS.EMMETT, {
        kind: LEARNING_OBJECT_KINDS.STRATEGY,
        statement: 'Market timing misread.',
      }),
      false
    );
  });
});
