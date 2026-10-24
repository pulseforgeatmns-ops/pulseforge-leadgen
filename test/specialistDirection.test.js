'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMemoryStore,
  interpretOperatorFeedback,
  persistContentRecommendation,
  applyOperatorDirection,
  recoverOperatorDirectionContext,
  getApplicableDirections,
  getContentRecommendation,
  looksLikeRefinementFeedback,
  looksLikeDirectionRecoveryQuestion,
  SpecialistDirectionError,
} = require('../services/specialistDirection');
const { createMemoryStore: createLearningStore } = require('../services/contentLearning');

const ACCEPTANCE_FIXTURE = {
  recommendedDirection: 'AI systems should understand uncertainty before acting.',
  reason: 'Tests whether operator-centered AI framing continues to produce qualified out-of-network discovery.',
  confidence: 0.25,
  payload: {
    experiment: {
      preserve: [
        'operator-centered framing',
        'strong declarative thesis',
        'AI / business-software subject',
        'linkedin channel',
      ],
      vary: ['specific argument'],
      supporting_learning_ids: ['learn-tech-1'],
    },
    supporting_learning_ids: ['learn-tech-1'],
    supporting_publication_ids: ['pub-tech-1'],
    uncertainties: ['Whether breakout distribution is repeatable.'],
  },
  campaignId: 'max-launch-runway',
  channel: 'linkedin',
  objective: 'Build qualified attention before the public Max reveal.',
};

const OPERATOR_FEEDBACK =
  "I like the experiment structure, but that direction is going to keep attracting AI builders. We're already attracting them successfully. I want the next post aimed more directly at SMB operators without losing the operator-first framing.";

describe('SPEC-096 specialistDirection service', () => {
  /** @type {ReturnType<typeof createMemoryStore>} */
  let store;
  /** @type {ReturnType<typeof createLearningStore>} */
  let learningStore;

  beforeEach(() => {
    store = createMemoryStore();
    learningStore = createLearningStore();
  });

  it('interprets acceptance fixture operator feedback as refine with SMB direction', () => {
    const interpretation = interpretOperatorFeedback({
      operatorMessage: OPERATOR_FEEDBACK,
      recommendation: ACCEPTANCE_FIXTURE,
    });

    assert.equal(interpretation.disposition, 'refine');
    assert.equal(interpretation.needsClarification, false);
    assert.ok(interpretation.acceptedElements.some((e) => /experiment|operator/i.test(e)));
    assert.ok(interpretation.changedElements.some((e) => /argument|audience/i.test(e)));
    assert.match(interpretation.updatedDirection, /SMB|accessible|operator/i);
    assert.match(interpretation.rationale, /technical|SMB|buyer/i);
    assert.equal(interpretation.scope, 'experiment_campaign');
  });

  it('asks clarification for ambiguous scope feedback', () => {
    const interpretation = interpretOperatorFeedback({
      operatorMessage: "That's too technical.",
      recommendation: ACCEPTANCE_FIXTURE,
    });
    assert.equal(interpretation.needsClarification, true);
    assert.match(interpretation.clarificationPrompt, /this recommendation only|broader/i);
  });

  it('happy path: recommend → discuss → refine → Paige recompute', async () => {
    const rec = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        ...ACCEPTANCE_FIXTURE,
        recommendation: ACCEPTANCE_FIXTURE,
      },
      { store }
    );

    const result = await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage: OPERATOR_FEEDBACK,
      },
      { store, learningOpts: { store: learningStore } }
    );

    assert.equal(result.ok, true);
    assert.equal(result.disposition, 'refine');
    assert.ok(result.refinedRecommendation);
    assert.match(
      result.refinedRecommendation.recommendedDirection,
      /Small business owners shouldn't have to become AI experts|operator|AI/i
    );
    assert.equal(result.direction.disposition, 'refine');
    assert.equal(result.direction.refinementState, 'completed');
    assert.deepEqual(
      result.refinedRecommendation.supportingLearningIds,
      ['learn-tech-1']
    );
  });

  it('recommendation-only scope stays local', async () => {
    const rec = await persistContentRecommendation(
      {
        ...ACCEPTANCE_FIXTURE,
        tenantId: '1',
        clientId: 1,
        campaignId: null,
        recommendation: { ...ACCEPTANCE_FIXTURE, campaignId: null },
      },
      { store }
    );

    const result = await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage:
          "Don't use that hook for this recommendation only — try something more operator-accessible.",
      },
      { store, learningOpts: { store: learningStore } }
    );

    assert.equal(result.direction.scope, 'recommendation_only');
    assert.equal(result.direction.operatorLearningId, null);
  });

  it('campaign-level direction creates operator learning', async () => {
    const rec = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        campaignId: 'max-launch-runway',
        ...ACCEPTANCE_FIXTURE,
        recommendation: ACCEPTANCE_FIXTURE,
      },
      { store }
    );

    await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage: OPERATOR_FEEDBACK,
      },
      { store, learningOpts: { store: learningStore } }
    );

    const learnings = await learningStore.listLearnings({ clientId: 1 });
    const operatorLearning = learnings.find(
      (l) => l.scope && l.scope.learningSource === 'operator_direction'
    );
    assert.ok(operatorLearning);
    assert.match(operatorLearning.statement, /SMB|accessible/i);
    assert.doesNotMatch(operatorLearning.statement, /Technical content performs poorly/i);
    assert.doesNotMatch(operatorLearning.statement, /Never write about uncertainty/i);
  });

  it('durable preference persists when explicitly established', async () => {
    const rec = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        campaignId: 'max-launch-runway',
        ...ACCEPTANCE_FIXTURE,
        recommendation: ACCEPTANCE_FIXTURE,
      },
      { store }
    );

    const result = await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage:
          'I generally prefer explaining technical concepts through operating problems rather than technical terminology.',
      },
      { store, learningOpts: { store: learningStore } }
    );

    assert.equal(result.direction.scope, 'durable_preference');
    assert.ok(result.direction.operatorLearningId);
  });

  it('contradictory direction triggers clarification', async () => {
    const rec = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        campaignId: 'max-launch-runway',
        ...ACCEPTANCE_FIXTURE,
        recommendation: ACCEPTANCE_FIXTURE,
      },
      { store }
    );

    await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage: OPERATOR_FEEDBACK,
      },
      { store, learningOpts: { store: learningStore } }
    );

    const contradiction = await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage: 'I want the next post to go deep technically.',
      },
      { store, learningOpts: { store: learningStore } }
    );

    assert.equal(contradiction.needsClarification, true);
    assert.equal(contradiction.contradiction, true);
  });

  it('fresh Max session recovers operator direction', async () => {
    const rec = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        campaignId: 'max-launch-runway',
        ...ACCEPTANCE_FIXTURE,
        recommendation: ACCEPTANCE_FIXTURE,
      },
      { store }
    );

    await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage: OPERATOR_FEEDBACK,
      },
      { store, learningOpts: { store: learningStore } }
    );

    const recovery = await recoverOperatorDirectionContext(
      {
        tenantId: '1',
        campaignId: 'max-launch-runway',
        question: 'Why is Paige moving toward SMB-oriented arguments?',
      },
      { store }
    );

    assert.equal(recovery.recovered, true);
    assert.match(recovery.explanation, /previously directed|SMB|technical/i);
    assert.match(recovery.explanation, /responding strongly|buyer/i);
  });

  it('Paige failure preserves original recommendation', async () => {
    const rec = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        ...ACCEPTANCE_FIXTURE,
        recommendation: ACCEPTANCE_FIXTURE,
      },
      { store }
    );

    const result = await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage: OPERATOR_FEEDBACK,
      },
      {
        store,
        refineContentRecommendation: async () => {
          throw new Error('Paige unavailable');
        },
      }
    );

    assert.equal(result.failed, true);
    assert.equal(result.ok, false);
    const original = await store.getRecommendation(rec.id, '1');
    assert.equal(original.status, 'pending');
    assert.equal(original.recommendedDirection, ACCEPTANCE_FIXTURE.recommendedDirection);
  });

  it('missing recommendation fails safely', async () => {
    await assert.rejects(
      () =>
        applyOperatorDirection(
          {
            tenantId: '1',
            clientId: 1,
            recommendationId: '00000000-0000-4000-8000-000000000001',
            operatorMessage: OPERATOR_FEEDBACK,
          },
          { store }
        ),
      (err) => err instanceof SpecialistDirectionError && err.code === 'recommendation_not_found'
    );
  });

  it('tenant isolation: tenant A direction unavailable to tenant B', async () => {
    const recA = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        campaignId: 'campaign-a',
        ...ACCEPTANCE_FIXTURE,
        recommendation: ACCEPTANCE_FIXTURE,
      },
      { store }
    );

    await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: recA.id,
        operatorMessage: OPERATOR_FEEDBACK,
      },
      { store, learningOpts: { store: learningStore } }
    );

    const tenantBDirections = await getApplicableDirections(
      { tenantId: '2', campaignId: 'campaign-a' },
      { store }
    );
    assert.equal(tenantBDirections.length, 0);

    await assert.rejects(
      () => getContentRecommendation(recA.id, '2', { store }),
      (err) => err instanceof SpecialistDirectionError
    );
  });

  it('evidence integrity: operator correction does not mutate learnings', async () => {
    await learningStore.upsertLearning({
      id: 'learn-tech-1',
      clientId: 1,
      tenantId: '1',
      fingerprint: 'tech:engagement',
      learningType: 'audience_response',
      statement: 'Technical/AI audiences responded strongly to operator-centered AI content.',
      scope: { learningSource: 'observed_outcome' },
      objective: 'category_creation',
      channel: 'linkedin',
      confidence: 0.7,
      observationConfidence: 0.85,
      generalizationConfidence: 0.4,
      sampleSize: 1,
      supportingPublicationIds: ['pub-tech-1'],
      contradictingPublicationIds: [],
      evidenceSummary: 'Strong builder engagement observed.',
      status: 'emerging',
      firstObservedAt: new Date().toISOString(),
      lastEvaluatedAt: new Date().toISOString(),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });

    const rec = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        supportingLearningIds: ['learn-tech-1'],
        ...ACCEPTANCE_FIXTURE,
        recommendation: ACCEPTANCE_FIXTURE,
      },
      { store }
    );

    await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage: OPERATOR_FEEDBACK,
      },
      { store, learningOpts: { store: learningStore } }
    );

    const observed = await learningStore.getLearning('learn-tech-1', 1);
    assert.match(observed.statement, /Technical\/AI audiences responded strongly/i);
    assert.equal(observed.scope.learningSource, 'observed_outcome');
  });

  it('accept disposition marks recommendation accepted', async () => {
    const rec = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        ...ACCEPTANCE_FIXTURE,
        recommendation: ACCEPTANCE_FIXTURE,
      },
      { store }
    );

    const result = await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage: 'Accept this direction.',
      },
      { store }
    );

    assert.equal(result.disposition, 'accept');
    const updated = await store.getRecommendation(rec.id, '1');
    assert.equal(updated.status, 'accepted');
  });

  it('detects refinement feedback patterns', () => {
    assert.equal(looksLikeRefinementFeedback(OPERATOR_FEEDBACK), true);
    assert.equal(looksLikeRefinementFeedback('Accept this direction.'), false);
    assert.equal(
      looksLikeDirectionRecoveryQuestion('Why is Paige moving toward SMB-oriented arguments?'),
      true
    );
  });
});
