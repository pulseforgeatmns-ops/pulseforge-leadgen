'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  maybeHandleSpecialistDirectionTurn,
} = require('../SpecialistDirectionContext');
const {
  createMemoryStore,
  persistContentRecommendation,
  applyOperatorDirection,
} = require('../../../../services/specialistDirection');
const { createMemoryStore: createLearningStore } = require('../../../../services/contentLearning');
const delegation = require('../../../../services/maxPaigeCampaignDelegation');

describe('SPEC-096 specialist direction workspace routing', () => {
  /** @type {ReturnType<typeof createMemoryStore>} */
  let directionStore;
  /** @type {ReturnType<typeof createLearningStore>} */
  let learningStore;

  beforeEach(() => {
    directionStore = createMemoryStore();
    learningStore = createLearningStore();
  });

  it('acceptance scenario: discuss → refine → present revised recommendation', async () => {
    const recommendation = delegation.normalizePaigeRecommendation(
      {
        objective: 'Build qualified attention before the public Max reveal.',
        recommended_direction:
          'AI systems should understand uncertainty before acting.',
        reason:
          'Tests whether operator-centered AI framing continues to produce qualified out-of-network discovery.',
        confidence: 0.25,
        uncertainties: ['Repeatability unknown.'],
        supporting_learning_ids: ['learn-1'],
        supporting_publication_ids: ['pub-1'],
        experiment: {
          preserve: [
            'operator-centered framing',
            'strong declarative thesis',
            'AI / business-software subject',
            'linkedin channel',
          ],
          vary: ['specific argument'],
        },
      },
      { clientId: 1, channel: 'linkedin', campaignId: 'max-launch-runway' }
    );

    const persisted = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        campaignId: 'max-launch-runway',
        recommendation,
      },
      { store: directionStore }
    );

    const refined = await maybeHandleSpecialistDirectionTurn({
      question:
        "I like the experiment structure, but that direction is going to keep attracting AI builders. We're already attracting them successfully. I want the next post aimed more directly at SMB operators without losing the operator-first framing.",
      context: {
        tenantId: '1',
        recommendationId: persisted.id,
      },
      session: {
        context: {
          tenantId: '1',
          recommendationId: persisted.id,
          paigeRecommendation: { ...recommendation, recommendationId: persisted.id },
        },
      },
      directionOpts: { store: directionStore, learningOpts: { store: learningStore } },
    });

    assert.ok(refined);
    assert.equal(refined.reason, 'specialist_direction_refine');
    assert.match(refined.prose, /Understood|preserve|SMB/i);
    assert.match(
      refined.prose,
      /Small business owners shouldn't have to become AI experts|revise/i
    );
    assert.ok(refined.refinedRecommendation);
    assert.ok(
      refined.structured.recommendedActions.some(
        (a) => a.type === 'accept_recommendation' || a.type === 'discuss_with_max'
      )
    );
  });

  it('discuss with Max opens conversational context', async () => {
    const rec = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        recommendedDirection: 'AI systems should understand uncertainty before acting.',
        reason: 'Test',
        confidence: 0.25,
        recommendation: {
          recommended_direction: 'AI systems should understand uncertainty before acting.',
        },
      },
      { store: directionStore }
    );

    const result = await maybeHandleSpecialistDirectionTurn({
      question: "I'd like to discuss Paige's recommendation.",
      action: 'discuss_with_max',
      context: {
        tenantId: '1',
        action: 'discuss_with_max',
        recommendationId: rec.id,
        discussRecommendation: true,
      },
      session: { context: { tenantId: '1', recommendationId: rec.id } },
      directionOpts: { store: directionStore },
    });

    assert.equal(result.reason, 'specialist_direction_discuss');
    assert.match(result.prose, /discuss Paige's recommendation/i);
    assert.match(result.prose, /already in context/i);
  });

  it('fresh session recovers operator direction explanation', async () => {
    const rec = await persistContentRecommendation(
      {
        tenantId: '1',
        clientId: 1,
        campaignId: 'max-launch-runway',
        recommendedDirection: 'AI systems should understand uncertainty before acting.',
        reason: 'Test',
        confidence: 0.25,
        recommendation: {
          recommended_direction: 'AI systems should understand uncertainty before acting.',
          experiment: { preserve: ['operator-centered framing'], vary: ['specific argument'] },
        },
      },
      { store: directionStore }
    );

    await applyOperatorDirection(
      {
        tenantId: '1',
        clientId: 1,
        recommendationId: rec.id,
        operatorMessage:
          "I like the experiment structure, but that direction is going to keep attracting AI builders. We're already attracting them successfully. I want the next post aimed more directly at SMB operators without losing the operator-first framing.",
      },
      { store: directionStore, learningOpts: { store: learningStore } }
    );

    const result = await maybeHandleSpecialistDirectionTurn({
      question: 'Why is Paige moving toward SMB-oriented arguments?',
      context: { tenantId: '1', campaignId: 'max-launch-runway' },
      session: { context: { tenantId: '1' } },
      directionOpts: { store: directionStore },
    });

    assert.equal(result.reason, 'specialist_direction_recovery');
    assert.match(result.prose, /previously directed|SMB|technical audiences/i);
  });
});
