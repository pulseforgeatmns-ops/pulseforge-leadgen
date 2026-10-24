'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  KIND,
  SOURCE,
  NEXT_OPTIONS,
  looksLikePaigeCampaignContentRequest,
  looksLikeGenericPipelineQuestion,
  hasCampaignObjectiveContext,
  shouldDelegateToPaige,
  resolveCampaignContentContext,
  normalizePaigeRecommendation,
  assertRecommendationIsAdvisoryOnly,
  formatMaxPaigeCampaignRecommendation,
  composeMaxPaigeCampaignStructuredResponse,
  delegateCampaignContentRecommendation,
  ContentLearningError,
} = require('../services/maxPaigeCampaignDelegation');

describe('SPEC-094 maxPaigeCampaignDelegation', () => {
  it('detects launch runway / LinkedIn / ask Paige content requests', () => {
    assert.equal(
      looksLikePaigeCampaignContentRequest(
        'Ask Paige for a LinkedIn content experiment for the Max launch runway.'
      ),
      true
    );
    assert.equal(
      looksLikePaigeCampaignContentRequest(
        'We need a thought leadership recommendation for category creation on LinkedIn.'
      ),
      true
    );
    assert.equal(
      looksLikePaigeCampaignContentRequest(
        'Recommend the next content experiment before the public launch.'
      ),
      true
    );
    assert.equal(
      looksLikePaigeCampaignContentRequest("What's in the pipeline today?"),
      false
    );
  });

  it('does not treat generic pipeline questions as Paige content asks', () => {
    assert.equal(
      looksLikeGenericPipelineQuestion("What's in the pipeline?"),
      true
    );
    assert.equal(
      looksLikeGenericPipelineQuestion('Show me the setter queue status'),
      true
    );
    assert.equal(
      shouldDelegateToPaige("What's in the pipeline?", {
        campaignId: 'max-launch-runway',
        clientId: 1,
      }),
      false
    );
  });

  it('requires campaign/objective context before delegating', () => {
    assert.equal(
      shouldDelegateToPaige('Ask Paige for a LinkedIn content recommendation', {
        clientId: 1,
      }),
      false
    );
    assert.equal(
      shouldDelegateToPaige(
        'Ask Paige for a LinkedIn content recommendation for the Max launch runway',
        { clientId: 1 }
      ),
      true
    );
    assert.equal(
      shouldDelegateToPaige('Ask Paige for a LinkedIn content recommendation', {
        clientId: 1,
        campaignId: 'max-launch-runway',
      }),
      true
    );
    assert.equal(
      hasCampaignObjectiveContext({
        firstCampaignPlanPreview: {
          campaignObjective: 'Build qualified attention before Max reveal',
        },
      }),
      true
    );
  });

  it('resolves durable campaign context without inventing state', () => {
    const request = resolveCampaignContentContext({
      clientId: 1,
      operatorMessage:
        'Ask Paige for a LinkedIn content experiment on the launch runway.',
      context: {
        campaignId: 'max-launch-runway',
        firstCampaignPlanPreview: {
          campaignObjective:
            'Build qualified attention and category understanding before the public Max reveal.',
        },
        campaignPlanning: { status: 'preview_ready' },
        channel: 'linkedin',
      },
    });

    assert.ok(request);
    assert.equal(request.clientId, 1);
    assert.equal(request.tenantId, 1);
    assert.equal(request.campaignId, 'max-launch-runway');
    assert.equal(request.channel, 'linkedin');
    assert.match(request.objective, /qualified attention|Max reveal/i);
    assert.equal(request.campaignContext.hasFirstCampaignPlanPreview, true);
    assert.equal(request.source, 'client_intelligence_campaign');
  });

  it('returns null context when tenant/client is missing', () => {
    const request = resolveCampaignContentContext({
      operatorMessage: 'Ask Paige about LinkedIn launch runway content',
      context: { campaignId: 'max-launch-runway' },
    });
    assert.equal(request, null);
  });

  it('normalizes SPEC-093 snake_case into Max camelCase and preserves IDs', () => {
    const payload = normalizePaigeRecommendation(
      {
        kind: 'content_recommendation',
        objective: 'Build qualified attention',
        recommended_direction: 'Test a distinct operator-centered thesis',
        reason: 'Based on 2 relevant learning(s).',
        confidence: 0.55,
        uncertainties: ['Whether breakout is repeatable.'],
        supporting_learning_ids: ['learn-1', 'learn-2'],
        supporting_publication_ids: ['pub-1'],
        experiment: {
          hypothesis: 'Operator-centered critiques generate discovery.',
          preserve: ['operator-centered framing'],
          vary: ['specific argument'],
          supporting_learning_ids: ['learn-1'],
        },
        campaignId: 'max-launch-runway',
        generated_at: '2026-08-13T12:00:00.000Z',
      },
      { tenantId: 1, clientId: 1, campaignId: 'max-launch-runway', channel: 'linkedin' }
    );

    assert.equal(payload.kind, KIND);
    assert.equal(payload.source, SOURCE);
    assert.equal(payload.recommendedDirection, 'Test a distinct operator-centered thesis');
    assert.deepEqual(payload.supportingLearningIds, ['learn-1', 'learn-2']);
    assert.deepEqual(payload.supportingPublicationIds, ['pub-1']);
    assert.equal(payload.autonomousPublish, false);
    assert.equal(payload.reviewFirst, true);
    assert.equal(payload.nextOptions.length, NEXT_OPTIONS.length);
    assert.equal(payload.experiment.supportingLearningIds[0], 'learn-1');
  });

  it('formats Max response with Paige attribution, evidence, uncertainty, next options', () => {
    const payload = normalizePaigeRecommendation(
      {
        objective: 'Build attention',
        recommended_direction: 'Argue that AI must understand uncertainty first.',
        reason: 'Based on learnings.',
        confidence: 0.6,
        uncertainties: ['Attribution remains uncertain.'],
        supporting_learning_ids: ['L1'],
        supporting_publication_ids: ['P1'],
        experiment: { hypothesis: 'Distinct thesis beats clones.' },
      },
      { clientId: 1, campaignId: 'max-launch-runway' }
    );
    const prose = formatMaxPaigeCampaignRecommendation(payload);
    assert.match(prose, /Paige is recommending/i);
    assert.match(prose, /not publishing/i);
    assert.match(prose, /L1/);
    assert.match(prose, /P1/);
    assert.match(prose, /Uncertainty/i);
    assert.match(prose, /Accept/i);
    assert.match(prose, /Discuss with Max/i);
    assert.match(prose, /Nothing will be published/i);
    assert.doesNotMatch(prose, /\b(has been published|publishing now|sent via Buffer|wrote to CRM)\b/i);
  });

  it('no-autonomy guard rejects publish/send flags', () => {
    assert.throws(
      () =>
        assertRecommendationIsAdvisoryOnly({
          autonomousPublish: true,
        }),
      (err) => err instanceof ContentLearningError && err.code === 'autonomy_forbidden'
    );
    assert.throws(
      () =>
        assertRecommendationIsAdvisoryOnly({
          autonomousPublish: false,
          send: true,
        }),
      (err) => err instanceof ContentLearningError && err.code === 'autonomy_forbidden'
    );
  });

  it('structured response never emits publish/send/CRM action types', () => {
    const payload = normalizePaigeRecommendation(
      {
        recommended_direction: 'Test a new thesis',
        reason: 'Evidence',
        confidence: 0.5,
        uncertainties: ['Thin sample'],
        supporting_learning_ids: ['L1'],
        supporting_publication_ids: [],
      },
      { clientId: 1 }
    );
    const structured = composeMaxPaigeCampaignStructuredResponse(payload);
    assert.match(structured.answer, /Paige is recommending/i);
    assert.equal(structured.metadata.autonomousPublish, false);
    assert.equal(structured.metadata.reviewFirst, true);
    for (const action of structured.recommendedActions) {
      assert.ok(['accept_recommendation', 'discuss_with_max'].includes(action.type));
      assert.equal(action.payload.autonomousPublish, false);
      assert.doesNotMatch(action.id, /publish|send|buffer|crm/i);
    }
  });

  it('delegates through generateContentRecommendation with stubbed learning service', async () => {
    let seen = null;
    const result = await delegateCampaignContentRecommendation(
      {
        question:
          'Ask Paige for a LinkedIn content experiment for the Max launch runway.',
        clientId: 1,
        context: {
          campaignId: 'max-launch-runway',
          objective:
            'Build qualified attention and category understanding before the public Max reveal.',
        },
      },
      {
        generateContentRecommendation: async (ctx) => {
          seen = ctx;
          return {
            kind: 'content_recommendation',
            objective: ctx.objective,
            recommended_direction: 'Publish a distinct trust/uncertainty thesis.',
            reason: 'Based on 1 relevant learning(s) from SPEC-092 outcomes.',
            confidence: 0.52,
            uncertainties: ['Whether breakout distribution is repeatable.'],
            supporting_learning_ids: ['learning-abc'],
            supporting_publication_ids: ['pub-xyz'],
            experiment: {
              hypothesis: 'Operator-centered critiques generate discovery.',
              preserve: ['operator-centered framing'],
              vary: ['specific argument'],
              supporting_learning_ids: ['learning-abc'],
            },
            autonomousPublish: false,
            operatorAuthority: true,
            campaignId: ctx.campaignId,
            generated_at: '2026-08-13T15:00:00.000Z',
          };
        },
      }
    );

    assert.equal(result.ok, true);
    assert.equal(result.skipped, false);
    assert.equal(seen.clientId, 1);
    assert.equal(seen.campaignId, 'max-launch-runway');
    assert.equal(seen.channel, 'linkedin');
    assert.ok(seen.objective);
    assert.equal(result.recommendation.kind, KIND);
    assert.deepEqual(result.recommendation.supportingLearningIds, ['learning-abc']);
    assert.deepEqual(result.recommendation.supportingPublicationIds, ['pub-xyz']);
    assert.equal(result.recommendation.autonomousPublish, false);
    assert.match(result.prose, /Paige is recommending/i);
    assert.match(result.structured.answer, /learning-abc|Evidence basis/i);
  });

  it('enforces tenant isolation — client A learnings never requested for client B', async () => {
    const calls = [];
    await delegateCampaignContentRecommendation(
      {
        question: 'Ask Paige for LinkedIn launch runway content recommendation',
        clientId: 2,
        context: { campaignId: 'mshi-launch', objective: 'MSHI attention' },
      },
      {
        generateContentRecommendation: async (ctx) => {
          calls.push(ctx);
          return {
            recommended_direction: 'Keep operator framing',
            reason: 'Scoped',
            confidence: 0.4,
            uncertainties: ['Thin evidence'],
            supporting_learning_ids: ['tenant-2-only'],
            supporting_publication_ids: [],
            autonomousPublish: false,
          };
        },
      }
    );

    assert.equal(calls.length, 1);
    assert.equal(calls[0].clientId, 2);
    assert.equal(calls[0].tenantId, 2);
    assert.notEqual(calls[0].clientId, 1);
  });

  it('skips delegation when campaign context is absent for content-ish asks', async () => {
    const result = await delegateCampaignContentRecommendation(
      {
        question: 'Ask Paige for a LinkedIn content recommendation',
        clientId: 1,
        context: {},
      },
      {
        generateContentRecommendation: async () => {
          throw new Error('should not be called');
        },
      }
    );
    assert.equal(result.ok, false);
    assert.equal(result.skipped, true);
    assert.equal(result.reason, 'missing_campaign_context');
  });

  it('skips generic pipeline questions even with campaign context present', async () => {
    const result = await delegateCampaignContentRecommendation(
      {
        question: "What's in the pipeline?",
        clientId: 1,
        context: { campaignId: 'max-launch-runway' },
      },
      {
        generateContentRecommendation: async () => {
          throw new Error('should not be called');
        },
      }
    );
    assert.equal(result.ok, false);
    assert.equal(result.skipped, true);
    assert.equal(result.reason, 'generic_pipeline_question');
  });

  it('does not hallucinate supporting IDs when the learning service returns none', async () => {
    const result = await delegateCampaignContentRecommendation(
      {
        question: 'Ask Paige for LinkedIn category creation content for Max launch',
        clientId: 1,
        context: {
          campaignId: 'max-launch-runway',
          objective: 'Build qualified attention before Max reveal',
        },
      },
      {
        generateContentRecommendation: async () => ({
          recommended_direction: 'Start with a first instrumented experiment.',
          reason: 'No prior learnings.',
          confidence: 0.25,
          uncertainties: ['No durable content learnings exist yet for this tenant.'],
          supporting_learning_ids: [],
          supporting_publication_ids: [],
          autonomousPublish: false,
        }),
      }
    );

    assert.equal(result.ok, true);
    assert.deepEqual(result.recommendation.supportingLearningIds, []);
    assert.deepEqual(result.recommendation.supportingPublicationIds, []);
    assert.doesNotMatch(result.prose, /\b[0-9a-f]{8}-[0-9a-f]{4}\b/i);
    assert.match(result.prose, /no durable learnings\/publications cited/i);
  });
});
