'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { createWorkspaceEngine } = require('../../index');
const {
  maybeHandlePaigeCampaignContentDelegation,
} = require('../PaigeCampaignDelegationContext');
const delegation = require('../../../../services/maxPaigeCampaignDelegation');

describe('SPEC-094 Paige campaign content workspace routing', () => {
  it('routes launch runway / LinkedIn / ask Paige into Paige delegation', async () => {
    let delegated = false;
    const workspace = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      paigeCampaignDelegationService: {
        shouldDelegateToPaige: delegation.shouldDelegateToPaige,
        delegateCampaignContentRecommendation: async (input) => {
          delegated = true;
          assert.equal(Number(input.clientId || input.tenantId), 1);
          assert.equal(input.context.campaignId, 'max-launch-runway');
          const recommendation = delegation.normalizePaigeRecommendation(
            {
              objective: input.context.objective,
              recommended_direction: 'Test a distinct AI-trust thesis.',
              reason: 'Based on learnings.',
              confidence: 0.55,
              uncertainties: ['Repeatability unknown.'],
              supporting_learning_ids: ['learn-1'],
              supporting_publication_ids: ['pub-1'],
              autonomousPublish: false,
            },
            {
              clientId: 1,
              tenantId: 1,
              campaignId: 'max-launch-runway',
              channel: 'linkedin',
            }
          );
          return {
            ok: true,
            skipped: false,
            recommendation,
            structured:
              delegation.composeMaxPaigeCampaignStructuredResponse(recommendation),
            prose: delegation.formatMaxPaigeCampaignRecommendation(recommendation),
          };
        },
      },
    });

    const result = await workspace.ask({
      question:
        'Ask Paige for a LinkedIn content experiment recommendation for the Max launch runway.',
      context: {
        tenantId: '1',
        page: 'command-deck',
        campaignId: 'max-launch-runway',
        objective:
          'Build qualified attention and category understanding before the public Max reveal.',
        channel: 'linkedin',
      },
    });

    assert.equal(delegated, true);
    assert.equal(result.executionDomain, 'workspace');
    assert.equal(result.domainDecision.reason, 'paige_campaign_content_delegation');
    assert.match(result.prose, /Paige is recommending/i);
    assert.match(result.prose, /learn-1|Evidence basis/i);
    assert.ok(result.paigeRecommendation);
    assert.equal(result.paigeRecommendation.autonomousPublish, false);
    assert.equal(result.mission, null);
    for (const action of result.recommendedActions || []) {
      assert.ok(['accept_recommendation', 'discuss_with_max'].includes(action.type));
      assert.doesNotMatch(String(action.id), /publish|send|buffer|crm/i);
    }
  });

  it('falls back to normal Max behavior for generic pipeline questions', async () => {
    let delegated = false;
    const workspace = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      paigeCampaignDelegationService: {
        shouldDelegateToPaige: delegation.shouldDelegateToPaige,
        delegateCampaignContentRecommendation: async () => {
          delegated = true;
          throw new Error('should not delegate');
        },
      },
    });

    const result = await workspace.ask({
      question: "What's in the pipeline today?",
      context: {
        tenantId: '1',
        page: 'command-deck',
        campaignId: 'max-launch-runway',
      },
    });

    assert.equal(delegated, false);
    assert.notEqual(
      result.domainDecision && result.domainDecision.reason,
      'paige_campaign_content_delegation'
    );
    assert.equal(result.paigeRecommendation, undefined);
  });

  it('does not route when campaign context is absent', async () => {
    const handled = await maybeHandlePaigeCampaignContentDelegation({
      question: 'Ask Paige for a LinkedIn content recommendation',
      context: { tenantId: '1', page: 'command-deck' },
      session: { context: { tenantId: '1' } },
      delegationService: delegation,
    });
    assert.equal(handled, null);
  });

  it('keeps tenant isolation on the workspace hook', async () => {
    const handled = await maybeHandlePaigeCampaignContentDelegation({
      question: 'Ask Paige for LinkedIn launch runway content recommendation',
      context: {
        tenantId: '2',
        clientId: 2,
        campaignId: 'client-2-campaign',
        objective: 'MSHI category attention',
      },
      session: { context: { tenantId: '2' } },
      delegationService: {
        shouldDelegateToPaige: delegation.shouldDelegateToPaige,
        delegateCampaignContentRecommendation: async (input) => {
          assert.equal(Number(input.clientId), 2);
          assert.notEqual(Number(input.clientId), 1);
          const recommendation = delegation.normalizePaigeRecommendation(
            {
              recommended_direction: 'Operator-centered renovation thesis',
              reason: 'Tenant-scoped',
              confidence: 0.4,
              uncertainties: ['Thin'],
              supporting_learning_ids: ['c2-learn'],
              supporting_publication_ids: [],
              autonomousPublish: false,
            },
            { clientId: 2, tenantId: 2, campaignId: 'client-2-campaign' }
          );
          return {
            ok: true,
            recommendation,
            structured:
              delegation.composeMaxPaigeCampaignStructuredResponse(recommendation),
            prose: delegation.formatMaxPaigeCampaignRecommendation(recommendation),
          };
        },
      },
    });

    assert.ok(handled);
    assert.equal(handled.recommendation.clientId, 2);
    assert.deepEqual(handled.recommendation.supportingLearningIds, ['c2-learn']);
  });
});
