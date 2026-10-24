'use strict';

/**
 * SPEC-094 — Max Workspace thin adapter for Paige campaign content delegation.
 * Max remains the operator-facing responder. Paige is advisory only.
 */

function defaultDelegationService() {
  return require('../../../services/maxPaigeCampaignDelegation');
}

/**
 * @param {object} input
 * @param {string} input.question
 * @param {object} [input.session]
 * @param {object} [input.context]
 * @param {object} [input.delegationService]
 * @param {object} [input.learningOpts]
 * @returns {Promise<object|null>}
 */
async function maybeHandlePaigeCampaignContentDelegation(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;

  const service = input.delegationService || defaultDelegationService();
  const session = input.session || null;
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};

  const context = {
    ...sessionCtx,
    ...envelope,
    tenantId:
      envelope.tenantId ||
      sessionCtx.tenantId ||
      (session && session.context && session.context.tenantId) ||
      null,
    clientId:
      envelope.clientId ||
      envelope.client_id ||
      sessionCtx.clientId ||
      sessionCtx.tenantId ||
      null,
  };

  if (
    typeof service.shouldDelegateToPaige === 'function' &&
    !service.shouldDelegateToPaige(question, context)
  ) {
    return null;
  }

  const result = await service.delegateCampaignContentRecommendation(
    {
      question,
      operatorMessage: question,
      context,
      tenantId: context.tenantId,
      clientId: context.clientId || context.tenantId,
    },
    {
      ...(input.learningOpts || {}),
      directionOpts: input.directionOpts || {},
    }
  );

  if (!result || !result.ok || result.skipped) {
    return null;
  }

  return {
    reason: 'paige_campaign_content_delegation',
    structured: result.structured,
    recommendation: result.recommendation,
    recommendationId: result.recommendationId || null,
    request: result.request,
    prose: result.prose,
  };
}

module.exports = {
  maybeHandlePaigeCampaignContentDelegation,
};
