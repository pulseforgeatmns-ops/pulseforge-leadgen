'use strict';

/**
 * SPEC-096 — Max Workspace adapter for specialist direction & operator rationale.
 * Operator discusses recommendations with Max; Max interprets and delegates to Paige.
 */

function defaultDirectionService() {
  return require('../../../services/specialistDirection');
}

/**
 * Detect whether this turn should route to specialist direction handling.
 *
 * @param {object} input
 * @returns {boolean}
 */
function shouldHandleSpecialistDirection(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return false;

  const service = input.directionService || defaultDirectionService();
  const context = input.context || {};
  const action = input.action || context.action || null;

  if (
    action === 'accept_recommendation' ||
    action === 'discuss_with_max' ||
    context.recommendationId ||
    context.pendingRecommendationId
  ) {
    return true;
  }

  const recId =
    context.recommendationId ||
    context.pendingRecommendationId ||
    (context.paigeRecommendation && context.paigeRecommendation.recommendationId);

  if (recId && service.looksLikeRefinementFeedback(question)) {
    return true;
  }

  if (recId && service.looksLikeAcceptAction(question)) {
    return true;
  }

  if (service.looksLikeDirectionRecoveryQuestion(question)) {
    return true;
  }

  return false;
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleSpecialistDirectionTurn(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;

  const service = input.directionService || defaultDirectionService();
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
      envelope.clientId ||
      sessionCtx.clientId ||
      null,
    clientId:
      envelope.clientId ||
      envelope.client_id ||
      sessionCtx.clientId ||
      sessionCtx.tenantId ||
      null,
  };

  if (!shouldHandleSpecialistDirection({ question, context, action: input.action })) {
    return null;
  }

  const tenantId = context.tenantId || context.clientId;
  if (tenantId == null) return null;

  const directionOpts = input.directionOpts || {};

  // Fresh-session direction recovery
  const recovery = await service.recoverOperatorDirectionContext(
    {
      tenantId,
      clientId: context.clientId || tenantId,
      campaignId: context.campaignId || context.campaign_id,
      question,
    },
    directionOpts
  );

  if (recovery.recovered && service.looksLikeDirectionRecoveryQuestion(question)) {
    const structured = {
      answer: recovery.explanation,
      reasoning: [
        'Recovered durable operator direction from persisted store (SPEC-096).',
        `Direction ID: ${recovery.direction.id}.`,
        `Scope: ${recovery.direction.scope}.`,
      ],
      supportingEvidence: [],
      confidence: recovery.direction.confidence,
      recommendedActions: [],
      metadata: {
        specialistDirection: true,
        directionRecovery: true,
        directionId: recovery.direction.id,
        executionDomain: 'workspace',
        surface: 'specialist_direction_recovery',
      },
    };
    return {
      reason: 'specialist_direction_recovery',
      structured,
      prose: recovery.explanation,
      direction: recovery.direction,
    };
  }

  const recommendationId =
    input.recommendationId ||
    context.recommendationId ||
    context.pendingRecommendationId ||
    (context.paigeRecommendation && context.paigeRecommendation.recommendationId) ||
    (sessionCtx.paigeRecommendation && sessionCtx.paigeRecommendation.recommendationId);

  const action = input.action || context.action;

  if (action === 'accept_recommendation' && recommendationId) {
    const result = await service.applyOperatorDirection(
      {
        tenantId,
        clientId: context.clientId || tenantId,
        recommendationId,
        operatorMessage: 'Accept this direction.',
      },
      directionOpts
    );
    if (!result.handled) return null;
    const structured = service.composeDirectionStructuredResponse(result);
    return {
      reason: 'specialist_direction_accept',
      structured,
      prose: structured.answer,
      direction: result.direction,
      recommendationId,
    };
  }

  if (
    (action === 'discuss_with_max' || context.discussRecommendation) &&
    recommendationId
  ) {
    const structured = {
      answer: [
        "I'm ready to discuss Paige's recommendation with you.",
        "Tell me what you would like to change and why — I will interpret your direction and ask Paige to refine if needed.",
        'The recommendation is already in context; you do not need to restate it.',
      ].join(' '),
      reasoning: ['Discuss-with-Max mode — recommendation loaded in conversational context.'],
      recommendedActions: [],
      metadata: {
        specialistDirection: true,
        discussMode: true,
        recommendationId,
        executionDomain: 'workspace',
        surface: 'specialist_direction_discuss',
      },
    };
    return {
      reason: 'specialist_direction_discuss',
      structured,
      prose: structured.answer,
      recommendationId,
      discussMode: true,
    };
  }

  if (recommendationId && service.looksLikeRefinementFeedback(question)) {
    let result;
    try {
      result = await service.applyOperatorDirection(
      {
        tenantId,
        clientId: context.clientId || tenantId,
        recommendationId,
        operatorMessage: question,
      },
      directionOpts
      );
    } catch (err) {
      if (err && err.code === 'recommendation_not_found') return null;
      throw err;
    }
    if (!result.handled) return null;

    let structured = service.composeDirectionStructuredResponse(result);

    if (result.disposition === 'refine' && result.refinedRecommendation && !result.failed) {
      const ack = service.formatDirectionAcknowledgment(
        {
          acceptedElements: result.direction.acceptedElements,
          changedElements: result.direction.changedElements,
          updatedDirection: result.direction.updatedDirection,
          rationale: result.direction.rationale,
          scope: result.direction.scope,
        },
        result.originalRecommendation
      );
      structured.answer = `${ack}\n\n${structured.answer}`;
    }

    return {
      reason: result.needsClarification
        ? 'specialist_direction_clarification'
        : result.failed
          ? 'specialist_direction_failed'
          : 'specialist_direction_refine',
      structured,
      prose: structured.answer,
      direction: result.direction,
      refinedRecommendation: result.refinedRecommendation,
      recommendationId: result.refinedRecommendation?.id || recommendationId,
    };
  }

  if (recommendationId && service.looksLikeAcceptAction(question)) {
    let result;
    try {
      result = await service.applyOperatorDirection(
      {
        tenantId,
        clientId: context.clientId || tenantId,
        recommendationId,
        operatorMessage: question,
      },
      directionOpts
      );
    } catch (err) {
      if (err && err.code === 'recommendation_not_found') return null;
      throw err;
    }
    if (!result.handled) return null;
    const structured = service.composeDirectionStructuredResponse(result);
    return {
      reason: 'specialist_direction_accept',
      structured,
      prose: structured.answer,
      direction: result.direction,
      recommendationId,
    };
  }

  return null;
}

module.exports = {
  shouldHandleSpecialistDirection,
  maybeHandleSpecialistDirectionTurn,
};
