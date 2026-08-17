'use strict';

const { PAGE_TYPES } = require('./WorkspaceTypes');
const { contextFocusLabel } = require('./ContextEnvelope');

/**
 * Deterministic opening message from MaxContext.
 * Never invents counts — only surfaces fields present on the envelope.
 *
 * @param {object} context - normalized MaxContext
 * @param {{ hour?: number }} [options]
 * @returns {{ greeting: string, body: string[], prompt: string, fullText: string }}
 */
function buildOpeningState(context, options = {}) {
  const hour =
    Number.isFinite(options.hour) ? options.hour : new Date().getHours();
  const greeting = hour < 12 ? 'Good morning.' : hour < 17 ? 'Good afternoon.' : 'Good evening.';

  // SPEC-104 — operator context brief takes precedence on command-deck open
  if (
    context.page === PAGE_TYPES.COMMAND_DECK &&
    context.sessionBrief &&
    context.sessionBrief.reviewedBeforeArrival &&
    context.sessionBrief.fullText
  ) {
    const brief = context.sessionBrief;
    return {
      greeting: brief.greeting || greeting,
      body: Array.isArray(brief.body) ? brief.body : [String(brief.body || '')],
      prompt: brief.prompt || 'What would you like to investigate?',
      fullText: brief.fullText,
      reviewedBeforeArrival: true,
      recommendations: brief.recommendations || [],
    };
  }

  if (context.page === PAGE_TYPES.COMPANY) {
    return companyOpening(context, greeting);
  }
  if (context.page === PAGE_TYPES.RECOMMENDATION) {
    return recommendationOpening(context, greeting);
  }
  if (context.page === PAGE_TYPES.TIMELINE) {
    return timelineOpening(context, greeting);
  }
  if (context.page === PAGE_TYPES.MARKET) {
    return marketOpening(context, greeting);
  }
  return commandDeckOpening(context, greeting);
}

function commandDeckOpening(context, greeting) {
  const brief = context.briefing || {};
  const body = ["You're reviewing today's briefing."];

  const marketChanges = Number(brief.marketChanges);
  if (Number.isFinite(marketChanges) && marketChanges > 0) {
    body.push(
      marketChanges === 1
        ? 'One opportunity improved overnight.'
        : `${marketChanges} opportunities improved overnight.`
    );
  }

  const watchAlertCount = Number(brief.watchAlertCount);
  if (Number.isFinite(watchAlertCount) && watchAlertCount > 0) {
    body.push(
      watchAlertCount === 1
        ? 'One watch alert requires attention.'
        : `${watchAlertCount} watch alerts require attention.`
    );
  }

  if (body.length === 1) {
    const headline = brief.headline || brief.summary;
    if (headline) body.push(String(headline));
    else body.push('No urgent shifts are flagged in the current envelope.');
  }

  const prompt = 'What would you like to investigate?';
  return finalize(greeting, body, prompt);
}

function companyOpening(context, greeting) {
  const name = contextFocusLabel(context);
  const body = [`You're viewing ${name}.`];

  const entity = context.selectedEntity || {};
  const card = findCardForEntity(context);
  const payload = (card && card.payload) || {};

  const opportunity =
    payload.opportunity != null
      ? payload.opportunity
      : card && card.confidence != null
        ? null
        : null;
  // Surface only explicit payload fields — never invent deltas
  if (payload.opportunityDelta != null && Number.isFinite(Number(payload.opportunityDelta))) {
    const delta = Number(payload.opportunityDelta);
    body.push(
      delta >= 0
        ? `Opportunity increased ${delta} points this week.`
        : `Opportunity decreased ${Math.abs(delta)} points this week.`
    );
  } else if (opportunity != null && Number.isFinite(Number(opportunity))) {
    body.push(`Opportunity score in view: ${Number(opportunity)}.`);
  }

  const confidence =
    payload.confidence != null
      ? Number(payload.confidence)
      : card && card.confidence != null
        ? Number(card.confidence)
        : null;
  if (confidence != null && Number.isFinite(confidence)) {
    body.push(
      confidence >= 80
        ? 'Confidence remains high.'
        : `Confidence in view: ${confidence}.`
    );
  } else {
    body.push('Confidence is not available in the current context.');
  }

  if (entity.name && body.length === 1) {
    body.push('Ask about signals, history, or comparisons.');
  }

  return finalize(greeting, body, 'What would you like to understand?');
}

function recommendationOpening(context, greeting) {
  const body = ["You're reviewing today's highest leverage recommendation."];
  const label = contextFocusLabel(context);
  if (label && label !== "today's briefing") {
    body.push(`Focus: ${label}.`);
  }
  body.push(
    'I can explain the reasoning, compare alternatives, or inspect supporting evidence.'
  );
  return finalize(
    greeting,
    body,
    'What would you like to investigate?'
  );
}

function timelineOpening(context, greeting) {
  const label = contextFocusLabel(context);
  return finalize(
    greeting,
    [`You're reviewing the timeline for ${label}.`],
    'What period or signal should we inspect?'
  );
}

function marketOpening(context, greeting) {
  const brief = context.briefing || {};
  const body = ["You're reviewing the market view."];
  if (brief.summary) body.push(String(brief.summary));
  return finalize(greeting, body, 'What market shift should we investigate?');
}

function findCardForEntity(context) {
  const id = context.companyId || (context.selectedEntity && context.selectedEntity.id);
  if (!id) return null;
  return (context.visibleCards || []).find((c) => {
    const payload = c.payload || {};
    return (
      String(c.id) === String(id) ||
      String(payload.companyId) === String(id) ||
      (payload.recommendation &&
        String(payload.recommendation.companyId) === String(id))
    );
  });
}

function finalize(greeting, body, prompt) {
  const lines = [greeting, '', ...body, '', prompt];
  return {
    greeting,
    body,
    prompt,
    fullText: lines.join('\n'),
  };
}

module.exports = {
  buildOpeningState,
};
