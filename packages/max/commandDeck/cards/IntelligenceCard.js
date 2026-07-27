'use strict';

const { ACTION_TYPES, CARD_TYPES } = require('../CommandDeckTypes');

/**
 * Build a stable IntelligenceCard.
 * Every visible Command Deck surface implements this contract.
 *
 * @param {object} input
 * @returns {object}
 */
function buildIntelligenceCard(input) {
  if (!input || !input.id) {
    throw new Error('IntelligenceCard requires id');
  }
  if (!input.type) {
    throw new Error('IntelligenceCard requires type');
  }

  return {
    id: String(input.id),
    type: String(input.type),
    priority: Number.isFinite(Number(input.priority)) ? Number(input.priority) : 0,
    title: input.title != null ? String(input.title) : '',
    summary: input.summary != null ? String(input.summary) : '',
    confidence:
      input.confidence == null || !Number.isFinite(Number(input.confidence))
        ? null
        : Number(input.confidence),
    updatedAt: input.updatedAt || null,
    actions: Array.isArray(input.actions)
      ? input.actions.map(normalizeAction)
      : [],
    sources: Array.isArray(input.sources) ? input.sources.slice() : [],
    reasoningId: input.reasoningId != null ? String(input.reasoningId) : null,
    policyId: input.policyId != null ? String(input.policyId) : null,
    briefingId: input.briefingId != null ? String(input.briefingId) : null,
    payload: input.payload && typeof input.payload === 'object' ? input.payload : null,
  };
}

/**
 * @param {object} action
 */
function normalizeAction(action) {
  return {
    id: String(action.id || action.type || 'action'),
    type: String(action.type || ACTION_TYPES.ASK_MAX),
    label: String(action.label || action.type || 'Action'),
    payload:
      action.payload && typeof action.payload === 'object' ? action.payload : null,
  };
}

/**
 * Standard action set for recommendation-backed cards.
 * @param {object} input
 */
function recommendationActions(input = {}) {
  const actions = [];
  if (input.recommendationId) {
    actions.push({
      id: 'review',
      type: ACTION_TYPES.REVIEW_RECOMMENDATION,
      label: 'Review Recommendation',
      payload: { recommendationId: input.recommendationId },
    });
  }
  actions.push({
    id: 'ask_max',
    type: ACTION_TYPES.ASK_MAX,
    label: 'Ask Max',
    payload: {
      recommendationId: input.recommendationId || null,
      companyId: input.companyId || null,
      context: input.askContext || 'command_deck',
    },
  });
  if (input.companyId) {
    actions.push({
      id: 'open_company',
      type: ACTION_TYPES.OPEN_COMPANY,
      label: 'Open Company',
      payload: { companyId: input.companyId },
    });
  }
  actions.push({
    id: 'dismiss',
    type: ACTION_TYPES.DISMISS,
    label: 'Dismiss',
    payload: {
      recommendationId: input.recommendationId || null,
      cardId: input.cardId || null,
    },
  });
  actions.push({
    id: 'snooze',
    type: ACTION_TYPES.SNOOZE,
    label: 'Snooze',
    payload: {
      recommendationId: input.recommendationId || null,
      cardId: input.cardId || null,
    },
  });
  return actions;
}

/**
 * Stable briefing id for explainability (no recomputation on "Why?").
 * @param {{ tenantId: string, asOf: string, period: string }} meta
 */
function buildBriefingId(meta) {
  return `briefing:${meta.tenantId}:${meta.period}:${meta.asOf}`;
}

module.exports = {
  buildIntelligenceCard,
  normalizeAction,
  recommendationActions,
  buildBriefingId,
  CARD_TYPES,
  ACTION_TYPES,
};
