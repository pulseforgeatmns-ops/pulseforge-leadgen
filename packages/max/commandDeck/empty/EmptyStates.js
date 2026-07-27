'use strict';

const { ACTION_TYPES, CARD_TYPES } = require('../CommandDeckTypes');
const { buildIntelligenceCard } = require('../cards/IntelligenceCard');

/**
 * Composer-owned empty states. UI never invents these.
 */
const EMPTY_CATALOG = Object.freeze({
  priorities: Object.freeze({
    key: 'priorities',
    title: 'No high-priority recommendations.',
    summary:
      'Suggested focus:\nContinue market discovery.\nReview newly enriched companies.',
  }),
  watchAlerts: Object.freeze({
    key: 'watchAlerts',
    title: 'No watch alerts.',
    summary:
      'Suggested focus:\nReview priority queue.\nInspect overnight market changes.',
  }),
  marketTrends: Object.freeze({
    key: 'marketTrends',
    title: 'No market trends to surface.',
    summary:
      'Suggested focus:\nContinue market discovery.\nWait for the next memory window.',
  }),
  highestLeverage: Object.freeze({
    key: 'highestLeverage',
    title: 'No highest-leverage action today.',
    summary:
      'Suggested focus:\nContinue market discovery.\nReview newly enriched companies.',
  }),
});

/**
 * @param {string} key
 * @param {{ briefingId: string, updatedAt: string }} meta
 * @returns {object} IntelligenceCard
 */
function buildEmptyStateCard(key, meta) {
  const entry = EMPTY_CATALOG[key];
  if (!entry) {
    throw new Error(`Unknown empty state key: ${key}`);
  }
  return buildIntelligenceCard({
    id: `empty:${key}`,
    type: CARD_TYPES.EMPTY,
    priority: 0,
    title: entry.title,
    summary: entry.summary,
    confidence: null,
    updatedAt: meta.updatedAt,
    actions: [
      {
        id: 'ask_max',
        type: ACTION_TYPES.ASK_MAX,
        label: 'Ask Max',
        payload: { context: `empty:${key}` },
      },
    ],
    sources: [],
    reasoningId: null,
    policyId: null,
    briefingId: meta.briefingId,
    payload: { emptyKey: key },
  });
}

/**
 * Build emptyStates map for sections that have no items.
 * @param {object} flags
 * @param {{ briefingId: string, updatedAt: string }} meta
 */
function buildEmptyStates(flags, meta) {
  /** @type {Record<string, object>} */
  const out = {};
  for (const key of Object.keys(EMPTY_CATALOG).sort()) {
    if (flags[key]) {
      out[key] = buildEmptyStateCard(key, meta);
    }
  }
  return out;
}

module.exports = {
  EMPTY_CATALOG,
  buildEmptyStateCard,
  buildEmptyStates,
};
