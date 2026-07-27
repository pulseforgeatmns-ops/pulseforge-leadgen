'use strict';

const { PAGE_TYPES } = require('./WorkspaceTypes');
const { contextFocusLabel } = require('./ContextEnvelope');

/**
 * Contextual suggested investigations from MaxContext.
 * Templates keyed by page; filled from envelope — not a global hardcoded list.
 *
 * @param {object} context - normalized MaxContext
 * @returns {string[]}
 */
function buildSuggestions(context) {
  const topName = topCompanyName(context);
  const focus = contextFocusLabel(context);

  if (context.page === PAGE_TYPES.COMPANY) {
    return [
      'Why did confidence increase?',
      'Show relationship history.',
      'Explain supporting signals.',
      `Compare with similar companies.`,
    ].map((s) => (focus && focus !== "today's briefing" ? s : s));
  }

  if (context.page === PAGE_TYPES.RECOMMENDATION) {
    return [
      'Explain this recommendation.',
      'Show contradicting evidence.',
      'Walk through policy evaluation.',
      'What happens if I wait?',
    ];
  }

  if (context.page === PAGE_TYPES.TIMELINE) {
    return [
      'What changed most recently?',
      'Show the strongest supporting signals.',
      'Summarize movement this period.',
    ];
  }

  if (context.page === PAGE_TYPES.MARKET) {
    return [
      'What shifted overnight?',
      'Show watch alerts.',
      'Compare top opportunities.',
    ];
  }

  // command-deck
  const suggestions = [];
  if (topName) {
    suggestions.push(`Why is ${topName} #1?`);
  } else {
    suggestions.push('Why is the top opportunity ranked first?');
  }
  suggestions.push('What changed overnight?');
  suggestions.push("Compare today's top opportunities.");
  suggestions.push('Show risks.');

  const watchCount = Number(context.briefing && context.briefing.watchAlertCount);
  if (Number.isFinite(watchCount) && watchCount > 0) {
    suggestions.push('Explain the watch alerts.');
  }

  return suggestions.slice(0, 5);
}

function topCompanyName(context) {
  if (context.selectedEntity && context.selectedEntity.name) {
    return context.selectedEntity.name;
  }
  const cards = context.visibleCards || [];
  for (const card of cards) {
    const payload = card.payload || {};
    const name =
      (payload.recommendation && payload.recommendation.companyName) ||
      payload.companyName ||
      (card.type === 'highest_leverage' || card.type === 'priority_item'
        ? extractNameFromTitle(card.title)
        : null);
    if (name) return name;
  }
  if (context.deck && context.deck.highestLeverageAction) {
    const hla = context.deck.highestLeverageAction;
    if (hla.recommendation && hla.recommendation.companyName) {
      return hla.recommendation.companyName;
    }
  }
  if (context.deck && context.deck.priorityQueue && context.deck.priorityQueue.items) {
    const first = context.deck.priorityQueue.items[0];
    if (first && first.companyName) return first.companyName;
    if (first && first.title) return extractNameFromTitle(first.title);
  }
  return null;
}

function extractNameFromTitle(title) {
  if (!title) return null;
  const cleaned = String(title)
    .replace(/^(Review|Pursue|Contact|Open|Call|Email)\s+/i, '')
    .trim();
  return cleaned || null;
}

module.exports = {
  buildSuggestions,
  topCompanyName,
};
