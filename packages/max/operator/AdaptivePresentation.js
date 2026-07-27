'use strict';

const {
  INTERACTION_TYPES,
  SECTION_IDS,
  DEFAULT_SECTION_ORDER,
  DOMINANCE,
} = require('./OperatorTypes');

/**
 * Adaptive presentation — personalize section order / visual dominance.
 * Never hides sections. Never alters intelligence facts.
 */

/**
 * Build section engagement counts from interaction events.
 * @param {object[]} events
 */
function summarizeSectionEngagement(events) {
  const counts = {
    [SECTION_IDS.MORNING_BRIEF]: 0,
    [SECTION_IDS.HIGHEST_LEVERAGE]: 0,
    [SECTION_IDS.WATCH_ALERTS]: 0,
    [SECTION_IDS.MARKET_TRENDS]: 0,
    [SECTION_IDS.PRIORITY_QUEUE]: 0,
  };

  for (const ev of events || []) {
    const section = resolveSection(ev);
    if (!section || counts[section] == null) continue;
    counts[section] += weightForEvent(ev);
  }
  return counts;
}

/**
 * @param {object} input
 * @param {object[]} input.events - InteractionEvents for tenant
 * @param {string[]} [input.baseOrder]
 * @returns {{ sectionOrder: string[], sectionDominance: object, engagement: object }}
 */
function buildAdaptivePresentation(input = {}) {
  const baseOrder = (input.baseOrder || DEFAULT_SECTION_ORDER).slice();
  const engagement = summarizeSectionEngagement(input.events || []);

  // Morning brief stays first (landing). Adapt the rest by engagement.
  const fixed = SECTION_IDS.MORNING_BRIEF;
  const flexible = baseOrder.filter((s) => s !== fixed);
  flexible.sort((a, b) => {
    const diff = (engagement[b] || 0) - (engagement[a] || 0);
    if (diff !== 0) return diff;
    return baseOrder.indexOf(a) - baseOrder.indexOf(b);
  });

  const sectionOrder = [fixed, ...flexible];

  const values = flexible.map((s) => engagement[s] || 0);
  const max = Math.max(0, ...values);
  const sectionDominance = {};
  sectionDominance[SECTION_IDS.MORNING_BRIEF] = DOMINANCE.NORMAL;

  for (const s of flexible) {
    const n = engagement[s] || 0;
    if (max === 0) {
      sectionDominance[s] = DOMINANCE.NORMAL;
    } else if (n >= max && n > 0) {
      sectionDominance[s] = DOMINANCE.HIGH;
    } else if (n === 0 || n < max * 0.35) {
      sectionDominance[s] = DOMINANCE.QUIET;
    } else {
      sectionDominance[s] = DOMINANCE.NORMAL;
    }
  }

  return {
    sectionOrder,
    sectionDominance,
    engagement,
  };
}

/**
 * Attach presentation envelope to a CommandDeckModel (shallow decorate).
 * Does not mutate frozen models in place — returns a new object.
 * @param {object} model
 * @param {object} presentation
 */
function decorateDeck(model, presentation) {
  if (!model || typeof model !== 'object') return model;
  const meta = model.meta ? { ...model.meta } : {};
  return {
    ...model,
    presentation: {
      sectionOrder: (presentation.sectionOrder || DEFAULT_SECTION_ORDER).slice(),
      sectionDominance: {
        ...(presentation.sectionDominance || {}),
      },
      preferences: presentation.preferences || { topIntents: [] },
      engagement: presentation.engagement || {},
    },
    meta: {
      ...meta,
      operatorPresentation: true,
    },
  };
}

function resolveSection(ev) {
  if (ev.section && Object.values(SECTION_IDS).includes(ev.section)) {
    return ev.section;
  }
  if (ev.type === INTERACTION_TYPES.ASKED_MAX) {
    const ctx = ev.payload && ev.payload.context;
    if (ctx === 'morning_brief') return SECTION_IDS.MORNING_BRIEF;
    if (ctx === 'watch_alert' || ctx === 'watch_alerts') {
      return SECTION_IDS.WATCH_ALERTS;
    }
    if (ctx === 'market_trend' || ctx === 'market_trends') {
      return SECTION_IDS.MARKET_TRENDS;
    }
    if (ctx === 'highest_leverage') return SECTION_IDS.HIGHEST_LEVERAGE;
    if (ctx === 'priority_queue' || ctx === 'priority_item') {
      return SECTION_IDS.PRIORITY_QUEUE;
    }
  }
  if (
    ev.type === INTERACTION_TYPES.VIEWED_RECOMMENDATION ||
    ev.type === INTERACTION_TYPES.APPROVED_RECOMMENDATION ||
    ev.type === INTERACTION_TYPES.DISMISSED_CARD ||
    ev.type === INTERACTION_TYPES.IGNORED_RECOMMENDATION
  ) {
    return SECTION_IDS.PRIORITY_QUEUE;
  }
  return null;
}

function weightForEvent(ev) {
  switch (ev.type) {
    case INTERACTION_TYPES.OPENED_SECTION:
      return 2;
    case INTERACTION_TYPES.ASKED_MAX:
      return 3;
    case INTERACTION_TYPES.APPROVED_RECOMMENDATION:
      return 4;
    case INTERACTION_TYPES.VIEWED_RECOMMENDATION:
      return 1;
    case INTERACTION_TYPES.DISMISSED_CARD:
      return 0.5;
    default:
      return 1;
  }
}

module.exports = {
  summarizeSectionEngagement,
  buildAdaptivePresentation,
  decorateDeck,
};
