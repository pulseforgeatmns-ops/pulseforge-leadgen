'use strict';

const {
  ACTION_TYPES,
  CARD_TYPES,
  DEFAULT_MARKET_TREND_LIMIT,
  TREND_DIRECTIONS,
} = require('../CommandDeckTypes');
const { buildIntelligenceCard } = require('../cards/IntelligenceCard');

/**
 * Market Trends — summarize briefing metrics/changes into trend cards.
 * Never invents percentages; reports assembled counts and direction only.
 *
 * @param {object} input
 * @param {object} input.briefing
 * @param {string} input.briefingId
 * @param {string} input.generatedAt
 * @param {number} [input.limit]
 */
function composeMarketTrends(input) {
  const summary = (input.briefing && input.briefing.summary) || {};
  const changes = (input.briefing && input.briefing.changes) || {};
  const byType = changes.byType || {};
  const opportunity = summary.opportunityTrend || {};
  const confidence = summary.confidenceTrend || {};
  const limit =
    input.limit != null ? Number(input.limit) : DEFAULT_MARKET_TREND_LIMIT;

  /** @type {object[]} */
  const trends = [];

  pushTrend(trends, {
    id: 'overflow_demand',
    title: 'Overflow demand',
    count:
      Number(byType.new_opportunity_signal) ||
      Number(opportunity.newOpportunitySignals) ||
      0,
    upCount: Number(opportunity.scoreIncreased) || 0,
    downCount: Number(opportunity.scoreDecreased) || 0,
    evidenceField: 'opportunityTrend',
  });

  pushTrend(trends, {
    id: 'hiring_activity',
    title: 'Hiring activity',
    count:
      Number(byType.new_hiring_signal) || Number(summary.newHiringSignals) || 0,
    upCount: Number(summary.newHiringSignals) || 0,
    downCount: 0,
    evidenceField: 'newHiringSignals',
  });

  const confUp = Number(confidence.confidenceIncreased) || 0;
  const confDown = Number(confidence.confidenceDecreased) || 0;
  pushTrend(trends, {
    id: 'response_confidence',
    title: 'Confidence movement',
    count: confUp + confDown,
    upCount: confUp,
    downCount: confDown,
    evidenceField: 'confidenceTrend',
  });

  const trendingUp = Number(opportunity.companiesTrendingUp) || 0;
  const trendingDown = Number(opportunity.companiesTrendingDown) || 0;
  pushTrend(trends, {
    id: 'market_momentum',
    title: 'Market momentum',
    count: trendingUp + trendingDown,
    upCount: trendingUp,
    downCount: trendingDown,
    evidenceField: 'opportunityTrend',
  });

  const dmCount =
    Number(byType.new_decision_maker) || Number(summary.newDecisionMakers) || 0;
  pushTrend(trends, {
    id: 'decision_maker_activity',
    title: 'Decision-maker activity',
    count: dmCount,
    upCount: dmCount,
    downCount: 0,
    evidenceField: 'newDecisionMakers',
  });

  const active = trends
    .filter((t) => t.count > 0)
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return String(a.id).localeCompare(String(b.id));
    })
    .slice(0, limit);

  const cards = active.map((trend, index) => {
    const cardId = `card:trend:${trend.id}`;
    return buildIntelligenceCard({
      id: cardId,
      type: CARD_TYPES.MARKET_TREND,
      priority: 600 - index,
      title: trend.title,
      summary: trend.summary,
      confidence: trend.confidence,
      updatedAt: input.generatedAt,
      actions: [
        {
          id: 'ask_max',
          type: ACTION_TYPES.ASK_MAX,
          label: 'Ask Max',
          payload: { context: 'market_trend', trendId: trend.id },
        },
      ],
      sources: [
        { kind: 'briefing', id: input.briefingId, field: trend.evidenceField },
        { kind: 'briefing', id: input.briefingId, field: 'changes' },
      ],
      reasoningId: null,
      policyId: null,
      briefingId: input.briefingId,
      payload: {
        trendId: trend.id,
        direction: trend.direction,
        count: trend.count,
        upCount: trend.upCount,
        downCount: trend.downCount,
        supportingEvidence: trend.supportingEvidence,
      },
    });
  });

  return { marketTrends: cards, items: active };
}

function pushTrend(trends, spec) {
  const direction = deriveDirection(spec.upCount, spec.downCount, spec.count);
  const summary = buildTrendSummary(spec, direction);
  trends.push({
    id: spec.id,
    title: spec.title,
    count: spec.count,
    upCount: spec.upCount,
    downCount: spec.downCount,
    direction,
    confidence: confidenceFromCount(spec.count),
    summary,
    evidenceField: spec.evidenceField,
    supportingEvidence: [
      {
        kind: 'count',
        id: `${spec.id}:count`,
        summary: `count=${spec.count}`,
      },
      {
        kind: 'count',
        id: `${spec.id}:up`,
        summary: `up=${spec.upCount}`,
      },
      {
        kind: 'count',
        id: `${spec.id}:down`,
        summary: `down=${spec.downCount}`,
      },
    ],
  });
}

function deriveDirection(up, down, count) {
  if (!count) return TREND_DIRECTIONS.INSUFFICIENT;
  if (up > down) return TREND_DIRECTIONS.UP;
  if (down > up) return TREND_DIRECTIONS.DOWN;
  if (up === 0 && down === 0 && count > 0) return TREND_DIRECTIONS.UP;
  return TREND_DIRECTIONS.FLAT;
}

function buildTrendSummary(spec, direction) {
  const arrow =
    direction === TREND_DIRECTIONS.UP
      ? '↑'
      : direction === TREND_DIRECTIONS.DOWN
        ? '↓'
        : direction === TREND_DIRECTIONS.FLAT
          ? '→'
          : '·';
  return `${spec.title} ${arrow}${spec.count} in period`;
}

function confidenceFromCount(count) {
  if (count <= 0) return null;
  if (count >= 10) return 90;
  if (count >= 5) return 75;
  if (count >= 2) return 60;
  return 50;
}

module.exports = {
  composeMarketTrends,
  deriveDirection,
};
