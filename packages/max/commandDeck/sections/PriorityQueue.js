'use strict';

const {
  buildIntelligenceCard,
  recommendationActions,
  CARD_TYPES,
} = require('../cards/IntelligenceCard');
const { DEFAULT_PRIORITY_QUEUE_LIMIT } = require('../CommandDeckTypes');

/**
 * Priority Queue — already sorted by Briefing Prioritizer.
 * Composer enriches with movement + policy ids; does not re-rank.
 *
 * @param {object} input
 * @param {object} input.briefing
 * @param {Map<string, object>|Record<string, object>} [input.policyByRecId]
 * @param {string} input.briefingId
 * @param {string} input.generatedAt
 * @param {number} [input.limit]
 */
function composePriorityQueue(input) {
  const priorities = (input.briefing && input.briefing.priorities) || [];
  const limit =
    input.limit != null ? Number(input.limit) : DEFAULT_PRIORITY_QUEUE_LIMIT;
  const policyMap = toMap(input.policyByRecId);

  const items = priorities.slice(0, limit).map((p, index) => {
    const policy = policyMap.get(String(p.id)) || null;
    const movement = formatMovement(p.scoreDelta);
    return {
      rank: index + 1,
      companyId: p.companyId,
      company: p.companyName || p.companyId,
      opportunity: p.score != null ? Number(p.score) : null,
      confidence: p.confidence != null ? Number(p.confidence) : null,
      trend: p.trend || 'insufficient',
      movement,
      scoreDelta: p.scoreDelta != null ? Number(p.scoreDelta) : 0,
      summary: buildItemSummary(p, movement),
      recommendationId: p.id || null,
      recommendedAction: p.recommendedAction || null,
      type: p.type || null,
      priority: p.priority || null,
      policyId: (policy && policy.audit && policy.audit.id) || null,
      policyOutcome: policy ? policy.outcome : null,
    };
  });

  const cards = items.map((item, index) => {
    const cardId = `card:priority:${item.recommendationId || item.companyId}:${item.rank}`;
    return buildIntelligenceCard({
      id: cardId,
      type: CARD_TYPES.PRIORITY_ITEM,
      priority: 500 - index,
      title: `${String(item.rank).padStart(2, '0')} ${item.company}`,
      summary: item.summary,
      confidence: item.confidence,
      updatedAt: input.generatedAt,
      actions: recommendationActions({
        recommendationId: item.recommendationId,
        companyId: item.companyId,
        cardId,
        askContext: 'priority_queue',
      }),
      sources: [
        { kind: 'briefing', id: input.briefingId, field: 'priorities' },
        item.recommendationId
          ? { kind: 'recommendation', id: item.recommendationId }
          : null,
        item.policyId ? { kind: 'policy', id: item.policyId } : null,
      ].filter(Boolean),
      reasoningId: item.recommendationId,
      policyId: item.policyId,
      briefingId: input.briefingId,
      payload: item,
    });
  });

  return { priorityQueue: items, cards };
}

function formatMovement(scoreDelta) {
  const delta = Number(scoreDelta) || 0;
  if (delta > 0) return `↑${delta}`;
  if (delta < 0) return `↓${Math.abs(delta)}`;
  return '—';
}

function buildItemSummary(p, movement) {
  const parts = [];
  if (movement && movement !== '—') parts.push(movement);
  if (p.score != null) parts.push(`Opportunity ${p.score}`);
  if (p.confidence != null) parts.push(`Confidence ${p.confidence}`);
  if (p.trend && p.trend !== 'insufficient') parts.push(`Trend ${p.trend}`);
  return parts.join(' · ') || 'Ranked priority';
}

function toMap(value) {
  if (!value) return new Map();
  if (value instanceof Map) return value;
  const map = new Map();
  for (const key of Object.keys(value).sort()) {
    map.set(String(key), value[key]);
  }
  return map;
}

module.exports = {
  composePriorityQueue,
  formatMovement,
};
