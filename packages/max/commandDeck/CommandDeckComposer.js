'use strict';

const { deepFreeze } = require('../reasoning/ReasoningTypes');
const { BRIEFING_PERIODS } = require('../briefing/BriefingTypes');
const {
  COMMAND_DECK_PERFORMANCE_TARGET_MS,
} = require('./CommandDeckTypes');
const { buildBriefingId } = require('./cards/IntelligenceCard');
const { buildEmptyStates } = require('./empty/EmptyStates');
const { buildMorningBrief } = require('./sections/MorningBrief');
const {
  buildHighestLeverageAction,
} = require('./sections/HighestLeverageAction');
const { composeWatchAlerts } = require('./sections/WatchAlerts');
const { composeMarketTrends } = require('./sections/MarketTrends');
const { composePriorityQueue } = require('./sections/PriorityQueue');

/**
 * Command Deck Composer — presenter for the intelligence stack.
 *
 * May: sort, merge, rank, summarize, group.
 * May not: reason, score, infer, invent.
 *
 *   await max.compose({ tenantId, asOf, period })
 */
class CommandDeckComposer {
  /**
   * @param {object} deps
   * @param {import('../briefing/BriefingEngine').BriefingEngine} deps.briefing
   * @param {import('../policy/engine/PolicyEngine').PolicyEngine} deps.policy
   */
  constructor(deps) {
    if (!deps || !deps.briefing) {
      throw new Error('CommandDeckComposer requires briefing');
    }
    if (!deps.policy) {
      throw new Error('CommandDeckComposer requires policy');
    }
    this._briefing = deps.briefing;
    this._policy = deps.policy;
  }

  /** @returns {import('../briefing/BriefingEngine').BriefingEngine} */
  get briefing() {
    return this._briefing;
  }

  /** @returns {import('../policy/engine/PolicyEngine').PolicyEngine} */
  get policy() {
    return this._policy;
  }

  /**
   * Build today's immutable CommandDeckModel.
   *
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} [input.asOf]
   * @param {'daily'|'weekly'|'monthly'} [input.period='daily']
   * @param {string} [input.periodStart]
   * @param {string} [input.periodEnd]
   * @param {object} [input.briefing] - pre-built briefing (skips brief())
   * @param {Record<string, object>|Map} [input.policyDecisions] - precomputed decisions
   * @param {boolean} [input.evaluatePolicy=true] - call policy for priority items
   * @param {number} [input.priorityLimit]
   * @param {number} [input.watchAlertLimit]
   * @param {number} [input.marketTrendLimit]
   * @param {string} [input.operator]
   * @returns {Promise<object>} CommandDeckModel
   */
  async compose(input) {
    if (!input || !input.tenantId) {
      throw new Error('compose requires tenantId');
    }

    const started = process.hrtime.bigint();
    const tenantId = String(input.tenantId);
    const period = input.period || BRIEFING_PERIODS.DAILY;
    const asOf = input.asOf || new Date().toISOString();

    const briefing =
      input.briefing ||
      (await this._briefing.brief({
        tenantId,
        asOf,
        period,
        periodStart: input.periodStart,
        periodEnd: input.periodEnd,
        priorityLimit: input.priorityLimit,
      }));

    const briefingId = buildBriefingId({
      tenantId,
      asOf: (briefing.meta && briefing.meta.asOf) || asOf,
      period: (briefing.meta && briefing.meta.period) || period,
    });
    const generatedAt =
      (briefing.meta && briefing.meta.builtAt) ||
      (briefing.meta && briefing.meta.asOf) ||
      asOf;

    const priorities = briefing.priorities || [];
    const policyByRecId = await this._resolvePolicies({
      tenantId,
      asOf: generatedAt,
      operator: input.operator,
      priorities,
      precomputed: input.policyDecisions,
      evaluatePolicy: input.evaluatePolicy !== false,
      recommendations:
        (briefing.recommendations && briefing.recommendations.items) || [],
    });

    const { morningBrief, card: morningCard } = buildMorningBrief({
      briefing,
      briefingId,
      generatedAt,
    });

    const top = priorities[0] || null;
    const topRec = findRecommendation(
      top,
      (briefing.recommendations && briefing.recommendations.items) || []
    );
    const topPolicy = top && top.id ? policyByRecId.get(String(top.id)) : null;

    const { highestLeverageAction, card: hlaCard } =
      buildHighestLeverageAction({
        topPriority: top,
        recommendation: topRec,
        policyDecision: topPolicy,
        briefingId,
        generatedAt,
      });

    const { watchAlerts } = composeWatchAlerts({
      briefing,
      briefingId,
      generatedAt,
      limit: input.watchAlertLimit,
    });

    const { marketTrends } = composeMarketTrends({
      briefing,
      briefingId,
      generatedAt,
      limit: input.marketTrendLimit,
    });

    const { priorityQueue, cards: priorityCards } = composePriorityQueue({
      briefing,
      policyByRecId,
      briefingId,
      generatedAt,
      limit: input.priorityLimit,
    });

    const emptyStates = buildEmptyStates(
      {
        priorities: priorityQueue.length === 0,
        watchAlerts: watchAlerts.length === 0,
        marketTrends: marketTrends.length === 0,
        highestLeverage: !highestLeverageAction,
      },
      { briefingId, updatedAt: generatedAt }
    );

    /** @type {object[]} */
    const cards = [];
    if (morningCard) cards.push(morningCard);
    if (hlaCard) {
      cards.push(hlaCard);
    } else if (emptyStates.highestLeverage) {
      cards.push(emptyStates.highestLeverage);
    }
    for (const c of watchAlerts) cards.push(c);
    if (watchAlerts.length === 0 && emptyStates.watchAlerts) {
      cards.push(emptyStates.watchAlerts);
    }
    for (const c of marketTrends) cards.push(c);
    if (marketTrends.length === 0 && emptyStates.marketTrends) {
      cards.push(emptyStates.marketTrends);
    }
    for (const c of priorityCards) cards.push(c);
    if (priorityCards.length === 0 && emptyStates.priorities) {
      cards.push(emptyStates.priorities);
    }

    const buildTimeMs = Number(process.hrtime.bigint() - started) / 1e6;

    const model = {
      morningBrief,
      highestLeverageAction,
      watchAlerts,
      marketTrends,
      priorityQueue,
      cards,
      emptyStates,
      meta: {
        tenantId,
        briefingId,
        generatedAt,
        asOf: (briefing.meta && briefing.meta.asOf) || asOf,
        period: (briefing.meta && briefing.meta.period) || period,
        windowStart: (briefing.meta && briefing.meta.windowStart) || null,
        windowEnd: (briefing.meta && briefing.meta.windowEnd) || null,
        buildTimeMs,
        withinTarget: buildTimeMs <= COMMAND_DECK_PERFORMANCE_TARGET_MS,
        performanceTargetMs: COMMAND_DECK_PERFORMANCE_TARGET_MS,
        cardCount: cards.length,
        policyDecisionCount: policyByRecId.size,
      },
    };

    return deepFreeze(model);
  }

  /**
   * @param {object} input
   * @returns {Promise<Map<string, object>>}
   */
  async _resolvePolicies(input) {
    const map = new Map();
    if (input.precomputed) {
      if (input.precomputed instanceof Map) {
        for (const [k, v] of input.precomputed.entries()) {
          map.set(String(k), v);
        }
      } else {
        for (const key of Object.keys(input.precomputed).sort()) {
          map.set(String(key), input.precomputed[key]);
        }
      }
    }

    if (!input.evaluatePolicy) return map;

    const recById = new Map();
    for (const r of input.recommendations || []) {
      if (r && r.id) recById.set(String(r.id), r);
    }

    for (const p of input.priorities || []) {
      if (!p || !p.id) continue;
      const id = String(p.id);
      if (map.has(id)) continue;

      const recommendation = recById.get(id) || priorityAsRecommendation(p);
      const decision = await this._policy.evaluate({
        tenantId: input.tenantId,
        recommendation,
        asOf: input.asOf,
        operator: input.operator,
        context: {
          contradictionSeverity: p.contradictionSeverity,
          evidenceAgeDays: 0,
        },
      });
      map.set(id, decision);
    }

    return map;
  }
}

function findRecommendation(top, items) {
  if (!top || !top.id) return null;
  return items.find((r) => r && String(r.id) === String(top.id)) || null;
}

function priorityAsRecommendation(p) {
  return {
    id: p.id,
    subject: {
      id: p.companyId,
      name: p.companyName || p.companyId,
      type: 'company',
    },
    type: p.type,
    priority: p.priority,
    score: p.score,
    confidence: p.confidence,
    recommendedAction: p.recommendedAction,
    supportingSignals: (p.why || []).map((w, i) => ({
      kind: 'reason',
      id: `why:${i}`,
      summary: String(w),
    })),
    opposingSignals: (p.whyNot || []).map((w, i) => ({
      kind: 'reason',
      id: `whyNot:${i}`,
      summary: String(w),
    })),
    claims: [],
    evidence: [],
    reasoningSummary: {
      whyThis: p.why || [],
      whyNow: p.whyNow || [],
      whyNot: p.whyNot || [],
      confidenceBasis: [],
    },
  };
}

/**
 * @param {object} options
 * @param {import('../briefing/BriefingEngine').BriefingEngine} options.briefing
 * @param {import('../policy/engine/PolicyEngine').PolicyEngine} options.policy
 */
function createCommandDeckComposer(options) {
  return new CommandDeckComposer(options);
}

module.exports = {
  CommandDeckComposer,
  createCommandDeckComposer,
};
