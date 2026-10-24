'use strict';

const { randomUUID } = require('crypto');
const {
  FINDING_TYPES,
  RUNTIME_VERSION,
  REVIEW_PERIODS,
} = require('./types');
const { createTradeAnalyzer, dayKey } = require('./TradeAnalyzer');

/**
 * ReviewGenerator — daily and weekly operator briefings.
 */
class ReviewGenerator {
  /**
   * @param {object} [deps]
   * @param {import('./TradeAnalyzer').TradeAnalyzer} [deps.analyzer]
   * @param {() => string} [deps.idFactory]
   * @param {() => string} [deps.now]
   */
  constructor(deps = {}) {
    this.analyzer = deps.analyzer || createTradeAnalyzer();
    this._idFactory =
      typeof deps.idFactory === 'function' ? deps.idFactory : () => randomUUID();
    this._now =
      typeof deps.now === 'function'
        ? deps.now
        : () => new Date().toISOString();
  }

  /**
   * @param {object[]} trades
   * @param {object} [opts]
   * @param {string|Date} [opts.day]
   */
  generateDailyReview(trades = [], opts = {}) {
    const now = opts.now ? new Date(opts.now) : new Date(this._now());
    const day = opts.day || now;
    const sessionTrades = this.analyzer.tradesForDay(trades, day);
    const stats = this.analyzer.analyze(sessionTrades);
    const hypothesisPerf = this.analyzer.hypothesisPerformance(sessionTrades);
    const mistake = this.analyzer.largestMistake(sessionTrades);

    const ranked = [...hypothesisPerf].sort(
      (a, b) => (b.winRate || 0) - (a.winRate || 0)
    );
    const best = ranked[0] || null;
    const worst = ranked.length ? ranked[ranked.length - 1] : null;

    const review = Object.freeze({
      id: `daily:${dayKey(day)}`,
      kind: 'daily_review',
      period: REVIEW_PERIODS.TODAY,
      date: dayKey(day),
      title: "Today's Session",
      trades: stats.trades,
      wins: stats.wins,
      losses: stats.losses,
      winRate: stats.winRate,
      winRatePct: stats.winRate != null ? `${round(stats.winRate * 100, 1)}%` : null,
      averageHoldTime: stats.averageHoldTime,
      averageConfidence: stats.averageConfidence,
      bestPerformingHypothesis: best
        ? Object.freeze({
            hypothesis: best.hypothesis,
            winRate: best.winRate,
            trades: best.trades,
          })
        : null,
      worstPerformingHypothesis: worst
        ? Object.freeze({
            hypothesis: worst.hypothesis,
            winRate: worst.winRate,
            trades: worst.trades,
          })
        : null,
      largestMistake: mistake,
      tradeIds: sessionTrades.map((t) => t.id),
      createdAt: this._now(),
      runtimeVersion: RUNTIME_VERSION,
      immutable: true,
    });

    const finding = Object.freeze({
      id: this._idFactory(),
      type: FINDING_TYPES.REVIEW,
      title: review.title,
      summary: `${review.trades} trades · ${review.winRatePct || 'n/a'} win rate`,
      supportingEvidence: Object.freeze(sessionTrades.map((t) => ({ tradeId: t.id }))),
      contradictingEvidence: Object.freeze([]),
      confidence: stats.winRate,
      sampleSize: stats.trades,
      createdAt: review.createdAt,
      runtimeVersion: RUNTIME_VERSION,
      replayRefs: review.tradeIds,
      metadata: Object.freeze({ reviewId: review.id, kind: 'daily' }),
      immutable: true,
    });

    return Object.freeze({ review, finding });
  }

  /**
   * @param {object[]} trades
   * @param {object} [opts]
   */
  generateWeeklyReview(trades = [], opts = {}) {
    const now = opts.now ? new Date(opts.now) : new Date(this._now());
    const to = opts.to ? new Date(opts.to) : now;
    const from = opts.from
      ? new Date(opts.from)
      : new Date(to.getTime() - 7 * 24 * 60 * 60 * 1000);

    const weekTrades = this.analyzer.tradesInRange(trades, from, to);
    const stats = this.analyzer.analyze(weekTrades);
    const hypothesisPerf = this.analyzer.hypothesisPerformance(weekTrades);
    const mistake = this.analyzer.largestMistake(weekTrades);
    const bestWindow = this.analyzer.bestTradingWindow(weekTrades);
    const calibration = opts.calibration || null;

    const ranked = [...hypothesisPerf].sort(
      (a, b) => (b.winRate || 0) - (a.winRate || 0)
    );
    const best = ranked[0] || null;
    const worst = ranked.length ? ranked[ranked.length - 1] : null;
    const newestFinding = opts.largestNewFinding || null;

    const review = Object.freeze({
      id: `weekly:${dayKey(from)}:${dayKey(to)}`,
      kind: 'weekly_review',
      period: REVIEW_PERIODS.LAST_WEEK,
      from: from.toISOString(),
      to: to.toISOString(),
      title: 'This Week',
      trades: stats.trades,
      wins: stats.wins,
      losses: stats.losses,
      winRate: stats.winRate,
      winRatePct: stats.winRate != null ? `${round(stats.winRate * 100, 1)}%` : null,
      mostProfitableSetup: best
        ? Object.freeze({ hypothesis: best.hypothesis, winRate: best.winRate })
        : null,
      weakestSetup: worst
        ? Object.freeze({ hypothesis: worst.hypothesis, winRate: worst.winRate })
        : null,
      mostCommonError: mistake
        ? Object.freeze({ summary: mistake.summary, count: mistake.count })
        : null,
      bestTradingWindow: bestWindow,
      calibration: calibration
        ? Object.freeze({ trend: calibration.trend })
        : null,
      largestNewFinding: newestFinding,
      tradeIds: weekTrades.map((t) => t.id),
      createdAt: this._now(),
      runtimeVersion: RUNTIME_VERSION,
      immutable: true,
    });

    const finding = Object.freeze({
      id: this._idFactory(),
      type: FINDING_TYPES.REVIEW,
      title: review.title,
      summary: `${review.trades} trades · ${review.winRatePct || 'n/a'} win rate this week`,
      supportingEvidence: Object.freeze(weekTrades.map((t) => ({ tradeId: t.id }))),
      contradictingEvidence: Object.freeze([]),
      confidence: stats.winRate,
      sampleSize: stats.trades,
      createdAt: review.createdAt,
      runtimeVersion: RUNTIME_VERSION,
      replayRefs: review.tradeIds,
      metadata: Object.freeze({ reviewId: review.id, kind: 'weekly' }),
      immutable: true,
    });

    return Object.freeze({ review, finding });
  }
}

function round(n, digits = 4) {
  const f = 10 ** digits;
  return Math.round(n * f) / f;
}

function createReviewGenerator(deps) {
  return new ReviewGenerator(deps);
}

module.exports = {
  ReviewGenerator,
  createReviewGenerator,
};
