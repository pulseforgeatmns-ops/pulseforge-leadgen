'use strict';

const { randomUUID } = require('crypto');
const {
  INTELLIGENCE_RULES,
  RUNTIME_VERSION,
  FINDING_TYPES,
  REVIEW_PERIODS,
} = require('./types');
const { createTradeAnalyzer } = require('./TradeAnalyzer');
const { createPatternDiscovery } = require('./PatternDiscovery');
const { createCalibrationAnalyzer } = require('./CalibrationAnalyzer');
const { createReviewGenerator } = require('./ReviewGenerator');
const { createRecommendationEngine } = require('./RecommendationEngine');

/**
 * TradeIntelligenceEngine — convert captured trades into actionable intelligence.
 *
 * Does not execute trades. Evidence first. Everything reproducible through Replay.
 *
 * @example
 *   const intel = createTradeIntelligenceEngine({ captureEngine });
 *   const daily = intel.generateDailyReview();
 *   const recs = intel.getRecommendations();
 */
class TradeIntelligenceEngine {
  /**
   * @param {object} [deps]
   * @param {import('@pulseforge/trade-capture').CaptureEngine} [deps.captureEngine]
   * @param {object[]} [deps.trades]
   * @param {import('@pulseforge/learning').LearningEngine} [deps.learning]
   * @param {import('@pulseforge/replay').ReplayEngine} [deps.replayEngine]
   * @param {() => string} [deps.idFactory]
   * @param {() => string} [deps.now]
   */
  constructor(deps = {}) {
    this.rules = INTELLIGENCE_RULES;
    this.runtimeVersion = RUNTIME_VERSION;
    this._capture = deps.captureEngine || null;
    this._learning = deps.learning || null;
    this._replay = deps.replayEngine || null;
    this._idFactory =
      typeof deps.idFactory === 'function' ? deps.idFactory : () => randomUUID();
    this._now =
      typeof deps.now === 'function'
        ? deps.now
        : () => new Date().toISOString();

    this.analyzer = deps.analyzer || createTradeAnalyzer();
    this.patterns = deps.patterns || createPatternDiscovery({ now: this._now });
    this.calibration =
      deps.calibration || createCalibrationAnalyzer({ now: this._now });
    this.reviews =
      deps.reviews || createReviewGenerator({ analyzer: this.analyzer, now: this._now });
    this.recommendations =
      deps.recommendations ||
      createRecommendationEngine({ now: this._now });

    /** @type {object[]} */
    this._seedTrades = Array.isArray(deps.trades) ? deps.trades.slice() : [];

    /** @type {Map<string, object>} */
    this._findings = new Map();
    /** @type {Map<string, object>} */
    this._dailyReviews = new Map();
    /** @type {Map<string, object>} */
    this._weeklyReviews = new Map();
    /** @type {object[]} */
    this._recommendationCache = [];
    /** @type {object|null} */
    this._lastRun = null;
  }

  /**
   * All trades from capture engine or seed list.
   * @returns {object[]}
   */
  getTrades() {
    if (this._capture && typeof this._capture.findTrades === 'function') {
      return this._capture.findTrades();
    }
    return this._seedTrades.slice();
  }

  /**
   * Run full intelligence pipeline and persist Findings.
   * @param {object} [opts]
   */
  analyze(opts = {}) {
    const trades = opts.trades || this.getTrades();
    const patternFindings = this.patterns.discover(trades);
    const calibration = this.calibration.analyze(trades);
    const recs = this.recommendations.generate({
      trades,
      patterns: patternFindings,
      calibrationFindings: calibration.findings,
    });

    for (const finding of [
      ...patternFindings,
      ...calibration.findings,
      ...recs,
    ]) {
      this._persistFinding(finding);
    }

    this._recommendationCache = recs;

    const result = Object.freeze({
      trades: trades.length,
      patterns: patternFindings.length,
      calibration,
      recommendations: recs,
      findings: this.listFindings(),
      runtimeVersion: RUNTIME_VERSION,
      analyzedAt: this._now(),
      mutatesTrades: false,
      executesTrades: false,
    });

    this._lastRun = result;
    return result;
  }

  /**
   * @param {object} [opts]
   */
  generateDailyReview(opts = {}) {
    const trades = opts.trades || this.getTrades();
    const { review, finding } = this.reviews.generateDailyReview(trades, opts);
    this._dailyReviews.set(review.id, review);
    this._persistFinding(finding);
    return review;
  }

  /**
   * @param {object} [opts]
   */
  generateWeeklyReview(opts = {}) {
    const trades = opts.trades || this.getTrades();
    const calibration = this.calibration.analyze(trades).overall;
    const patterns = this.patterns.discover(trades);
    const largestNewFinding = patterns.length ? patterns[0] : null;

    const { review, finding } = this.reviews.generateWeeklyReview(trades, {
      ...opts,
      calibration,
      largestNewFinding: largestNewFinding
        ? Object.freeze({
            title: largestNewFinding.title,
            summary: largestNewFinding.summary,
          })
        : null,
    });
    this._weeklyReviews.set(review.id, review);
    this._persistFinding(finding);
    return review;
  }

  /**
   * Replay-integrated trade review.
   * @param {string} tradeId
   */
  reviewTrade(tradeId) {
    const trade = this._resolveTrade(tradeId);
    if (!trade) {
      throw new Error(`Unknown trade: ${tradeId}`);
    }

    const operatorView =
      this._capture && typeof this._capture.operatorView === 'function'
        ? this._capture.operatorView(tradeId)
        : { trade, screenshot: null, observation: null, evidence: [] };

    const similar = this.findSimilarTrades(tradeId, { limit: 5 });

    const replayRef = Object.freeze({
      tradeId,
      screenshotId: trade.screenshotId,
      observationId: trade.observationId,
      entryTime: trade.entryTime,
      subjectId: trade.subjectId || trade.symbol,
    });

    const finding = Object.freeze({
      id: this._idFactory(),
      type: FINDING_TYPES.SIMILARITY,
      title: `Trade review: ${trade.hypothesis} ${trade.direction}`,
      summary: `${trade.result} at confidence ${trade.confidence}.`,
      supportingEvidence: Object.freeze([{ tradeId, replayRef }]),
      contradictingEvidence: Object.freeze([]),
      confidence: trade.confidence ? trade.confidence / 5 : null,
      sampleSize: 1,
      createdAt: this._now(),
      runtimeVersion: RUNTIME_VERSION,
      replayRefs: Object.freeze([tradeId]),
      metadata: Object.freeze({ similarTradeIds: similar.map((t) => t.id) }),
      immutable: true,
    });
    this._persistFinding(finding);

    return Object.freeze({
      trade,
      operatorView,
      similarTrades: similar,
      replayRef,
      reproducible: true,
      finding,
    });
  }

  /**
   * Compare two weeks of trading activity.
   * @param {object} [opts]
   */
  compareWeek(opts = {}) {
    const trades = opts.trades || this.getTrades();
    const now = opts.now ? new Date(opts.now) : new Date(this._now());
    const currentEnd = opts.currentEnd ? new Date(opts.currentEnd) : now;
    const currentStart = new Date(
      currentEnd.getTime() - 7 * 24 * 60 * 60 * 1000
    );
    const priorEnd = currentStart;
    const priorStart = new Date(priorEnd.getTime() - 7 * 24 * 60 * 60 * 1000);

    const currentTrades = this.analyzer.tradesInRange(
      trades,
      currentStart,
      currentEnd
    );
    const priorTrades = this.analyzer.tradesInRange(
      trades,
      priorStart,
      priorEnd
    );

    const left = this.analyzer.analyze(currentTrades);
    const right = this.analyzer.analyze(priorTrades);

    return Object.freeze({
      kind: 'compareWeek',
      left: Object.freeze({
        label: 'current_week',
        from: currentStart.toISOString(),
        to: currentEnd.toISOString(),
        ...left,
      }),
      right: Object.freeze({
        label: 'prior_week',
        from: priorStart.toISOString(),
        to: priorEnd.toISOString(),
        ...right,
      }),
      delta: Object.freeze({
        winRate: delta(left.winRate, right.winRate),
        trades: left.trades - right.trades,
        averageConfidence: delta(left.averageConfidence, right.averageConfidence),
      }),
      reproducible: true,
      runtimeVersion: RUNTIME_VERSION,
    });
  }

  /**
   * Find comparable historical trades.
   * @param {string} tradeId
   * @param {object} [opts]
   */
  findSimilarTrades(tradeId, opts = {}) {
    const target = this._resolveTrade(tradeId);
    if (!target) return [];

    const limit = opts.limit != null ? Number(opts.limit) : 10;
    const candidates = this.getTrades().filter((t) => t.id !== tradeId);

    const scored = candidates
      .map((trade) =>
        Object.freeze({
          trade,
          score: similarityScore(target, trade),
        })
      )
      .filter((row) => row.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, limit);

    return scored.map((row) =>
      Object.freeze({
        ...row.trade,
        similarityScore: row.score,
      })
    );
  }

  /**
   * @returns {object[]}
   */
  getRecommendations() {
    if (!this._recommendationCache.length) {
      this.analyze();
    }
    return this._recommendationCache.slice();
  }

  /**
   * @returns {object[]}
   */
  listFindings() {
    return [...this._findings.values()];
  }

  /**
   * @param {string} id
   */
  getFinding(id) {
    return this._findings.get(String(id)) || null;
  }

  /**
   * Hypothesis performance table.
   */
  hypothesisPerformance(opts = {}) {
    return this.analyzer.hypothesisPerformance(opts.trades || this.getTrades());
  }

  /**
   * Pattern discovery only.
   */
  discoverPatterns(opts = {}) {
    const findings = this.patterns.discover(opts.trades || this.getTrades());
    for (const finding of findings) {
      this._persistFinding(finding);
    }
    return findings;
  }

  /**
   * Calibration analysis only.
   */
  analyzeCalibration(opts = {}) {
    return this.calibration.analyze(opts.trades || this.getTrades());
  }

  /**
   * Compare hypothesis strategies.
   */
  compareStrategies(opts = {}) {
    const perf = this.hypothesisPerformance(opts);
    return Object.freeze({
      kind: 'compareTradeStrategies',
      hypotheses: perf,
      isolated: true,
      mutatesProduction: false,
    });
  }

  /**
   * Compare time windows.
   */
  compareTimeWindows(opts = {}) {
    const trades = opts.trades || this.getTrades();
    const buckets = this.analyzer.analyze(trades).byHour;
    return Object.freeze({
      kind: 'compareTimeWindows',
      windows: buckets,
      best: this.analyzer.bestTradingWindow(trades),
      isolated: true,
    });
  }

  /**
   * Compare confidence bands.
   */
  compareConfidenceBands(opts = {}) {
    return this.calibration.compareBands(opts.trades || this.getTrades());
  }

  /**
   * Project into EQL catalog seed.
   */
  toCatalogSeed() {
    const trades = this.getTrades();
    if (!this._lastRun) this.analyze({ trades });

    const daily = [...this._dailyReviews.values()];
    const weekly = [...this._weeklyReviews.values()];
    const findings = this.listFindings();
    const recommendations = this.getRecommendations();
    const calibration = this.calibration.analyze(trades);
    const hypotheses = [...this.hypothesisPerformance({ trades })].sort(
      (a, b) => (b.winRate || 0) - (a.winRate || 0)
    );

    return {
      trades,
      daily_reviews: daily,
      weekly_reviews: weekly,
      findings,
      recommendations,
      trade_calibrations: calibration.bands,
      best_hypotheses: hypotheses,
      periods: [
        { id: REVIEW_PERIODS.TODAY, label: REVIEW_PERIODS.TODAY },
        { id: REVIEW_PERIODS.LAST_WEEK, label: REVIEW_PERIODS.LAST_WEEK },
      ],
    };
  }

  /**
   * Resolve review by period id (Today, LastWeek).
   * @param {string} periodId
   */
  reviewForPeriod(periodId) {
    const key = String(periodId);
    if (key === REVIEW_PERIODS.TODAY || key.toLowerCase() === 'today') {
      const todayKey = `daily:${new Date(this._now()).toISOString().slice(0, 10)}`;
      return (
        this._dailyReviews.get(todayKey) || this.generateDailyReview()
      );
    }
    if (
      key === REVIEW_PERIODS.LAST_WEEK ||
      key.toLowerCase() === 'lastweek'
    ) {
      const existing = [...this._weeklyReviews.values()][0];
      return existing || this.generateWeeklyReview();
    }
    return null;
  }

  /**
   * Similar trades for EQL SHOW SimilarTrades FOR Trade("…").
   * @param {string} tradeId
   */
  similarTradesFor(tradeId) {
    return this.findSimilarTrades(tradeId);
  }

  /**
   * @param {object} finding
   * @private
   */
  _persistFinding(finding) {
    if (!finding || !finding.id) return;
    if (this._findings.has(finding.id)) return;
    this._findings.set(finding.id, Object.freeze({ ...finding }));
  }

  /**
   * @param {string} tradeId
   * @private
   */
  _resolveTrade(tradeId) {
    if (this._capture && typeof this._capture.getTrade === 'function') {
      return this._capture.getTrade(tradeId);
    }
    return this.getTrades().find((t) => t.id === tradeId) || null;
  }
}

/**
 * Similarity score — hypothesis, timeframe, indicators, metadata.
 * Future: chart embeddings (no API change).
 */
function similarityScore(a, b) {
  let score = 0;
  if (a.hypothesis && a.hypothesis === b.hypothesis) score += 0.35;
  if (a.timeframe && a.timeframe === b.timeframe) score += 0.15;
  if (a.direction && a.direction === b.direction) score += 0.1;
  if (a.symbol && b.symbol && a.symbol === b.symbol) score += 0.1;
  if (a.exchange && b.exchange && a.exchange === b.exchange) score += 0.05;

  if (a.atr != null && b.atr != null) {
    const diff = Math.abs(Number(a.atr) - Number(b.atr));
    const base = Math.max(Number(a.atr), Number(b.atr), 1);
    score += Math.max(0, 0.1 * (1 - diff / base));
  }
  if (a.vwap != null && b.vwap != null && a.currentPrice != null && b.currentPrice) {
    const aAbove = Number(a.currentPrice) > Number(a.vwap);
    const bAbove = Number(b.currentPrice) > Number(b.vwap);
    if (aAbove === bAbove) score += 0.1;
  }
  if (a.volume != null && b.volume != null) {
    const diff = Math.abs(Number(a.volume) - Number(b.volume));
    const base = Math.max(Number(a.volume), Number(b.volume), 1);
    score += Math.max(0, 0.05 * (1 - diff / base));
  }

  return round(score);
}

function delta(left, right) {
  if (left == null || right == null) return null;
  return round(left - right);
}

function round(n) {
  return Math.round(n * 10000) / 10000;
}

function createTradeIntelligenceEngine(deps) {
  return new TradeIntelligenceEngine(deps);
}

module.exports = {
  TradeIntelligenceEngine,
  createTradeIntelligenceEngine,
  similarityScore,
};
