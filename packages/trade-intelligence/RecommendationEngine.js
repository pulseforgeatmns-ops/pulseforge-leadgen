'use strict';

const { randomUUID } = require('crypto');
const {
  FINDING_TYPES,
  RUNTIME_VERSION,
  MIN_RECOMMENDATION_SAMPLE,
  RECOMMENDATION_CONFIDENCE,
} = require('./types');
const { isWin } = require('./TradeAnalyzer');

/**
 * RecommendationEngine — explainable, evidence-backed operator recommendations.
 */
class RecommendationEngine {
  /**
   * @param {object} [deps]
   * @param {() => string} [deps.idFactory]
   * @param {() => string} [deps.now]
   */
  constructor(deps = {}) {
    this._idFactory =
      typeof deps.idFactory === 'function' ? deps.idFactory : () => randomUUID();
    this._now =
      typeof deps.now === 'function'
        ? deps.now
        : () => new Date().toISOString();
  }

  /**
   * @param {object} input
   * @param {object[]} input.trades
   * @param {object[]} [input.patterns]
   * @param {object[]} [input.calibrationFindings]
   */
  generate(input = {}) {
    const trades = input.trades || [];
    const patterns = input.patterns || [];
    const calibrationFindings = input.calibrationFindings || [];
    const recommendations = [];

    recommendations.push(...this._fromPatterns(trades, patterns));
    recommendations.push(...this._fromCalibration(calibrationFindings));
    recommendations.push(...this._vwapMomentum(trades));
    recommendations.push(...this._atrBreakoutDegradation(trades));

    return Object.freeze(
      recommendations.filter(Boolean).map((rec) => Object.freeze(rec))
    );
  }

  /**
   * @param {object[]} trades
   * @param {object[]} patterns
   * @private
   */
  _fromPatterns(trades, patterns) {
    return patterns
      .filter((p) => p.sampleSize >= MIN_RECOMMENDATION_SAMPLE)
      .slice(0, 5)
      .map((pattern) =>
        this._recommendation({
          title: pattern.title,
          summary: pattern.summary,
          confidenceLabel:
            pattern.confidence >= 0.7
              ? RECOMMENDATION_CONFIDENCE.HIGH
              : pattern.confidence >= 0.45
                ? RECOMMENDATION_CONFIDENCE.MEDIUM
                : RECOMMENDATION_CONFIDENCE.LOW,
          supportingEvidence: pattern.supportingEvidence,
          contradictingEvidence: pattern.contradictingEvidence,
          sampleSize: pattern.sampleSize,
          replayRefs: pattern.replayRefs,
          metadata: { sourceFindingId: pattern.id, type: pattern.type },
        })
      );
  }

  /**
   * @param {object[]} calibrationFindings
   * @private
   */
  _fromCalibration(calibrationFindings) {
    return calibrationFindings.map((finding) =>
      this._recommendation({
        title: finding.title,
        summary: finding.summary,
        confidenceLabel: RECOMMENDATION_CONFIDENCE.MEDIUM,
        supportingEvidence: finding.supportingEvidence,
        contradictingEvidence: finding.contradictingEvidence,
        sampleSize: finding.sampleSize,
        replayRefs: finding.replayRefs,
        metadata: { sourceFindingId: finding.id, type: 'calibration' },
      })
    );
  }

  /**
   * @param {object[]} trades
   * @private
   */
  _vwapMomentum(trades) {
    const velocityAboveVwap = trades.filter(
      (t) =>
        t.hypothesis === 'Velocity' &&
        t.currentPrice != null &&
        t.vwap != null &&
        Number(t.currentPrice) > Number(t.vwap)
    );
    if (velocityAboveVwap.length < MIN_RECOMMENDATION_SAMPLE) return [];

    const winRate =
      velocityAboveVwap.filter(isWin).length / velocityAboveVwap.length;
    if (winRate < 0.55) return [];

    const wins = velocityAboveVwap.filter(isWin);
    const losses = velocityAboveVwap.filter((t) => !isWin(t));

    return [
      this._recommendation({
        title: 'Velocity trades above VWAP continue outperforming.',
        summary: `Win rate ${pct(winRate)} when Velocity entries occur above VWAP.`,
        confidenceLabel:
          winRate >= 0.65
            ? RECOMMENDATION_CONFIDENCE.HIGH
            : RECOMMENDATION_CONFIDENCE.MEDIUM,
        supportingEvidence: wins.map((t) => tradeRef(t)),
        contradictingEvidence: losses.map((t) => tradeRef(t)),
        sampleSize: velocityAboveVwap.length,
        replayRefs: velocityAboveVwap.map((t) => t.id),
        metadata: { pattern: 'velocity_above_vwap' },
      }),
    ];
  }

  /**
   * @param {object[]} trades
   * @private
   */
  _atrBreakoutDegradation(trades) {
    const breakouts = trades.filter((t) => t.hypothesis === 'Breakout');
    if (breakouts.length < MIN_RECOMMENDATION_SAMPLE) return [];

    const sorted = [...breakouts].sort(
      (a, b) =>
        Date.parse(String(a.entryTime || a.capturedAt)) -
        Date.parse(String(b.entryTime || b.capturedAt))
    );
    const recent = sorted.slice(-Math.min(50, sorted.length));
    const older = sorted.slice(0, Math.max(0, sorted.length - recent.length));
    if (recent.length < MIN_RECOMMENDATION_SAMPLE) return [];

    const recentWinRate = recent.filter(isWin).length / recent.length;
    const olderWinRate =
      older.length >= MIN_RECOMMENDATION_SAMPLE
        ? older.filter(isWin).length / older.length
        : null;

    if (olderWinRate != null && recentWinRate >= olderWinRate - 0.05) {
      return [];
    }

    const highAtrRecent = recent.filter(
      (t) => t.atr != null && Number(t.atr) > 0
    );
    if (highAtrRecent.length < MIN_RECOMMENDATION_SAMPLE) return [];

    return [
      this._recommendation({
        title:
          'Breakout entries after ATR spikes have degraded over the last 50 trades.',
        summary: `Recent breakout win rate ${pct(recentWinRate)}${
          olderWinRate != null ? ` vs ${pct(olderWinRate)} historically` : ''
        }.`,
        confidenceLabel: RECOMMENDATION_CONFIDENCE.MEDIUM,
        supportingEvidence: recent.filter((t) => !isWin(t)).map((t) => tradeRef(t)),
        contradictingEvidence: recent.filter(isWin).map((t) => tradeRef(t)),
        sampleSize: recent.length,
        replayRefs: recent.map((t) => t.id),
        metadata: { pattern: 'breakout_atr_degradation' },
      }),
    ];
  }

  /**
   * @param {object} input
   * @private
   */
  _recommendation(input) {
    return Object.freeze({
      id: this._idFactory(),
      type: FINDING_TYPES.RECOMMENDATION,
      title: input.title,
      summary: input.summary,
      confidence: input.confidenceLabel,
      confidenceLabel: input.confidenceLabel,
      supportingEvidence: Object.freeze(input.supportingEvidence || []),
      contradictingEvidence: Object.freeze(input.contradictingEvidence || []),
      sampleSize: input.sampleSize || 0,
      lastUpdated: this._now(),
      createdAt: this._now(),
      runtimeVersion: RUNTIME_VERSION,
      replayRefs: Object.freeze(input.replayRefs || []),
      metadata: Object.freeze(input.metadata || {}),
      immutable: true,
      explainable: true,
    });
  }
}

function tradeRef(trade) {
  return Object.freeze({
    tradeId: trade.id,
    hypothesis: trade.hypothesis,
    result: trade.result,
    confidence: trade.confidence,
  });
}

function pct(rate) {
  return `${Math.round(rate * 1000) / 10}%`;
}

function createRecommendationEngine(deps) {
  return new RecommendationEngine(deps);
}

module.exports = {
  RecommendationEngine,
  createRecommendationEngine,
};
