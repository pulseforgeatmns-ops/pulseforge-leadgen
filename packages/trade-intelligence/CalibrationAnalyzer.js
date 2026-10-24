'use strict';

const { randomUUID } = require('crypto');
const {
  FINDING_TYPES,
  RUNTIME_VERSION,
  CONFIDENCE_BANDS,
  MIN_PATTERN_SAMPLE,
} = require('./types');
const { isWin } = require('./TradeAnalyzer');

/**
 * CalibrationAnalyzer — measure decision quality (confidence vs outcomes).
 */
class CalibrationAnalyzer {
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
   * @param {object[]} trades
   * @returns {object[]}
   */
  analyze(trades = []) {
    const bands = [];
    const findings = [];

    for (const level of CONFIDENCE_BANDS) {
      const rows = trades.filter((t) => Number(t.confidence) === level);
      if (!rows.length) continue;

      const wins = rows.filter(isWin).length;
      const winRate = wins / rows.length;
      const expected = level / 5;
      const gap = winRate - expected;

      let recommendation = null;
      if (level >= 4 && winRate < 0.55) {
        recommendation =
          'Reduce confidence until more evidence exists.';
      } else if (level <= 2 && winRate > 0.65) {
        recommendation = 'You consistently underestimate these setups.';
      } else if (gap > 0.15) {
        recommendation = 'Calibration is strong at this confidence level.';
      } else if (gap < -0.15) {
        recommendation = 'Recalibrate — outcomes lag stated confidence.';
      }

      const band = Object.freeze({
        id: `calibration:confidence:${level}`,
        confidenceLevel: level,
        trades: rows.length,
        wins,
        losses: rows.length - wins,
        winRate: round(winRate),
        expectedWinRate: round(expected),
        gap: round(gap),
        recommendation,
        tradeIds: rows.map((t) => t.id),
      });
      bands.push(band);

      if (rows.length >= MIN_PATTERN_SAMPLE && recommendation) {
        findings.push(
          Object.freeze({
            id: this._idFactory(),
            type: FINDING_TYPES.CALIBRATION,
            title: `Confidence ${level} calibration`,
            summary: `Win rate ${pct(winRate)} at confidence ${level}. ${recommendation}`,
            supportingEvidence: Object.freeze(
              rows.filter(isWin).map((t) => tradeEvidence(t))
            ),
            contradictingEvidence: Object.freeze(
              rows.filter((t) => !isWin(t)).map((t) => tradeEvidence(t))
            ),
            confidence: Math.min(1, rows.length / 20),
            sampleSize: rows.length,
            createdAt: this._now(),
            runtimeVersion: RUNTIME_VERSION,
            replayRefs: Object.freeze(rows.map((t) => t.id)),
            metadata: Object.freeze({ confidenceLevel: level, winRate, gap }),
            immutable: true,
          })
        );
      }
    }

    return Object.freeze({
      bands: Object.freeze(bands),
      findings: Object.freeze(findings),
      overall: overallCalibration(bands),
    });
  }

  /**
   * Compare confidence bands side-by-side.
   * @param {object[]} trades
   */
  compareBands(trades = []) {
    const { bands } = this.analyze(trades);
    return Object.freeze({
      kind: 'compareConfidenceBands',
      bands,
      isolated: true,
    });
  }
}

function overallCalibration(bands) {
  if (!bands.length) return null;
  const gaps = bands.map((b) => Math.abs(b.gap || 0));
  const avgGap = gaps.reduce((a, b) => a + b, 0) / gaps.length;
  let trend = 'Stable';
  if (avgGap < 0.1) trend = 'Improving';
  if (avgGap > 0.2) trend = 'Needs attention';
  return Object.freeze({ averageGap: round(avgGap), trend });
}

function tradeEvidence(trade) {
  return Object.freeze({
    tradeId: trade.id,
    result: trade.result,
    confidence: trade.confidence,
    hypothesis: trade.hypothesis,
  });
}

function round(n) {
  return Math.round(n * 10000) / 10000;
}

function pct(rate) {
  return `${Math.round(rate * 1000) / 10}%`;
}

function createCalibrationAnalyzer(deps) {
  return new CalibrationAnalyzer(deps);
}

module.exports = {
  CalibrationAnalyzer,
  createCalibrationAnalyzer,
};
