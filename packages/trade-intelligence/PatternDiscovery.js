'use strict';

const { createHash, randomUUID } = require('crypto');
const {
  FINDING_TYPES,
  RUNTIME_VERSION,
  MIN_PATTERN_SAMPLE,
} = require('./types');
const { isWin, isLoss, groupBy } = require('./TradeAnalyzer');

/**
 * PatternDiscovery — search for recurring explainable patterns.
 */
class PatternDiscovery {
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
  discover(trades = []) {
    const findings = [];
    findings.push(...this._hypothesisOutcomePatterns(trades));
    findings.push(...this._confidencePatterns(trades));
    findings.push(...this._timeOfDayPatterns(trades));
    findings.push(...this._indicatorPatterns(trades));
    findings.push(...this._volumePatterns(trades));
    return findings.filter(Boolean);
  }

  /**
   * @param {object[]} trades
   * @private
   */
  _hypothesisOutcomePatterns(trades) {
    const findings = [];
    const groups = groupBy(trades, (t) => t.hypothesis || 'Unknown');

    for (const [hypothesis, rows] of Object.entries(groups)) {
      if (rows.length < MIN_PATTERN_SAMPLE) continue;
      const wins = rows.filter(isWin);
      const losses = rows.filter(isLoss);
      const winRate = wins.length / rows.length;

      if (winRate >= 0.6) {
        findings.push(
          this._finding({
            type: FINDING_TYPES.PATTERN,
            title: `Winning ${hypothesis} trades`,
            summary: `${hypothesis} shows a ${pct(winRate)} win rate across ${rows.length} trades.`,
            supportingEvidence: wins.map((t) => evidenceRef(t)),
            contradictingEvidence: losses.map((t) => evidenceRef(t)),
            confidence: winRate,
            sampleSize: rows.length,
            metadata: { hypothesis, winRate },
          })
        );
      } else if (winRate <= 0.4 && losses.length >= MIN_PATTERN_SAMPLE) {
        findings.push(
          this._finding({
            type: FINDING_TYPES.PATTERN,
            title: `Losing ${hypothesis} trades`,
            summary: `${hypothesis} is underperforming with a ${pct(winRate)} win rate.`,
            supportingEvidence: losses.map((t) => evidenceRef(t)),
            contradictingEvidence: wins.map((t) => evidenceRef(t)),
            confidence: 1 - winRate,
            sampleSize: rows.length,
            metadata: { hypothesis, winRate },
          })
        );
      }
    }
    return findings;
  }

  /**
   * @param {object[]} trades
   * @private
   */
  _confidencePatterns(trades) {
    const findings = [];
    const highConfLosses = trades.filter(
      (t) => Number(t.confidence) >= 4 && isLoss(t)
    );
    const lowConfWins = trades.filter(
      (t) => Number(t.confidence) <= 2 && isWin(t)
    );

    if (highConfLosses.length >= MIN_PATTERN_SAMPLE) {
      findings.push(
        this._finding({
          type: FINDING_TYPES.PATTERN,
          title: 'High-confidence losses',
          summary: `${highConfLosses.length} losses occurred at confidence 4–5.`,
          supportingEvidence: highConfLosses.map((t) => evidenceRef(t)),
          contradictingEvidence: [],
          confidence: highConfLosses.length / trades.length,
          sampleSize: highConfLosses.length,
          metadata: { pattern: 'high_confidence_loss' },
        })
      );
    }

    if (lowConfWins.length >= MIN_PATTERN_SAMPLE) {
      findings.push(
        this._finding({
          type: FINDING_TYPES.PATTERN,
          title: 'Low-confidence wins',
          summary: `${lowConfWins.length} wins occurred at confidence 1–2.`,
          supportingEvidence: lowConfWins.map((t) => evidenceRef(t)),
          contradictingEvidence: [],
          confidence: lowConfWins.length / trades.length,
          sampleSize: lowConfWins.length,
          metadata: { pattern: 'low_confidence_win' },
        })
      );
    }

    return findings;
  }

  /**
   * @param {object[]} trades
   * @private
   */
  _timeOfDayPatterns(trades) {
    const findings = [];
    const buckets = groupBy(trades, (t) => {
      const d = new Date(t.entryTime || t.capturedAt || '');
      if (Number.isNaN(d.getTime())) return 'unknown';
      return `${String(d.getUTCHours()).padStart(2, '0')}:00`;
    });

    for (const [hour, rows] of Object.entries(buckets)) {
      if (hour === 'unknown' || rows.length < MIN_PATTERN_SAMPLE) continue;
      const winRate = rows.filter(isWin).length / rows.length;
      if (winRate >= 0.65 || winRate <= 0.35) {
        findings.push(
          this._finding({
            type: FINDING_TYPES.TIME_WINDOW,
            title: `Time-of-day effect at ${hour} UTC`,
            summary: `Win rate ${pct(winRate)} during the ${hour} hour (${rows.length} trades).`,
            supportingEvidence: rows.map((t) => evidenceRef(t)),
            contradictingEvidence: [],
            confidence: Math.abs(winRate - 0.5) * 2,
            sampleSize: rows.length,
            metadata: { hour, winRate },
          })
        );
      }
    }
    return findings;
  }

  /**
   * @param {object[]} trades
   * @private
   */
  _indicatorPatterns(trades) {
    const findings = [];
    const withAtr = trades.filter((t) => t.atr != null);
    if (withAtr.length < MIN_PATTERN_SAMPLE) return findings;

    const medianAtr = median(withAtr.map((t) => Number(t.atr)));
    const highAtr = withAtr.filter((t) => Number(t.atr) >= medianAtr);
    const lowAtr = withAtr.filter((t) => Number(t.atr) < medianAtr);

    if (highAtr.length >= MIN_PATTERN_SAMPLE) {
      const winRate = highAtr.filter(isWin).length / highAtr.length;
      findings.push(
        this._finding({
          type: FINDING_TYPES.PATTERN,
          title: 'ATR regime — elevated volatility',
          summary: `Win rate ${pct(winRate)} when ATR is above median (${highAtr.length} trades).`,
          supportingEvidence: highAtr.map((t) => evidenceRef(t)),
          contradictingEvidence: lowAtr.map((t) => evidenceRef(t)),
          confidence: Math.abs(winRate - 0.5) * 2,
          sampleSize: highAtr.length,
          metadata: { atrRegime: 'elevated', winRate },
        })
      );
    }

    const aboveVwap = trades.filter(
      (t) =>
        t.currentPrice != null &&
        t.vwap != null &&
        Number(t.currentPrice) > Number(t.vwap)
    );
    if (aboveVwap.length >= MIN_PATTERN_SAMPLE) {
      const winRate = aboveVwap.filter(isWin).length / aboveVwap.length;
      findings.push(
        this._finding({
          type: FINDING_TYPES.PATTERN,
          title: 'Price above VWAP',
          summary: `Win rate ${pct(winRate)} when price is above VWAP (${aboveVwap.length} trades).`,
          supportingEvidence: aboveVwap.map((t) => evidenceRef(t)),
          contradictingEvidence: [],
          confidence: Math.abs(winRate - 0.5) * 2,
          sampleSize: aboveVwap.length,
          metadata: { vwapRelation: 'above', winRate },
        })
      );
    }

    return findings;
  }

  /**
   * @param {object[]} trades
   * @private
   */
  _volumePatterns(trades) {
    const withVolume = trades.filter((t) => t.volume != null);
    if (withVolume.length < MIN_PATTERN_SAMPLE) return [];

    const medianVol = median(withVolume.map((t) => Number(t.volume)));
    const highVol = withVolume.filter((t) => Number(t.volume) >= medianVol);
    if (highVol.length < MIN_PATTERN_SAMPLE) return [];

    const winRate = highVol.filter(isWin).length / highVol.length;
    return [
      this._finding({
        type: FINDING_TYPES.PATTERN,
        title: 'High-volume setups',
        summary: `Win rate ${pct(winRate)} on above-median volume (${highVol.length} trades).`,
        supportingEvidence: highVol.map((t) => evidenceRef(t)),
        contradictingEvidence: [],
        confidence: Math.abs(winRate - 0.5) * 2,
        sampleSize: highVol.length,
        metadata: { volumeRegime: 'high', winRate },
      }),
    ];
  }

  /**
   * @param {object} input
   * @private
   */
  _finding(input) {
    return Object.freeze({
      id: this._idFactory(),
      type: input.type,
      title: input.title,
      summary: input.summary,
      supportingEvidence: Object.freeze(input.supportingEvidence || []),
      contradictingEvidence: Object.freeze(input.contradictingEvidence || []),
      confidence: input.confidence,
      sampleSize: input.sampleSize || 0,
      createdAt: this._now(),
      runtimeVersion: RUNTIME_VERSION,
      replayRefs: Object.freeze(
        (input.supportingEvidence || []).map((e) => e.tradeId).filter(Boolean)
      ),
      metadata: Object.freeze(input.metadata || {}),
      immutable: true,
    });
  }
}

function evidenceRef(trade) {
  return Object.freeze({
    tradeId: trade.id,
    hypothesis: trade.hypothesis,
    result: trade.result,
    confidence: trade.confidence,
    screenshotId: trade.screenshotId,
    observationId: trade.observationId,
  });
}

function pct(rate) {
  return `${Math.round(rate * 1000) / 10}%`;
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function createPatternDiscovery(deps) {
  return new PatternDiscovery(deps);
}

module.exports = {
  PatternDiscovery,
  createPatternDiscovery,
  evidenceRef,
};
