'use strict';

/**
 * TradeAnalyzer — aggregate statistics over captured trades.
 */
class TradeAnalyzer {
  /**
   * @param {object[]} trades
   * @returns {object}
   */
  analyze(trades = []) {
    const list = trades.filter(Boolean);
    const wins = list.filter((t) => isWin(t));
    const losses = list.filter((t) => isLoss(t));

    const confidenceValues = list
      .map((t) => Number(t.confidence))
      .filter((n) => Number.isFinite(n));

    const holdTimes = list
      .map((t) => holdTimeMs(t))
      .filter((n) => n != null && n >= 0);

    return Object.freeze({
      trades: list.length,
      wins: wins.length,
      losses: losses.length,
      winRate: list.length ? round(wins.length / list.length) : null,
      averageConfidence: avg(confidenceValues),
      averageHoldTimeMs: avg(holdTimes),
      averageHoldTime: formatDuration(avg(holdTimes)),
      byHypothesis: hypothesisBreakdown(list),
      byDirection: directionBreakdown(list),
      byHour: timeOfDayBreakdown(list),
    });
  }

  /**
   * Per-hypothesis performance with trend.
   * @param {object[]} trades
   */
  hypothesisPerformance(trades = []) {
    const groups = groupBy(trades, (t) => t.hypothesis || 'Unknown');
    return Object.freeze(
      Object.entries(groups).map(([hypothesis, rows]) => {
        const stats = this.analyze(rows);
        const trend = computeTrend(rows);
        return Object.freeze({
          hypothesis,
          trades: rows.length,
          winRate: stats.winRate,
          averageReturn: estimateReturn(rows),
          trend,
          averageConfidence: stats.averageConfidence,
          tradeIds: rows.map((t) => t.id),
        });
      })
    );
  }

  /**
   * Filter trades to a calendar day (UTC date of entryTime).
   * @param {object[]} trades
   * @param {Date|string} day
   */
  tradesForDay(trades, day) {
    const key = dayKey(day);
    return trades.filter((t) => dayKey(t.entryTime || t.capturedAt) === key);
  }

  /**
   * Filter trades to a date range [from, to).
   * @param {object[]} trades
   * @param {Date|string} from
   * @param {Date|string} to
   */
  tradesInRange(trades, from, to) {
    const fromMs = Date.parse(String(from));
    const toMs = Date.parse(String(to));
    return trades.filter((t) => {
      const ms = Date.parse(String(t.entryTime || t.capturedAt || ''));
      return Number.isFinite(ms) && ms >= fromMs && ms < toMs;
    });
  }

  /**
   * Best trading window by hour bucket win rate.
   * @param {object[]} trades
   */
  bestTradingWindow(trades = []) {
    const buckets = timeOfDayBreakdown(trades);
    const ranked = Object.entries(buckets)
      .filter(([, v]) => v.trades >= MIN_WINDOW_SAMPLE)
      .sort((a, b) => (b[1].winRate || 0) - (a[1].winRate || 0));
    if (!ranked.length) return null;
    const [label, stats] = ranked[0];
    return Object.freeze({ window: label, ...stats });
  }

  /**
   * Most common error pattern among losses.
   * @param {object[]} trades
   */
  largestMistake(trades = []) {
    const losses = trades.filter(isLoss);
    if (!losses.length) return null;

    const byHypothesis = groupBy(losses, (t) => t.hypothesis || 'Unknown');
    const ranked = Object.entries(byHypothesis).sort(
      (a, b) => b[1].length - a[1].length
    );
    const [hypothesis, rows] = ranked[0];
    const highConfidenceLosses = rows.filter((t) => Number(t.confidence) >= 4);

    if (highConfidenceLosses.length >= 2) {
      return Object.freeze({
        summary: `High-confidence ${hypothesis} entries that failed.`,
        hypothesis,
        count: highConfidenceLosses.length,
        tradeIds: highConfidenceLosses.map((t) => t.id),
      });
    }

    if (hypothesis === 'Breakout') {
      return Object.freeze({
        summary: 'Entering after momentum had already exhausted.',
        hypothesis,
        count: rows.length,
        tradeIds: rows.map((t) => t.id),
      });
    }

    return Object.freeze({
      summary: `Repeated ${hypothesis} losses without edge confirmation.`,
      hypothesis,
      count: rows.length,
      tradeIds: rows.map((t) => t.id),
    });
  }
}

const MIN_WINDOW_SAMPLE = 2;

function isWin(trade) {
  return String(trade.result || '').toLowerCase() === 'win';
}

function isLoss(trade) {
  return String(trade.result || '').toLowerCase() === 'loss';
}

function holdTimeMs(trade) {
  const start = Date.parse(String(trade.entryTime || ''));
  const end = Date.parse(String(trade.capturedAt || trade.entryTime || ''));
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  return end - start;
}

function avg(values) {
  if (!values.length) return null;
  return round(values.reduce((a, b) => a + b, 0) / values.length);
}

function round(n) {
  return Math.round(n * 10000) / 10000;
}

function formatDuration(ms) {
  if (ms == null || !Number.isFinite(ms)) return null;
  const totalSec = Math.round(ms / 1000);
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  return `${min}m ${sec}s`;
}

function dayKey(value) {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '';
  return d.toISOString().slice(0, 10);
}

function groupBy(list, keyFn) {
  const out = {};
  for (const row of list) {
    const key = keyFn(row);
    if (!out[key]) out[key] = [];
    out[key].push(row);
  }
  return out;
}

function hypothesisBreakdown(trades) {
  const groups = groupBy(trades, (t) => t.hypothesis || 'Unknown');
  const out = {};
  for (const [hypothesis, rows] of Object.entries(groups)) {
    const wins = rows.filter(isWin).length;
    out[hypothesis] = Object.freeze({
      trades: rows.length,
      wins,
      losses: rows.length - wins,
      winRate: rows.length ? round(wins / rows.length) : null,
    });
  }
  return Object.freeze(out);
}

function directionBreakdown(trades) {
  const groups = groupBy(trades, (t) => t.direction || 'Unknown');
  const out = {};
  for (const [direction, rows] of Object.entries(groups)) {
    const wins = rows.filter(isWin).length;
    out[direction] = Object.freeze({
      trades: rows.length,
      winRate: rows.length ? round(wins / rows.length) : null,
    });
  }
  return Object.freeze(out);
}

function timeOfDayBreakdown(trades) {
  const buckets = {};
  for (const trade of trades) {
    const d = new Date(trade.entryTime || trade.capturedAt || '');
    if (Number.isNaN(d.getTime())) continue;
    const hour = d.getUTCHours();
    const label = `${String(hour).padStart(2, '0')}:00–${String(hour).padStart(2, '0')}:59`;
    if (!buckets[label]) buckets[label] = [];
    buckets[label].push(trade);
  }
  const out = {};
  for (const [label, rows] of Object.entries(buckets)) {
    const wins = rows.filter(isWin).length;
    out[label] = Object.freeze({
      trades: rows.length,
      winRate: rows.length ? round(wins / rows.length) : null,
    });
  }
  return Object.freeze(out);
}

function computeTrend(trades) {
  if (trades.length < 4) return 'Insufficient data';
  const sorted = [...trades].sort(
    (a, b) =>
      Date.parse(String(a.entryTime || a.capturedAt)) -
      Date.parse(String(b.entryTime || b.capturedAt))
  );
  const mid = Math.floor(sorted.length / 2);
  const first = sorted.slice(0, mid);
  const second = sorted.slice(mid);
  const firstRate = first.filter(isWin).length / first.length;
  const secondRate = second.filter(isWin).length / second.length;
  const delta = secondRate - firstRate;
  if (delta > 0.05) return 'Improving';
  if (delta < -0.05) return 'Degrading';
  return 'Stable';
}

function estimateReturn(trades) {
  if (!trades.length) return null;
  const wins = trades.filter(isWin).length;
  const losses = trades.length - wins;
  return round((wins - losses) / trades.length);
}

function createTradeAnalyzer() {
  return new TradeAnalyzer();
}

module.exports = {
  TradeAnalyzer,
  createTradeAnalyzer,
  isWin,
  isLoss,
  dayKey,
  groupBy,
};
