'use strict';

const { BRIEFING_PERIODS } = require('../BriefingTypes');

const MS_DAY = 24 * 60 * 60 * 1000;

/**
 * Resolve digest window from period + asOf.
 * Does not compute insight — only clock arithmetic.
 *
 * @param {object} input
 * @param {string} [input.period='daily']
 * @param {string} [input.asOf]
 * @param {string} [input.periodStart]
 * @param {string} [input.periodEnd]
 */
function resolvePeriodWindow(input = {}) {
  const asOfIso = input.asOf || new Date().toISOString();
  const asOfMs = Date.parse(asOfIso);
  if (!Number.isFinite(asOfMs)) {
    throw new Error(`Invalid asOf timestamp: ${input.asOf}`);
  }

  if (input.periodStart && input.periodEnd) {
    const startMs = Date.parse(input.periodStart);
    const endMs = Date.parse(input.periodEnd);
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
      throw new Error('Invalid periodStart/periodEnd');
    }
    if (startMs > endMs) {
      throw new Error('periodStart must be <= periodEnd');
    }
    return {
      period: input.period || 'custom',
      asOf: new Date(asOfMs).toISOString(),
      start: new Date(startMs).toISOString(),
      end: new Date(endMs).toISOString(),
      startMs,
      endMs,
    };
  }

  const period = normalizePeriod(input.period);
  const durationMs = periodDurationMs(period);
  const endMs = asOfMs;
  const startMs = asOfMs - durationMs;

  return {
    period,
    asOf: new Date(asOfMs).toISOString(),
    start: new Date(startMs).toISOString(),
    end: new Date(endMs).toISOString(),
    startMs,
    endMs,
  };
}

/**
 * @param {string} [period]
 */
function normalizePeriod(period) {
  const p = String(period || BRIEFING_PERIODS.DAILY).toLowerCase();
  if (
    p !== BRIEFING_PERIODS.DAILY &&
    p !== BRIEFING_PERIODS.WEEKLY &&
    p !== BRIEFING_PERIODS.MONTHLY
  ) {
    throw new Error(`Unsupported briefing period: ${period}`);
  }
  return p;
}

/**
 * @param {string} period
 */
function periodDurationMs(period) {
  if (period === BRIEFING_PERIODS.WEEKLY) return 7 * MS_DAY;
  if (period === BRIEFING_PERIODS.MONTHLY) return 30 * MS_DAY;
  return MS_DAY;
}

/**
 * Pick baseline + latest snapshots for a period window.
 * Baseline = latest snapshot at or before period start (else first in/near window).
 * Latest = latest snapshot at or before period end.
 *
 * @param {object[]} snapshots - chronological ascending
 * @param {{ startMs: number, endMs: number }} window
 */
function selectPeriodSnapshots(snapshots, window) {
  const rows = snapshots || [];
  if (rows.length === 0) {
    return { baseline: null, latest: null, inWindow: [] };
  }

  const inWindow = rows.filter((s) => {
    const t = Date.parse(s.timestamp);
    return t >= window.startMs && t <= window.endMs;
  });

  let latest = null;
  for (const s of rows) {
    const t = Date.parse(s.timestamp);
    if (t <= window.endMs) latest = s;
  }

  let baseline = null;
  for (const s of rows) {
    const t = Date.parse(s.timestamp);
    if (t <= window.startMs) baseline = s;
  }
  // If nothing before window, use first snapshot at/before end as baseline
  // so period motion is relative to earliest available in/before window.
  if (!baseline) {
    for (const s of rows) {
      const t = Date.parse(s.timestamp);
      if (t <= window.endMs) {
        baseline = s;
        break;
      }
    }
  }

  return { baseline, latest, inWindow };
}

module.exports = {
  resolvePeriodWindow,
  normalizePeriod,
  periodDurationMs,
  selectPeriodSnapshots,
  MS_DAY,
};
