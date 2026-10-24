'use strict';

const { DEFAULT_OUTCOME_CONFIG } = require('../config/defaultConfig');

/**
 * Label PASS/FAIL/UNRESOLVED from price path after observation time.
 * Uses only prices at or after observedAt (no hindsight before P0).
 *
 * @param {object} args
 * @param {number} args.entryPrice — P0
 * @param {Date|string} args.observedAt
 * @param {{ occurredAt: Date|string, price: number }[]} args.pricePath — sorted by occurredAt
 * @param {Partial<typeof DEFAULT_OUTCOME_CONFIG>} [args.config]
 */
function labelMarketOutcome({ entryPrice, observedAt, pricePath, config = {} }) {
  const cfg = { ...DEFAULT_OUTCOME_CONFIG, ...config };
  const p0 = Number(entryPrice);
  if (!Number.isFinite(p0) || p0 <= 0) {
    throw new Error('entryPrice must be a positive number');
  }

  const observedMs = new Date(observedAt).getTime();
  const horizonMs = cfg.horizonHours * 60 * 60 * 1000;
  const passPrice = p0 * cfg.passMultiple;
  const failPrice = p0 * cfg.failMultiple;

  const path = [...pricePath]
    .map(p => ({ occurredAt: new Date(p.occurredAt), price: Number(p.price) }))
    .filter(p => p.occurredAt.getTime() >= observedMs && Number.isFinite(p.price))
    .sort((a, b) => a.occurredAt - b.occurredAt);

  let label = 'UNRESOLVED';
  let mfe = 0;
  let mae = 0;
  let timeTo2x = null;
  let timeToMinus30 = null;

  for (const point of path) {
    const ret = point.price / p0 - 1;
    mfe = Math.max(mfe, ret);
    mae = Math.min(mae, ret);

    if (timeTo2x == null && point.price >= passPrice) {
      timeTo2x = (point.occurredAt.getTime() - observedMs) / 1000;
    }
    if (timeToMinus30 == null && point.price <= failPrice) {
      timeToMinus30 = (point.occurredAt.getTime() - observedMs) / 1000;
    }

    const hitPass = point.price >= passPrice;
    const hitFail = point.price <= failPrice;
    if (hitPass && (timeToMinus30 == null || timeTo2x <= timeToMinus30)) {
      label = 'PASS';
      break;
    }
    if (hitFail && (timeTo2x == null || timeToMinus30 < timeTo2x)) {
      label = 'FAIL';
      break;
    }

    if (point.occurredAt.getTime() - observedMs > horizonMs) {
      break;
    }
  }

  // Re-evaluate ordering: PASS only if +100% before -30%
  if (timeTo2x != null && timeToMinus30 != null) {
    label = timeTo2x < timeToMinus30 ? 'PASS' : 'FAIL';
  } else if (timeTo2x != null) {
    label = 'PASS';
  } else if (timeToMinus30 != null) {
    label = 'FAIL';
  } else {
    label = 'UNRESOLVED';
  }

  const returns = computeHorizonReturns(p0, path, observedMs);

  return {
    label,
    mfe,
    mae,
    timeTo2xSeconds: timeTo2x,
    timeToMinus30Seconds: timeToMinus30,
    ...returns,
    horizonHours: cfg.horizonHours,
  };
}

function computeHorizonReturns(p0, path, observedMs) {
  const pickReturn = minutes => {
    const target = observedMs + minutes * 60 * 1000;
    const point = path.find(p => p.occurredAt.getTime() >= target);
    if (!point) return null;
    return point.price / p0 - 1;
  };
  return {
    return15m: pickReturn(15),
    return1h: pickReturn(60),
    return6h: pickReturn(360),
    return24h: pickReturn(1440),
  };
}

module.exports = {
  labelMarketOutcome,
};
