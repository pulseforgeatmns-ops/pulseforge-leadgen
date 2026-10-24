'use strict';

const { labelMarketOutcome } = require('../outcomes/marketOutcomes');
const { DEFAULT_RESEARCH_CONFIG, DEFAULT_OUTCOME_CONFIG } = require('../config/defaultConfig');
const { resolveAchievableObservationPrice } = require('./achievablePrice');

/**
 * @param {object} args
 * @param {Date|string} args.observationAt
 * @param {{ occurredAt: Date|string, price: number }[]} args.pricePath
 * @param {number[]} [args.delaysSeconds]
 */
function evaluateObservationOutcomes({ observationAt, pricePath, delaysSeconds, outcomeConfig = {} }) {
  const delays = delaysSeconds || DEFAULT_RESEARCH_CONFIG.executionDelaySeconds;
  const cfg = { ...DEFAULT_OUTCOME_CONFIG, ...outcomeConfig };
  const results = [];

  for (const delaySeconds of delays) {
    const { price, priceAt } = resolveAchievableObservationPrice(
      observationAt,
      delaySeconds,
      pricePath
    );
    const availability = classifyDataAvailability(observationAt, pricePath, price, cfg.horizonHours);

    if (price == null || availability === 'INSUFFICIENT_MARKET_DATA' || availability === 'UNAVAILABLE') {
      results.push({
        executionDelaySeconds: delaySeconds,
        dataAvailability: availability === 'AVAILABLE' ? 'INSUFFICIENT_MARKET_DATA' : availability,
        entryPrice: null,
        label: null,
        mfe: null,
        mae: null,
        timeTo2xSeconds: null,
        timeToMinus30Seconds: null,
        return15m: null,
        return1h: null,
        return6h: null,
        return24h: null,
        horizonHours: cfg.horizonHours,
        metadata: { priceAt: priceAt ? priceAt.toISOString() : null },
      });
      continue;
    }

    const outcome = labelMarketOutcome({
      entryPrice: price,
      observedAt: priceAt || observationAt,
      pricePath,
      config: cfg,
    });

    results.push({
      executionDelaySeconds: delaySeconds,
      dataAvailability: availability,
      entryPrice: price,
      label: outcome.label,
      mfe: outcome.mfe,
      mae: outcome.mae,
      timeTo2xSeconds: outcome.timeTo2xSeconds,
      timeToMinus30Seconds: outcome.timeToMinus30Seconds,
      return15m: outcome.return15m,
      return1h: outcome.return1h,
      return6h: outcome.return6h,
      return24h: outcome.return24h,
      horizonHours: outcome.horizonHours,
      metadata: { priceAt: priceAt.toISOString(), p0DelaySeconds: delaySeconds },
    });
  }

  return results;
}

function classifyDataAvailability(observationAt, pricePath, entryPrice, horizonHours) {
  if (!pricePath || !pricePath.length) return 'UNAVAILABLE';
  if (entryPrice == null) return 'INSUFFICIENT_MARKET_DATA';

  const observedMs = new Date(observationAt).getTime();
  const horizonMs = horizonHours * 60 * 60 * 1000;
  const horizonEnd = observedMs + horizonMs;

  const points = pricePath
    .map(p => ({ t: new Date(p.occurredAt).getTime(), price: Number(p.price) }))
    .filter(p => Number.isFinite(p.price) && p.t >= observedMs)
    .sort((a, b) => a.t - b.t);

  if (!points.length) return 'INSUFFICIENT_MARKET_DATA';

  const lastT = points[points.length - 1].t;
  if (lastT >= horizonEnd) return 'AVAILABLE';
  if (lastT > observedMs + 15 * 60 * 1000) return 'PARTIAL';
  return 'INSUFFICIENT_MARKET_DATA';
}

module.exports = {
  evaluateObservationOutcomes,
  classifyDataAvailability,
};
