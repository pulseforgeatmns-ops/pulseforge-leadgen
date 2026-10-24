'use strict';

/**
 * First valid market price at or after observationAt + delaySeconds.
 * Does not use candle lows or best-price lookahead.
 *
 * @param {Date|string} observationAt
 * @param {number} delaySeconds
 * @param {{ occurredAt: Date|string, price: number }[]} pricePath
 * @returns {{ price: number|null, priceAt: Date|null }}
 */
function resolveAchievableObservationPrice(observationAt, delaySeconds, pricePath) {
  const targetMs = new Date(observationAt).getTime() + delaySeconds * 1000;
  const path = [...(pricePath || [])]
    .map(p => ({ occurredAt: new Date(p.occurredAt), price: Number(p.price) }))
    .filter(p => Number.isFinite(p.price))
    .sort((a, b) => a.occurredAt - b.occurredAt);

  for (const point of path) {
    if (point.occurredAt.getTime() >= targetMs) {
      return { price: point.price, priceAt: point.occurredAt };
    }
  }
  return { price: null, priceAt: null };
}

module.exports = {
  resolveAchievableObservationPrice,
};
