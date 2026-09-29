'use strict';

const OBSERVED_INTEREST_LEVELS = Object.freeze(['low', 'medium', 'high']);

function normalizeObservedInterestLevel(value) {
  if (value == null || value === '') return null;
  const normalized = String(value).trim().toLowerCase();
  return OBSERVED_INTEREST_LEVELS.includes(normalized) ? normalized : null;
}

function hasObservedInterest(value) {
  return normalizeObservedInterestLevel(value) != null;
}

function observedInterestRankWeight(value) {
  const observed = normalizeObservedInterestLevel(value);
  if (!observed) return 0;
  return { high: 3, medium: 2, low: 1 }[observed];
}

module.exports = {
  OBSERVED_INTEREST_LEVELS,
  normalizeObservedInterestLevel,
  hasObservedInterest,
  observedInterestRankWeight,
};
