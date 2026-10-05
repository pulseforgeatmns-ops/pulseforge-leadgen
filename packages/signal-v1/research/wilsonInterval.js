'use strict';

/**
 * Wilson score interval for a binomial proportion (95% default).
 *
 * @param {number} successes
 * @param {number} trials
 * @param {number} [z] — normal z for confidence level (1.96 ≈ 95%)
 */
function wilsonInterval(successes, trials, z = 1.96) {
  if (trials <= 0) {
    return { n: 0, successes: 0, lower: null, upper: null, pointEstimate: null };
  }
  const p = successes / trials;
  const z2 = z * z;
  const denom = 1 + z2 / trials;
  const center = (p + z2 / (2 * trials)) / denom;
  const margin =
    (z * Math.sqrt((p * (1 - p)) / trials + z2 / (4 * trials * trials))) / denom;
  return {
    n: trials,
    successes,
    lower: Math.max(0, center - margin),
    upper: Math.min(1, center + margin),
    pointEstimate: p,
  };
}

module.exports = {
  wilsonInterval,
};
