'use strict';

const { DEFAULT_RESEARCH_CONFIG } = require('../config/defaultConfig');
const { SOURCE_PERFORMANCE_VERSION } = require('../types');

/**
 * Source quality as-of timestamp T — no future performance.
 *
 * @returns {{ quality: 'unavailable' | number, sampleSize: number, passRate: number|null, failRate: number|null }}
 */
function getSourceQualityAsOf(store, sourceId, asOf, config = {}) {
  const cfg = { ...DEFAULT_RESEARCH_CONFIG, ...config };
  if (!sourceId) {
    return unavailableQuality();
  }
  const perf = store.getSourcePerformance(sourceId, asOf);
  if (!perf || perf.version !== SOURCE_PERFORMANCE_VERSION) {
    return unavailableQuality();
  }
  if ((perf.sampleSize || 0) < cfg.minSourceQualitySampleSize) {
    return { ...unavailableQuality(), sampleSize: perf.sampleSize || 0 };
  }
  if (typeof perf.score !== 'number' || !Number.isFinite(perf.score)) {
    return { ...unavailableQuality(), sampleSize: perf.sampleSize || 0 };
  }
  return {
    quality: perf.score,
    sampleSize: perf.sampleSize,
    passRate: perf.passRate ?? null,
    failRate: perf.failRate ?? null,
  };
}

function unavailableQuality() {
  return {
    quality: 'unavailable',
    sampleSize: 0,
    passRate: null,
    failRate: null,
  };
}

module.exports = {
  getSourceQualityAsOf,
  unavailableQuality,
};
