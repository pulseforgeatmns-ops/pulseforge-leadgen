'use strict';

const { randomUUID } = require('crypto');
const { SOURCE_PERFORMANCE_VERSION } = require('../types');
const { DEFAULT_RESEARCH_CONFIG } = require('../config/defaultConfig');

function median(values) {
  const nums = values.filter(v => typeof v === 'number' && Number.isFinite(v)).sort((a, b) => a - b);
  if (!nums.length) return null;
  const mid = Math.floor(nums.length / 2);
  return nums.length % 2 ? nums[mid] : (nums[mid - 1] + nums[mid]) / 2;
}

/**
 * Build source performance from completed research observations (FIRST_CALLER trigger sources).
 *
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
 * @param {string} sourceId
 * @param {Date|string} asOf
 */
function computeSourcePerformanceAsOf(store, sourceId, asOf, config = {}) {
  const cfg = { ...DEFAULT_RESEARCH_CONFIG, ...config };
  const asOfMs = new Date(asOf).getTime();

  const observations = store.researchObservations.filter(o => {
    if (o.observationType !== 'FIRST_CALLER') return false;
    if (new Date(o.occurredAt).getTime() >= asOfMs) return false;
    const sourceIdInMeta = o.metadata?.sourceId;
    const triggerSources = (o.triggerEventIds || [])
      .map(id => store.events.find(e => e.id === id))
      .filter(Boolean)
      .map(e => e.sourceId);
    return sourceIdInMeta === sourceId || triggerSources.includes(sourceId);
  });

  const labels = [];
  const mfes = [];
  const maes = [];
  const timeTo2x = [];

  for (const obs of observations) {
    const outcome = pickCanonicalOutcome(store, obs.id, cfg.defaultEvaluationDelaySeconds);
    if (!outcome || !outcome.label) continue;
    labels.push(outcome.label);
    if (outcome.mfe != null) mfes.push(outcome.mfe);
    if (outcome.mae != null) maes.push(outcome.mae);
    if (outcome.timeTo2xSeconds != null) timeTo2x.push(outcome.timeTo2xSeconds);
  }

  const passCount = labels.filter(l => l === 'PASS').length;
  const failCount = labels.filter(l => l === 'FAIL').length;
  const unresolvedCount = labels.filter(l => l === 'UNRESOLVED').length;
  const resolved = passCount + failCount;
  const sampleSize = labels.length;

  if (sampleSize < cfg.minSourceQualitySampleSize) {
    return {
      sourceId,
      asOf: new Date(asOf),
      sampleSize,
      passRate: null,
      failRate: null,
      medianMfe: null,
      medianMae: null,
      medianTimeTo2xSeconds: null,
      score: null,
      version: SOURCE_PERFORMANCE_VERSION,
      metadata: { passCount, failCount, unresolvedCount, quality: 'unavailable' },
    };
  }

  const passRate = resolved ? passCount / resolved : null;
  const failRate = resolved ? failCount / resolved : null;
  const score = passRate != null ? Math.max(0, Math.min(1, passRate * 0.7 + (median(mfes) || 0) * 0.3)) : null;

  return {
    sourceId,
    asOf: new Date(asOf),
    sampleSize,
    passRate,
    failRate,
    medianMfe: median(mfes),
    medianMae: median(maes),
    medianTimeTo2xSeconds: median(timeTo2x),
    score,
    version: SOURCE_PERFORMANCE_VERSION,
    metadata: { passCount, failCount, unresolvedCount },
  };
}

function pickCanonicalOutcome(store, observationId, delaySeconds) {
  const rows = store.researchObservationOutcomes.filter(o => o.observationId === observationId);
  return (
    rows.find(r => r.executionDelaySeconds === delaySeconds) ||
    rows.sort((a, b) => a.executionDelaySeconds - b.executionDelaySeconds)[0] ||
    null
  );
}

function refreshAllSourcePerformance(store, asOf, config = {}) {
  const sourceIds = [...store.sources.keys()];
  const records = sourceIds.map(id => computeSourcePerformanceAsOf(store, id, asOf, config));
  for (const record of records) {
    if (record.score != null) {
      store.upsertSourcePerformance({ id: randomUUID(), ...record });
    }
  }
  return records;
}

module.exports = {
  computeSourcePerformanceAsOf,
  refreshAllSourcePerformance,
  median,
};
