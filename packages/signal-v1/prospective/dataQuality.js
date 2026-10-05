'use strict';

const { median } = require('../research/sourcePerformanceEngine');
const { ingestionLatencyMs } = require('./knowledgeClock');

function computeProspectiveDataQuality(store, { since } = {}) {
  const sinceMs = since ? new Date(since).getTime() : 0;
  const events = (store.events || []).filter(
    e => e.eventType === 'CALL' && e.ingestedAt.getTime() >= sinceMs
  );
  const latencies = events.map(e => ingestionLatencyMs(e.occurredAt, e.ingestedAt)).filter(n => Number.isFinite(n));
  latencies.sort((a, b) => a - b);

  const rawEvidence = (store.rawCallerEvidence || []).filter(r => new Date(r.ingestedAt).getTime() >= sinceMs);
  const parseFailures = rawEvidence.filter(r => !r.extractedCa).length;

  const uniqueTokens = new Set(events.map(e => e.tokenAddress)).size;

  let marketAttempts = 0;
  let marketSuccess = 0;
  for (const obs of store.researchObservations || []) {
    if (obs.metadata?.marketCaptureFailed) marketAttempts += 1;
    if (obs.metadata?.marketSnapshot) {
      marketAttempts += 1;
      marketSuccess += 1;
    }
  }

  const jobs = store.prospectiveJobs || [];
  const pendingOutcomes = jobs.filter(j => j.jobType === 'OUTCOME_24H' && j.status === 'PENDING_OUTCOME').length;
  const completedOutcomes = jobs.filter(j => j.jobType === 'OUTCOME_24H' && j.status === 'COMPLETE').length;

  const unknownClusterEvents = events.filter(e => {
    const clusterId = e.sourceClusterId || store.getClusterIdForSource(e.sourceId);
    return !clusterId;
  }).length;

  const collectors = store.collectorHealth || {};

  return {
    callsIngested: events.length,
    uniqueTokens,
    caParseFailures: parseFailures,
    duplicateRate: null,
    medianIngestionLatencyMs: median(latencies),
    p95IngestionLatencyMs: percentile(latencies, 0.95),
    marketCaptureSuccessRate: marketAttempts ? marketSuccess / marketAttempts : null,
    pendingOutcomes24h: pendingOutcomes,
    completedOutcomes24h: completedOutcomes,
    unknownClusterRate: events.length ? unknownClusterEvents / events.length : null,
    provenanceContamination: 0,
    collectors,
    activeSourceCount: store.sourceRegistry ? store.sourceRegistry.size : null,
  };
}

function percentile(sorted, p) {
  if (!sorted.length) return null;
  const idx = Math.min(sorted.length - 1, Math.floor(p * sorted.length));
  return sorted[idx];
}

module.exports = {
  computeProspectiveDataQuality,
};
