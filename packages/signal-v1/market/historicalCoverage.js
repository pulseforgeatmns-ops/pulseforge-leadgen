'use strict';

/**
 * @typedef {'AVAILABLE' | 'PARTIAL' | 'UNAVAILABLE' | 'INVALID_RANGE'} HistoricalDataStatus
 */

/**
 * @typedef {'COMPLETE' | 'NO_ENTRY' | 'INSUFFICIENT_MARKET_DATA' | 'HISTORICAL_DATA_UNAVAILABLE' | 'ENTRY_BUT_OUTCOME_UNRESOLVED'} ReplayStatus
 */

const HISTORICAL_DATA_UNAVAILABLE = 'HISTORICAL_DATA_UNAVAILABLE';

/**
 * @param {object} args
 * @param {Date|string} args.requestedStart
 * @param {Date|string} args.requestedEnd
 * @param {{ occurredAt: Date|string }[]} args.observations
 * @param {Date|string} [args.decisionAnchor] — event / call time; replay needs obs at or after this
 * @param {number} [args.intervalSeconds]
 */
function validateHistoricalCoverage({
  requestedStart,
  requestedEnd,
  observations,
  decisionAnchor,
  intervalSeconds = 60,
}) {
  const start = new Date(requestedStart);
  const end = new Date(requestedEnd);
  if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime()) || end <= start) {
    return buildCoverageResult({
      status: 'INVALID_RANGE',
      requestedStart: start,
      requestedEnd: end,
      observations: observations || [],
      intervalSeconds,
      decisionAnchor,
    });
  }

  const obs = (observations || [])
    .map(o => ({ ...o, occurredAt: new Date(o.occurredAt) }))
    .filter(o => Number.isFinite(o.occurredAt.getTime()))
    .sort((a, b) => a.occurredAt - b.occurredAt);

  if (!obs.length) {
    return buildCoverageResult({
      status: 'UNAVAILABLE',
      requestedStart: start,
      requestedEnd: end,
      observations: obs,
      intervalSeconds,
      decisionAnchor,
      reason: 'Provider returned no observations for requested historical event window',
    });
  }

  const startMs = start.getTime();
  const endMs = end.getTime();
  const inWindow = obs.filter(o => {
    const t = o.occurredAt.getTime();
    return t >= startMs && t <= endMs;
  });

  const providerEarliest = obs[0].occurredAt;
  const providerLatest = obs[obs.length - 1].occurredAt;

  if (!inWindow.length) {
    return buildCoverageResult({
      status: 'UNAVAILABLE',
      requestedStart: start,
      requestedEnd: end,
      observations: obs,
      intervalSeconds,
      decisionAnchor,
      providerEarliest,
      providerLatest,
      reason: 'Provider observations fall entirely outside the requested historical event window',
    });
  }

  const firstInWindow = inWindow[0].occurredAt;
  const lastInWindow = inWindow[inWindow.length - 1].occurredAt;

  const missingLeadingDurationMs = Math.max(0, firstInWindow.getTime() - startMs);
  const missingTrailingDurationMs = Math.max(0, endMs - lastInWindow.getTime());
  const missingInternalIntervals = countMissingInternalIntervals(
    inWindow,
    start,
    end,
    intervalSeconds
  );

  const hasGap =
    missingLeadingDurationMs > 0 ||
    missingTrailingDurationMs > 0 ||
    missingInternalIntervals > 0;

  let status = hasGap ? 'PARTIAL' : 'AVAILABLE';

  const anchor = decisionAnchor ? new Date(decisionAnchor) : null;
  let hasObservationAtOrAfterDecision = true;
  if (anchor && Number.isFinite(anchor.getTime())) {
    hasObservationAtOrAfterDecision = inWindow.some(
      o => o.occurredAt.getTime() >= anchor.getTime()
    );
    if (!hasObservationAtOrAfterDecision && status !== 'UNAVAILABLE') {
      status = 'PARTIAL';
    }
  }

  return buildCoverageResult({
    status,
    requestedStart: start,
    requestedEnd: end,
    observations: inWindow,
    intervalSeconds,
    decisionAnchor: anchor,
    providerEarliest,
    providerLatest,
    missingLeadingDurationMs,
    missingTrailingDurationMs,
    missingInternalIntervals,
    hasObservationAtOrAfterDecision,
  });
}

function buildCoverageResult(fields) {
  const {
    status,
    requestedStart,
    requestedEnd,
    observations,
    intervalSeconds,
    decisionAnchor,
    providerEarliest,
    providerLatest,
    missingLeadingDurationMs = 0,
    missingTrailingDurationMs = 0,
    missingInternalIntervals = 0,
    hasObservationAtOrAfterDecision = true,
    reason,
  } = fields;

  const obs = observations || [];
  const earliest = providerEarliest || (obs[0] && obs[0].occurredAt) || null;
  const latest = providerLatest || (obs[obs.length - 1] && obs[obs.length - 1].occurredAt) || null;

  return {
    status,
    requestedStart: requestedStart.toISOString(),
    requestedEnd: requestedEnd.toISOString(),
    providerEarliestObservation: earliest ? earliest.toISOString() : null,
    providerLatestObservation: latest ? latest.toISOString() : null,
    observationCountInWindow: obs.length,
    missingLeadingDurationMs,
    missingTrailingDurationMs,
    missingInternalIntervals,
    missingLeadingDuration: formatDuration(missingLeadingDurationMs),
    missingTrailingDuration: formatDuration(missingTrailingDurationMs),
    decisionAnchor:
      decisionAnchor && Number.isFinite(new Date(decisionAnchor).getTime())
        ? new Date(decisionAnchor).toISOString()
        : null,
    hasObservationAtOrAfterDecision,
    intervalSeconds,
    reason: reason || null,
  };
}

function countMissingInternalIntervals(observations, startTime, endTime, intervalSeconds) {
  if (!observations.length || !intervalSeconds) return 0;
  const set = new Set(
    observations.map(o => bucketKey(o.occurredAt, intervalSeconds))
  );
  const startMs = new Date(startTime).getTime();
  const endMs = new Date(endTime).getTime();
  let missing = 0;
  for (let t = startMs; t <= endMs; t += intervalSeconds * 1000) {
    const key = bucketKey(new Date(t), intervalSeconds);
    if (!set.has(key)) missing += 1;
  }
  return missing;
}

function bucketKey(date, intervalSeconds) {
  const ms = new Date(date).getTime();
  return String(Math.floor(ms / (intervalSeconds * 1000)));
}

function formatDuration(ms) {
  if (!ms || ms <= 0) return 'PT0S';
  const sec = Math.round(ms / 1000);
  if (sec < 60) return `PT${sec}S`;
  const min = Math.floor(sec / 60);
  const rem = sec % 60;
  return rem ? `PT${min}M${rem}S` : `PT${min}M`;
}

function buildHistoricalUnavailablePayload({
  tokenAddress,
  provider,
  coverage,
}) {
  return {
    error: HISTORICAL_DATA_UNAVAILABLE,
    tokenAddress,
    requestedStart: coverage.requestedStart,
    requestedEnd: coverage.requestedEnd,
    provider: provider || 'unknown',
    providerEarliestObservation: coverage.providerEarliestObservation,
    providerLatestObservation: coverage.providerLatestObservation,
    status: coverage.status,
    reason:
      coverage.reason ||
      'Provider does not expose observations for requested historical event window',
  };
}

/**
 * @param {object} args
 * @param {import('./historicalCoverage').ReplayStatus} [args.replayStatus]
 * @param {object} [args.coverage]
 * @param {boolean} args.hadEntry
 * @param {boolean} args.hadResolvableOutcomes
 */
function resolveReplayStatus({ coverage, hadEntry, hadResolvableOutcomes, replayRan }) {
  if (!replayRan) {
    return 'HISTORICAL_DATA_UNAVAILABLE';
  }
  if (coverage) {
    if (coverage.status === 'UNAVAILABLE' || coverage.status === 'INVALID_RANGE') {
      return 'HISTORICAL_DATA_UNAVAILABLE';
    }
    if (!coverage.hasObservationAtOrAfterDecision) {
      return 'INSUFFICIENT_MARKET_DATA';
    }
  }
  if (!hadEntry) {
    return 'NO_ENTRY';
  }
  if (!hadResolvableOutcomes) {
    return 'ENTRY_BUT_OUTCOME_UNRESOLVED';
  }
  return 'COMPLETE';
}

module.exports = {
  HISTORICAL_DATA_UNAVAILABLE,
  validateHistoricalCoverage,
  buildHistoricalUnavailablePayload,
  resolveReplayStatus,
  countMissingInternalIntervals,
};
