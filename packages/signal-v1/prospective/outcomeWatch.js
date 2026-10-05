'use strict';

const { createHash } = require('crypto');
const { PROSPECTIVE_FROZEN_HYPOTHESIS, JOB_STATUS } = require('./constants');
const { resolveAchievableObservationPrice } = require('../research/achievablePrice');
const { labelMarketOutcome } = require('../outcomes/marketOutcomes');
const { DEFAULT_OUTCOME_CONFIG } = require('../config/defaultConfig');

function deterministicJobId(seed) {
  return createHash('sha256').update(seed).digest('hex').slice(0, 32);
}

function scheduleDelayCaptureJobs(observation, knowledgeAtIso) {
  const jobs = [];
  for (const delaySeconds of PROSPECTIVE_FROZEN_HYPOTHESIS.executionDelaySeconds) {
    const runAfter = new Date(new Date(knowledgeAtIso).getTime() + delaySeconds * 1000);
    jobs.push({
      id: deterministicJobId(`${observation.id}|delay|${delaySeconds}`),
      tokenAddress: observation.tokenAddress,
      observationId: observation.id,
      jobType: 'DELAY_CAPTURE',
      status: JOB_STATUS.PENDING_DELAY_CAPTURE,
      runAfter,
      targetDelaySeconds: delaySeconds,
      payload: { knowledgeAt: knowledgeAtIso },
    });
  }
  return jobs;
}

function scheduleOutcomeJob(observation, knowledgeAtIso) {
  const horizonMs = PROSPECTIVE_FROZEN_HYPOTHESIS.horizonHours * 60 * 60 * 1000;
  return {
    id: deterministicJobId(`${observation.id}|outcome|24h`),
    tokenAddress: observation.tokenAddress,
    observationId: observation.id,
    jobType: 'OUTCOME_24H',
    status: JOB_STATUS.PENDING_OUTCOME,
    runAfter: new Date(new Date(knowledgeAtIso).getTime() + horizonMs),
    targetDelaySeconds: null,
    payload: { knowledgeAt: knowledgeAtIso, horizonHours: PROSPECTIVE_FROZEN_HYPOTHESIS.horizonHours },
  };
}

function buildPricePathFromObservations(observations) {
  return [...observations]
    .map(o => ({
      occurredAt: o.occurredAt,
      price: Number(o.priceUsd),
    }))
    .filter(p => Number.isFinite(p.price))
    .sort((a, b) => new Date(a.occurredAt) - new Date(b.occurredAt));
}

/**
 * Process delay capture using first real observation at or after target.
 */
function processDelayCaptureJob(job, observation, pricePath) {
  const knowledgeAt = job.payload?.knowledgeAt || observation.occurredAt;
  const delaySeconds = job.targetDelaySeconds;
  const { price, priceAt } = resolveAchievableObservationPrice(knowledgeAt, delaySeconds, pricePath);

  const metadata = {
    targetTime: new Date(new Date(knowledgeAt).getTime() + delaySeconds * 1000).toISOString(),
    actualObservationTime: priceAt ? priceAt.toISOString() : null,
    latencyMs: priceAt ? priceAt.getTime() - (new Date(knowledgeAt).getTime() + delaySeconds * 1000) : null,
    provider: pricePath.length ? 'market_observations' : null,
    availability: price != null ? 'AVAILABLE' : 'DATA_INSUFFICIENT',
  };

  return {
    executionDelaySeconds: delaySeconds,
    dataAvailability: price != null ? 'AVAILABLE' : 'INSUFFICIENT_MARKET_DATA',
    entryPrice: price,
    label: null,
    mfe: null,
    mae: null,
    timeTo2xSeconds: null,
    timeToMinus30Seconds: null,
    return15m: null,
    return1h: null,
    return6h: null,
    return24h: null,
    horizonHours: PROSPECTIVE_FROZEN_HYPOTHESIS.horizonHours,
    metadata,
    jobStatus: price != null ? JOB_STATUS.COMPLETE : JOB_STATUS.DATA_INSUFFICIENT,
  };
}

function processOutcomeJob(observation, pricePath, outcomeRowForPrimaryDelay) {
  const cfg = {
    ...DEFAULT_OUTCOME_CONFIG,
    horizonHours: PROSPECTIVE_FROZEN_HYPOTHESIS.horizonHours,
    passMultiple: PROSPECTIVE_FROZEN_HYPOTHESIS.passMultiple,
    failMultiple: PROSPECTIVE_FROZEN_HYPOTHESIS.failMultiple,
  };

  const primaryDelay = PROSPECTIVE_FROZEN_HYPOTHESIS.primaryExecutionDelaySeconds;
  let entryPrice = outcomeRowForPrimaryDelay?.entryPrice;
  let priceAt = outcomeRowForPrimaryDelay?.metadata?.actualObservationTime
    ? new Date(outcomeRowForPrimaryDelay.metadata.actualObservationTime)
    : observation.occurredAt;

  if (entryPrice == null) {
    const resolved = resolveAchievableObservationPrice(observation.occurredAt, primaryDelay, pricePath);
    entryPrice = resolved.price;
    priceAt = resolved.priceAt || observation.occurredAt;
  }

  if (entryPrice == null) {
    return {
      jobStatus: JOB_STATUS.DATA_INSUFFICIENT,
      outcomes: [],
    };
  }

  const labeled = labelMarketOutcome({
    entryPrice,
    observedAt: priceAt,
    pricePath,
    config: cfg,
  });

  const outcome = {
    executionDelaySeconds: primaryDelay,
    dataAvailability: 'AVAILABLE',
    entryPrice,
    label: labeled.label,
    mfe: labeled.mfe,
    mae: labeled.mae,
    timeTo2xSeconds: labeled.timeTo2xSeconds,
    timeToMinus30Seconds: labeled.timeToMinus30Seconds,
    return15m: labeled.return15m,
    return1h: labeled.return1h,
    return6h: labeled.return6h,
    return24h: labeled.return24h,
    horizonHours: cfg.horizonHours,
    metadata: {
      priceAt: priceAt.toISOString(),
      evaluatedAt: new Date().toISOString(),
    },
  };

  return {
    jobStatus: labeled.label === 'UNRESOLVED' ? JOB_STATUS.DATA_INSUFFICIENT : JOB_STATUS.COMPLETE,
    outcomes: [outcome],
  };
}

module.exports = {
  scheduleDelayCaptureJobs,
  scheduleOutcomeJob,
  buildPricePathFromObservations,
  processDelayCaptureJob,
  processOutcomeJob,
  deterministicJobId,
};
