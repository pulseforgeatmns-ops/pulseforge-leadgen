'use strict';

const { DEFAULT_OUTCOME_CONFIG } = require('../config/defaultConfig');
const { labelMarketOutcome } = require('./marketOutcomes');
const { resolveAchievableEntry } = require('./achievableEntry');

const DEFAULT_EXECUTION_DELAYS_SECONDS = Object.freeze([15, 30, 60, 180, 300]);

/**
 * @param {object} args
 * @param {Date|string} args.decisionAt — ENTRY decision timestamp
 * @param {{ occurredAt: Date|string, priceUsd: number, intervalSeconds?: number }[]} args.observations — full path (evaluator may use future)
 * @param {number[]} [args.executionDelaysSeconds]
 */
function evaluateEntryOutcomesForDelays({
  decisionAt,
  observations,
  executionDelaysSeconds = DEFAULT_EXECUTION_DELAYS_SECONDS,
  config = {},
}) {
  const path = observations.map(o => ({
    occurredAt: new Date(o.occurredAt),
    price: Number(o.priceUsd ?? o.price),
    intervalSeconds: o.intervalSeconds,
  }));

  return executionDelaysSeconds.map(delaySeconds => {
    const entry = resolveAchievableEntry(decisionAt, delaySeconds, observations);
    if (!entry) {
      return {
        executionDelaySeconds: delaySeconds,
        entry: null,
        outcome: null,
      };
    }

    const labeled = labelMarketOutcome({
      entryPrice: entry.effectivePrice,
      observedAt: entry.effectiveExecutionAt,
      pricePath: path,
      config,
    });

    const horizonMs = (config.horizonHours ?? DEFAULT_OUTCOME_CONFIG.horizonHours) * 3600 * 1000;
    const startMs = entry.effectiveExecutionAt.getTime();
    const window = path.filter(
      p =>
        p.occurredAt.getTime() >= startMs &&
        p.occurredAt.getTime() <= startMs + horizonMs &&
        Number.isFinite(p.price)
    );

    let highestPrice = entry.effectivePrice;
    let lowestPrice = entry.effectivePrice;
    for (const p of window) {
      highestPrice = Math.max(highestPrice, p.price);
      lowestPrice = Math.min(lowestPrice, p.price);
    }

    let outcomeTimestamp = null;
    if (labeled.label === 'PASS' && labeled.timeTo2xSeconds != null) {
      outcomeTimestamp = new Date(startMs + labeled.timeTo2xSeconds * 1000);
    } else if (labeled.label === 'FAIL' && labeled.timeToMinus30Seconds != null) {
      outcomeTimestamp = new Date(startMs + labeled.timeToMinus30Seconds * 1000);
    }

    return {
      executionDelaySeconds: delaySeconds,
      entry,
      outcome: {
        ...labeled,
        highestPrice,
        lowestPrice,
        outcomeTimestamp,
      },
    };
  });
}

module.exports = {
  DEFAULT_EXECUTION_DELAYS_SECONDS,
  evaluateEntryOutcomesForDelays,
};
