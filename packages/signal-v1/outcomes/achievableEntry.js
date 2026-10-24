'use strict';

/**
 * First market observation at or after decision + execution delay (never backfill earlier).
 *
 * @param {Date|string} decisionAt
 * @param {number} executionDelaySeconds
 * @param {{ occurredAt: Date|string, priceUsd: number, intervalSeconds?: number }[]} observations — sorted ascending
 */
function resolveAchievableEntry(decisionAt, executionDelaySeconds, observations) {
  const decisionMs = new Date(decisionAt).getTime();
  const targetMs = decisionMs + executionDelaySeconds * 1000;

  const sorted = [...observations].sort(
    (a, b) => new Date(a.occurredAt).getTime() - new Date(b.occurredAt).getTime()
  );
  for (const obs of sorted) {
    const t = new Date(obs.occurredAt).getTime();
    if (t >= targetMs && Number.isFinite(Number(obs.priceUsd)) && Number(obs.priceUsd) > 0) {
      return {
        decisionAt: new Date(decisionAt),
        executionDelaySeconds,
        effectiveExecutionAt: new Date(obs.occurredAt),
        effectivePrice: Number(obs.priceUsd),
        dataResolutionSeconds: obs.intervalSeconds ?? null,
      };
    }
  }

  return null;
}

module.exports = {
  resolveAchievableEntry,
};
