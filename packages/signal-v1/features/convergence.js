'use strict';

const CALL_EVENT_TYPES = new Set(['CALL', 'TOKEN_MENTION', 'AMPLIFIER_ENTRY']);

/**
 * Decision-time convergence metrics for a token.
 *
 * @param {object} args
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} args.store
 * @param {string} args.tokenAddress
 * @param {Date|string} args.evaluatedAt
 * @param {number} args.windowMinutes
 */
function calculateConvergence({ store, tokenAddress, evaluatedAt, windowMinutes, events: preloadedEvents }) {
  const evaluatedMs = new Date(evaluatedAt).getTime();
  const windowStartMs = evaluatedMs - windowMinutes * 60 * 1000;

  const baseEvents =
    preloadedEvents ||
    store.getEventsForToken(tokenAddress, { maxOccurredAt: evaluatedAt });

  const events = baseEvents.filter(
      e =>
        CALL_EVENT_TYPES.has(e.eventType) &&
        e.occurredAt.getTime() >= windowStartMs &&
        e.occurredAt.getTime() <= evaluatedMs
    );

  const rawSourceIds = [];
  const clusterIds = new Set();
  const independentSourceIds = new Set();
  const sourceFirstSeen = new Map();

  for (const event of events) {
    if (event.sourceId) {
      rawSourceIds.push(event.sourceId);
      const clusterId =
        event.sourceClusterId || store.getClusterIdForSource(event.sourceId) || event.sourceId;
      clusterIds.add(clusterId);
      if (!independentSourceIds.has(clusterId)) {
        independentSourceIds.add(clusterId);
        sourceFirstSeen.set(clusterId, event.occurredAt.getTime());
      }
    }
  }

  const uniqueSourceCount = new Set(rawSourceIds.filter(Boolean)).size;
  const independentClusterCount = clusterIds.size;

  let qualityWeightedConvergence = 0;
  for (const sourceId of new Set(rawSourceIds.filter(Boolean))) {
    const perf = store.getSourcePerformance(sourceId, evaluatedAt);
    const quality = perf && typeof perf.score === 'number' ? perf.score : 0.5;
    qualityWeightedConvergence += quality;
  }

  const independentTimes = [...sourceFirstSeen.values()].sort((a, b) => a - b);
  let convergenceVelocity = null;
  if (independentTimes.length >= 2) {
    const spanMinutes = (independentTimes[independentTimes.length - 1] - independentTimes[0]) / 60000;
    convergenceVelocity = spanMinutes > 0 ? independentClusterCount / spanMinutes : independentClusterCount;
  }

  return {
    windowMinutes,
    rawSourceCount: rawSourceIds.length,
    uniqueSourceCount,
    independentClusterCount,
    qualityWeightedConvergence: qualityWeightedConvergence || null,
    convergenceVelocity,
    evidenceEventIds: events.map(e => e.id),
  };
}

module.exports = {
  calculateConvergence,
  CALL_EVENT_TYPES,
};
