'use strict';

/**
 * Chronological token event timeline (ordered by occurredAt).
 *
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
 * @param {string} tokenAddress
 * @param {Date|string} [startTime]
 * @param {Date|string} [endTime]
 */
function getTokenTimeline(store, tokenAddress, startTime, endTime) {
  return store.getEventsForToken(tokenAddress, { startTime, endTime });
}

module.exports = {
  getTokenTimeline,
};
