'use strict';

/**
 * Temporal firewall — feature/scoring must never see future market data.
 */

function filterEventsAtOrBefore(events, at) {
  const maxMs = new Date(at).getTime();
  return events.filter(e => e.occurredAt.getTime() <= maxMs);
}

function filterObservationsAtOrBefore(observations, at) {
  const maxMs = new Date(at).getTime();
  return observations.filter(o => o.occurredAt.getTime() <= maxMs);
}

function filterObservationsAfter(observations, after) {
  const minMs = new Date(after).getTime();
  return observations.filter(o => o.occurredAt.getTime() >= minMs);
}

module.exports = {
  filterEventsAtOrBefore,
  filterObservationsAtOrBefore,
  filterObservationsAfter,
};
