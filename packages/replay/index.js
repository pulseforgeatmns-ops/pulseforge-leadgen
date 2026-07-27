'use strict';

const {
  ReplayEngine,
  createReplayEngine,
  createTemporalQueries,
  normalizeObservationList,
  toImmutableObservation,
  deterministicObservationId,
} = require('./ReplayEngine');
const {
  ReplayTimeline,
  createReplayTimeline,
} = require('./ReplayTimeline');
const {
  ReplaySession,
  createReplaySession,
} = require('./ReplaySession');
const {
  ReplayComparator,
  createReplayComparator,
  canonicalizeResult,
  hashJson,
} = require('./ReplayComparator');
const { REPLAY_RULES, DEFAULT_RUNTIME_VERSION } = require('./types');

module.exports = {
  ReplayEngine,
  createReplayEngine,
  createTemporalQueries,
  normalizeObservationList,
  toImmutableObservation,
  deterministicObservationId,
  ReplayTimeline,
  createReplayTimeline,
  ReplaySession,
  createReplaySession,
  ReplayComparator,
  createReplayComparator,
  canonicalizeResult,
  hashJson,
  REPLAY_RULES,
  DEFAULT_RUNTIME_VERSION,
};
