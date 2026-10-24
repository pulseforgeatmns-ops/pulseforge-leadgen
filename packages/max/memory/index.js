'use strict';

const {
  MemoryEngine,
  createMemoryEngine,
  SnapshotEngine,
  DiffEngine,
  ChangeDetector,
  TimelineBuilder,
  WatchRegistry,
  RecommendationEvolution,
  TemporalExplanationEngine,
  InMemorySnapshotRepository,
  CHANGE_TYPES,
  WATCH_OPS,
  TREND_DIRECTIONS,
} = require('./MemoryEngine');
const {
  SerializingSnapshotRepository,
} = require('./snapshots/InMemorySnapshotRepository');
const {
  assertSnapshotRepository,
  isSnapshotRepository,
  SNAPSHOT_REPOSITORY_METHODS,
} = require('./snapshots/SnapshotRepository');
const {
  deepClone,
  stableStringify,
  snapshotId,
} = require('./snapshots/MemoryTypes');

module.exports = {
  MemoryEngine,
  createMemoryEngine,
  SnapshotEngine,
  DiffEngine,
  ChangeDetector,
  TimelineBuilder,
  WatchRegistry,
  RecommendationEvolution,
  TemporalExplanationEngine,
  InMemorySnapshotRepository,
  SerializingSnapshotRepository,
  assertSnapshotRepository,
  isSnapshotRepository,
  SNAPSHOT_REPOSITORY_METHODS,
  CHANGE_TYPES,
  WATCH_OPS,
  TREND_DIRECTIONS,
  deepClone,
  stableStringify,
  snapshotId,
};
