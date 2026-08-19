'use strict';

/**
 * SPEC-118 — Acquisition Mission Orchestration.
 * Max manages missions. Capabilities contribute.
 */

const types = require('./types');
const { assertContract, contractFor, FORBIDDEN, PRODUCES } = require('./Contracts');
const { createMission, snapshotMission, normalizePriority } = require('./Mission');
const lifecycle = require('./Lifecycle');
const { createEvent, formatTimeline } = require('./Timeline');
const { buildSharedContext, formatSharedContext } = require('./Context');
const { buildWorkspace, formatWorkspace, bar } = require('./Workspace');
const { createBlocker, inferBlockers, currentBlocker } = require('./Blockers');
const { buildHealth, formatHealth } = require('./Health');
const { recordSegmentOutcome, summarizeLearning, formatLearning } = require('./Learning');
const { explainWhy, formatExplain, collectEvidence } = require('./Explain');
const { createObservation, formatMemory } = require('./Memory');
const { createMemoryAmoStore } = require('./Store');
const { createAcquisitionMissionEngine } = require('./Engine');

module.exports = {
  ...types,
  assertContract,
  contractFor,
  FORBIDDEN,
  PRODUCES,
  createMission,
  snapshotMission,
  normalizePriority,
  ...lifecycle,
  createEvent,
  formatTimeline,
  buildSharedContext,
  formatSharedContext,
  buildWorkspace,
  formatWorkspace,
  bar,
  createBlocker,
  inferBlockers,
  currentBlocker,
  buildHealth,
  formatHealth,
  recordSegmentOutcome,
  summarizeLearning,
  formatLearning,
  explainWhy,
  formatExplain,
  collectEvidence,
  createObservation,
  formatMemory,
  createMemoryAmoStore,
  createAcquisitionMissionEngine,
};
