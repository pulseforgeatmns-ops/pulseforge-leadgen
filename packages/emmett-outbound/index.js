'use strict';

/**
 * SPEC-117 — Emmett Outbound Infrastructure Intelligence.
 * Reputation is capital. Emmett protects it.
 */

const types = require('./types');
const { scoreInboxHealth, healthLabel, FACTOR_MAX } = require('./InboxHealth');
const { recommendCapacity } = require('./Capacity');
const { evaluateGovernor, evaluateSend, acknowledgeHalt, actorIsOperator } = require('./Governor');
const { buildTodayQueue, queueScore } = require('./Queue');
const { paceVerticals, pacingWarning } = require('./Pacing');
const { buildRecommendations } = require('./Recommendations');
const { recordOutcome, learningRecords, normalizeOutcomeType, sinksFor } = require('./Outcomes');
const { routeOutcome, learningForSink } = require('./Learning');
const { buildDashboard } = require('./Dashboard');
const { createMemoryEoiStore } = require('./Store');
const { createOutboundEngine, localDateOf } = require('./Engine');

module.exports = {
  ...types,
  scoreInboxHealth,
  healthLabel,
  FACTOR_MAX,
  recommendCapacity,
  evaluateGovernor,
  evaluateSend,
  acknowledgeHalt,
  actorIsOperator,
  buildTodayQueue,
  queueScore,
  paceVerticals,
  pacingWarning,
  buildRecommendations,
  recordOutcome,
  learningRecords,
  normalizeOutcomeType,
  sinksFor,
  routeOutcome,
  learningForSink,
  buildDashboard,
  createMemoryEoiStore,
  createOutboundEngine,
  localDateOf,
};
