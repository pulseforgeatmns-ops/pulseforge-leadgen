'use strict';

/**
 * SPEC-117 — Emmett Outbound Infrastructure Intelligence.
 * SPEC-255 — tenant mailbox auth bridge + bootstrap capacity.
 * Reputation is capital. Emmett protects it.
 */

const types = require('./types');
const { scoreInboxHealth, healthLabel, FACTOR_MAX } = require('./InboxHealth');
const { recommendCapacity, recommendCapacityNormal } = require('./Capacity');
const { evaluateGovernor, evaluateSend, acknowledgeHalt, actorIsOperator } = require('./Governor');
const { buildTodayQueue, queueScore } = require('./Queue');
const { paceVerticals, pacingWarning } = require('./Pacing');
const { buildRecommendations } = require('./Recommendations');
const { recordOutcome, learningRecords, normalizeOutcomeType, sinksFor } = require('./Outcomes');
const { routeOutcome, learningForSink } = require('./Learning');
const { buildDashboard } = require('./Dashboard');
const { createMemoryEoiStore } = require('./Store');
const { createOutboundEngine, localDateOf } = require('./Engine');
const tenantMailboxCapacity = require('./TenantMailboxCapacity');

module.exports = {
  ...types,
  scoreInboxHealth,
  healthLabel,
  FACTOR_MAX,
  recommendCapacity,
  recommendCapacityNormal,
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
  ...tenantMailboxCapacity,
};
