'use strict';

/**
 * SPEC-165 — Strategic Decision public exports.
 * ADR-085 — Allocate finite effort toward the best business outcome.
 */

const {
  ACTIVITY_TYPES,
  ACTIVITY_LABELS,
  ACTIVITY_PRESENTATION_ORDER,
  ALLOCATION_KINDS,
  EXPECTED_ARR_USD,
  DEFAULT_CONSTRAINTS,
  roundHours,
  formatDuration,
  formatUsd,
  buildResourceConstraints,
  buildAllocationBlock,
  buildExpectedBusinessOutcome,
  buildTradeoff,
} = require('./types');

const {
  estimateHoursRequired,
  estimateExpectedOutcome,
  evaluateTradeoff,
  allocateResources,
  buildStrategicDecision,
  attachStrategicDecision,
  ensureStrategicDecision,
  formatCapacityStatement,
  explainAllocation,
  compareAllocations,
  presentStrategicDecision,
} = require('./StrategicDecisionEngine');

module.exports = {
  ACTIVITY_TYPES,
  ACTIVITY_LABELS,
  ACTIVITY_PRESENTATION_ORDER,
  ALLOCATION_KINDS,
  EXPECTED_ARR_USD,
  DEFAULT_CONSTRAINTS,
  roundHours,
  formatDuration,
  formatUsd,
  buildResourceConstraints,
  buildAllocationBlock,
  buildExpectedBusinessOutcome,
  buildTradeoff,
  estimateHoursRequired,
  estimateExpectedOutcome,
  evaluateTradeoff,
  allocateResources,
  buildStrategicDecision,
  attachStrategicDecision,
  ensureStrategicDecision,
  formatCapacityStatement,
  explainAllocation,
  compareAllocations,
  presentStrategicDecision,
};
