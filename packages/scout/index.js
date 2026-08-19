'use strict';

/**
 * @pulseforge/scout — Unified Scout Discovery (SPEC-123)
 *
 * Scout owns Discovery. Operators issue objectives; Scout determines strategy.
 * Mission Engine calls Scout.discover() — never implementation-specific paths.
 */

const { discover, selectDiscoveryStrategy, buildDelegationFromMission } = require('./Discovery');
const {
  DISCOVERY_PHASES,
  DISCOVERY_STRATEGIES,
  DISCOVERY_OUTCOMES,
  SCOUT_DISCOVERY_EVENTS,
  buildDiscoveryResult,
} = require('./types');
const {
  emitDiscoveryStarted,
  emitDiscoveryPhase,
  emitGapAnalysis,
  emitExternalDiscovery,
  emitVerification,
  emitEnrichment,
  emitRanking,
  emitDiscoveryCompleted,
  listPhaseLog,
  clearPhaseLog,
} = require('./observability');

const Scout = Object.freeze({
  discover,
});

module.exports = {
  Scout,
  discover,
  selectDiscoveryStrategy,
  buildDelegationFromMission,
  DISCOVERY_PHASES,
  DISCOVERY_STRATEGIES,
  DISCOVERY_OUTCOMES,
  SCOUT_DISCOVERY_EVENTS,
  buildDiscoveryResult,
  emitDiscoveryStarted,
  emitDiscoveryPhase,
  emitGapAnalysis,
  emitExternalDiscovery,
  emitVerification,
  emitEnrichment,
  emitRanking,
  emitDiscoveryCompleted,
  listPhaseLog,
  clearPhaseLog,
};
