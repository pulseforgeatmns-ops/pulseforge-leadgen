'use strict';

/**
 * @pulseforge/scout — Unified Scout Discovery (SPEC-123) + Intelligence Pipeline (SPEC-141)
 *
 * Scout owns Discovery and Investigation. Operators issue objectives;
 * Scout determines strategy and evidence requirements.
 * Mission Engine calls Scout.discover() or Scout.investigate().
 */

const { discover, selectDiscoveryStrategy, buildDelegationFromMission } = require('./Discovery');
const { investigate } = require('./Investigate');
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
const intelligence = require('./intelligence');

const Scout = Object.freeze({
  discover,
  investigate,
});

module.exports = {
  Scout,
  discover,
  investigate,
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
  intelligence,
};
