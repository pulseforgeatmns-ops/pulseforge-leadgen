'use strict';

/**
 * @pulseforge/scout — Unified Scout Discovery (SPEC-123) + Intelligence Pipeline (SPEC-141)
 * + Evidence-Driven Investigation Engine (SPEC-142) + Adaptive Investigation Planning (SPEC-145)
 *
 * Scout owns Discovery and Investigation. Operators issue objectives;
 * Scout determines strategy and evidence requirements through hypothesis-driven loops.
 * Mission Engine calls Scout.discover() or Scout.investigate().
 * SPEC-146 — Evidence Conflict Resolution between collection and qualification.
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
const investigation = require('./investigation');
const credibility = require('./credibility');
const memory = require('./memory');
const conflict = require('./conflict');

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
  investigation,
  credibility,
  memory,
  conflict,
};
