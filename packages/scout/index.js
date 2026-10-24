'use strict';

/**
 * @pulseforge/scout — Unified Scout Discovery (SPEC-123, SPEC-154)
 *
 * Scout exposes one public capability: Scout.discover()
 * Investigation runs internally via DiscoveryPipeline (CoverageEngine mandatory).
 */

const {
  discover,
  selectDiscoveryStrategy,
  buildDelegationFromMission,
  runDiscoveryPipeline,
} = require('./Discovery');
const { investigate } = require('./Investigate');
const {
  DISCOVERY_PIPELINE_STAGES,
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
const synthesis = require('./synthesis');
const heuristics = require('./heuristics');
const opportunity = require('./opportunity');

/** SPEC-154 — single public Scout capability. */
const Scout = Object.freeze({
  discover,
});

module.exports = {
  Scout,
  discover,
  /** @deprecated SPEC-154 — use Scout.discover(); investigation is internal. */
  investigate,
  runDiscoveryPipeline,
  selectDiscoveryStrategy,
  buildDelegationFromMission,
  DISCOVERY_PIPELINE_STAGES,
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
  synthesis,
  heuristics,
  opportunity,
  explainability: require('./explainability'),
  identity: require('./identity'),
  hypothesis: require('./hypothesis'),
  coverage: require('./coverage'),
  universe: require('./universe'),
  adapters: {
    ...require('./adapters/ScoutDiscoveryArtifact'),
  },
};
