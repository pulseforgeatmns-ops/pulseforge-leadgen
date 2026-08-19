'use strict';

/**
 * SPEC-123 — Scout Discovery phase observability.
 * Every phase emits explicit events; implementation never leaks to operators.
 */

const ScoutDiscoveryAudit = require('../mission-engine/ScoutDiscoveryAudit');
const { SCOUT_DISCOVERY_EVENTS } = require('./types');

/** @type {object[]} */
const _phaseLog = [];

/**
 * @param {string} event
 * @param {object} payload
 * @returns {object}
 */
function emitDiscoveryEvent(event, payload = {}) {
  const row = ScoutDiscoveryAudit.logScoutDiscoveryEvent(event, {
    timestamp: new Date().toISOString(),
    ...payload,
  });
  _phaseLog.push(row);
  return row;
}

function emitDiscoveryStarted(payload) {
  return emitDiscoveryEvent(SCOUT_DISCOVERY_EVENTS.STARTED, payload);
}

function emitDiscoveryPhase(phase, payload) {
  return emitDiscoveryEvent(SCOUT_DISCOVERY_EVENTS.PHASE, { phase, ...payload });
}

function emitGapAnalysis(payload) {
  return emitDiscoveryEvent(SCOUT_DISCOVERY_EVENTS.GAP_ANALYSIS, payload);
}

function emitExternalDiscovery(payload) {
  return emitDiscoveryEvent(SCOUT_DISCOVERY_EVENTS.EXTERNAL_DISCOVERY, payload);
}

function emitVerification(payload) {
  return emitDiscoveryEvent(SCOUT_DISCOVERY_EVENTS.VERIFICATION, payload);
}

function emitEnrichment(payload) {
  return emitDiscoveryEvent(SCOUT_DISCOVERY_EVENTS.ENRICHMENT, payload);
}

function emitRanking(payload) {
  return emitDiscoveryEvent(SCOUT_DISCOVERY_EVENTS.RANKING, payload);
}

function emitDiscoveryCompleted(payload) {
  return emitDiscoveryEvent(SCOUT_DISCOVERY_EVENTS.COMPLETED, payload);
}

function listPhaseLog() {
  return _phaseLog.map((row) => ({ ...row }));
}

function clearPhaseLog() {
  _phaseLog.length = 0;
}

module.exports = {
  emitDiscoveryEvent,
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
