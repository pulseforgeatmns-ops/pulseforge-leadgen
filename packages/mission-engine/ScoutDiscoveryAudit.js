'use strict';

/**
 * AUDIT-006 — Scout Discovery execution instrumentation.
 * Emits explicit discovery strategy, evidence sources, outcomes, and mission updates.
 */

/** @type {object[]} */
const _discoveryAuditLog = [];

const DISCOVERY_STRATEGIES = Object.freeze({
  STORED_MARKET_INTELLIGENCE: 'Stored Market Intelligence',
  EXTERNAL_DISCOVERY: 'External Discovery',
  HYBRID: 'Hybrid',
  NO_STRATEGY_SELECTED: 'No Strategy Selected',
});

const DISCOVERY_OUTCOMES = Object.freeze({
  COMPLETED: 'DISCOVERY_COMPLETED',
  PARTIAL: 'DISCOVERY_PARTIAL',
  BLOCKED: 'DISCOVERY_BLOCKED',
  FAILED: 'DISCOVERY_FAILED',
});

/**
 * @param {string} event
 * @param {object} payload
 * @returns {object}
 */
function logScoutDiscoveryEvent(event, payload = {}) {
  const row = {
    event,
    missionId: payload.missionId || null,
    discoveryStrategy: payload.discoveryStrategy || null,
    evidenceSources: payload.evidenceSources || null,
    outcome: payload.outcome || null,
    blockReason: payload.blockReason || null,
    timestamp: new Date().toISOString(),
    ...payload,
  };
  _discoveryAuditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info(`[${event}]`, JSON.stringify(row));
  }
  return row;
}

function logScoutDiscoveryStrategy(payload) {
  return logScoutDiscoveryEvent('SCOUT_DISCOVERY_STRATEGY', payload);
}

function logScoutEvidenceSource(payload) {
  return logScoutDiscoveryEvent('SCOUT_EVIDENCE_SOURCE', payload);
}

function logScoutDiscoveryOutcome(payload) {
  return logScoutDiscoveryEvent('SCOUT_DISCOVERY_OUTCOME', payload);
}

function logScoutBlockReason(payload) {
  return logScoutDiscoveryEvent('SCOUT_BLOCK_REASON', payload);
}

function logMissionDiscoveryUpdate(payload) {
  return logScoutDiscoveryEvent('MISSION_DISCOVERY_UPDATE', payload);
}

function logMissionDiscoveryResponse(payload) {
  return logScoutDiscoveryEvent('MISSION_DISCOVERY_RESPONSE', payload);
}

function listScoutDiscoveryAuditLog() {
  return _discoveryAuditLog.map((row) => ({ ...row }));
}

function clearScoutDiscoveryAuditLog() {
  _discoveryAuditLog.length = 0;
}

module.exports = {
  DISCOVERY_STRATEGIES,
  DISCOVERY_OUTCOMES,
  logScoutDiscoveryEvent,
  logScoutDiscoveryStrategy,
  logScoutEvidenceSource,
  logScoutDiscoveryOutcome,
  logScoutBlockReason,
  logMissionDiscoveryUpdate,
  logMissionDiscoveryResponse,
  listScoutDiscoveryAuditLog,
  clearScoutDiscoveryAuditLog,
};
