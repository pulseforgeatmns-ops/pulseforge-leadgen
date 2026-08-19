'use strict';

/**
 * SPEC-123 — Unified Scout Discovery contract types.
 * One canonical Discovery interface; strategy is internal.
 */

const DISCOVERY_PHASES = Object.freeze({
  EXISTING_INTELLIGENCE: 'existing_intelligence',
  GAP_ANALYSIS: 'gap_analysis',
  EXTERNAL_DISCOVERY: 'external_discovery',
  VERIFICATION: 'verification',
  ENRICHMENT: 'enrichment',
  RANKING: 'ranking',
  MISSION_UPDATE: 'mission_update',
});

const DISCOVERY_STRATEGIES = Object.freeze({
  RETRIEVE_ONLY: 'Retrieve Only',
  HYBRID: 'Hybrid',
  EXTERNAL_HEAVY: 'External Heavy',
  VERIFICATION_ONLY: 'Verification Only',
});

const DISCOVERY_OUTCOMES = Object.freeze({
  COMPLETED: 'DISCOVERY_COMPLETED',
  PARTIAL: 'DISCOVERY_PARTIAL',
  BLOCKED: 'DISCOVERY_BLOCKED',
  FAILED: 'DISCOVERY_FAILED',
});

const SCOUT_DISCOVERY_EVENTS = Object.freeze({
  STARTED: 'SCOUT_DISCOVERY_STARTED',
  PHASE: 'SCOUT_PHASE',
  GAP_ANALYSIS: 'SCOUT_GAP_ANALYSIS',
  EXTERNAL_DISCOVERY: 'SCOUT_EXTERNAL_DISCOVERY',
  VERIFICATION: 'SCOUT_VERIFICATION',
  ENRICHMENT: 'SCOUT_ENRICHMENT',
  RANKING: 'SCOUT_RANKING',
  COMPLETED: 'SCOUT_DISCOVERY_COMPLETED',
});

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildDiscoveryResult(partial = {}) {
  return {
    outcome: partial.outcome || DISCOVERY_OUTCOMES.BLOCKED,
    strategy: partial.strategy || null,
    blockReason: partial.blockReason || null,
    phases: Array.isArray(partial.phases) ? partial.phases : [],
    gapAnalysis: partial.gapAnalysis || null,
    existingIntelligence: partial.existingIntelligence || null,
    prospectCount: partial.prospectCount != null ? Number(partial.prospectCount) : 0,
    companies: Array.isArray(partial.companies) ? partial.companies : [],
    prospects: Array.isArray(partial.prospects) ? partial.prospects : [],
    signals: Array.isArray(partial.signals) ? partial.signals : [],
    confidence: partial.confidence != null ? partial.confidence : null,
    recommendations: Array.isArray(partial.recommendations) ? partial.recommendations : [],
    mission: partial.mission || null,
    discoveryReport: partial.discoveryReport || null,
  };
}

module.exports = {
  DISCOVERY_PHASES,
  DISCOVERY_STRATEGIES,
  DISCOVERY_OUTCOMES,
  SCOUT_DISCOVERY_EVENTS,
  buildDiscoveryResult,
};
