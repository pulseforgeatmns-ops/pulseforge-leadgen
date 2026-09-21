'use strict';

const THRESHOLD = 0.85;
const high = value => typeof value === 'number' && Number.isFinite(value) && value >= THRESHOLD;

function likelyMissionInspection(row) {
  return row.status === 'evaluated' && row.provider === 'jev' && row.comparison === 'mismatch'
    && row.current_route?.failed !== true
    && (row.current_route?.route === 'conversation' || row.current_route?.raw_route === 'intelligence')
    && (row.intent === 'status_check' || row.recommended_route === 'inspection')
    && (high(row.confidence) || high(row.inspection_probability));
}

function buildRoutingWarning(row) {
  if (!likelyMissionInspection(row)) return null;
  return {
    event: 'DECISION_SHADOW_ROUTING_WARNING',
    spec: 'SPEC-JEV-003',
    schema_version: 1,
    mode: 'shadow_warning',
    severity: 'review',
    reason: 'likely_mission_inspection',
    decision_id: row.decision_id,
    source: row.source,
    session_id: row.session_id,
    tenant_id: row.tenant_id,
    mission_id: row.mission_id,
    timestamp: row.timestamp,
    current_route: row.current_route || null,
    intent: row.intent || null,
    confidence: row.confidence ?? null,
    inspection_probability: row.inspection_probability ?? null,
    recommended_route: row.recommended_route || null,
    comparison: row.comparison || null,
    action: 'review_current_route_without_changing_routing',
  };
}

module.exports = { THRESHOLD, likelyMissionInspection, buildRoutingWarning };
