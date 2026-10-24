'use strict';

const CONFIDENCE_THRESHOLD = 0.9;
const INSPECTION_THRESHOLD = 0.85;

function numberAtLeast(value, threshold) {
  return typeof value === 'number' && Number.isFinite(value) && value >= threshold;
}

function routeLooksLikeConversation(currentRoute = {}) {
  return (
    currentRoute.route === 'conversation'
    || currentRoute.raw_route === 'intelligence'
    || currentRoute.pipeline === 'ClientIntelligence'
  );
}

function routeLooksLikeInspection(row = {}) {
  return (
    row.recommended_route === 'inspection'
    || row.intent === 'inspection'
    || row.intent === 'mission_inspection'
    || row.intent === 'status_check'
  );
}

function hasProviderError(row = {}) {
  return row.status !== 'evaluated' || (Array.isArray(row.errors) && row.errors.length > 0);
}

function classifyDecisionMismatch(row = {}) {
  if (!row || hasProviderError(row)) return null;
  if (row.route_matches === true) return null;
  if (row.comparison === 'match') return null;
  if (row.comparison !== 'mismatch' && row.route_matches !== false) return null;
  if (!routeLooksLikeInspection(row)) return null;
  if (row.recommended_route !== 'inspection') return null;
  if (!numberAtLeast(row.confidence, CONFIDENCE_THRESHOLD)) return null;
  if (!numberAtLeast(row.inspection_probability, INSPECTION_THRESHOLD)) return null;
  if (!routeLooksLikeConversation(row.current_route || {})) return null;
  if (row.current_route?.failed === true) return null;

  return {
    warning_type: 'likely_mission_inspection_misroute',
    severity: 'review',
    reason: 'Jev classified this operator message as high-confidence mission inspection/status routing, but production routed it as conversation/intelligence.',
    decision_id: row.decision_id || null,
    tenant_id: row.tenant_id || null,
    mission_id: row.mission_id || null,
    current_route: row.current_route || null,
    recommended_route: row.recommended_route || null,
    intent: row.intent || null,
    confidence: row.confidence ?? null,
    inspection_probability: row.inspection_probability ?? null,
    mission_bound_probability: row.mission_bound_probability ?? null,
    route_matches: row.route_matches ?? null,
    comparison: row.comparison || null,
    timestamp: row.timestamp || row.created_at || null,
  };
}

module.exports = {
  CONFIDENCE_THRESHOLD,
  INSPECTION_THRESHOLD,
  classifyDecisionMismatch,
  routeLooksLikeConversation,
  routeLooksLikeInspection,
  hasProviderError,
};
