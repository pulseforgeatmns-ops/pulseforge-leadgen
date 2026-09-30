'use strict';

const {
  CONFIDENCE_THRESHOLD,
  INSPECTION_THRESHOLD,
} = require('./mismatchClassifier');

const DEFAULT_ACTIVE_CONFIDENCE = CONFIDENCE_THRESHOLD;
const DEFAULT_ACTIVE_INSPECTION = INSPECTION_THRESHOLD;

const ALLOWED_ACTIVE_INTENTS = Object.freeze([
  'status_check',
  'inspection_question',
]);

const ALLOWED_ACTIVE_ROUTES = Object.freeze([
  'inspection',
  'intelligence',
]);

const BLOCKED_ACTIVE_ROUTES = Object.freeze([
  'mission',
  'approval',
  'clarification',
  'specialist',
  'session_configuration',
  'identity',
  'unknown',
]);

function boundedProbability(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n >= 0 && n <= 1 ? n : fallback;
}

function readActiveRoutingConfig(env = process.env) {
  return {
    enabled: env.JEV_ACTIVE_ROUTING_ENABLED === 'true',
    confidenceMin: boundedProbability(
      env.JEV_ACTIVE_ROUTING_CONFIDENCE,
      DEFAULT_ACTIVE_CONFIDENCE
    ),
    inspectionMin: boundedProbability(
      env.JEV_ACTIVE_ROUTING_INSPECTION_PROB,
      DEFAULT_ACTIVE_INSPECTION
    ),
  };
}

function isAllowedActiveIntent(intent) {
  return ALLOWED_ACTIVE_INTENTS.includes(intent);
}

function isAllowedActiveRoute(route) {
  return ALLOWED_ACTIVE_ROUTES.includes(route);
}

function isBlockedActiveRoute(route) {
  return BLOCKED_ACTIVE_ROUTES.includes(route);
}

/**
 * @param {import('./types').RoutingDecision|null|undefined} decision
 * @param {ReturnType<typeof readActiveRoutingConfig>} [config]
 */
function assessJevDecisionForActivePromotion(decision, config = readActiveRoutingConfig()) {
  if (!decision) return { eligible: false, reason: 'missing_decision' };
  if (decision.confidence < config.confidenceMin) {
    return { eligible: false, reason: 'low_confidence' };
  }
  if (isBlockedActiveRoute(decision.recommended_route)) {
    return { eligible: false, reason: 'unsafe_route' };
  }
  if (!isAllowedActiveRoute(decision.recommended_route)) {
    return { eligible: false, reason: 'route_not_allowed' };
  }
  if (!isAllowedActiveIntent(decision.intent)) {
    return { eligible: false, reason: 'intent_not_allowed' };
  }
  if (decision.approval_probability >= 0.5) {
    return { eligible: false, reason: 'approval_signal' };
  }
  if (decision.inspection_probability < config.inspectionMin) {
    return { eligible: false, reason: 'low_inspection_probability' };
  }
  if (decision.requires_human_clarification) {
    return { eligible: false, reason: 'requires_clarification' };
  }
  if (decision.risk_if_misrouted !== 'low') {
    return { eligible: false, reason: 'elevated_risk' };
  }
  return { eligible: true, reason: null };
}

function ownerToRouteVocabulary(owner) {
  if (!owner) return null;
  if (owner === 'mission_inspection' || owner === 'execution_state_manager') return 'inspection';
  if (owner === 'session_state_manager') return 'session_configuration';
  if (owner === 'conversation_identity') return 'identity';
  if (['active_mission', 'mission_creation'].includes(owner)) return 'mission';
  if (String(owner).startsWith('specialist_')) return 'specialist';
  if (['conversation_layer', 'reflection', 'reasoning'].includes(owner)) return 'conversation';
  if (owner === 'knowledge_retrieval') return 'intelligence';
  return null;
}

function buildActiveRoutingAuditRow({
  decisionId = null,
  productionOwner = null,
  productionRoute = null,
  jevRoute = null,
  jevConfidence = null,
  selectedRoute = null,
  promoted = false,
  blockedReason = null,
  jevIntent = null,
} = {}) {
  return {
    event: 'DECISION_ACTIVE_ROUTING',
    spec: 'SPEC-JEV-006',
    schema_version: 1,
    mode: 'active_routing',
    decision_id: decisionId,
    production_owner: productionOwner,
    production_route: productionRoute,
    jev_route: jevRoute,
    jev_intent: jevIntent,
    jev_confidence: jevConfidence,
    selected_route: selectedRoute,
    promoted,
    blocked_reason: blockedReason,
    timestamp: new Date().toISOString(),
  };
}

module.exports = {
  DEFAULT_ACTIVE_CONFIDENCE,
  DEFAULT_ACTIVE_INSPECTION,
  ALLOWED_ACTIVE_INTENTS,
  ALLOWED_ACTIVE_ROUTES,
  BLOCKED_ACTIVE_ROUTES,
  readActiveRoutingConfig,
  assessJevDecisionForActivePromotion,
  isAllowedActiveIntent,
  isAllowedActiveRoute,
  isBlockedActiveRoute,
  ownerToRouteVocabulary,
  buildActiveRoutingAuditRow,
};
