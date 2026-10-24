'use strict';

const INTENTS = Object.freeze([
  'mission_instruction', 'approval', 'rejection', 'inspection_question',
  'status_check', 'general_chat', 'correction', 'credential_or_access_update',
  'candidate_review', 'ad_management_request', 'capability_request', 'unknown',
]);
const ROUTES = Object.freeze([
  'mission', 'approval', 'inspection', 'clarification', 'conversation',
  'identity', 'session_configuration', 'specialist', 'intelligence', 'unknown',
]);
const RISKS = Object.freeze(['low', 'medium', 'high', 'irreversible']);
const probability = Object.freeze({ type: 'number', minimum: 0, maximum: 1 });
const DECISION_SCHEMA = Object.freeze({
  $id: 'pulseforge/mission-routing-decision/v1',
  type: 'object', additionalProperties: false,
  required: [
    'intent', 'confidence', 'mission_bound_probability', 'approval_probability',
    'inspection_probability', 'requires_human_clarification',
    'risk_if_misrouted', 'recommended_route',
  ],
  properties: {
    intent: { enum: INTENTS }, confidence: probability,
    mission_bound_probability: probability, approval_probability: probability,
    inspection_probability: probability,
    requires_human_clarification: { type: 'boolean' },
    risk_if_misrouted: { enum: RISKS }, recommended_route: { enum: ROUTES },
  },
});

class DecisionValidationError extends Error {
  constructor(field) {
    // Field names come from our schema; never include untrusted values.
    super(`Invalid decision field: ${field}`);
    this.code = 'invalid_response';
  }
}
function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function isProbability(value) {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1;
}
function parseDecision(value) {
  if (!isObject(value)) throw new DecisionValidationError('decision');
  const parsed = {};
  for (const [key, spec] of Object.entries(DECISION_SCHEMA.properties)) {
    const v = value[key];
    if (spec.enum ? !spec.enum.includes(v)
      : spec.type === 'number' ? !isProbability(v) : typeof v !== 'boolean') {
      throw new DecisionValidationError(key);
    }
    parsed[key] = v;
  }
  if (Object.keys(value).some(key => !DECISION_SCHEMA.required.includes(key))) {
    throw new DecisionValidationError('unexpected_fields');
  }
  return Object.freeze(parsed);
}

module.exports = { INTENTS, ROUTES, RISKS, DECISION_SCHEMA, DecisionValidationError, isObject, isProbability, parseDecision };
