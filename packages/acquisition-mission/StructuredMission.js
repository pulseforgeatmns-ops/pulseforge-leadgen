'use strict';

/**
 * SPEC-130 — Structured Mission Contract.
 * Canonical representation of operator intent. Immutable after operator approval.
 * Mission Planning owns interpretation. Specialists own execution.
 */

const crypto = require('crypto');
const { asText, clone, amoError } = require('./types');
const { CONTEXT_PRECEDENCE } = require('./ContextPrecedence');

const STRUCTURED_MISSION_VERSION = '1.1.0';

const MISSION_TYPES = Object.freeze({
  ACQUISITION: 'acquisition',
  RETENTION: 'retention',
  EXPANSION: 'expansion',
  MARKETING: 'marketing',
  HIRING: 'hiring',
  OPERATIONS: 'operations',
  RESEARCH: 'research',
  SUPPORT: 'support',
  KNOWLEDGE: 'knowledge',
});

const SUCCESS_METRIC_TYPES = Object.freeze({
  CUSTOMERS: 'customers',
  RECURRING_CLIENTS: 'recurring_clients',
  REVENUE: 'revenue',
  MEETINGS: 'meetings',
  REPLIES: 'replies',
  /** P0 — review-only missions: success is the operator's review, not acquisition. */
  OPERATOR_REVIEW: 'operator_review',
});

const EXECUTION_STATES = Object.freeze({
  DRAFTING: 'drafting',
  PLANNED: 'planned',
  APPROVED: 'approved',
  CANCELLED: 'cancelled',
});

const EVIDENCE_THRESHOLDS = Object.freeze({
  LOW: 'low',
  MEDIUM: 'medium',
  HIGH: 'high',
});

const REQUIRED_FIELDS = Object.freeze([
  'missionType',
  'objective',
  'successMetric',
  'market',
  'geography',
  'constraints',
  'priority',
]);

const CANONICAL_HASH_FIELDS = Object.freeze([
  'missionType',
  'objective',
  'successMetric',
  'market',
  'geography',
  'constraints',
  'priority',
  'evidence',
  // Absent on ordinary plans (JSON drops undefined), so their hashes are unchanged.
  'researchPlanning',
]);

function normalizeSuccessMetric(input = {}) {
  const rawType = asText(input.type || input.metric).toLowerCase() || SUCCESS_METRIC_TYPES.CUSTOMERS;
  const type = Object.values(SUCCESS_METRIC_TYPES).includes(rawType)
    ? rawType
    : SUCCESS_METRIC_TYPES.CUSTOMERS;
  const target = Number(input.target);
  return {
    type,
    metric: type,
    target: Number.isFinite(target) && target > 0 ? Math.round(target) : 1,
  };
}

function normalizeMarket(input = {}) {
  const eligibleSubsegments = Array.isArray(input.eligibleSubsegments)
    ? [...new Set(input.eligibleSubsegments.map(asText).filter(Boolean))]
    : [];
  return {
    segment: asText(input.segment) || null,
    industry: asText(input.industry) || null,
    buyer: asText(input.buyer) || null,
    label: asText(input.label) || null,
    eligibleSubsegments,
  };
}

function normalizeGeography(input = {}) {
  const cities = Array.isArray(input.cities)
    ? input.cities.map(asText).filter(Boolean)
    : [];
  const scope = asText(input.scope).toLowerCase() || null;
  return {
    region: asText(input.region) || null,
    cities,
    mention: asText(input.mention) || null,
    scope: scope === 'nationwide' || scope === 'regional' || scope === 'local' ? scope : null,
  };
}

/**
 * P0 — research planning deferred because provider research is prohibited.
 * Ordinary plans carry no marker (undefined), which keeps them unchanged.
 */
function normalizeResearchPlanning(input) {
  if (!input || typeof input !== 'object' || input.deferred !== true) return undefined;
  return {
    deferred: true,
    reason: asText(input.reason) || 'review_only',
    missing: Array.isArray(input.missing) ? [...new Set(input.missing.map(asText).filter(Boolean))] : [],
    deferredAmbiguities: normalizeAmbiguities(input.deferredAmbiguities),
  };
}

function isResearchPlanningDeferred(plan) {
  return Boolean(plan && plan.researchPlanning && plan.researchPlanning.deferred === true);
}

function normalizeConstraints(value) {
  if (!Array.isArray(value)) return [];
  const mapped = value.map((row) => {
    const text = asText(row).toLowerCase();
    if (text === 'recurring_only') return 'recurring';
    return text;
  }).filter(Boolean);
  return [...new Set(mapped)];
}

function normalizePriority(value) {
  const n = Number(value);
  if (Number.isFinite(n) && n >= 1 && n <= 5) return Math.round(n);
  return 1;
}

function evidenceThresholdLabel(minimumConfidence) {
  const n = Number(minimumConfidence);
  if (n >= 0.85) return EVIDENCE_THRESHOLDS.HIGH;
  if (n >= 0.70) return EVIDENCE_THRESHOLDS.MEDIUM;
  return EVIDENCE_THRESHOLDS.LOW;
}

function normalizeEvidence(input = {}) {
  const minimumConfidence = Number(input.minimumConfidence != null
    ? input.minimumConfidence
    : input.minimum_confidence);
  const minimumBuyingSignals = Number(input.minimumBuyingSignals != null
    ? input.minimumBuyingSignals
    : input.minimum_buying_signals);
  const confidence = Number.isFinite(minimumConfidence) && minimumConfidence > 0
    ? Math.min(1, minimumConfidence)
    : 0.7;
  const signals = Number.isFinite(minimumBuyingSignals) && minimumBuyingSignals > 0
    ? Math.round(minimumBuyingSignals)
    : 2;
  return {
    minimumConfidence: confidence,
    minimum_confidence: confidence,
    minimumBuyingSignals: signals,
    minimum_buying_signals: signals,
    thresholdLabel: asText(input.thresholdLabel).toLowerCase() || evidenceThresholdLabel(confidence),
  };
}

function normalizeExecution(input = {}, fallbackState) {
  const state = asText(input.state).toLowerCase() || fallbackState || EXECUTION_STATES.PLANNED;
  return {
    state: Object.values(EXECUTION_STATES).includes(state) ? state : EXECUTION_STATES.PLANNED,
  };
}

function normalizeProvenance(value) {
  if (!Array.isArray(value)) return [];
  return value.map((row) => ({
    field: asText(row.field),
    value: row.value,
    confidence: Number.isFinite(Number(row.confidence)) ? Number(row.confidence) : null,
    reason: asText(row.reason) || null,
    source: asText(row.source) || 'operator',
  })).filter((row) => row.field);
}

function normalizeAmbiguities(value) {
  if (!Array.isArray(value)) return [];
  return value.map((row) => ({
    field: asText(row.field),
    question: asText(row.question),
    choices: Array.isArray(row.choices)
      ? row.choices.map((choice) => ({
        id: asText(choice.id),
        label: asText(choice.label),
        value: choice.value,
      }))
      : [],
    reason: asText(row.reason) || null,
  })).filter((row) => row.field && row.question);
}

function provenanceFor(plan, field) {
  const rows = (plan && plan.provenance) || [];
  return rows.find((row) => row.field === field) || null;
}

/**
 * Build a structured mission contract.
 * Drafts may be incomplete while ambiguities are open (`allowIncomplete`).
 * @param {object} input
 * @param {object} [opts]
 * @returns {object}
 */
function createStructuredMission(input = {}, opts = {}) {
  const allowIncomplete = opts.allowIncomplete === true;
  const missionType = asText(input.missionType || input.type).toLowerCase()
    || MISSION_TYPES.ACQUISITION;
  if (!Object.values(MISSION_TYPES).includes(missionType) && !allowIncomplete) {
    throw amoError('amo_structured_type_invalid', `Unknown mission type: ${missionType}`);
  }

  const objective = asText(input.objective && input.objective.text ? input.objective.text : input.objective);
  if (!objective && !allowIncomplete) {
    throw amoError('amo_structured_objective_required', 'Structured mission objective is required.');
  }

  const market = normalizeMarket(input.market || {});
  if (!market.segment && !allowIncomplete) {
    throw amoError('amo_structured_segment_required', 'Structured mission market.segment is required.');
  }

  const geography = normalizeGeography(input.geography || {});
  if (!geography.region && !geography.cities.length && !allowIncomplete) {
    throw amoError('amo_structured_geography_required', 'Structured mission geography is required.');
  }

  const ambiguities = normalizeAmbiguities(input.ambiguities);
  const executionState = ambiguities.length
    ? EXECUTION_STATES.DRAFTING
    : (input.execution && input.execution.state) || EXECUTION_STATES.PLANNED;

  return {
    spec: 'SPEC-130',
    version: STRUCTURED_MISSION_VERSION,
    missionType,
    type: missionType,
    objective,
    successMetric: normalizeSuccessMetric(input.successMetric || input.success || {}),
    success: normalizeSuccessMetric(input.successMetric || input.success || {}),
    market,
    geography,
    constraints: normalizeConstraints(input.constraints),
    priority: normalizePriority(input.priority),
    evidence: normalizeEvidence(input.evidence || input.evidencePolicy || {}),
    evidencePolicy: normalizeEvidence(input.evidence || input.evidencePolicy || {}),
    execution: normalizeExecution(input.execution || {}, executionState),
    provenance: normalizeProvenance(input.provenance),
    ambiguities,
    sourceText: asText(input.sourceText) || null,
    contextPrecedence: CONTEXT_PRECEDENCE.slice(),
    ...(normalizeResearchPlanning(input.researchPlanning)
      ? { researchPlanning: normalizeResearchPlanning(input.researchPlanning) }
      : {}),
  };
}

function validateStructuredMission(plan) {
  if (!plan || typeof plan !== 'object') {
    return { ok: false, errors: ['Structured mission is required.'] };
  }
  const errors = [];
  for (const field of REQUIRED_FIELDS) {
    if (plan[field] == null || plan[field] === '') errors.push(`Missing ${field}.`);
  }
  // A review-only plan defers Scout targeting: market and region are collected
  // only if research is later authorized (planning reopens then).
  const researchDeferred = isResearchPlanningDeferred(plan);
  if (!researchDeferred && plan.market && !plan.market.segment) errors.push('Missing market.segment.');
  if (
    !researchDeferred &&
    plan.geography &&
    !plan.geography.region &&
    (!Array.isArray(plan.geography.cities) || !plan.geography.cities.length)
  ) {
    errors.push('Missing geography.region or geography.cities.');
  }
  if (Array.isArray(plan.ambiguities) && plan.ambiguities.length) {
    errors.push('Unresolved ambiguities must be answered before lock.');
  }
  return { ok: errors.length === 0, errors };
}

function isReadyForLock(plan) {
  return validateStructuredMission(plan).ok;
}

function canonicalContract(plan) {
  const payload = {};
  for (const field of CANONICAL_HASH_FIELDS) {
    payload[field] = plan[field];
  }
  return payload;
}

function hashStructuredMission(plan) {
  const canonical = JSON.stringify(canonicalContract(plan));
  return crypto.createHash('sha256').update(canonical).digest('hex').slice(0, 16);
}

/**
 * Freeze structured mission after operator approval — no further mutation.
 * @param {object} plan
 * @param {object} [meta]
 * @returns {object}
 */
function freezeStructuredMission(plan, meta = {}) {
  const validated = validateStructuredMission(plan);
  if (!validated.ok) {
    throw amoError('amo_structured_invalid', validated.errors.join(' '));
  }
  const frozen = Object.freeze(clone({
    ...plan,
    ambiguities: [],
    execution: { state: EXECUTION_STATES.APPROVED },
  }));
  return Object.freeze({
    ...frozen,
    immutable: true,
    approvedAt: meta.approvedAt || new Date().toISOString(),
    approvedBy: asText(meta.approvedBy) || 'operator',
    contractHash: hashStructuredMission(frozen),
  });
}

function isStructuredMissionApproved(mission) {
  if (!mission) return false;
  if (mission.structuredMission && mission.structuredMission.immutable) return true;
  return Boolean(mission.structuredMissionApproved);
}

/**
 * P0 — scope lines for a plan whose research planning was deferred, derived
 * from the mission's CURRENT authorization when supplied (canonical
 * describeProhibitedActions). Discovery is called out separately because
 * nothing that depends on it can start; other actions are listed only if the
 * authorization actually prohibits them. Without constraints, only the plan's
 * own history is stated, never a claim about current authorization.
 * @param {object|null} authorizationConstraints
 * @returns {string[]}
 */
function deferredPlanScopeLines(authorizationConstraints) {
  if (!authorizationConstraints) {
    return ['Planned as review-only.', 'Market and region were not collected.'];
  }
  const {
    describeProhibitedActions,
    evaluateAction,
    ACTION_CLASSES,
  } = require('./AuthorizationConstraints');
  const researchAllowed = evaluateAction({ authorizationConstraints }, ACTION_CLASSES.PROVIDER_RESEARCH).allowed;
  const others = describeProhibitedActions({
    prohibited: (authorizationConstraints.prohibited || []).filter((cls) => cls !== ACTION_CLASSES.PROVIDER_RESEARCH),
  });
  const lines = [];
  if (authorizationConstraints.reviewOnly === true) lines.push('Review only.');
  if (!researchAllowed) {
    lines.push('Discovery (provider research) is not authorized.');
  } else {
    lines.push('Research planning has not been completed.');
  }
  if (others.length) lines.push(`Also not authorized: ${others.join(', ')}.`);
  lines.push(researchAllowed
    ? 'Market and region must be collected and the plan approved again before discovery.'
    : 'Market and region are collected only if discovery is later authorized.');
  return lines;
}

function formatMissionUnderstanding(plan, opts = {}) {
  if (!plan) return null;
  const marketLabel =
    (plan.market && (plan.market.label || [plan.market.buyer, plan.market.segment].filter(Boolean).join(' · '))) ||
    (plan.market && plan.market.segment) ||
    null;
  const evidence = plan.evidence || plan.evidencePolicy || {};
  const success = plan.successMetric || plan.success || {};
  const geography = plan.geography || {};
  return {
    objective: plan.objective,
    market: marketLabel,
    region: geography.region || (geography.cities || []).join(', ') || null,
    buyer: (plan.market && (plan.market.buyer || plan.market.segment)) || null,
    industry: (plan.market && plan.market.industry) || null,
    constraints: plan.constraints || [],
    successMetric: success,
    success: (success.type || success.metric) === SUCCESS_METRIC_TYPES.OPERATOR_REVIEW
      ? 'Operator review complete (no acquisition target).'
      : success.target === 1 && /recurr/i.test(String(plan.objective || ''))
        ? 'One recurring client.'
        : `${success.target || 1} ${success.type || success.metric || 'customers'}`,
    ...(isResearchPlanningDeferred(plan)
      ? { researchPlanningDeferred: true, scope: deferredPlanScopeLines(opts.authorizationConstraints || null) }
      : {}),
    cities: geography.cities || [],
    evidenceThreshold: evidence.thresholdLabel || evidenceThresholdLabel(evidence.minimumConfidence),
    missionType: plan.missionType || plan.type,
  };
}

function formatMissionUnderstandingProse(plan, opts = {}) {
  const understanding = formatMissionUnderstanding(plan, opts);
  if (!understanding) return '';
  const lines = [
    'Mission Understanding',
    '',
    'Objective',
    understanding.objective,
    '',
  ];
  if (understanding.researchPlanningDeferred) {
    lines.push('Scope', ...understanding.scope);
  } else {
    lines.push('Market', understanding.market || 'Unknown');
    if (understanding.industry) {
      lines.push('', 'Industry', understanding.industry);
    }
    lines.push('', 'Region', understanding.region || 'Unknown');
    if (understanding.cities.length) {
      lines.push('', 'Cities', understanding.cities.join(', '));
    }
    lines.push('', 'Buyer', understanding.buyer || 'Unknown');
  }
  if (understanding.constraints.length) {
    lines.push('', 'Constraints', ...understanding.constraints.map((row) => `• ${row}`));
  }
  lines.push(
    '',
    'Success',
    understanding.success,
    '',
    'Evidence Threshold',
    String(understanding.evidenceThreshold).charAt(0).toUpperCase() +
      String(understanding.evidenceThreshold).slice(1) + '.'
  );
  return lines.join('\n');
}

/**
 * SPEC-168 — Mission Understanding displays only the canonical business objective.
 * Never the full operator prompt or execution instructions.
 * @param {object} mission
 * @returns {string}
 */
function formatCanonicalObjectiveDisplay(mission) {
  const resolved = mission && mission.resolvedObjective;
  const objective = resolved && resolved.objective
    ? resolved.objective
    : (mission && mission.objective) || '';
  if (!objective) return '';
  return [
    'Mission Understanding',
    '',
    objective,
  ].join('\n');
}

/**
 * Operator confirmation copy. Approve / Edit / Cancel — never silent execute.
 */
function formatOperatorConfirmation(plan, opts = {}) {
  const prose = formatMissionUnderstandingProse(plan, opts);
  if (!prose) return '';
  return [prose, '', 'Proceed?', 'Approve', 'Edit', 'Cancel'].join('\n');
}

function formatAmbiguityPrompt(ambiguity) {
  if (!ambiguity) return '';
  const lines = [ambiguity.question];
  if (ambiguity.choices && ambiguity.choices.length) {
    lines.push(...ambiguity.choices.map((choice) => choice.label));
  }
  return lines.join('\n');
}

module.exports = {
  STRUCTURED_MISSION_VERSION,
  MISSION_TYPES,
  SUCCESS_METRIC_TYPES,
  EXECUTION_STATES,
  EVIDENCE_THRESHOLDS,
  CONTEXT_PRECEDENCE,
  createStructuredMission,
  validateStructuredMission,
  freezeStructuredMission,
  hashStructuredMission,
  isStructuredMissionApproved,
  isReadyForLock,
  isResearchPlanningDeferred,
  formatMissionUnderstanding,
  formatMissionUnderstandingProse,
  formatCanonicalObjectiveDisplay,
  formatOperatorConfirmation,
  formatAmbiguityPrompt,
  provenanceFor,
  normalizeMarket,
  normalizeGeography,
  normalizeSuccessMetric,
  normalizeEvidence,
  evidenceThresholdLabel,
};
