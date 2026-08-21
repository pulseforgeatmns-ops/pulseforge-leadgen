'use strict';

/**
 * SPEC-130 — Structured Mission Contract.
 * Canonical representation of operator intent. Immutable after operator approval.
 */

const crypto = require('crypto');
const { asText, clone, amoError } = require('./types');

const STRUCTURED_MISSION_VERSION = '1.0.0';

const MISSION_TYPES = Object.freeze({
  ACQUISITION: 'acquisition',
  RETENTION: 'retention',
  EXPANSION: 'expansion',
});

const SUCCESS_METRIC_TYPES = Object.freeze({
  CUSTOMERS: 'customers',
  REVENUE: 'revenue',
  MEETINGS: 'meetings',
  REPLIES: 'replies',
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

function normalizeSuccessMetric(input = {}) {
  const type = asText(input.type).toLowerCase() || SUCCESS_METRIC_TYPES.CUSTOMERS;
  const target = Number(input.target);
  return {
    type: Object.values(SUCCESS_METRIC_TYPES).includes(type)
      ? type
      : SUCCESS_METRIC_TYPES.CUSTOMERS,
    target: Number.isFinite(target) && target > 0 ? Math.round(target) : 1,
  };
}

function normalizeMarket(input = {}) {
  return {
    segment: asText(input.segment) || null,
    industry: asText(input.industry) || null,
    buyer: asText(input.buyer) || null,
    label: asText(input.label) || null,
  };
}

function normalizeGeography(input = {}) {
  const cities = Array.isArray(input.cities)
    ? input.cities.map(asText).filter(Boolean)
    : [];
  return {
    region: asText(input.region) || null,
    cities,
  };
}

function normalizeConstraints(value) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.map((row) => asText(row).toLowerCase()).filter(Boolean))];
}

function normalizePriority(value) {
  const n = Number(value);
  if (Number.isFinite(n) && n >= 1 && n <= 5) return Math.round(n);
  return 1;
}

/**
 * Build a validated structured mission contract.
 * @param {object} input
 * @returns {object}
 */
function createStructuredMission(input = {}) {
  const missionType = asText(input.missionType).toLowerCase() || MISSION_TYPES.ACQUISITION;
  const objective = asText(input.objective);
  if (!objective) throw amoError('amo_structured_objective_required', 'Structured mission objective is required.');

  const market = normalizeMarket(input.market || {});
  if (!market.segment) throw amoError('amo_structured_segment_required', 'Structured mission market.segment is required.');

  const geography = normalizeGeography(input.geography || {});
  if (!geography.region && !geography.cities.length) {
    throw amoError('amo_structured_geography_required', 'Structured mission geography is required.');
  }

  return {
    spec: 'SPEC-130',
    version: STRUCTURED_MISSION_VERSION,
    missionType,
    objective,
    successMetric: normalizeSuccessMetric(input.successMetric || {}),
    market,
    geography,
    constraints: normalizeConstraints(input.constraints),
    priority: normalizePriority(input.priority),
    sourceText: asText(input.sourceText) || null,
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
  if (plan.market && !plan.market.segment) errors.push('Missing market.segment.');
  if (
    plan.geography &&
    !plan.geography.region &&
    (!Array.isArray(plan.geography.cities) || !plan.geography.cities.length)
  ) {
    errors.push('Missing geography.region or geography.cities.');
  }
  return { ok: errors.length === 0, errors };
}

function hashStructuredMission(plan) {
  const canonical = JSON.stringify(plan, Object.keys(plan).sort());
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
  const frozen = Object.freeze(clone(plan));
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

function formatMissionUnderstanding(plan) {
  if (!plan) return null;
  const marketLabel =
    plan.market.label ||
    [plan.market.buyer, plan.market.segment].filter(Boolean).join(' · ') ||
    plan.market.segment;
  return {
    objective: plan.objective,
    market: marketLabel,
    region: plan.geography.region || plan.geography.cities.join(', '),
    buyer: plan.market.buyer || plan.market.segment,
    industry: plan.market.industry || null,
    constraints: plan.constraints || [],
    successMetric: plan.successMetric,
    cities: plan.geography.cities || [],
  };
}

function formatMissionUnderstandingProse(plan) {
  const understanding = formatMissionUnderstanding(plan);
  if (!understanding) return '';
  const lines = [
    'Mission Understanding',
    '',
    'Objective',
    understanding.objective,
    '',
    'Market',
    understanding.market,
  ];
  if (understanding.industry) {
    lines.push('', 'Industry', understanding.industry);
  }
  lines.push('', 'Region', understanding.region);
  if (understanding.cities.length) {
    lines.push('', 'Cities', understanding.cities.join(', '));
  }
  lines.push('', 'Buyer', understanding.buyer);
  if (understanding.constraints.length) {
    lines.push('', 'Constraints', ...understanding.constraints.map((row) => `• ${row}`));
  }
  lines.push(
    '',
    'Success Metric',
    `${understanding.successMetric.target} ${understanding.successMetric.type}`
  );
  return lines.join('\n');
}

module.exports = {
  STRUCTURED_MISSION_VERSION,
  MISSION_TYPES,
  SUCCESS_METRIC_TYPES,
  createStructuredMission,
  validateStructuredMission,
  freezeStructuredMission,
  hashStructuredMission,
  isStructuredMissionApproved,
  formatMissionUnderstanding,
  formatMissionUnderstandingProse,
  normalizeMarket,
  normalizeGeography,
  normalizeSuccessMetric,
};
