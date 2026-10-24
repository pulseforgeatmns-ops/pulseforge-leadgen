'use strict';

/**
 * SPEC-165 — Strategic Decision types.
 * ADR-085 — Allocate finite effort toward the best business outcome.
 */

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

const ACTIVITY_TYPES = Object.freeze({
  PHONE: 'phone',
  DOOR_KNOCKING: 'door_knocking',
  PROPOSAL_FOLLOW_UP: 'proposal_follow_up',
  SCOUT_REVIEW: 'scout_review',
  DIRECT_MAIL: 'direct_mail',
  OPPORTUNITY_PURSUIT: 'opportunity_pursuit',
});

const ACTIVITY_LABELS = Object.freeze({
  [ACTIVITY_TYPES.PHONE]: 'Phone',
  [ACTIVITY_TYPES.DOOR_KNOCKING]: 'Door knocking',
  [ACTIVITY_TYPES.PROPOSAL_FOLLOW_UP]: 'Proposal follow-up',
  [ACTIVITY_TYPES.SCOUT_REVIEW]: 'Review Scout discoveries',
  [ACTIVITY_TYPES.DIRECT_MAIL]: 'Direct mail',
  [ACTIVITY_TYPES.OPPORTUNITY_PURSUIT]: 'Pursue opportunity',
});

const ACTIVITY_PRESENTATION_ORDER = Object.freeze([
  ACTIVITY_TYPES.PHONE,
  ACTIVITY_TYPES.DOOR_KNOCKING,
  ACTIVITY_TYPES.PROPOSAL_FOLLOW_UP,
  ACTIVITY_TYPES.SCOUT_REVIEW,
  ACTIVITY_TYPES.DIRECT_MAIL,
  ACTIVITY_TYPES.OPPORTUNITY_PURSUIT,
]);

const ALLOCATION_KINDS = Object.freeze({
  MIXED: 'mixed',
  CONCENTRATED: 'concentrated',
});

const EXPECTED_ARR_USD = Object.freeze({
  high: 2800,
  medium: 1400,
  low: 600,
});

const DEFAULT_CONSTRAINTS = Object.freeze({
  availableHours: 4,
  availableAOs: 1,
});

function roundHours(hours) {
  const n = Number(hours);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.round(n * 2) / 2;
}

function formatDuration(hours) {
  const h = roundHours(hours);
  if (h === 0) return '0 minutes';
  if (h >= 1 && h % 1 === 0) return `${h} hour${h === 1 ? '' : 's'}`;
  const minutes = Math.round(h * 60);
  return `${minutes} minutes`;
}

function formatUsd(amount) {
  const n = Math.round(Number(amount) || 0);
  return `+$${n.toLocaleString('en-US')} ARR`;
}

function competingWorkLabel(work) {
  if (!work) return '';
  if (typeof work === 'string') {
    return ACTIVITY_LABELS[work] || asText(work).replace(/_/g, ' ');
  }
  return asText(work.label || work.name || work.type || work.activity);
}

/**
 * @param {object} partial
 * @returns {object}
 */
function buildResourceConstraints(partial = {}) {
  const availableHours = Number(partial.availableHours ?? DEFAULT_CONSTRAINTS.availableHours);
  const availableAOs = Number(partial.availableAOs ?? DEFAULT_CONSTRAINTS.availableAOs);
  const hours = Number.isFinite(availableHours) && availableHours > 0 ? availableHours : DEFAULT_CONSTRAINTS.availableHours;
  const aos = Number.isFinite(availableAOs) && availableAOs > 0 ? availableAOs : DEFAULT_CONSTRAINTS.availableAOs;
  return Object.freeze({
    availableHours: hours,
    availableAOs: aos,
    totalHours: roundHours(hours * aos),
  });
}

/**
 * @param {object} partial
 * @returns {object}
 */
function buildAllocationBlock(partial = {}) {
  const activity = asText(partial.activity) || ACTIVITY_TYPES.PHONE;
  const hours = roundHours(partial.hours);
  return Object.freeze({
    activity,
    label: asText(partial.label) || ACTIVITY_LABELS[activity] || activity,
    hours,
    duration: formatDuration(hours),
    expectedContribution: Number(partial.expectedContribution) || 0,
    reason: asText(partial.reason) || null,
    opportunities: Array.isArray(partial.opportunities) ? partial.opportunities.map(asText).filter(Boolean) : [],
    maximizesMissionObjective: partial.maximizesMissionObjective !== false,
    notInherentlyGood: true,
  });
}

/**
 * @param {object} partial
 * @returns {object}
 */
function buildExpectedBusinessOutcome(partial = {}) {
  const arr = Number(partial.arr) || 0;
  const confidence = Number(partial.confidence) || 0;
  return Object.freeze({
    arr,
    expectedValue: Number(partial.expectedValue) || Math.round(arr * confidence),
    label: asText(partial.label) || formatUsd(arr),
    confidence,
    confidencePercent: Math.round(confidence * 100),
    unit: 'ARR',
  });
}

/**
 * @param {object} partial
 * @returns {object}
 */
function buildTradeoff(partial = {}) {
  return Object.freeze({
    entity: asText(partial.entity) || null,
    recommendedAction: asText(partial.recommendedAction) || null,
    hoursRequired: roundHours(partial.hoursRequired || 0),
    pros: Array.isArray(partial.pros) ? partial.pros.map(asText).filter(Boolean) : [],
    cons: Array.isArray(partial.cons) ? partial.cons.map(asText).filter(Boolean) : [],
    expectedOutcome: partial.expectedOutcome || buildExpectedBusinessOutcome(),
    confidence: Number(partial.confidence) || 0,
    confidencePercent: Math.round((Number(partial.confidence) || 0) * 100),
    delayed: Array.isArray(partial.delayed) ? partial.delayed.map(asText).filter(Boolean) : [],
  });
}

module.exports = {
  ACTIVITY_TYPES,
  ACTIVITY_LABELS,
  ACTIVITY_PRESENTATION_ORDER,
  ALLOCATION_KINDS,
  EXPECTED_ARR_USD,
  DEFAULT_CONSTRAINTS,
  asText,
  roundHours,
  formatDuration,
  formatUsd,
  competingWorkLabel,
  buildResourceConstraints,
  buildAllocationBlock,
  buildExpectedBusinessOutcome,
  buildTradeoff,
};
