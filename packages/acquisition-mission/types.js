'use strict';

/**
 * SPEC-118 — Acquisition Mission Orchestration types.
 * Max manages missions. Capabilities contribute.
 */

const crypto = require('crypto');

/** SPEC-138 — AMO runtime generation; bump on breaking lifecycle changes. */
const RUNTIME_VERSION = 2;

const STAGES = Object.freeze({
  DISCOVER: 'discover',
  UNDERSTAND: 'understand',
  PLAN: 'plan',
  PREPARE: 'prepare',
  READY: 'ready',
  EXECUTE: 'execute',
  OBSERVE: 'observe',
  LEARN: 'learn',
  IMPROVE: 'improve',
});

const STAGE_ORDER = Object.freeze([
  STAGES.DISCOVER,
  STAGES.UNDERSTAND,
  STAGES.PLAN,
  STAGES.PREPARE,
  STAGES.READY,
  STAGES.EXECUTE,
  STAGES.OBSERVE,
  STAGES.LEARN,
  STAGES.IMPROVE,
]);

const STAGE_LABELS = Object.freeze({
  [STAGES.DISCOVER]: 'Discovering',
  [STAGES.UNDERSTAND]: 'Understanding',
  [STAGES.PLAN]: 'Planning',
  [STAGES.PREPARE]: 'Preparing',
  [STAGES.READY]: 'Ready',
  [STAGES.EXECUTE]: 'Executing',
  [STAGES.OBSERVE]: 'Observing',
  [STAGES.LEARN]: 'Learning',
  [STAGES.IMPROVE]: 'Improving',
});

const STAGE_PROGRESS_BASE = Object.freeze({
  [STAGES.DISCOVER]: 8,
  [STAGES.UNDERSTAND]: 20,
  [STAGES.PLAN]: 36,
  [STAGES.PREPARE]: 48,
  [STAGES.READY]: 70,
  [STAGES.EXECUTE]: 78,
  [STAGES.OBSERVE]: 86,
  [STAGES.LEARN]: 93,
  [STAGES.IMPROVE]: 100,
});

const PRIORITIES = Object.freeze({
  HIGH: 'high',
  NORMAL: 'normal',
  LOW: 'low',
});

const SPECIALISTS = Object.freeze({
  SCOUT: 'scout',
  MAX: 'max',
  PAIGE: 'paige',
  EMMETT: 'emmett',
  VERA: 'vera',
  REX: 'rex',
  OPERATOR: 'operator',
});

const BLOCKER_KINDS = Object.freeze({
  WAITING_FOR_OPERATOR: 'waiting_for_operator',
  WAITING_FOR_PAIGE: 'waiting_for_paige',
  WAITING_FOR_EMMETT: 'waiting_for_emmett',
  WAITING_FOR_SCOUT: 'waiting_for_scout',
  WAITING_FOR_MAX: 'waiting_for_max',
  WAITING_FOR_DOMAIN_WARMUP: 'waiting_for_domain_warmup',
  WAITING_FOR_MORE_PROSPECTS: 'waiting_for_more_prospects',
  PAUSED_DELIVERABILITY_RISK: 'paused_deliverability_risk',
  EXTERNAL_DISCOVERY_CAPABILITY: 'external_discovery_capability_unavailable',
});

const BLOCKER_LABELS = Object.freeze({
  [BLOCKER_KINDS.WAITING_FOR_OPERATOR]: 'Waiting for Operator',
  [BLOCKER_KINDS.WAITING_FOR_PAIGE]: 'Waiting for Paige',
  [BLOCKER_KINDS.WAITING_FOR_EMMETT]: 'Waiting for Emmett',
  [BLOCKER_KINDS.WAITING_FOR_SCOUT]: 'Waiting for Scout',
  [BLOCKER_KINDS.WAITING_FOR_MAX]: 'Waiting for Max',
  [BLOCKER_KINDS.WAITING_FOR_DOMAIN_WARMUP]: 'Waiting for Domain Warm-up',
  [BLOCKER_KINDS.WAITING_FOR_MORE_PROSPECTS]: 'Waiting for More Prospects',
  [BLOCKER_KINDS.PAUSED_DELIVERABILITY_RISK]: 'Paused — Deliverability Risk',
  [BLOCKER_KINDS.EXTERNAL_DISCOVERY_CAPABILITY]: 'External Discovery Capability Unavailable',
});

const EVENT_KINDS = Object.freeze({
  MISSION_CREATED: 'mission_created',
  STAGE_TRANSITION: 'stage_transition',
  CONTRIBUTION: 'contribution',
  CONTRACT_REJECTED: 'contract_rejected',
  BLOCKER_SET: 'blocker_set',
  BLOCKER_CLEARED: 'blocker_cleared',
  OBSERVATION: 'observation',
  OUTCOME: 'outcome',
  OPERATOR_EDIT: 'operator_edit',
  QUEUED: 'queued',
  LAUNCHED: 'launched',
  LEARNING: 'learning',
  EXECUTION_COMMITTED: 'execution_committed',
});

const CONTRIBUTION_KINDS = Object.freeze({
  DISCOVERY: 'discovery',
  PRIORITIZATION: 'prioritization',
  VARIANTS: 'variants',
  CAPACITY: 'capacity',
  APPROVAL: 'approval',
  EDIT: 'edit',
  OBJECTIVE: 'objective',
  CONSTRAINTS: 'constraints',
  MISSION_PLAN: 'mission_plan',
});

/** SPEC-130 — operator decision kinds before specialist execution. */
const OPERATOR_DECISION_KINDS = Object.freeze({
  PLAN_CLARIFICATION: 'plan_clarification',
  PLAN_APPROVAL: 'plan_approval',
  PLAN_EDIT: 'plan_edit',
  DISCOVERY_APPROVAL: 'discovery_approval',
  /** SPEC-141 — operator reviews ranked prospects before Understand. */
  PRIORITIZATION_APPROVAL: 'prioritization_approval',
});

const SPECIALIST_STATES = Object.freeze({
  PENDING: 'pending',
  WAITING: 'waiting',
  IN_PROGRESS: 'in_progress',
  GENERATING: 'generating',
  COMPLETE: 'complete',
  APPROVAL_REQUIRED: 'approval_required',
  APPROVED: 'approved',
});

const HEALTH_LABELS = Object.freeze({
  HEALTHY: 'healthy',
  AT_RISK: 'at_risk',
  BLOCKED: 'blocked',
  PAUSED: 'paused',
});

const RISK_LEVELS = Object.freeze({
  LOW: 'low',
  MEDIUM: 'medium',
  HIGH: 'high',
});

const OUTCOME_TYPES = Object.freeze({
  QUEUED: 'queued',
  SENT: 'sent',
  OPEN: 'open',
  REPLY: 'reply',
  BOUNCE: 'bounce',
  MEETING_BOOKED: 'meeting_booked',
  WALKTHROUGH_BOOKED: 'walkthrough_booked',
});

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function nowIso(now) {
  return (now instanceof Date ? now : new Date(now || Date.now())).toISOString();
}

function newId(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function amoError(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, Number(value) || 0));
}

function round2(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

function pct(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return n > 1 && n <= 100 ? n / 100 : clamp(n, 0, 1);
}

function titleCaseSegment(segment) {
  const text = asText(segment);
  if (!text) return 'Acquisition Mission';
  return text.replace(/\b\w/g, (ch) => ch.toUpperCase());
}

function formatClock(iso) {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return '';
  let hours = date.getHours();
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const suffix = hours >= 12 ? 'PM' : 'AM';
  hours = hours % 12 || 12;
  return `${hours}:${minutes}`;
}

module.exports = {
  RUNTIME_VERSION,
  STAGES,
  STAGE_ORDER,
  STAGE_LABELS,
  STAGE_PROGRESS_BASE,
  PRIORITIES,
  SPECIALISTS,
  BLOCKER_KINDS,
  BLOCKER_LABELS,
  EVENT_KINDS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  SPECIALIST_STATES,
  HEALTH_LABELS,
  RISK_LEVELS,
  OUTCOME_TYPES,
  clone,
  asText,
  nowIso,
  newId,
  amoError,
  clamp,
  round2,
  pct,
  titleCaseSegment,
  formatClock,
};
