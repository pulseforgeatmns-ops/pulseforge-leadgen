'use strict';

/**
 * ADR-090 / SPEC-171 — Canonical Execution Request (CER).
 *
 * Surfaces produce this object. Only the Execution Router consumes it.
 * Frozen after create. Unique id. Replayable. Auditable.
 */

const { asText, nowIso, newId, amoError, clone } = require('./types');
const { OPERATOR_DECISION_KINDS } = require('./types');
const { RUNTIME_OWNERS, resolveMissionRuntimeOwner } = require('./MissionRuntimeOwnership');

const EXECUTION_SOURCES = Object.freeze({
  CHAT: 'chat',
  WORKSPACE: 'workspace',
  VOICE: 'voice',
  API: 'api',
  APPROVAL_BUTTON: 'approval_button',
  COMMAND_DECK: 'command_deck',
});

const EXECUTION_INTENTS = Object.freeze({
  APPROVE_PLAN: 'APPROVE_PLAN',
  APPROVE_DISCOVERY: 'APPROVE_DISCOVERY',
  START_DISCOVERY: 'START_DISCOVERY',
  CONTINUE_INVESTIGATION: 'CONTINUE_INVESTIGATION',
  APPROVE_PRIORITIZATION: 'APPROVE_PRIORITIZATION',
  CLARIFY_PLAN: 'CLARIFY_PLAN',
  CANCEL_PLAN: 'CANCEL_PLAN',
  EDIT_PLAN: 'EDIT_PLAN',
  APPLY_PLAN_EDITS: 'APPLY_PLAN_EDITS',
  AUTONOMOUS_PROGRESSION: 'AUTONOMOUS_PROGRESSION',
  GENERATE_VARIANTS: 'GENERATE_VARIANTS',
  GENERATE_CAPACITY: 'GENERATE_CAPACITY',
  APPROVE_EXECUTION: 'APPROVE_EXECUTION',
  EXECUTE_OUTBOUND: 'EXECUTE_OUTBOUND',
  OPERATOR_APPROVED: 'OPERATOR_APPROVED',
});

const INTENT_SET = new Set(Object.values(EXECUTION_INTENTS));
const SOURCE_SET = new Set(Object.values(EXECUTION_SOURCES));

const ACTION_TO_INTENT = Object.freeze({
  plan_approved: EXECUTION_INTENTS.APPROVE_PLAN,
  discovery_approved: EXECUTION_INTENTS.APPROVE_DISCOVERY,
  discovery_investigation_continued: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
  prioritization_approved: EXECUTION_INTENTS.APPROVE_PRIORITIZATION,
  plan_clarified: EXECUTION_INTENTS.CLARIFY_PLAN,
  plan_cancelled: EXECUTION_INTENTS.CANCEL_PLAN,
  plan_edit: EXECUTION_INTENTS.EDIT_PLAN,
  plan_edited: EXECUTION_INTENTS.APPLY_PLAN_EDITS,
  operator_approved: EXECUTION_INTENTS.OPERATOR_APPROVED,
  autonomous_progression: EXECUTION_INTENTS.AUTONOMOUS_PROGRESSION,
  generate_variants: EXECUTION_INTENTS.GENERATE_VARIANTS,
  generate_capacity: EXECUTION_INTENTS.GENERATE_CAPACITY,
  execution_approved: EXECUTION_INTENTS.APPROVE_EXECUTION,
  execute_outbound: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
});

const INTENT_TO_ACTION = Object.freeze({
  [EXECUTION_INTENTS.APPROVE_PLAN]: 'plan_approved',
  [EXECUTION_INTENTS.APPROVE_DISCOVERY]: 'discovery_approved',
  [EXECUTION_INTENTS.START_DISCOVERY]: 'discovery_approved',
  [EXECUTION_INTENTS.CONTINUE_INVESTIGATION]: 'discovery_investigation_continued',
  [EXECUTION_INTENTS.APPROVE_PRIORITIZATION]: 'prioritization_approved',
  [EXECUTION_INTENTS.CLARIFY_PLAN]: 'plan_clarified',
  [EXECUTION_INTENTS.CANCEL_PLAN]: 'plan_cancelled',
  [EXECUTION_INTENTS.EDIT_PLAN]: 'plan_edit',
  [EXECUTION_INTENTS.APPLY_PLAN_EDITS]: 'plan_edited',
  [EXECUTION_INTENTS.AUTONOMOUS_PROGRESSION]: 'autonomous_progression',
  [EXECUTION_INTENTS.GENERATE_VARIANTS]: 'generate_variants',
  [EXECUTION_INTENTS.GENERATE_CAPACITY]: 'generate_capacity',
  [EXECUTION_INTENTS.APPROVE_EXECUTION]: 'execution_approved',
  [EXECUTION_INTENTS.EXECUTE_OUTBOUND]: 'execute_outbound',
  [EXECUTION_INTENTS.OPERATOR_APPROVED]: 'operator_approved',
});

const PENDING_KIND_TO_INTENT = Object.freeze({
  [OPERATOR_DECISION_KINDS.PLAN_APPROVAL]: EXECUTION_INTENTS.APPROVE_PLAN,
  [OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL]: EXECUTION_INTENTS.APPROVE_DISCOVERY,
  [OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION]: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
  [OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL]: EXECUTION_INTENTS.APPROVE_PRIORITIZATION,
  [OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL]: EXECUTION_INTENTS.APPROVE_EXECUTION,
  [OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION]: EXECUTION_INTENTS.CLARIFY_PLAN,
  [OPERATOR_DECISION_KINDS.PLAN_EDIT]: EXECUTION_INTENTS.EDIT_PLAN,
});

const INTENT_SPECIALISTS = Object.freeze({
  [EXECUTION_INTENTS.APPROVE_PLAN]: 'max',
  [EXECUTION_INTENTS.APPROVE_DISCOVERY]: 'scout',
  [EXECUTION_INTENTS.START_DISCOVERY]: 'scout',
  [EXECUTION_INTENTS.CONTINUE_INVESTIGATION]: 'scout',
  [EXECUTION_INTENTS.APPROVE_PRIORITIZATION]: 'max',
  [EXECUTION_INTENTS.CLARIFY_PLAN]: 'max',
  [EXECUTION_INTENTS.CANCEL_PLAN]: 'max',
  [EXECUTION_INTENTS.EDIT_PLAN]: 'max',
  [EXECUTION_INTENTS.APPLY_PLAN_EDITS]: 'max',
  [EXECUTION_INTENTS.AUTONOMOUS_PROGRESSION]: 'max',
  [EXECUTION_INTENTS.GENERATE_VARIANTS]: 'paige',
  [EXECUTION_INTENTS.GENERATE_CAPACITY]: 'emmett',
  [EXECUTION_INTENTS.APPROVE_EXECUTION]: 'operator',
  [EXECUTION_INTENTS.EXECUTE_OUTBOUND]: 'emmett',
  [EXECUTION_INTENTS.OPERATOR_APPROVED]: 'operator',
});

const DISCOVERY_INTENTS = new Set([
  EXECUTION_INTENTS.APPROVE_DISCOVERY,
  EXECUTION_INTENTS.START_DISCOVERY,
]);

const SCOUT_INVESTIGATION_INTENTS = new Set([
  ...DISCOVERY_INTENTS,
  EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
]);

function deepFreeze(value) {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
  Object.keys(value).forEach((key) => deepFreeze(value[key]));
  return Object.freeze(value);
}

function isPlainObject(value) {
  return value != null && typeof value === 'object' && !Array.isArray(value);
}

function normalizePermissions(value) {
  const src = isPlainObject(value) ? value : {};
  return {
    canExecute: src.canExecute !== false,
    role: asText(src.role) || null,
  };
}

function intentFromAction(action) {
  const key = asText(action);
  return ACTION_TO_INTENT[key] || null;
}

function actionFromIntent(intent) {
  const key = asText(intent);
  return INTENT_TO_ACTION[key] || null;
}

function intentFromPendingDecision(pending) {
  if (!pending || typeof pending !== 'object') return null;
  const kind = asText(pending.kind);
  return PENDING_KIND_TO_INTENT[kind] || null;
}

function specialistForIntent(intent) {
  return INTENT_SPECIALISTS[asText(intent)] || null;
}

function isDiscoveryIntent(intent) {
  return DISCOVERY_INTENTS.has(asText(intent));
}

function isExecutionIntent(intent) {
  return INTENT_SET.has(asText(intent));
}

function isExecutionSource(source) {
  return SOURCE_SET.has(asText(source));
}

function isCanonicalExecutionRequest(value) {
  return Boolean(
    value &&
      typeof value === 'object' &&
      value.spec === 'SPEC-171' &&
      asText(value.id).startsWith('cer_') &&
      isExecutionIntent(value.intent) &&
      isExecutionSource(value.source) &&
      Object.isFrozen(value)
  );
}

/**
 * Mint an immutable Canonical Execution Request.
 * @param {object} input
 * @returns {Readonly<object>}
 */
function createExecutionRequest(input = {}) {
  const intent = asText(input.intent);
  if (!INTENT_SET.has(intent)) {
    throw amoError('cer_unknown_intent', `Unknown execution intent: ${intent || '(empty)'}.`);
  }
  const source = asText(input.source);
  if (!SOURCE_SET.has(source)) {
    throw amoError('cer_invalid', `Unknown execution source: ${source || '(empty)'}.`);
  }

  const missionId = asText(input.missionId) || null;
  const runtimeOwner = asText(input.runtimeOwner)
    || (input.mission ? resolveMissionRuntimeOwner(input.mission) : null)
    || (missionId ? resolveMissionRuntimeOwner(missionId) : null)
    || RUNTIME_OWNERS.AMO;

  const record = {
    spec: 'SPEC-171',
    id: asText(input.id) && asText(input.id).startsWith('cer_') && input.reuseId === true
      ? asText(input.id)
      : newId('cer'),
    source,
    missionId,
    operatorId: input.operatorId != null ? asText(input.operatorId) || null : null,
    intent,
    stage: asText(input.stage) || null,
    executionMode: asText(input.executionMode) || null,
    approval: input.approval && typeof input.approval === 'object' ? clone(input.approval) : null,
    permissions: normalizePermissions(input.permissions),
    runtimeOwner,
    objective: asText(input.objective) || null,
    payload: isPlainObject(input.payload) ? clone(input.payload) : {},
    metadata: isPlainObject(input.metadata) ? clone(input.metadata) : {},
    createdAt: asText(input.createdAt) || nowIso(),
  };

  return deepFreeze(record);
}

function createExecutionRequestFromChat(input = {}) {
  return createExecutionRequest({
    ...input,
    source: EXECUTION_SOURCES.CHAT,
    intent: input.intent || intentFromAction(input.action),
    payload: {
      question: asText(input.question) || null,
      ...(isPlainObject(input.payload) ? input.payload : {}),
    },
    metadata: {
      surface: 'chat',
      ...(isPlainObject(input.metadata) ? input.metadata : {}),
    },
  });
}

function createExecutionRequestFromApprovalButton(input = {}) {
  return createExecutionRequest({
    ...input,
    source: EXECUTION_SOURCES.APPROVAL_BUTTON,
    intent: input.intent || intentFromPendingDecision(input.pendingOperatorDecision),
    metadata: {
      surface: 'approval_button',
      ...(isPlainObject(input.metadata) ? input.metadata : {}),
    },
  });
}

function createExecutionRequestFromApi(input = {}) {
  return createExecutionRequest({
    ...input,
    source: input.source && SOURCE_SET.has(asText(input.source))
      ? asText(input.source)
      : EXECUTION_SOURCES.API,
    metadata: {
      surface: 'api',
      ...(isPlainObject(input.metadata) ? input.metadata : {}),
    },
  });
}

function createExecutionRequestFromVoice(input = {}) {
  return createExecutionRequest({
    ...input,
    source: EXECUTION_SOURCES.VOICE,
    intent: input.intent || EXECUTION_INTENTS.START_DISCOVERY,
    metadata: {
      surface: 'voice',
      ...(isPlainObject(input.metadata) ? input.metadata : {}),
    },
  });
}

function createExecutionRequestFromCommandDeck(input = {}) {
  return createExecutionRequest({
    ...input,
    source: EXECUTION_SOURCES.COMMAND_DECK,
    metadata: {
      surface: 'command_deck',
      ...(isPlainObject(input.metadata) ? input.metadata : {}),
    },
  });
}

function canonicalRequestShape(request) {
  if (!isCanonicalExecutionRequest(request)) return null;
  return {
    intent: request.intent,
    missionId: request.missionId,
    operatorId: request.operatorId,
    stage: request.stage,
    executionMode: request.executionMode,
    runtimeOwner: request.runtimeOwner,
    objective: request.objective,
    approval: request.approval,
    permissions: request.permissions,
    payload: request.payload,
  };
}

module.exports = {
  EXECUTION_SOURCES,
  EXECUTION_INTENTS,
  ACTION_TO_INTENT,
  INTENT_TO_ACTION,
  PENDING_KIND_TO_INTENT,
  INTENT_SPECIALISTS,
  createExecutionRequest,
  createExecutionRequestFromChat,
  createExecutionRequestFromApprovalButton,
  createExecutionRequestFromApi,
  createExecutionRequestFromVoice,
  createExecutionRequestFromCommandDeck,
  intentFromAction,
  actionFromIntent,
  intentFromPendingDecision,
  specialistForIntent,
  isDiscoveryIntent,
  isExecutionIntent,
  isExecutionSource,
  isCanonicalExecutionRequest,
  canonicalRequestShape,
};
