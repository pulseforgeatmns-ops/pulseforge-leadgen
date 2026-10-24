'use strict';

/**
 * Pending operator decision response typing — yes/no gates vs free-text clarifications.
 */

const {
  OPERATOR_DECISION_KINDS,
  OPERATOR_DECISION_RESPONSE_TYPES,
} = require('./types');

const YES_NO_KINDS = new Set([
  OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
  OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
  OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
  OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
  OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
]);

function choiceCount(source) {
  if (!source || typeof source !== 'object') return 0;
  return Array.isArray(source.choices) ? source.choices.length : 0;
}

function responseTypeForAmbiguity(ambiguity) {
  if (!ambiguity || typeof ambiguity !== 'object') {
    return OPERATOR_DECISION_RESPONSE_TYPES.YES_NO;
  }
  if (ambiguity.responseType) return ambiguity.responseType;
  if (ambiguity.field === 'geography.region') {
    return choiceCount(ambiguity) > 1
      ? OPERATOR_DECISION_RESPONSE_TYPES.CHOICE
      : OPERATOR_DECISION_RESPONSE_TYPES.TEXT;
  }
  if (choiceCount(ambiguity) > 1) return OPERATOR_DECISION_RESPONSE_TYPES.CHOICE;
  if (choiceCount(ambiguity) === 1) return OPERATOR_DECISION_RESPONSE_TYPES.CHOICE;
  return OPERATOR_DECISION_RESPONSE_TYPES.TEXT;
}

function responseTypeForPendingDecision(pending) {
  if (!pending || typeof pending !== 'object') return null;
  if (pending.responseType) return pending.responseType;
  if (pending.kind === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION) {
    if (pending.field === 'geography.region') {
      return choiceCount(pending) > 1
        ? OPERATOR_DECISION_RESPONSE_TYPES.CHOICE
        : OPERATOR_DECISION_RESPONSE_TYPES.TEXT;
    }
    return choiceCount(pending) > 1
      ? OPERATOR_DECISION_RESPONSE_TYPES.CHOICE
      : OPERATOR_DECISION_RESPONSE_TYPES.TEXT;
  }
  if (pending.kind === OPERATOR_DECISION_KINDS.PLAN_EDIT) {
    return OPERATOR_DECISION_RESPONSE_TYPES.TEXT;
  }
  if (YES_NO_KINDS.has(pending.kind)) {
    return OPERATOR_DECISION_RESPONSE_TYPES.YES_NO;
  }
  return OPERATOR_DECISION_RESPONSE_TYPES.YES_NO;
}

function isStructuredClarificationPending(pending) {
  const type = responseTypeForPendingDecision(pending);
  return (
    type === OPERATOR_DECISION_RESPONSE_TYPES.TEXT ||
    type === OPERATOR_DECISION_RESPONSE_TYPES.CHOICE
  );
}

function isYesNoPendingDecision(pending) {
  return responseTypeForPendingDecision(pending) === OPERATOR_DECISION_RESPONSE_TYPES.YES_NO;
}

module.exports = {
  responseTypeForAmbiguity,
  responseTypeForPendingDecision,
  isStructuredClarificationPending,
  isYesNoPendingDecision,
};
