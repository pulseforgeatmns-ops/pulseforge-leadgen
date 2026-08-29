'use strict';

/**
 * SPEC-197 — Pending Decision Conversational Resolution (ADR-061 extension).
 * SPEC-202 — Pending decision turn ownership + constrained typo normalization.
 * Natural-language operator answers resolve against the active pending decision
 * before generic operator cognition may demote or reinterpret the utterance.
 */

const { OPERATOR_DECISION_KINDS } = require('../../acquisition-mission/types');
const {
  EXECUTION_INTENTS,
  actionFromIntent,
} = require('../../acquisition-mission/ExecutionRequest');
const {
  normalizeText,
  normalizePendingDecisionTypos,
  levenshtein,
} = require('./BoundedTypoNormalization');

const RESOLUTION_OUTCOMES = Object.freeze({
  AFFIRM: 'affirm',
  REJECT: 'reject',
  MODIFY: 'modify',
  QUESTION: 'question',
  UNRELATED: 'unrelated',
  AMBIGUOUS: 'ambiguous',
});

const PENDING_ALLOWED_ACTIONS = Object.freeze({
  [OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION]: [
    'continue_investigation',
    'modify_mission',
    'cancel',
  ],
  [OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL]: [
    'approve_prioritization',
    'modify_mission',
    'cancel',
  ],
  [OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL]: [
    'approve_discovery',
    'modify_mission',
    'cancel',
  ],
  [OPERATOR_DECISION_KINDS.PLAN_APPROVAL]: ['approve_plan', 'modify_mission', 'cancel'],
  [OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL]: [
    'approve_execution',
    'modify_mission',
    'cancel',
  ],
});

const DEFAULT_AFFIRM_ACTION = Object.freeze({
  [OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION]: 'continue_investigation',
  [OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL]: 'approve_prioritization',
  [OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL]: 'approve_discovery',
  [OPERATOR_DECISION_KINDS.PLAN_APPROVAL]: 'approve_plan',
  [OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL]: 'approve_execution',
});

const ACTION_EXECUTION_INTENT = Object.freeze({
  continue_investigation: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
  approve_prioritization: EXECUTION_INTENTS.APPROVE_PRIORITIZATION,
  approve_discovery: EXECUTION_INTENTS.APPROVE_DISCOVERY,
  approve_plan: EXECUTION_INTENTS.APPROVE_PLAN,
  approve_execution: EXECUTION_INTENTS.APPROVE_EXECUTION,
  cancel: EXECUTION_INTENTS.CANCEL_PLAN,
});

function pendingFromMission(mission) {
  if (!mission || !mission.pendingOperatorDecision) return null;
  return mission.pendingOperatorDecision;
}

function isHoldOrDefer(q) {
  return (
    /\b(?:hold on|wait a(?: minute| sec)|not yet|pause for now)\b/i.test(q) &&
    !/\b(?:stop the mission|cancel)\b/i.test(q)
  );
}

function isQuestionAboutDecision(q) {
  if (/^(?:why|what|how|where|when)\??$/i.test(q)) return true;
  if (/\bwhat (?:happens|does|would|will)\b/i.test(q)) return true;
  if (/\bwhat(?:'s| is| are) the (?:biggest )?risks?\b/i.test(q)) return true;
  if (/\b(?:explain|tell me about|clarify)\b/i.test(q) && /\?/.test(q)) return true;
  if (
    /\?/.test(q) &&
    /\b(?:continu|investig|approv|priorit|launch|outbound|campaign|decision)\b/i.test(q)
  ) {
    return true;
  }
  return false;
}

function isUnrelatedTopic(q) {
  if (/\b(?:today'?s? briefing|daily briefing|manager briefing)\b/i.test(q)) return true;
  if (/\b(?:show me|what(?:'s| is)) today'?s? pipeline\b/i.test(q)) return true;
  if (/\bshow me today'?s? pipeline instead\b/i.test(q)) return true;
  if (/\bwhy did scout reject\b/i.test(q)) return true;
  return false;
}

function isCancelPhrase(q) {
  return /\b(?:cancel(?:\s+(?:it|the mission|this))?|stop(?:\s+the)?\s+mission|abort(?:\s+the\s+mission)?)\b/i.test(
    q
  );
}

function isModifyPhrase(q) {
  if (/^(?:yes|yep|yeah|approved?|go ahead|proceed|continue|sounds good|do it)\.?$/i.test(q)) {
    return false;
  }
  return /\b(?:change|modify|update|edit|adjust|instead|retarget|target(?:ing)?)\b/i.test(q);
}

function isGenericAffirmative(q) {
  if (/^(?:yes|yep|yeah|yup|ok(?:ay)?|sure|approved?|sounds good|do it)\.?$/i.test(q)) {
    return true;
  }
  if (/^(?:go ahead|proceed|keep going)\.?$/i.test(q)) return true;
  if (/\b(?:looks good|move forward)\b/i.test(q)) return true;
  if (/\b(?:go ahead|approved?|sounds good)\b/i.test(q) && q.split(/\s+/).length <= 8) {
    return true;
  }
  return false;
}

function isBareContinuation(q) {
  return /^(?:continue|proceed|resume|next)\.?$/i.test(q);
}

function classifyDiscoveryInvestigation(q) {
  if (isCancelPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.REJECT, action: 'cancel', confidence: 0.95 };
  }
  if (isModifyPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.MODIFY, action: 'modify_mission', confidence: 0.92 };
  }
  if (isBareContinuation(q)) {
    return {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'continue_investigation',
      confidence: 0.98,
    };
  }
  if (
    /\b(?:continue|retry|resume|proceed|keep)\b/i.test(q) &&
    /\binvestig/i.test(q)
  ) {
    return {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'continue_investigation',
      confidence: 0.98,
    };
  }
  if (/\b(?:keep investigating|continue investigating)\b/i.test(q)) {
    return {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'continue_investigation',
      confidence: 0.97,
    };
  }
  if (
    /\b(?:continue|keep going|go ahead|proceed|approved?|sounds good|do it)\b/i.test(q)
  ) {
    return {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'continue_investigation',
      confidence: 0.95,
    };
  }
  if (isGenericAffirmative(q)) {
    return {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'continue_investigation',
      confidence: 0.94,
    };
  }
  return null;
}

function classifyPrioritizationApproval(q) {
  if (isCancelPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.REJECT, action: 'cancel', confidence: 0.95 };
  }
  if (isModifyPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.MODIFY, action: 'modify_mission', confidence: 0.92 };
  }
  if (
    /\b(?:approv(e|al|ed)|priorit|go ahead|proceed|looks good|move forward)\b/i.test(q) ||
    isGenericAffirmative(q) ||
    isBareContinuation(q)
  ) {
    return {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'approve_prioritization',
      confidence: 0.94,
    };
  }
  return null;
}

function classifyDiscoveryApproval(q) {
  if (isCancelPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.REJECT, action: 'cancel', confidence: 0.95 };
  }
  if (isModifyPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.MODIFY, action: 'modify_mission', confidence: 0.92 };
  }
  if (
    /\b(?:approv(e|al|ed)|begin|start|run|discover|go ahead|proceed)\b/i.test(q) ||
    isGenericAffirmative(q)
  ) {
    return {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'approve_discovery',
      confidence: 0.94,
    };
  }
  return null;
}

function classifyPlanApproval(q) {
  if (isCancelPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.REJECT, action: 'cancel', confidence: 0.95 };
  }
  if (/\bedit\b/i.test(q) && !/\bapprov/i.test(q)) {
    return { outcome: RESOLUTION_OUTCOMES.MODIFY, action: 'modify_mission', confidence: 0.9 };
  }
  if (isModifyPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.MODIFY, action: 'modify_mission', confidence: 0.92 };
  }
  if (/\b(?:approv(e|al|ed)|proceed|go ahead)\b/i.test(q) || isGenericAffirmative(q)) {
    return {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'approve_plan',
      confidence: 0.94,
    };
  }
  return null;
}

function classifyExecutionApproval(q) {
  if (isCancelPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.REJECT, action: 'cancel', confidence: 0.95 };
  }
  if (isModifyPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.MODIFY, action: 'modify_mission', confidence: 0.92 };
  }
  if (
    /\b(?:approv(e|al|ed)|authoriz(e|ed|ation)|execute|launch|go ahead|proceed|outbound|send)\b/i.test(
      q
    ) ||
    isGenericAffirmative(q)
  ) {
    return {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'approve_execution',
      confidence: 0.94,
    };
  }
  return null;
}

const KIND_CLASSIFIERS = Object.freeze({
  [OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION]: classifyDiscoveryInvestigation,
  [OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL]: classifyPrioritizationApproval,
  [OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL]: classifyDiscoveryApproval,
  [OPERATOR_DECISION_KINDS.PLAN_APPROVAL]: classifyPlanApproval,
  [OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL]: classifyExecutionApproval,
});

function classifyByKind(kind, q) {
  const classifier = KIND_CLASSIFIERS[kind];
  if (classifier) {
    const match = classifier(q);
    if (match) return match;
  }
  const defaultAction = DEFAULT_AFFIRM_ACTION[kind];
  if (defaultAction && isGenericAffirmative(q)) {
    return { outcome: RESOLUTION_OUTCOMES.AFFIRM, action: defaultAction, confidence: 0.9 };
  }
  return null;
}

function buildResolution(mission, pending, classification) {
  const executionIntent = ACTION_EXECUTION_INTENT[classification.action] || null;
  const executable =
    classification.outcome === RESOLUTION_OUTCOMES.AFFIRM ||
    classification.outcome === RESOLUTION_OUTCOMES.REJECT;

  return {
    pending: true,
    resolved: true,
    resolvedFromPendingDecision: executable,
    decisionKind: pending.kind,
    action: classification.action,
    outcome: classification.outcome,
    confidence: classification.confidence,
    missionId: mission.id,
    prompt: pending.prompt || null,
    allowedActions: PENDING_ALLOWED_ACTIONS[pending.kind] || [],
    executionIntent,
    executionAction: executionIntent ? actionFromIntent(executionIntent) : null,
  };
}

function buildUnresolvedResolution(mission, pending, outcome) {
  return {
    pending: true,
    resolved: false,
    outcome,
    decisionKind: pending.kind,
    missionId: mission.id,
    prompt: pending.prompt || null,
    allowedActions: PENDING_ALLOWED_ACTIONS[pending.kind] || [],
  };
}

function pendingDecisionOwnsTurn(resolution) {
  if (!resolution || resolution.pending !== true) return false;
  if (resolution.resolved === true) return false;
  if (resolution.outcome === RESOLUTION_OUTCOMES.UNRELATED) return false;
  return true;
}

/**
 * Resolve operator utterance against an active pending operator decision.
 * @param {string} question
 * @param {object|null} mission
 * @returns {object}
 */
function resolvePendingOperatorDecision(question, mission) {
  const pending = pendingFromMission(mission);
  if (!pending || !pending.kind) {
    return { resolved: false };
  }

  const raw = normalizeText(question);
  if (!raw) return { resolved: false };

  const q = normalizePendingDecisionTypos(raw);

  if (isHoldOrDefer(q)) {
    return buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.AMBIGUOUS);
  }

  if (isUnrelatedTopic(q)) {
    return buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.UNRELATED);
  }

  if (isQuestionAboutDecision(q)) {
    return buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.QUESTION);
  }

  const classification = classifyByKind(pending.kind, q);
  if (!classification) {
    return buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.AMBIGUOUS);
  }

  return buildResolution(mission, pending, classification);
}

function pendingDecisionRequestsExecution(resolution) {
  return Boolean(resolution && resolution.resolvedFromPendingDecision && resolution.executionIntent);
}

module.exports = {
  RESOLUTION_OUTCOMES,
  PENDING_ALLOWED_ACTIONS,
  resolvePendingOperatorDecision,
  pendingDecisionRequestsExecution,
  pendingDecisionOwnsTurn,
  normalizePendingDecisionTypos,
  levenshtein,
};
