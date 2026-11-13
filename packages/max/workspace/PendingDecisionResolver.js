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
const {
  CAPTURE_INTENTS,
  safeClassifyPendingDecisionCaptureIntent,
  logPendingDecisionCaptureGuarded,
} = require('./pendingDecisionCaptureGuard');
const { isStructuredClarificationPending } = require('../../acquisition-mission/PendingDecisionResponseType');
const {
  stripNegatedDirectives,
  isConstraintQualifiedRequest,
  classifyAuthorizationPolarity,
} = require('./NegatedDirective');

/**
 * P0 execution-intent routing — an approval that names a specific operational
 * action only resolves a pending decision of the matching kind. "Go ahead and
 * send the emails" must not approve a pending plan or discovery decision.
 * Targets are read from the affirmative text (negated directives stripped).
 */
const ACTION_TARGETS = Object.freeze({
  outbound: /\b(?:send(?:s|ing)?|e-?mails?|outreach|outbound|campaign|launch(?:es|ing)?)\b/i,
  discovery: /\b(?:scout|discover(?:y|ies)?|find\s+(?:new\s+|more\s+)?(?:prospects|leads))\b/i,
  investigation: /\binvestigat/i,
  prioritization: /\bprioriti[sz]/i,
  plan: /\bplan\b/i,
});

const KIND_TARGETS = Object.freeze({
  // Plan approval is the internal gate in front of discovery and only advances
  // to the discovery decision (no Scout run), so "approved, begin discovery"
  // resolves it. Outbound never does.
  [OPERATOR_DECISION_KINDS.PLAN_APPROVAL]: ['plan', 'discovery'],
  [OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL]: ['discovery'],
  [OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION]: ['discovery', 'investigation'],
  [OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL]: ['prioritization'],
  [OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL]: ['outbound'],
});

/**
 * @returns {string[]|null} named targets when none of them matches the pending kind
 */
function mismatchedActionTargets(kind, q) {
  const allowed = KIND_TARGETS[kind];
  if (!allowed) return null;
  const affirmative = stripNegatedDirectives(q);
  const named = Object.keys(ACTION_TARGETS).filter((t) => ACTION_TARGETS[t].test(affirmative));
  if (!named.length || named.every((t) => allowed.includes(t))) return null;
  return named;
}

const RESOLUTION_OUTCOMES = Object.freeze({
  AFFIRM: 'affirm',
  REJECT: 'reject',
  MODIFY: 'modify',
  QUESTION: 'question',
  REQUEST_REVISION: 'request_revision',
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
  [OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION]: ['clarify_plan', 'modify_mission', 'cancel'],
  [OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL]: [
    'approve_execution',
    'revise_prepared_outreach',
    'request_revision',
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
  clarify_plan: EXECUTION_INTENTS.CLARIFY_PLAN,
  approve_execution: EXECUTION_INTENTS.APPROVE_EXECUTION,
  revise_prepared_outreach: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
  request_revision: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
  cancel: EXECUTION_INTENTS.CANCEL_PLAN,
});

function pendingFromMission(mission) {
  if (!mission || !mission.pendingOperatorDecision) return null;
  return mission.pendingOperatorDecision;
}

function isHoldOrDefer(q) {
  return (
    /\b(?:hold on|hold off|wait a(?: minute| sec)|not yet|pause for now)\b/i.test(q) &&
    !/\b(?:stop the mission|cancel)\b/i.test(q)
  );
}

function isQuestionAboutDecision(text) {
  // P0 #1089 — decision vocabulary inside a negated clause ("do not approve")
  // is a constraint, not a question about the pending decision.
  const q = stripNegatedDirectives(text) || String(text || '');
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
  // P0 authorization polarity — "don't send it" is a prohibition (held by the
  // polarity guard), not a mission cancellation; it was removed from this list.
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

function classifyPlanClarification(q, pending) {
  if (isCancelPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.REJECT, action: 'cancel', confidence: 0.95 };
  }
  if (isQuestionAboutDecision(q)) {
    return null;
  }
  if (isModifyPhrase(q) && !/\b(?:decision\s*:\s*)?region\s*[=:]/i.test(q)) {
    return { outcome: RESOLUTION_OUTCOMES.MODIFY, action: 'modify_mission', confidence: 0.9 };
  }
  if (
    pending &&
    pending.field === 'geography.region' &&
    (/\b(?:decision\s*:\s*)?region\s*[=:]/i.test(q) || q.trim().length > 0)
  ) {
    return { outcome: RESOLUTION_OUTCOMES.AFFIRM, action: 'clarify_plan', confidence: 0.94 };
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

const EXPLICIT_REVISION_RE = /\b(?:regenerate|redo|rework|revise|rewrite|rebuild|reset|reprepare|renew|prepare\s+a\s+new\s+version|bring\s+me\s+a\s+new\s+package|bring\s+me\s+another\s+version|new\s+version|new\s+package|update\s+the\s+messag|update\s+the\s+plan|rewrite\s+it|change\s+the\s+copy|change\s+the\s+messag)\b/i;

function isRevisionRequest(q) {
  return (
    (/\b(?:regenerate|rewrite|revise|re-?prepare|replace)\b/i.test(q)
      && /\b(?:message|messaging|outreach|plan|package|bundle|before execution|before sending)\b/i.test(q))
    || EXPLICIT_REVISION_RE.test(q)
  );
}

function classifyExecutionApproval(q) {
  if (/\b(?:regenerate|rewrite|revise|re-?prepare|replace)\b/i.test(q)
    && /\b(?:message|messaging|outreach|plan|package|bundle|before execution|before sending)\b/i.test(q)) {
    return { outcome: RESOLUTION_OUTCOMES.REQUEST_REVISION, action: 'request_revision', confidence: 0.96 };
  }
  if (isCancelPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.REJECT, action: 'cancel', confidence: 0.95 };
  }

  const explicitRevision = EXPLICIT_REVISION_RE.test(q);
  const explicitNoSend = /\b(?:do\s+not\s+send|don't\s+send|do\s+not\s+authorize|don't\s+authorize|do\s+not\s+approve|don't\s+approve|not\s+authorize|not\s+approve)\b/i.test(q);
  const explicitNegative = /\b(?:no\b|don't|do\s+not|not)\b/i.test(q) && /\b(?:send|authorize|approve)\b/i.test(q);

  if (explicitRevision || (/\b(?:regenerate|redo|rework|revise|rewrite)\b/i.test(q) && explicitNegative)) {
    return {
      outcome: RESOLUTION_OUTCOMES.REQUEST_REVISION,
      action: 'request_revision',
      confidence: 0.98,
    };
  }

  if (isModifyPhrase(q)) {
    return { outcome: RESOLUTION_OUTCOMES.MODIFY, action: 'modify_mission', confidence: 0.92 };
  }

  if (explicitNoSend || explicitNegative) {
    return { outcome: RESOLUTION_OUTCOMES.REJECT, action: 'cancel', confidence: 0.93 };
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
  [OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION]: classifyPlanClarification,
  [OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL]: classifyExecutionApproval,
});

function classifyByKind(kind, q, pending) {
  const classifier = KIND_CLASSIFIERS[kind];
  if (classifier) {
    const match = kind === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION
      ? classifier(q, pending)
      : classifier(q);
    if (match) return match;
  }
  const defaultAction = DEFAULT_AFFIRM_ACTION[kind];
  if (defaultAction && isGenericAffirmative(q)) {
    return { outcome: RESOLUTION_OUTCOMES.AFFIRM, action: defaultAction, confidence: 0.9 };
  }
  return null;
}

function buildResolution(mission, pending, classification, operatorUtterance = '') {
  const executionIntent = ACTION_EXECUTION_INTENT[classification.action] || null;
  const executable =
    classification.outcome === RESOLUTION_OUTCOMES.AFFIRM ||
    classification.outcome === RESOLUTION_OUTCOMES.REJECT ||
    classification.action === 'revise_prepared_outreach' ||
    classification.action === 'request_revision';

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
    canonicalResolution:
      classification.outcome === RESOLUTION_OUTCOMES.REQUEST_REVISION
        ? {
            kind: 'execution_approval',
            resolution: RESOLUTION_OUTCOMES.REQUEST_REVISION,
            missionId: mission.id,
            decisionKind: pending.kind,
            action: classification.action,
            operatorUtterance: String(operatorUtterance || '').trim() || pending.prompt || null,
          }
        : null,
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

function guardedUnresolved(mission, pending, outcome, captureIntent, options = {}) {
  try {
    if (
      outcome === RESOLUTION_OUTCOMES.UNRELATED &&
      captureIntent &&
      captureIntent !== CAPTURE_INTENTS.DECISION_RESPONSE &&
      captureIntent !== CAPTURE_INTENTS.AMBIGUOUS_SHORT_RESPONSE
    ) {
      logPendingDecisionCaptureGuarded({
        sessionId: options.sessionId || null,
        tenantId: options.tenantId || null,
        missionId: mission && mission.id ? mission.id : null,
        pendingDecision: pending,
        classification: captureIntent,
        messageChars: options.messageChars || 0,
        jevReason: Boolean(options.jevGuarded),
      });
    }
  } catch (_) {
    /* audit cannot break pending-decision resolution */
  }
  const resolution = buildUnresolvedResolution(mission, pending, outcome);
  if (captureIntent) resolution.captureIntent = captureIntent;
  if (options.captureGuarded) resolution.captureGuarded = true;
  return resolution;
}

/**
 * P0 authorization polarity verdict for a message against a decision kind.
 *
 *   clear    no prohibition of authorization/cancellation
 *   cancel   explicit affirmative cancellation (keeps its meaning)
 *   revision explicit revision request at the execution gate
 *   mixed    affirmative authorization contradicted by a prohibition of this
 *            decision (or an untargeted one) — ambiguous, never dispatched
 *   held     prohibition only — the decision is declined/held, never
 *            approved, sent, or cancelled
 *
 * @param {string} q
 * @param {string|null} kind
 */
function authorizationPolarityVerdict(q, kind = null) {
  const polarity = classifyAuthorizationPolarity(q);
  const nomination = classifyByKind(kind, polarity.affirmative);
  const affirmativeDecision = polarity.affirmativeAuthorization || polarity.affirmativeCancel ||
    isGenericAffirmative(polarity.affirmative) ||
    nomination?.outcome === RESOLUTION_OUTCOMES.AFFIRM || nomination?.action === 'cancel';
  // Authority requires an unqualified speech act. A hedge, question or
  // condition cannot be discarded while retaining its approval vocabulary.
  // No scheduler/constraint transaction is attached to this resolver, so
  // future-stage restrictions must be clarified rather than silently lost.
  if (kind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL && isRevisionRequest(q) &&
      !/\b(?:maybe|perhaps|possibly|if|unless)\b/i.test(q)) return { verdict: 'revision', polarity };
  const uncertain = /\b(?:maybe|perhaps|possibly|probably|tentatively|potentially|might|could|would|should|unsure|uncertain|not\s+(?:sure|certain|ready|convinced)|i\s+(?:think|guess|suppose)|on\s+second\s+thought|undecided)\b/i.test(q);
  const qualified = /\b(?:if|unless|until|after|once|when|provided|providing|assuming|pending|subject\s+to|only|as\s+long\s+as|hold|wait|defer|delay|postpone|later|tomorrow|next\s+week|but|however|except)\b/i.test(q);
  if (affirmativeDecision &&
      (uncertain || qualified || /\?/.test(q))) {
    return { verdict: 'mixed', polarity, qualification: true };
  }
  if (affirmativeDecision && !polarity.affirmativeCancel && polarity.negatedClauses.length) {
    return { verdict: 'mixed', polarity, qualification: true };
  }
  if (!polarity.negatedAuthorization && !polarity.negatedCancel) return { verdict: 'clear', polarity };
  if (polarity.affirmativeCancel) return { verdict: 'cancel', polarity };
  if (kind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL && isRevisionRequest(q)) return { verdict: 'revision', polarity };
  if (polarity.affirmativeAuthorization && !polarity.negatedCancel) {
    return { verdict: 'mixed', polarity };
  }
  return { verdict: polarity.affirmativeAuthorization ? 'mixed' : 'held', polarity };
}

/**
 * Returns a NON-dispatching resolution for a `held` or `mixed` verdict:
 * - held  → outcome REJECT, action 'decline', resolved:false: the operator
 *           declined this decision; nothing is approved, sent, or cancelled
 *           and the decision stays pending.
 * - mixed → outcome AMBIGUOUS: the operator is asked to answer explicitly.
 * Otherwise null (normal classification continues).
 */
function heldByAuthorizationPolarity(mission, pending, q, { onlyBareProhibition = false } = {}) {
  const { verdict, polarity, qualification } = authorizationPolarityVerdict(q, pending.kind);
  if (verdict !== 'held' && verdict !== 'mixed') return null;
  if (onlyBareProhibition && polarity.affirmative.replace(/[^a-z]/gi, '').length > 0) return null;
  const mixed = verdict === 'mixed';
  return {
    ...buildUnresolvedResolution(mission, pending, mixed ? RESOLUTION_OUTCOMES.AMBIGUOUS : RESOLUTION_OUTCOMES.REJECT),
    action: mixed ? null : 'decline',
    held: true,
    negatedAuthorization: polarity.negatedAuthorization,
    negatedCancelOnly: polarity.negatedCancel && !polarity.negatedAuthorization,
    mixedPolarity: mixed,
    qualifiedAuthorization: Boolean(qualification),
    executionIntent: null,
    executionAction: null,
    resolvedFromPendingDecision: false,
  };
}

/**
 * Resolve operator utterance against an active pending operator decision.
 * SPEC-JEV-004 — capture only when the message plausibly answers the decision.
 * @param {string} question
 * @param {object|null} mission
 * @param {object} [options]
 * @returns {object}
 */
function resolvePendingOperatorDecision(question, mission, options = {}) {
  const pending = pendingFromMission(mission);
  if (!pending || !pending.kind) {
    return { resolved: false };
  }

  const raw = normalizeText(question);
  if (!raw) return { resolved: false };

  const q = normalizePendingDecisionTypos(raw);
  const captureInput = {
    message: q,
    pendingDecision: pending,
    shadowDecision: options.shadowDecision || null,
    sessionId: options.sessionId || null,
    tenantId: options.tenantId || null,
    missionId: mission && mission.id ? mission.id : null,
  };
  const captureIntent = safeClassifyPendingDecisionCaptureIntent(captureInput);
  const messageChars = q.length;
  const earlyHeld = heldByAuthorizationPolarity(mission, pending, q);
  if (earlyHeld && earlyHeld.mixedPolarity && !isConstraintQualifiedRequest(q) &&
      captureIntent !== CAPTURE_INTENTS.PENDING_DECISION_CLARIFICATION &&
      !isQuestionAboutDecision(q)) return earlyHeld;

  if (
    pending.kind === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION &&
    isStructuredClarificationPending(pending)
  ) {
    if (/^(?:cancel|abort)(?:\s+(?:it|the mission|this mission))?\.?$/i.test(q)) {
      return buildResolution(mission, pending, {
        outcome: RESOLUTION_OUTCOMES.REJECT, action: 'cancel', confidence: 0.95,
      }, question);
    }
    if (captureIntent !== CAPTURE_INTENTS.DECISION_RESPONSE) {
      return guardedUnresolved(mission, pending, RESOLUTION_OUTCOMES.UNRELATED, captureIntent, {
        ...options, messageChars, captureGuarded: true,
      });
    }
    // A bare prohibition ("Don't proceed.") is not a clarification answer.
    const clarificationHeld = heldByAuthorizationPolarity(mission, pending, q, { onlyBareProhibition: true });
    if (clarificationHeld) return clarificationHeld;
    return buildResolution(mission, pending, {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'clarify_plan',
      confidence: 0.94,
    }, question);
  }

  // P0 #1089 — prohibitions that qualify a separate informational request are
  // constraints on that request, never an approve/reject answer (a negated
  // "do not approve or resume missions" previously resolved as REJECT/cancel).
  if (isConstraintQualifiedRequest(q)) {
    return guardedUnresolved(mission, pending, RESOLUTION_OUTCOMES.UNRELATED, CAPTURE_INTENTS.UNRELATED_OR_QUESTION, {
      ...options, messageChars, captureGuarded: true,
    });
  }

  if (isHoldOrDefer(q)) {
    return buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.AMBIGUOUS);
  }

  if (isUnrelatedTopic(q)) {
    return buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.UNRELATED);
  }

  if (
    captureIntent === CAPTURE_INTENTS.PENDING_DECISION_CLARIFICATION ||
    isQuestionAboutDecision(q)
  ) {
    return buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.QUESTION);
  }

  if (
    captureIntent === CAPTURE_INTENTS.INSPECTION_OR_STATUS_QUESTION ||
    captureIntent === CAPTURE_INTENTS.UNRELATED_OR_QUESTION ||
    captureIntent === CAPTURE_INTENTS.AMBIGUOUS
  ) {
    return guardedUnresolved(
      mission,
      pending,
      RESOLUTION_OUTCOMES.UNRELATED,
      captureIntent,
      { ...options, messageChars, captureGuarded: true }
    );
  }

  // Weak fillers are not explicit approve/reject even though they are short.
  if (
    captureIntent === CAPTURE_INTENTS.AMBIGUOUS_SHORT_RESPONSE &&
    /^(?:sure|ok(?:ay)?|maybe|hmm|uh|um)\.?$/i.test(q)
  ) {
    return buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.AMBIGUOUS);
  }

  // P0 authorization polarity — a prohibition is never an approval and never a
  // mission cancellation. "Don't proceed." / "Do not approve this." / "No,
  // don't send this." previously resolved to AFFIRM (raw "proceed"/"approve")
  // or to REJECT → cancel the whole mission plan. Only these explicit forms
  // keep a dispatching outcome:
  //   - an affirmative cancellation in the same message ("Don't send it. Cancel
  //     the mission.") → explicit cancel;
  //   - an explicit revision request at the execution gate ("Don't send this —
  //     rewrite the messaging") → revision (handled by the kind classifier).
  const held = heldByAuthorizationPolarity(mission, pending, q);
  if (held) return held;

  const classification = classifyByKind(pending.kind, q, pending);
  if (classification) {
    if (classification.outcome === RESOLUTION_OUTCOMES.AFFIRM) {
      const mismatch = mismatchedActionTargets(pending.kind, q);
      if (mismatch) {
        return {
          ...buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.AMBIGUOUS),
          requestedActionMismatch: mismatch,
        };
      }
    }
    return buildResolution(mission, pending, classification, question);
  }

  if (
    captureIntent === CAPTURE_INTENTS.DECISION_RESPONSE &&
    pending.kind === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION
  ) {
    return buildResolution(mission, pending, {
      outcome: RESOLUTION_OUTCOMES.AFFIRM,
      action: 'clarify_plan',
      confidence: 0.88,
    }, question);
  }

  if (captureIntent === CAPTURE_INTENTS.DECISION_RESPONSE) {
    return buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.AMBIGUOUS);
  }

  if (captureIntent === CAPTURE_INTENTS.AMBIGUOUS_SHORT_RESPONSE || captureIntent == null) {
    return buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.AMBIGUOUS);
  }

  return guardedUnresolved(
    mission,
    pending,
    RESOLUTION_OUTCOMES.UNRELATED,
    captureIntent || CAPTURE_INTENTS.AMBIGUOUS,
    { ...options, messageChars, captureGuarded: true }
  );
}

function pendingDecisionRequestsExecution(resolution) {
  return Boolean(resolution && resolution.resolvedFromPendingDecision && resolution.executionIntent);
}

module.exports = {
  ACTION_TARGETS,
  KIND_TARGETS,
  authorizationPolarityVerdict,
  mismatchedActionTargets,
  RESOLUTION_OUTCOMES,
  PENDING_ALLOWED_ACTIONS,
  resolvePendingOperatorDecision,
  pendingDecisionRequestsExecution,
  pendingDecisionOwnsTurn,
  normalizePendingDecisionTypos,
  levenshtein,
};
