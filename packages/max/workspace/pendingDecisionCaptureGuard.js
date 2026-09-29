'use strict';

/**
 * SPEC-JEV-004 — Pending Decision Capture Guard.
 * Pending-decision resolution may capture an operator message only when it
 * plausibly answers the pending decision. Status, inspection, and unrelated
 * questions fall through to normal routing. Jev remains optional/shadow-only.
 */

const CAPTURE_INTENTS = Object.freeze({
  DECISION_RESPONSE: 'decision_response',
  PENDING_DECISION_CLARIFICATION: 'pending_decision_clarification',
  INSPECTION_OR_STATUS_QUESTION: 'inspection_or_status_question',
  UNRELATED_OR_QUESTION: 'unrelated_or_question',
  AMBIGUOUS_SHORT_RESPONSE: 'ambiguous_short_response',
  AMBIGUOUS: 'ambiguous',
});

const SHORT_RESPONSE_MAX_WORDS = 8;
const SHORT_RESPONSE_MAX_CHARS = 80;

const APPROVAL_PATTERNS = [
  /^yes\b/i,
  /^yep\b/i,
  /^yeah\b/i,
  /^yup\b/i,
  /^approved?\b/i,
  /\bapprove\b/i,
  /\bgo ahead\b/i,
  /\bdo it\b/i,
  /\bsounds good\b/i,
  /\bbegin\b.*\bdiscovery\b/i,
  /\bstart\b.*\bdiscovery\b/i,
  /\bkeep going\b/i,
  /^(?:continue|proceed|resume|next)\b/i,
];

const REJECTION_PATTERNS = [
  /^no\b/i,
  /\breject\b/i,
  /\bhold off\b/i,
  /\bhold on\b/i,
  /\bpause\b/i,
  /\bnot yet\b/i,
  /\bdon't\b.*\bapprove\b/i,
  /\bdo not\b.*\bapprove\b/i,
  /\bcancel\b/i,
  /\babort\b/i,
  /\bstop the mission\b/i,
];

const DECISION_CONTROL_PATTERNS = [
  /\b(?:change|modify|update|edit|adjust|instead|retarget)\b/i,
  /\b(?:regenerate|rewrite|revise|re-?prepare|rework)\b/i,
  /\bkeep investigating\b/i,
  /\bcontinue investigating\b/i,
];

const STATUS_INSPECTION_PATTERNS = [
  /\bstatus\b/i,
  /\bconfidence\b/i,
  /\bcurrent\b.*\bmission\b/i,
  /\bwhat\b.*\bwaiting\b/i,
  /\bblocked\b/i,
  /\bshow\b.*\bmission\b/i,
  /\bhow many\b.*\bprospects\b/i,
  /\bprospects\b.*\bloaded\b/i,
  /\bwhat happened\b/i,
  /\bhow confident\b/i,
  /\bmission state\b/i,
  /\bdo we have prospects\b/i,
];

const PENDING_DECISION_CLARIFICATION_PATTERNS = [
  /\bwhat\b.*\bapprov/i,
  /\bwhy\b.*\bapprov/i,
  /\bwhat happens\b.*\bapprov/i,
  /\bwhat does\b.*\bapprov/i,
  /\bexplain\b.*\bpending\b/i,
  /\bpending decision\b/i,
  /\bwhat exactly am i\b/i,
];

const GUARD_REASONS = Object.freeze({
  [CAPTURE_INTENTS.INSPECTION_OR_STATUS_QUESTION]:
    'operator_message_looks_like_status_or_inspection',
  [CAPTURE_INTENTS.UNRELATED_OR_QUESTION]:
    'operator_message_looks_like_unrelated_question',
  [CAPTURE_INTENTS.AMBIGUOUS]: 'operator_message_too_long_for_pending_capture',
  jev_shadow_status_check: 'jev_shadow_status_check',
});

/** @type {((input: object) => string)|null} */
let classifyOverride = null;

/** @type {object[]} */
const _auditLog = [];

function normalizeMessage(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function wordCount(message) {
  const trimmed = normalizeMessage(message);
  if (!trimmed) return 0;
  return trimmed.split(/\s+/).length;
}

function matchesAny(message, patterns) {
  return patterns.some((pattern) => pattern.test(message));
}

function isPrimarilyStatusQuestion(message, statusMatched) {
  if (!statusMatched) return false;
  if (/\?/.test(message)) return true;
  return !matchesAny(message, APPROVAL_PATTERNS) && !matchesAny(message, REJECTION_PATTERNS);
}

function readShadowDecision(shadowDecision) {
  if (!shadowDecision || typeof shadowDecision !== 'object') return null;
  const nested =
    shadowDecision.decision && typeof shadowDecision.decision === 'object'
      ? shadowDecision.decision
      : shadowDecision;
  return {
    intent: nested.intent || null,
    recommendedRoute: nested.recommended_route || nested.recommendedRoute || null,
    inspectionProbability: Number(
      nested.inspection_probability ?? nested.inspectionProbability
    ),
    approvalProbability: Number(
      nested.approval_probability ?? nested.approvalProbability
    ),
    confidence: Number(nested.confidence),
  };
}

/**
 * Optional Jev/shadow heuristic. Never required. Never overrides a clear
 * approval/rejection or a pending-decision clarification question.
 */
function isHighConfidenceJevInspection(shadowDecision) {
  const row = readShadowDecision(shadowDecision);
  if (!row) return false;
  return (
    row.intent === 'status_check' &&
    row.recommendedRoute === 'inspection' &&
    row.inspectionProbability >= 0.85 &&
    row.approvalProbability < 0.5 &&
    row.confidence >= 0.9
  );
}

function classifyPendingDecisionCaptureIntentImpl(input = {}) {
  const message = normalizeMessage(input.message || input.question);
  const pendingDecision = input.pendingDecision || null;

  if (!message || !pendingDecision) {
    return CAPTURE_INTENTS.UNRELATED_OR_QUESTION;
  }

  if (matchesAny(message, PENDING_DECISION_CLARIFICATION_PATTERNS)) {
    return CAPTURE_INTENTS.PENDING_DECISION_CLARIFICATION;
  }

  const approvalMatched = matchesAny(message, APPROVAL_PATTERNS);
  const rejectionMatched = matchesAny(message, REJECTION_PATTERNS);
  const controlMatched = matchesAny(message, DECISION_CONTROL_PATTERNS);
  const statusMatched = matchesAny(message, STATUS_INSPECTION_PATTERNS);

  if (
    (approvalMatched || rejectionMatched || controlMatched) &&
    !isPrimarilyStatusQuestion(message, statusMatched)
  ) {
    return CAPTURE_INTENTS.DECISION_RESPONSE;
  }

  if (statusMatched) {
    return CAPTURE_INTENTS.INSPECTION_OR_STATUS_QUESTION;
  }

  if (message.includes('?')) {
    return CAPTURE_INTENTS.UNRELATED_OR_QUESTION;
  }

  const short =
    wordCount(message) <= SHORT_RESPONSE_MAX_WORDS &&
    message.length <= SHORT_RESPONSE_MAX_CHARS;
  return short
    ? CAPTURE_INTENTS.AMBIGUOUS_SHORT_RESPONSE
    : CAPTURE_INTENTS.AMBIGUOUS;
}

/**
 * Classify whether an operator message should be captured as a pending-decision
 * response. Deterministic text rules are authoritative. Optional shadowDecision
 * may only prevent capture of non-decision messages.
 *
 * @param {object} input
 * @returns {string} one of CAPTURE_INTENTS
 */
function classifyPendingDecisionCaptureIntent(input = {}) {
  const classify = classifyOverride || classifyPendingDecisionCaptureIntentImpl;
  const classification = classify(input);
  if (
    classification !== CAPTURE_INTENTS.DECISION_RESPONSE &&
    classification !== CAPTURE_INTENTS.PENDING_DECISION_CLARIFICATION &&
    isHighConfidenceJevInspection(input.shadowDecision)
  ) {
    return CAPTURE_INTENTS.INSPECTION_OR_STATUS_QUESTION;
  }
  return classification;
}

function shouldAttemptPendingDecisionCapture(input = {}) {
  const intent = classifyPendingDecisionCaptureIntent(input);
  return (
    intent === CAPTURE_INTENTS.DECISION_RESPONSE ||
    intent === CAPTURE_INTENTS.AMBIGUOUS_SHORT_RESPONSE
  );
}

function shouldForcePendingDecisionClarification(input = {}) {
  return (
    classifyPendingDecisionCaptureIntent(input) ===
    CAPTURE_INTENTS.AMBIGUOUS_SHORT_RESPONSE
  );
}

function pendingDecisionId(pendingDecision, missionId) {
  if (!pendingDecision || typeof pendingDecision !== 'object') return null;
  if (pendingDecision.id) return String(pendingDecision.id);
  const kind = pendingDecision.kind || null;
  if (kind && missionId) return `${missionId}:${kind}`;
  return kind || null;
}

function logPendingDecisionCaptureGuarded(payload = {}) {
  const classification = payload.classification || null;
  const row = {
    event: 'PENDING_DECISION_CAPTURE_GUARDED',
    spec: 'SPEC-JEV-004',
    session_id: payload.sessionId || payload.session_id || null,
    tenant_id: payload.tenantId || payload.tenant_id || null,
    mission_id: payload.missionId || payload.mission_id || null,
    pending_decision_id:
      payload.pendingDecisionId ||
      payload.pending_decision_id ||
      pendingDecisionId(payload.pendingDecision, payload.missionId || payload.mission_id),
    classification,
    reason:
      payload.reason ||
      GUARD_REASONS[payload.jevReason ? 'jev_shadow_status_check' : classification] ||
      'operator_message_not_a_pending_decision_response',
    message_chars:
      typeof payload.messageChars === 'number'
        ? payload.messageChars
        : Number(payload.message_chars) || 0,
  };
  _auditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info('[PENDING_DECISION_CAPTURE_GUARDED]', JSON.stringify(row));
  }
  return row;
}

function logPendingDecisionCaptureGuardError(error, payload = {}) {
  const row = {
    event: 'PENDING_DECISION_CAPTURE_GUARD_ERROR',
    spec: 'SPEC-JEV-004',
    session_id: payload.sessionId || null,
    tenant_id: payload.tenantId || null,
    mission_id: payload.missionId || null,
    error: error && error.message ? String(error.message).slice(0, 200) : 'unknown',
  };
  _auditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.warn('[PENDING_DECISION_CAPTURE_GUARD_ERROR]', JSON.stringify(row));
  }
  return row;
}

/**
 * Safe wrapper: never throws. On classifier failure, returns null so callers
 * keep the previous pending-decision path.
 */
function safeClassifyPendingDecisionCaptureIntent(input = {}) {
  try {
    return classifyPendingDecisionCaptureIntent(input);
  } catch (error) {
    try {
      logPendingDecisionCaptureGuardError(error, input);
    } catch (_) {
      /* audit cannot break routing */
    }
    return null;
  }
}

function setPendingDecisionCaptureClassifierForTests(fn) {
  classifyOverride = typeof fn === 'function' ? fn : null;
}

function resetPendingDecisionCaptureClassifierForTests() {
  classifyOverride = null;
}

function listPendingDecisionCaptureGuardLog() {
  return _auditLog.map((row) => ({ ...row }));
}

function clearPendingDecisionCaptureGuardLog() {
  _auditLog.length = 0;
}

module.exports = {
  CAPTURE_INTENTS,
  GUARD_REASONS,
  SHORT_RESPONSE_MAX_WORDS,
  SHORT_RESPONSE_MAX_CHARS,
  classifyPendingDecisionCaptureIntent,
  classifyPendingDecisionCaptureIntentImpl,
  shouldAttemptPendingDecisionCapture,
  shouldForcePendingDecisionClarification,
  isHighConfidenceJevInspection,
  safeClassifyPendingDecisionCaptureIntent,
  logPendingDecisionCaptureGuarded,
  logPendingDecisionCaptureGuardError,
  pendingDecisionId,
  setPendingDecisionCaptureClassifierForTests,
  resetPendingDecisionCaptureClassifierForTests,
  listPendingDecisionCaptureGuardLog,
  clearPendingDecisionCaptureGuardLog,
};
