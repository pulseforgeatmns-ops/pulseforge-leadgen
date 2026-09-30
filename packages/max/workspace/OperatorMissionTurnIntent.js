'use strict';

/**
 * Guard operator turns that ask for operational status from mission approval / execution.
 * Status queries are read-only unless explicit mission approval language is also present.
 */

const OPERATOR_TURN_INTENT_TYPES = Object.freeze({
  STATUS_QUERY: 'STATUS_QUERY',
  MISSION_APPROVAL: 'MISSION_APPROVAL',
  OTHER: 'OTHER',
});

const STATUS_QUERY_DOMAINS = Object.freeze({
  PAIGE_SOCIAL: 'paige_social',
  OPERATOR_APPROVAL: 'operator_approval',
  GENERAL: 'general',
});

const STATUS_QUERY_ACTIONS = Object.freeze({
  READ_PENDING_APPROVAL_QUEUE: 'read_pending_approval_queue',
  READ_PENDING_OPERATOR_APPROVAL_ITEMS: 'read_pending_operator_approval_items',
});

const EXPLICIT_MISSION_APPROVAL_RES = [
  /\bapproved\b/i,
  /\bapprove(?:d)?\s*,?\s*proceed\b/i,
  /\byes\s+proceed\b/i,
  /\b(?:yes|yeah|yep)\s*,?\s*proceed\b/i,
  /\brun\s+it\b/i,
  /\bexecute\b/i,
  /\bstart\s+the\s+mission\b/i,
  /\bcontinue\s+with\s+this\s+mission\b/i,
  /\bgo\s+ahead\s+and\b/i,
  /\bapproved\s*,?\s*proceed\b/i,
  /\bproceed\s+with\b/i,
  /\bbegin\s+discovery\b/i,
  /\bapproved\.?\s*begin\b/i,
];

const STATUS_QUERY_RES = [
  /\bdoes\s+.+\s+have\b/i,
  /\bdo\s+we\s+have\b/i,
  /\bis\s+there\b/i,
  /\bare\s+there\b/i,
  /\bwhat(?:'s| is)\s+the\s+status\b/i,
  /\bwhat(?:'s| is)\s+pending\b/i,
  /\bshow\s+me\b/i,
  /\bcheck\b/i,
  /\bany\s+drafts?\b/i,
  /\banything\s+to\s+approve\b/i,
  /\banything\s+pending\b/i,
  /\bwhat(?:'s| are)\s+we\s+waiting\s+on\b/i,
  /\bwhat(?:'s| is)\s+next\b/i,
  /\bopen\s+the\s+mission\s+workspace\b/i,
];

const PAIGE_SOCIAL_STATUS_RES = [
  /\bdoes\s+paige\s+have\b/i,
  /\bpending\s+posts?\b/i,
  /\bposts?\s+to\s+approve\b/i,
  /\bdrafts?\s+to\s+approve\b/i,
  /\bpaige\s+queue\b/i,
  /\bsocial\s+drafts?\b/i,
  /\blinkedin\s+drafts?\b/i,
  /\bfacebook\s+drafts?\b/i,
  /\bpaige\b.*\bpending\b/i,
  /\bpending\b.*\bpaige\b/i,
];

const GENERAL_PENDING_APPROVAL_RES = [
  /\bdo\s+we\s+have\s+anything\s+pending\b/i,
  /\banything\s+(?:pending|to\s+approve)\b/i,
  /\bpending\s+for\s+approval\b/i,
  /\bwhat(?:'s| is)\s+pending\s+for\s+approval\b/i,
];

const APPROVAL_IN_QUESTION_CONTEXT_RE =
  /\b(?:for\s+me\s+to|to|i\s+can|i\s+should|need\s+to|waiting\s+(?:on|for\s+me\s+to))\s+approve\b/i;

const BARE_GO_AHEAD_RE = /^(?:go\s+ahead|proceed|continue)\.?$/i;

function normalizeText(text) {
  return String(text || '').replace(/\s+/g, ' ').trim();
}

function hasExplicitMissionApprovalLanguage(text) {
  const q = normalizeText(text);
  if (!q) return false;
  if (APPROVAL_IN_QUESTION_CONTEXT_RE.test(q) && !/\b(?:approved|approve\s*,?\s*proceed|yes\s+proceed)\b/i.test(q)) {
    return false;
  }
  return EXPLICIT_MISSION_APPROVAL_RES.some((re) => re.test(q));
}

function looksLikeOperationalStatusQuery(text) {
  const q = normalizeText(text);
  if (!q) return false;
  if (/\?\s*$/.test(q) || /\b(?:does|do|is|are|what|show|check|any)\b/i.test(q)) {
    if (STATUS_QUERY_RES.some((re) => re.test(q))) return true;
    if (PAIGE_SOCIAL_STATUS_RES.some((re) => re.test(q))) return true;
    if (GENERAL_PENDING_APPROVAL_RES.some((re) => re.test(q))) return true;
  }
  return false;
}

function resolveStatusQueryDomain(text) {
  const q = normalizeText(text);
  if (PAIGE_SOCIAL_STATUS_RES.some((re) => re.test(q))) {
    return STATUS_QUERY_DOMAINS.PAIGE_SOCIAL;
  }
  if (GENERAL_PENDING_APPROVAL_RES.some((re) => re.test(q))) {
    return STATUS_QUERY_DOMAINS.OPERATOR_APPROVAL;
  }
  return STATUS_QUERY_DOMAINS.GENERAL;
}

function resolveStatusQueryAction(domain) {
  if (domain === STATUS_QUERY_DOMAINS.PAIGE_SOCIAL) {
    return STATUS_QUERY_ACTIONS.READ_PENDING_APPROVAL_QUEUE;
  }
  if (domain === STATUS_QUERY_DOMAINS.OPERATOR_APPROVAL) {
    return STATUS_QUERY_ACTIONS.READ_PENDING_OPERATOR_APPROVAL_ITEMS;
  }
  return null;
}

/**
 * @param {string} message
 * @param {object} [context]
 * @param {boolean} [context.hasSinglePendingOperatorApproval]
 * @returns {{
 *   type: string,
 *   domain?: string,
 *   action?: string|null,
 *   mutatesMissionState: boolean,
 *   explicitApproval: boolean,
 *   ambiguousGoAhead?: boolean,
 * }}
 */
function classifyOperatorMissionTurnIntent(message, context = {}) {
  const q = normalizeText(message);
  const statusQuery = looksLikeOperationalStatusQuery(q);

  if (BARE_GO_AHEAD_RE.test(q)) {
    const hasSingle = context.hasSinglePendingOperatorApproval === true;
    if (!hasSingle) {
      return {
        type: OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY,
        domain: STATUS_QUERY_DOMAINS.GENERAL,
        action: null,
        mutatesMissionState: false,
        explicitApproval: false,
        ambiguousGoAhead: true,
      };
    }
    return {
      type: OPERATOR_TURN_INTENT_TYPES.MISSION_APPROVAL,
      mutatesMissionState: true,
      explicitApproval: true,
    };
  }

  const explicitApproval = hasExplicitMissionApprovalLanguage(q);

  if (statusQuery && !explicitApproval) {
    const domain = resolveStatusQueryDomain(q);
    return {
      type: OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY,
      domain,
      action: resolveStatusQueryAction(domain),
      mutatesMissionState: false,
      explicitApproval: false,
    };
  }

  if (explicitApproval) {
    return {
      type: OPERATOR_TURN_INTENT_TYPES.MISSION_APPROVAL,
      mutatesMissionState: true,
      explicitApproval: true,
    };
  }

  return {
    type: OPERATOR_TURN_INTENT_TYPES.OTHER,
    mutatesMissionState: false,
    explicitApproval: false,
  };
}

function isOperationalStatusQuery(message) {
  const intent = classifyOperatorMissionTurnIntent(message);
  return intent.type === OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY;
}

function approvalWordInQuestionContextOnly(message) {
  const q = normalizeText(message);
  return APPROVAL_IN_QUESTION_CONTEXT_RE.test(q) && !hasExplicitMissionApprovalLanguage(q);
}

module.exports = {
  OPERATOR_TURN_INTENT_TYPES,
  STATUS_QUERY_DOMAINS,
  STATUS_QUERY_ACTIONS,
  classifyOperatorMissionTurnIntent,
  hasExplicitMissionApprovalLanguage,
  looksLikeOperationalStatusQuery,
  isOperationalStatusQuery,
  approvalWordInQuestionContextOnly,
};
