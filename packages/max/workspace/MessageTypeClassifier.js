'use strict';

/**
 * SPEC-149 — Message Type Classifier (ADR-069).
 *
 * Pipeline position:
 *   Raw Operator Message → Message Type Classifier → Session State Manager (if applicable) → …
 *
 * Communication precedes cognition. Max cannot decide how to think until it
 * understands what kind of communication it received.
 */

const askPathTrace = require('./audit/AskPathTrace');
const { MESSAGE_TYPES, buildMessageClassification } = require('./MessageType');
const { normalizeText } = require('./SessionState');
const {
  detectSessionDirectiveSignals,
  isPersistentDirective,
  isSessionResetRequest,
  isSessionInspectionQuestion,
} = require('./SessionStateManager');
const {
  isMissionExecutionCommand,
  MISSION_CREATE_COMMAND_RE,
} = require('./ExecutionLanguageDetection');

const CORRECTION_RES = [
  /\b(?:you(?:'re| are) )?misunderstand(?:ing|ed)? me\b/i,
  /\bthat(?:'s| is) not what i meant\b/i,
  /\bthat(?:'s| is) not what i (?:was )?(?:ask(?:ing|ed)|say(?:ing|ing))\b/i,
  /\bi didn'?t mean that\b/i,
  /\bno,? that(?:'s| is) wrong\b/i,
  /\b(?:you )?got (?:that|it|me) wrong\b/i,
  /\b(?:you )?misinterpreted\b/i,
];

const APPROVAL_RES = [
  /^(?:approved?|approve)\.?$/i,
  /^(?:proceed|go ahead)\.?$/i,
  /\bapproved\.?\s*proceed\b/i,
  /\b(?:i )?approve(?:d)?(?:\.|,|\s|$)/i,
  /\bgo ahead and (?:execute|proceed|run|begin|continue)\b/i,
];

const REJECTION_RES = [
  /^(?:no|reject(?:ed)?|decline(?:d)?)\.?$/i,
  /\b(?:i )?reject(?:ed)?(?:\.|,|\s|$)/i,
  /\bdo not proceed\b/i,
  /\bdon'?t proceed\b/i,
  /\bnot approved\b/i,
  /\bhold off\b/i,
  /\bwait(?: on that)?\b/i,
];

const MISSION_CREATION_RES = [
  /\bcreate (?:a )?(?:new )?mission\b/i,
  /\bstart (?:a )?(?:new )?(?:acquisition )?mission\b/i,
  /\bbegin (?:a )?(?:new )?(?:acquisition )?mission\b/i,
  /\bnew acquisition mission\b/i,
  MISSION_CREATE_COMMAND_RE,
];

const COMMAND_RES = [
  /\brun scout\b/i,
  /\b(?:run|launch|start|execute|trigger)\s+(?:scout|paige|emmett|riley|sam|max)\b/i,
  /\b(?:find|search for|discover|identify)\b.{0,40}\b(?:prospects?|leads?|companies)\b/i,
  /\b(?:send|launch|run)\b.{0,30}\b(?:campaign|outreach|sequence)\b/i,
];

const INFORMATION_RES = [
  /\bwe signed (?:a )?(?:new )?client\b/i,
  /\b(?:new|signed) client\b/i,
  /\b(?:just|we) (?:closed|won|landed)\b/i,
  /\b(?:fyi|for your information|heads up)\b/i,
  /\b(?:client|account) (?:is|has been) (?:live|active|onboarded)\b/i,
];

const FEEDBACK_RES = [
  /\b(?:good|great|nice|excellent) (?:job|work|answer|response)\b/i,
  /\bthat (?:helped|works|makes sense)\b/i,
  /\b(?:thanks|thank you)(?: for that)?\.?$/i,
  /\b(?:not helpful|unhelpful|too verbose|too long)\b/i,
];

const SYSTEM_CONFIGURATION_RES = [
  /\b(?:set|update|change) (?:the )?(?:system|tenant|client) config\b/i,
  /\b(?:enable|disable) (?:agent|specialist)\b/i,
  /\bconfigure (?:the )?(?:system|workspace|tenant)\b/i,
  /\b(?:add|remove) (?:client|tenant)\b/i,
];

const QUESTION_RES = [
  /^(?:why|how|what|where|when|who)\??$/i,
  /\bwhy\b/i,
  /\bhow\b/i,
  /\bwhat (?:assumptions?|is|are|was|were|should|would|could)\b/i,
  /\bexplain (?:why|that|this|how)\b/i,
  /\bshow me\b/i,
];

/** Minimum independent heuristic configuration signals for classification without structured fields. */
const SESSION_CONFIGURATION_THRESHOLD = 2;

const SESSION_SCOPE_RES = [
  /\bfor the rest of (?:this )?conversation\b/i,
  /\bfor the remainder of (?:this )?conversation\b/i,
  /\bfor the rest of this session\b/i,
  /\bfor the remainder of this session\b/i,
  /\bfor this session\b/i,
  /\bfor today'?s session\b/i,
  /\bfor today'?s conversation\b/i,
  /\bduring this evaluation\b/i,
  /\bgoing forward\b/i,
  /\buntil i change it\b/i,
  /\buntil i (?:say|tell you) otherwise\b/i,
];

const SESSION_SETTING_RES = [
  /\b(?:don'?t|do not)\s+execute\b/i,
  /\bread[\s-]?only\b/i,
  /\bexplain (?:your )?reasoning naturally\b/i,
  /\bexplain your reasoning\b/i,
  /\banswer naturally\b/i,
  /\boperate as (?:the )?(?:business operating system|max)\b/i,
  /\boperate (?:according to|in) your role\b/i,
  /\b(?:work|function|behave) as\b/i,
  /\bevaluat(?:e|ing)\b.{0,40}\b(?:reasoning|how you operate|your operating model)\b/i,
  /\bevaluat(?:e|ing)\s+how you operate\b/i,
  /\b(?:i(?:'d| would)? like to|i want to|we'?re)\s+evaluat(?:e|ing)\b/i,
  /\bfor this session\s+evaluat(?:e|ing)\b/i,
  /\btreat .+ as (?:a )?(?:real )?production business\b/i,
  /\btreat .+ like (?:a )?(?:real )?production business\b/i,
  /\bassume .+ is (?:a )?(?:real )?production business\b/i,
  /\bconsider .+ (?:a )?(?:real )?production business\b/i,
  /\b(?:enable|disable|resume) execution\b/i,
  /\bautonomous execution\b/i,
  /\b(?:we'?re|i'?m)\s+evaluat(?:e|ing)\b/i,
];

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

function firstMatchLabel(text, entries) {
  for (const entry of entries) {
    if (entry.re.test(text)) return entry.label;
  }
  return null;
}

function countSessionSettingHits(text) {
  return SESSION_SETTING_RES.filter((re) => re.test(text)).length;
}

function hasSessionScopeMarker(text) {
  return isPersistentDirective(text) || matchesAny(text, SESSION_SCOPE_RES);
}

function hasStructuredSessionField(signals) {
  return (
    signals.executionPolicy != null ||
    signals.reasoningMode != null ||
    signals.conversationStyle != null ||
    signals.operatingMode != null ||
    signals.evaluationMode != null
  );
}

function isSessionConfigurationMessage(text) {
  const q = normalizeText(text);
  if (!q) return false;
  if (isSessionInspectionQuestion(q)) return false;

  if (isSessionResetRequest(q)) return true;

  const signals = detectSessionDirectiveSignals(q);
  if (hasStructuredSessionField(signals)) return true;

  const sessionSettingHits = countSessionSettingHits(q);
  if (sessionSettingHits >= SESSION_CONFIGURATION_THRESHOLD) return true;

  if (sessionSettingHits >= 1 && hasSessionScopeMarker(q)) return true;

  return false;
}

function computeSessionConfigurationConfidence(q, signals, evidence) {
  const sessionSettingHits = countSessionSettingHits(q);
  const hasScope = hasSessionScopeMarker(q);
  let confidence = 0.82;

  if (hasStructuredSessionField(signals)) confidence += 0.08;
  if (sessionSettingHits >= SESSION_CONFIGURATION_THRESHOLD) confidence += 0.06;
  else if (sessionSettingHits >= 1) confidence += 0.04;
  if (hasScope) confidence += 0.03;
  if (signals.persistent || isSessionResetRequest(q)) confidence += 0.02;
  if (evidence.length >= 3) confidence += 0.04;
  else if (evidence.length >= 2) confidence += 0.02;

  return Math.min(confidence, 0.99);
}

function isPureQuestion(text) {
  const q = normalizeText(text);
  if (!q) return false;
  if (isSessionConfigurationMessage(q)) return false;
  if (isSessionInspectionQuestion(q)) return false;
  if (/^(?:why|how|what|where|when|who)\??$/i.test(q)) return true;
  return matchesAny(q, QUESTION_RES) && !matchesAny(q, COMMAND_RES);
}

function isApprovalMessage(text) {
  const q = normalizeText(text);
  if (!q) return false;
  if (/\b(?:don'?t|do not)\s+(?:execute|approve|proceed)\b/i.test(q)) return false;
  return matchesAny(q, APPROVAL_RES);
}

function isRejectionMessage(text) {
  const q = normalizeText(text);
  if (!q) return false;
  return matchesAny(q, REJECTION_RES);
}

function isCorrectionMessage(text) {
  const q = normalizeText(text);
  if (!q) return false;
  return matchesAny(q, CORRECTION_RES);
}

function isMissionCreationMessage(text) {
  const q = normalizeText(text);
  if (!q) return false;
  return matchesAny(q, MISSION_CREATION_RES);
}

function isMissionExecutionMessage(text, input = {}) {
  const q = normalizeText(text);
  if (!q) return false;
  if (isSessionConfigurationMessage(q)) return false;
  if (isMissionCreationMessage(q)) return false;
  const hasActiveMission = Boolean(input.hasActiveMission || input.mission);
  if (!hasActiveMission && !isApprovalMessage(q)) return false;
  return isMissionExecutionCommand(q) || isApprovalMessage(q);
}

function isCommandMessage(text) {
  const q = normalizeText(text);
  if (!q) return false;
  if (isSessionConfigurationMessage(q)) return false;
  return matchesAny(q, COMMAND_RES);
}

/**
 * Classify communicative purpose before cognition, ownership, or mission routing.
 *
 * @param {string} question
 * @param {object} [input]
 * @param {boolean} [input.hasActiveMission]
 * @param {object} [input.mission]
 * @returns {import('./MessageType').MessageClassification}
 */
function classifyMessageType(question, input = {}) {
  const q = normalizeText(question);
  if (!q) {
    return buildMessageClassification(MESSAGE_TYPES.UNKNOWN, 0.4, ['empty_message'], {
      via: 'empty',
    });
  }

  if (isSessionInspectionQuestion(q)) {
    return buildMessageClassification(
      MESSAGE_TYPES.SESSION_INSPECTION,
      0.97,
      ['session_inspection'],
      { mutatesSession: false, mutatesMission: false, via: 'session_inspection' }
    );
  }

  if (isSessionConfigurationMessage(q)) {
    const evidence = [];
    if (isPersistentDirective(q)) evidence.push('persistent_directive');
    if (isSessionResetRequest(q)) evidence.push('session_reset');
    if (matchesAny(q, SESSION_SCOPE_RES)) evidence.push('session_scope');
    const signals = detectSessionDirectiveSignals(q);
    if (signals.executionPolicy) evidence.push(`execution_policy:${signals.executionPolicy}`);
    if (signals.reasoningMode) evidence.push(`reasoning_mode:${signals.reasoningMode}`);
    if (signals.operatingMode) evidence.push(`operating_mode:${signals.operatingMode}`);
    if (signals.evaluationMode) evidence.push(`evaluation_mode:${signals.evaluationMode}`);
    if (signals.conversationStyle) evidence.push(`conversation_style:${signals.conversationStyle}`);
    if (matchesAny(q, SESSION_SETTING_RES)) evidence.push('session_setting_heuristic');
    if (hasSessionScopeMarker(q)) evidence.push('session_scope');

    return buildMessageClassification(
      MESSAGE_TYPES.SESSION_CONFIGURATION,
      computeSessionConfigurationConfidence(q, signals, evidence),
      evidence.length ? evidence : ['session_directive'],
      { mutatesSession: true, mutatesMission: false, via: 'session_configuration' }
    );
  }

  if (matchesAny(q, SYSTEM_CONFIGURATION_RES)) {
    return buildMessageClassification(MESSAGE_TYPES.SYSTEM_CONFIGURATION, 0.92, ['system_config'], {
      via: 'system_configuration',
    });
  }

  if (isApprovalMessage(q)) {
    return buildMessageClassification(MESSAGE_TYPES.APPROVAL, 0.95, ['approval_phrase'], {
      mutatesMission: true,
      via: 'approval',
    });
  }

  if (isRejectionMessage(q)) {
    return buildMessageClassification(MESSAGE_TYPES.REJECTION, 0.93, ['rejection_phrase'], {
      via: 'rejection',
    });
  }

  if (isCorrectionMessage(q)) {
    return buildMessageClassification(MESSAGE_TYPES.CORRECTION, 0.94, ['correction_phrase'], {
      via: 'correction',
    });
  }

  if (isMissionCreationMessage(q)) {
    return buildMessageClassification(MESSAGE_TYPES.MISSION_CREATION, 0.96, ['mission_creation'], {
      mutatesMission: true,
      via: 'mission_creation',
    });
  }

  if (isMissionExecutionMessage(q, input)) {
    return buildMessageClassification(MESSAGE_TYPES.MISSION_EXECUTION, 0.94, ['mission_execution'], {
      mutatesMission: true,
      via: 'mission_execution',
    });
  }

  if (isCommandMessage(q)) {
    const label = firstMatchLabel(
      q,
      COMMAND_RES.map((re, i) => ({ re, label: `command_${i}` }))
    );
    return buildMessageClassification(MESSAGE_TYPES.COMMAND, 0.91, [label || 'command'], {
      mutatesMission: true,
      via: 'command',
    });
  }

  if (matchesAny(q, INFORMATION_RES)) {
    return buildMessageClassification(MESSAGE_TYPES.INFORMATION, 0.9, ['information'], {
      via: 'information',
    });
  }

  if (matchesAny(q, FEEDBACK_RES)) {
    return buildMessageClassification(MESSAGE_TYPES.FEEDBACK, 0.88, ['feedback'], {
      via: 'feedback',
    });
  }

  if (isPureQuestion(q)) {
    return buildMessageClassification(MESSAGE_TYPES.QUESTION, 0.9, ['interrogative'], {
      via: 'question',
    });
  }

  return buildMessageClassification(MESSAGE_TYPES.UNKNOWN, 0.5, ['unclassified'], {
    via: 'unknown',
  });
}

/**
 * Resolve message classification for a workspace turn. First step in pipeline (ADR-069).
 *
 * @param {object} input
 * @param {string} input.question
 * @param {object} [input.session]
 * @param {boolean} [input.hasActiveMission]
 * @param {object} [input.mission]
 * @returns {{ classification: import('./MessageType').MessageClassification }}
 */
function resolveMessageType(input = {}) {
  askPathTrace.traceEnter('resolveMessageType');
  const question = normalizeText(input.question);
  const classification = classifyMessageType(question, {
    hasActiveMission: input.hasActiveMission,
    mission: input.mission,
  });

  askPathTrace.traceBranch('message_type', {
    type: classification.type,
    confidence: classification.confidence,
    mutatesSession: classification.mutatesSession,
    mutatesMission: classification.mutatesMission,
    evidence: classification.evidence,
  });

  askPathTrace.traceEarlyReturn('resolveMessageType', classification.type);
  return { classification };
}

function messageTypeBypassesOwnership(type) {
  return (
    type === MESSAGE_TYPES.SESSION_CONFIGURATION ||
    type === MESSAGE_TYPES.SESSION_INSPECTION
  );
}

function messageTypeBypassesReasoning(type) {
  return (
    type === MESSAGE_TYPES.SESSION_CONFIGURATION ||
    type === MESSAGE_TYPES.SESSION_INSPECTION
  );
}

module.exports = {
  classifyMessageType,
  resolveMessageType,
  isSessionConfigurationMessage,
  isPureQuestion,
  isApprovalMessage,
  isCorrectionMessage,
  messageTypeBypassesOwnership,
  messageTypeBypassesReasoning,
  countSessionSettingHits,
  hasSessionScopeMarker,
  computeSessionConfigurationConfidence,
  SESSION_CONFIGURATION_THRESHOLD,
  SESSION_SETTING_RES,
  SESSION_SCOPE_RES,
  CORRECTION_RES,
  APPROVAL_RES,
};
