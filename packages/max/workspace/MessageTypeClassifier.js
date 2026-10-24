'use strict';

/**
 * SPEC-149 — Message Type Classifier (ADR-069, ADR-087).
 *
 * Pipeline position:
 *   Raw Operator Message → Message Type Classifier → Session State Manager (if applicable) → …
 *
 * Communication precedes cognition. Max cannot decide how to think until it
 * understands what kind of communication it received.
 *
 * ADR-087 — Primary business objective determines routing. Execution and
 * conversation modifiers may mutate session state but never displace routing.
 */

const askPathTrace = require('./audit/AskPathTrace');
const { MESSAGE_TYPES, buildMessageClassification } = require('./MessageType');
const { normalizeText } = require('./SessionState');
const {
  detectSessionDirectiveSignals,
  isSessionResetRequest,
  isSessionInspectionQuestion,
} = require('./SessionStateManager');
const {
  countSettingHits,
  hasScopeMarker,
  hasStructuredSessionField,
  isInterpretableSessionConfiguration,
  SESSION_CONFIGURATION_THRESHOLD,
  matchFieldDirectives,
} = require('./SessionDirectiveRegistry');
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
  /\bcreate\b.{0,40}\b(?:acquisition\s+)?mission\b/i,
  /\bcreate\s+a\s+(?:new\s+|production\s+)?acquisition\s+mission\b/i,
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

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

/**
 * Split operator message into segments for primary-objective resolution (ADR-087).
 * @param {string} question
 * @returns {string[]}
 */
function splitMessageSegments(question) {
  const raw = String(question || '').trim();
  if (!raw) return [];

  const paragraphs = raw
    .split(/\n\s*\n/)
    .map((part) => normalizeText(part))
    .filter(Boolean);

  if (paragraphs.length > 1) return paragraphs;

  const single = paragraphs[0] || normalizeText(raw);
  const sentenceParts = single
    .split(/(?<=[.!?])\s+(?=[A-Z"'])/)
    .map((part) => normalizeText(part))
    .filter(Boolean);

  if (sentenceParts.length > 1) return sentenceParts;
  return [single];
}

function collectSessionModifierEvidence(text) {
  const q = normalizeText(text);
  const evidence = [];
  const signals = detectSessionDirectiveSignals(q);
  if (signals.persistent) evidence.push('persistent_directive');
  if (isSessionResetRequest(q)) evidence.push('session_reset');
  if (hasScopeMarker(q)) evidence.push('session_scope');
  if (signals.executionPolicy) evidence.push(`execution_policy:${signals.executionPolicy}`);
  if (signals.reasoningMode) evidence.push(`reasoning_mode:${signals.reasoningMode}`);
  if (signals.operatingMode) evidence.push(`operating_mode:${signals.operatingMode}`);
  if (signals.evaluationMode) evidence.push(`evaluation_mode:${signals.evaluationMode}`);
  if (signals.conversationStyle) evidence.push(`conversation_style:${signals.conversationStyle}`);
  if (matchFieldDirectives(q).length > 0) evidence.push('session_setting_heuristic');
  return evidence.length ? evidence : ['session_directive'];
}

/**
 * Resolve the primary business objective for one message segment (ADR-087).
 * Returns null when the segment is only a modifier or unclassified.
 *
 * @param {string} segment
 * @param {object} [input]
 * @returns {import('./MessageType').MessageClassification|null}
 */
function classifySegmentPrimaryObjective(segment, input = {}) {
  const q = normalizeText(segment);
  if (!q || isSessionInspectionQuestion(q)) return null;

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

  return null;
}

/**
 * Resolve primary business objective across all segments (ADR-087).
 * First matching segment wins — executive directives lead with the objective.
 *
 * @param {string} text
 * @param {object} [input]
 * @returns {import('./MessageType').MessageClassification|null}
 */
function detectPrimaryObjective(text, input = {}) {
  const segments = splitMessageSegments(text);
  for (const segment of segments) {
    const objective = classifySegmentPrimaryObjective(segment, input);
    if (objective) return objective;
  }
  return null;
}

function firstMatchLabel(text, entries) {
  for (const entry of entries) {
    if (entry.re.test(text)) return entry.label;
  }
  return null;
}

function countSessionSettingHits(text) {
  return countSettingHits(text);
}

function hasSessionScopeMarker(text) {
  return hasScopeMarker(text);
}

function isSessionConfigurationMessage(text) {
  const q = normalizeText(text);
  if (!q) return false;
  if (isSessionInspectionQuestion(q)) return false;

  if (isSessionResetRequest(q)) return true;

  const signals = detectSessionDirectiveSignals(q);
  return isInterpretableSessionConfiguration(q, signals);
}

function computeSessionConfigurationConfidence(q, signals, evidence) {
  const sessionSettingHits = countSettingHits(q);
  const hasScope = hasScopeMarker(q);
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

  const sessionModifiersPresent = isSessionConfigurationMessage(q);
  const primaryObjective = detectPrimaryObjective(q, input);

  // ADR-087 — primary objective determines routing; modifiers attach without displacing it.
  if (primaryObjective) {
    const evidence = [...primaryObjective.evidence];
    if (sessionModifiersPresent) {
      evidence.push('session_modifiers_present');
      evidence.push(...collectSessionModifierEvidence(q));
    }
    return buildMessageClassification(
      primaryObjective.type,
      primaryObjective.confidence,
      evidence,
      {
        mutatesSession: sessionModifiersPresent || primaryObjective.mutatesSession,
        mutatesMission: primaryObjective.mutatesMission,
        via: primaryObjective.via,
        hasSessionModifiers: sessionModifiersPresent,
      }
    );
  }

  if (sessionModifiersPresent) {
    const evidence = [];
    const signals = detectSessionDirectiveSignals(q);
    if (signals.persistent) evidence.push('persistent_directive');
    if (isSessionResetRequest(q)) evidence.push('session_reset');
    if (hasScopeMarker(q)) evidence.push('session_scope');
    if (signals.executionPolicy) evidence.push(`execution_policy:${signals.executionPolicy}`);
    if (signals.reasoningMode) evidence.push(`reasoning_mode:${signals.reasoningMode}`);
    if (signals.operatingMode) evidence.push(`operating_mode:${signals.operatingMode}`);
    if (signals.evaluationMode) evidence.push(`evaluation_mode:${signals.evaluationMode}`);
    if (signals.conversationStyle) evidence.push(`conversation_style:${signals.conversationStyle}`);
    if (matchFieldDirectives(q).length > 0) evidence.push('session_setting_heuristic');
    if (hasScopeMarker(q)) evidence.push('session_scope');

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
  isMissionCreationMessage,
  messageTypeBypassesOwnership,
  messageTypeBypassesReasoning,
  countSessionSettingHits,
  hasSessionScopeMarker,
  computeSessionConfigurationConfidence,
  splitMessageSegments,
  detectPrimaryObjective,
  classifySegmentPrimaryObjective,
  collectSessionModifierEvidence,
  SESSION_CONFIGURATION_THRESHOLD,
  CORRECTION_RES,
  APPROVAL_RES,
};
