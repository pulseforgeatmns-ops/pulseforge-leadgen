'use strict';

/**
 * SPEC-151 — Multi-Intent Extraction (ADR-072).
 *
 * Operator messages may contain multiple independent intents. Each segment is
 * classified independently; compound messages never discard secondary intents.
 */

const { MESSAGE_TYPES } = require('./MessageType');
const {
  classifyMessageType,
  isSessionConfigurationMessage,
} = require('./MessageTypeClassifier');
const { isSessionInspectionQuestion } = require('./SessionStateManager');
const { detectAcquisitionObjective } = require('./AcquisitionObjectiveDetection');
const { detectSessionDirectiveSignals } = require('./SessionStateManager');
const { normalizeText } = require('./SessionState');
const {
  INTENT_TYPES,
  buildDetectedIntent,
  mapIntentToOwner,
} = require('./MultiIntentTypes');
const askPathTrace = require('./audit/AskPathTrace');

const BUSINESS_REASONING_RES = [
  /\bwhat should (?:happen|we do|i do) next\b/i,
  /\bwhat(?:'s| is) the next (?:step|action|move)\b/i,
  /\bbased on everything you (?:currently )?know\b/i,
  /\bwhat do you recommend (?:we do )?next\b/i,
  /\bhow should we proceed\b/i,
  /\bwhat would you do next\b/i,
];

const MISSION_CONTINUE_RES = [
  /\b(?:then )?continue (?:the )?(?:acquisition )?mission\b/i,
  /\b(?:then )?resume (?:the )?(?:acquisition )?mission\b/i,
  /\b(?:then )?proceed with (?:the )?(?:acquisition )?mission\b/i,
];

const REFLECTION_RES = [
  /\breflect on\b/i,
  /\bwhat did we learn\b/i,
  /\bstep back and\b/i,
];

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

/**
 * Split operator message into independent intent segments.
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

function segmentHasSessionConfiguration(segment) {
  if (isSessionConfigurationMessage(segment)) return true;
  const signals = detectSessionDirectiveSignals(segment);
  return Boolean(
    signals.executionPolicy ||
      signals.reasoningMode ||
      signals.operatingMode ||
      signals.conversationStyle ||
      signals.evaluationMode ||
      signals.persistent ||
      signals.reset
  );
}

/**
 * Classify one message segment into at most one primary intent.
 * @param {string} segment
 * @param {number} segmentIndex
 * @param {object} [input]
 * @returns {import('./MultiIntentTypes').DetectedIntent|null}
 */
function classifySegmentIntent(segment, segmentIndex, input = {}) {
  const text = normalizeText(segment);
  if (!text) return null;

  if (segmentHasSessionConfiguration(text)) {
    return buildDetectedIntent(INTENT_TYPES.SESSION_CONFIGURATION, {
      confidence: 0.92,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  if (isSessionInspectionQuestion(text)) {
    return buildDetectedIntent(INTENT_TYPES.INSPECTION, {
      confidence: 0.95,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  const messageType = classifyMessageType(text, {
    hasActiveMission: input.hasActiveMission,
    mission: input.mission,
  });

  if (messageType.type === MESSAGE_TYPES.SYSTEM_CONFIGURATION) {
    return buildDetectedIntent(INTENT_TYPES.SYSTEM_CONFIGURATION, {
      confidence: messageType.confidence,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  if (messageType.type === MESSAGE_TYPES.MISSION_CREATION) {
    return buildDetectedIntent(INTENT_TYPES.MISSION_CREATION, {
      confidence: messageType.confidence,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  if (messageType.type === MESSAGE_TYPES.MISSION_EXECUTION) {
    return buildDetectedIntent(INTENT_TYPES.MISSION_EXECUTION, {
      confidence: messageType.confidence,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  if (matchesAny(text, MISSION_CONTINUE_RES)) {
    return buildDetectedIntent(INTENT_TYPES.MISSION_EXECUTION, {
      confidence: 0.9,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  if (detectAcquisitionObjective(text)) {
    return buildDetectedIntent(INTENT_TYPES.BUSINESS_OPERATION, {
      confidence: 0.88,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  if (matchesAny(text, REFLECTION_RES)) {
    return buildDetectedIntent(INTENT_TYPES.REFLECTION, {
      confidence: 0.86,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  if (matchesAny(text, BUSINESS_REASONING_RES)) {
    return buildDetectedIntent(INTENT_TYPES.BUSINESS_REASONING, {
      confidence: 0.87,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  if (messageType.type === MESSAGE_TYPES.QUESTION) {
    return buildDetectedIntent(INTENT_TYPES.BUSINESS_REASONING, {
      confidence: messageType.confidence,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  if (messageType.type === MESSAGE_TYPES.COMMAND) {
    return buildDetectedIntent(INTENT_TYPES.BUSINESS_OPERATION, {
      confidence: messageType.confidence,
      segment: text,
      segmentIndex,
      sourceText: text,
    });
  }

  return null;
}

/**
 * Extract all intents from an operator message.
 * @param {object} input
 * @param {string} input.question
 * @param {boolean} [input.hasActiveMission]
 * @param {object} [input.mission]
 * @returns {{ intents: import('./MultiIntentTypes').DetectedIntent[], segments: string[] }}
 */
function extractIntents(input = {}) {
  askPathTrace.traceEnter('extractIntents');
  const question = String(input.question || '');
  const segments = splitMessageSegments(question);
  const intents = [];

  segments.forEach((segment, segmentIndex) => {
    const intent = classifySegmentIntent(segment, segmentIndex, input);
    if (intent) intents.push(intent);
  });

  if (!intents.length && question.trim()) {
    const fallback = classifySegmentIntent(question, 0, input);
    if (fallback) intents.push(fallback);
  }

  askPathTrace.traceBranch('intent_extraction', {
    segmentCount: segments.length,
    intentCount: intents.length,
    intents: intents.map((row) => ({
      type: row.type,
      owner: row.owner,
      segmentIndex: row.segmentIndex,
    })),
  });
  askPathTrace.traceEarlyReturn('extractIntents', intents.length);
  return { intents, segments };
}

/**
 * @param {import('./MultiIntentTypes').DetectedIntent[]} intents
 * @returns {boolean}
 */
function isCompoundMessage(intents) {
  if (!intents || intents.length <= 1) return false;
  const types = new Set(intents.map((row) => row.type));
  if (
    types.size === 1 &&
    types.has(INTENT_TYPES.SESSION_CONFIGURATION)
  ) {
    return false;
  }
  return types.size > 1;
}

/**
 * Whole-message classification may hide secondary intents — detect compound turns.
 * @param {object} input
 * @returns {boolean}
 */
function shouldUseMultiIntentPlanner(input = {}) {
  const { intents } = extractIntents(input);
  return isCompoundMessage(intents);
}

module.exports = {
  splitMessageSegments,
  classifySegmentIntent,
  extractIntents,
  isCompoundMessage,
  shouldUseMultiIntentPlanner,
  segmentHasSessionConfiguration,
  BUSINESS_REASONING_RES,
  MISSION_CONTINUE_RES,
};
