'use strict';

/**
 * SPEC-150 — Session Inspection Operator (ADR-070).
 *
 * Inspection reads stored Session State. It never reconstructs operating mode,
 * execution policy, or related fields from the current prompt.
 *
 * "What are you using?" → inspection (no business reasoning).
 * "Why are you using it?" → reasoning over Session State as evidence.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  getSessionStateHistory,
  normalizeText,
} = require('./SessionState');
const {
  getCurrentState,
  formatSessionInspection,
  formatOperatingModeLabel,
  formatExecutionPolicyLabel,
  formatReasoningModeLabel,
  formatConversationStyleLabel,
  formatEvaluationModeLabel,
  isSessionInspectionQuestion,
} = require('./SessionStateManager');

const SESSION_WHY_RES = [
  /\bwhy are you using that(?: operating mode)?\b/i,
  /\bwhy are you using it\b/i,
  /\bwhy (?:are you|is (?:that|this)) (?:using )?(?:that )?(?:operating mode|execution policy|reasoning mode|conversation style|evaluation mode)\b/i,
  /\bwhy is (?:that|this) (?:the )?(?:operating mode|execution policy|reasoning mode)\b/i,
];

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

/**
 * "Why are you using that operating mode?" is reasoning, not inspection.
 * @param {string} text
 * @returns {boolean}
 */
function isSessionStateExplanationQuestion(text) {
  const q = normalizeText(text);
  if (!q) return false;
  return matchesAny(q, SESSION_WHY_RES);
}

function detectExplanationField(question) {
  const q = normalizeText(question).toLowerCase();
  if (/\bexecution policy\b/.test(q)) return 'executionPolicy';
  if (/\breasoning mode\b/.test(q)) return 'reasoningMode';
  if (/\bconversation style\b/.test(q)) return 'conversationStyle';
  if (/\bevaluation mode\b/.test(q)) return 'evaluationMode';
  return 'operatingMode';
}

function fieldLabel(field) {
  switch (field) {
    case 'executionPolicy':
      return 'execution policy';
    case 'reasoningMode':
      return 'reasoning mode';
    case 'conversationStyle':
      return 'conversation style';
    case 'evaluationMode':
      return 'evaluation mode';
    default:
      return 'operating mode';
  }
}

function storedFieldLabel(state, field) {
  switch (field) {
    case 'executionPolicy':
      return formatExecutionPolicyLabel(state.executionPolicy);
    case 'reasoningMode':
      return formatReasoningModeLabel(state.reasoningMode);
    case 'conversationStyle':
      return formatConversationStyleLabel(state.conversationStyle);
    case 'evaluationMode':
      return formatEvaluationModeLabel(state.evaluationMode);
    default:
      return formatOperatingModeLabel(state.operatingMode);
  }
}

/**
 * Explain stored Session State. Uses history as evidence. Does not infer values.
 * @param {object} state
 * @param {string} question
 * @param {object[]} history
 * @returns {string}
 */
function composeSessionStateExplanation(state, question, history) {
  const field = detectExplanationField(question);
  const value = storedFieldLabel(state, field);
  const change = Array.isArray(history)
    ? [...history].reverse().find((entry) => entry && (entry.field === field || entry.field === '*'))
    : null;

  const parts = [
    `The stored ${fieldLabel(field)} is ${value}.`,
    'That value comes from Session State, not from inference about this question.',
  ];

  if (change && change.reason) {
    parts.push(
      `Session State last recorded this field because of ${String(change.reason).replace(/_/g, ' ')}.`
    );
  }

  parts.push(
    `Operating mode is ${formatOperatingModeLabel(state.operatingMode)}. ` +
      `Execution policy is ${formatExecutionPolicyLabel(state.executionPolicy)}.`
  );

  return parts.join(' ');
}

function sessionStateEvidence(state) {
  return {
    id: 'session_state',
    summary:
      `operatingMode=${state.operatingMode}; ` +
      `executionPolicy=${state.executionPolicy}; ` +
      `reasoningMode=${state.reasoningMode}`,
    sourceType: 'session_state',
  };
}

/**
 * Read-only inspection of stored Session State.
 * @param {object} input
 * @returns {{ handled: boolean, prose: string, structured: object, reason: string, sessionState: object }}
 */
function inspectCurrentSession(input = {}) {
  const state = input.sessionState || getCurrentState(input.session);
  const prose = formatSessionInspection(state);
  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: [
      'SPEC-150 — SESSION_INSPECTION reads stored Session State; no business reasoning.',
    ],
    supportingEvidence: [sessionStateEvidence(state)],
    confidence: 1,
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: ['spec_150', 'session_state'],
    metadata: {
      sessionInspection: true,
      sessionStateRead: true,
      businessIntelligenceUsed: false,
      sourcesUsed: {
        briefing: false,
        reasoning: false,
        memory: false,
        policy: true,
        knowledge: false,
      },
    },
  });

  return {
    handled: true,
    prose,
    structured,
    reason: 'session_inspection',
    sessionState: state,
    messageClassification: input.messageClassification || null,
  };
}

/**
 * Reasoning over stored Session State as evidence. Does not infer mode.
 * @param {object} input
 * @returns {{ handled: boolean, prose: string, structured: object, reason: string, sessionState: object }}
 */
function explainCurrentSession(input = {}) {
  const session = input.session || null;
  const state = input.sessionState || getCurrentState(session);
  const history = getSessionStateHistory(session);
  const prose = composeSessionStateExplanation(state, input.question, history);
  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: [
      'SPEC-150 — session explanation uses stored Session State as evidence.',
      `Stored operatingMode=${state.operatingMode}.`,
      'Operating mode was not inferred from this question.',
    ],
    supportingEvidence: [sessionStateEvidence(state)],
    confidence: 0.98,
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: ['spec_150', 'session_state_evidence'],
    metadata: {
      sessionInspection: false,
      sessionStateEvidence: true,
      sessionStateRead: true,
      businessIntelligenceUsed: false,
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: false,
        policy: true,
        knowledge: false,
      },
    },
  });

  return {
    handled: true,
    prose,
    structured,
    reason: 'session_state_explanation',
    sessionState: state,
    messageClassification: input.messageClassification || null,
  };
}

module.exports = {
  isSessionInspectionQuestion,
  isSessionStateExplanationQuestion,
  inspectCurrentSession,
  explainCurrentSession,
  composeSessionStateExplanation,
};
