'use strict';

/**
 * SPEC-148 — Session State types and session helpers (ADR-068).
 *
 * Session State governs how Max operates independent of any individual prompt.
 * Conversation is transient; Session State is persistent until changed or reset.
 */

const OPERATING_MODES = Object.freeze({
  BUSINESS_OPERATION: 'business_operation',
  REASONING_EVALUATION: 'reasoning_evaluation',
  MISSION_EXECUTION: 'mission_execution',
  ARCHITECTURE_REVIEW: 'architecture_review',
  DEBUGGING: 'debugging',
  LEARNING: 'learning',
  PLANNING: 'planning',
  BRAINSTORMING: 'brainstorming',
});

const EXECUTION_POLICIES = Object.freeze({
  NORMAL: 'normal',
  READ_ONLY: 'read_only',
  AUTONOMOUS: 'autonomous',
  OPERATOR_APPROVAL_REQUIRED: 'operator_approval_required',
  EXECUTION_DISABLED: 'execution_disabled',
});

const REASONING_MODES = Object.freeze({
  NATURAL: 'natural',
  THINK_ALOUD: 'think_aloud',
  CONCISE: 'concise',
  TEACHING: 'teaching',
  ANALYTICAL: 'analytical',
  REFLECTIVE: 'reflective',
});

const CONVERSATION_STYLES = Object.freeze({
  NATURAL: 'natural',
  TECHNICAL: 'technical',
  EXECUTIVE: 'executive',
  SPECIFICATION: 'specification',
  BRAINSTORM: 'brainstorm',
});

const EVALUATION_MODES = Object.freeze({
  MAX: 'max',
  SCOUT: 'scout',
  MISSION_RUNTIME: 'mission_runtime',
  BUSINESS: 'business',
  NONE: 'none',
});

const DEFAULT_SESSION_STATE = Object.freeze({
  operatingMode: OPERATING_MODES.BUSINESS_OPERATION,
  executionPolicy: EXECUTION_POLICIES.NORMAL,
  reasoningMode: REASONING_MODES.NATURAL,
  conversationStyle: CONVERSATION_STYLES.NATURAL,
  evaluationMode: EVALUATION_MODES.NONE,
  activeObjective: null,
  activeConversation: null,
  activeReasoningGoal: null,
  operatorPreferences: {},
  expires: null,
});

const READ_ONLY_EXECUTION_POLICIES = new Set([
  EXECUTION_POLICIES.READ_ONLY,
  EXECUTION_POLICIES.EXECUTION_DISABLED,
]);

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function cloneSessionState(state) {
  if (!state || typeof state !== 'object') return null;
  return {
    ...state,
    operatorPreferences: state.operatorPreferences
      ? { ...state.operatorPreferences }
      : {},
  };
}

function getSessionState(session) {
  if (session && session.sessionState && typeof session.sessionState === 'object') {
    return session.sessionState;
  }
  const ctx = session && session.context && typeof session.context === 'object' ? session.context : null;
  const state = ctx && ctx.sessionState;
  if (!state || typeof state !== 'object') return null;
  return state;
}

function setSessionState(session, state) {
  if (!session || typeof session !== 'object' || !state) return;
  session.sessionState = state;
  if (session.context && typeof session.context === 'object') {
    session.context.sessionState = state;
  }
}

function getSessionStateHistory(session) {
  if (session && Array.isArray(session.sessionStateHistory)) {
    return session.sessionStateHistory;
  }
  const ctx = session && session.context && typeof session.context === 'object' ? session.context : null;
  if (ctx && Array.isArray(ctx.sessionStateHistory)) {
    return ctx.sessionStateHistory;
  }
  return [];
}

function appendSessionStateHistory(session, change) {
  if (!session || !change) return;
  const history = getSessionStateHistory(session).slice();
  history.push(change);
  session.sessionStateHistory = history;
  if (session.context && typeof session.context === 'object') {
    session.context.sessionStateHistory = history;
  }
}

function createDefaultSessionState() {
  const now = new Date().toISOString();
  return {
    ...DEFAULT_SESSION_STATE,
    operatorPreferences: {},
    createdAt: now,
    updatedAt: null,
  };
}

function sessionStateBlocksExecution(state) {
  if (!state) return false;
  return READ_ONLY_EXECUTION_POLICIES.has(state.executionPolicy);
}

function sessionStateAllowsAutonomousExecution(state) {
  if (!state) return false;
  return state.executionPolicy === EXECUTION_POLICIES.AUTONOMOUS;
}

module.exports = {
  OPERATING_MODES,
  EXECUTION_POLICIES,
  REASONING_MODES,
  CONVERSATION_STYLES,
  EVALUATION_MODES,
  DEFAULT_SESSION_STATE,
  READ_ONLY_EXECUTION_POLICIES,
  normalizeText,
  cloneSessionState,
  getSessionState,
  setSessionState,
  getSessionStateHistory,
  appendSessionStateHistory,
  createDefaultSessionState,
  sessionStateBlocksExecution,
  sessionStateAllowsAutonomousExecution,
};
