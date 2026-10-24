'use strict';

/**
 * SPEC-155 — Conversation Contract Engine (ADR-062).
 *
 * First responsibility of WorkspaceEngine: determine the conversational contract
 * before ownership, operator intent, or mission runtime.
 *
 * Pipeline position:
 *   Raw Operator Message → Session State Manager → Conversation Contract Engine → Conversation State → …
 */

const {
  getConversationContract,
  setConversationContract,
  buildConversationContract,
  contractBlocksExecution,
  contractRequiresContinuity,
  contractLocksConversation,
} = require('./ConversationContract');
const { getConversationalState, setConversationalState } = require('./ConversationalStateMachine');
const { applySessionStateToContract } = require('./SessionStateManager');
const { getSessionState } = require('./SessionState');
const askPathTrace = require('./audit/AskPathTrace');

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

/**
 * Resolve the conversation contract for this turn.
 * Contract is immutable during the turn once resolved.
 *
 * @param {object} input
 * @param {string} input.question
 * @param {object} [input.session]
 * @param {object} [input.sessionState] — SPEC-148 session state (ADR-068)
 * @returns {{ contract: object, changed: boolean, reason: string|null, priorContract: object|null }}
 */
function resolveConversationContract(input = {}) {
  askPathTrace.traceEnter('resolveConversationContract');
  const question = normalizeText(input.question);
  const session = input.session || null;
  const sessionState = input.sessionState || getSessionState(session);
  const priorContract = getConversationContract(session);
  const priorState = getConversationalState(session);

  const built = buildConversationContract({
    question,
    priorContract,
  });

  const contract = applySessionStateToContract(sessionState, built.contract);
  setConversationContract(session, contract);

  if (priorState && session) {
    setConversationalState(session, {
      ...priorState,
      contract,
      goal: contract.conversationGoal || priorState.goal || null,
    });
  } else if (session && contract.conversationGoal) {
    setConversationalState(session, {
      contract,
      goal: contract.conversationGoal,
      subject: null,
      owner: null,
      depth: 0,
      updatedAt: new Date().toISOString(),
    });
  }

  askPathTrace.traceBranch('conversation_contract', {
    executionAllowed: contract.executionAllowed,
    reasoningMode: contract.reasoningMode,
    maintainContext: contract.maintainContext,
    naturalConversation: contract.naturalConversation,
    locked: contract.locked,
    conversationGoal: contract.conversationGoal,
    changed: built.changed,
    reason: built.reason,
  });

  if (built.changed) {
    askPathTrace.traceBranch('conversation_contract_changed', {
      reason: built.reason,
      executionAllowed: contract.executionAllowed,
    });
  }

  askPathTrace.traceEarlyReturn('resolveConversationContract', built.reason || 'resolved');
  return {
    contract,
    changed: built.changed,
    reason: built.reason,
    priorContract,
  };
}

/**
 * Whether mission ownership or execution branches must be skipped.
 * @param {object|null} contract
 * @returns {boolean}
 */
function missionOwnershipProhibited(contract) {
  return contractBlocksExecution(contract);
}

module.exports = {
  resolveConversationContract,
  missionOwnershipProhibited,
  contractBlocksExecution,
  contractRequiresContinuity,
  contractLocksConversation,
};
