'use strict';

/**
 * SPEC-153 — Single Source of Truth for Operator Intent (ADR-061).
 * Operator language is interpreted exactly once per turn. Downstream systems
 * consume structured OperatorIntent — they never re-parse raw utterances for
 * ownership or execution decisions.
 */

const { detectConversationSubject } = require('./ConversationSubject');
const {
  classifyOperatorCognition,
  attachSpecialists,
  THINKING_MODES,
} = require('../operatorCognition');
const { applyConversationalContinuity } = require('./ConversationalStateMachine');
const {
  hasExecutionLanguage,
  detectMissionExecutionLanguage,
} = require('./ExecutionLanguageDetection');
const { resolveAcquisitionActiveMission } = require('./ActiveMissionGuard');
const {
  sealOperatorIntent,
  resetOperatorIntentAudit,
} = require('./audit/OperatorIntentAudit');
const { missionMayOwnTurn } = require('./OperatorIntentContract');

/**
 * @typedef {object} OperatorIntent
 * @property {string} subject
 * @property {string} thinkingMode
 * @property {string} intent
 * @property {boolean} mutatesMission
 * @property {boolean} executionRequested
 * @property {boolean} planningRequested
 * @property {number} confidence
 * @property {object|null} ownerHints
 * @property {boolean} conversationLocked
 * @property {object} conversationSubject
 * @property {object} conversationIntent
 * @property {string} resolvedQuestion
 * @property {boolean} continuityApplied
 * @property {boolean} executionLanguagePresent
 * @property {boolean} missionCreationRequested
 * @property {string|null} missionCreationReason
 */

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function deriveExecutionRequested(conversationIntent) {
  if (!conversationIntent) return false;
  if (conversationIntent.continuity) return false;
  if (conversationIntent.via === 'conversational_continue') return false;
  return (
    conversationIntent.intent === THINKING_MODES.EXECUTE ||
    conversationIntent.intent === THINKING_MODES.EDIT ||
    conversationIntent.intent === THINKING_MODES.RESUME ||
    conversationIntent.via === 'execution_command' ||
    conversationIntent.via === 'execute_phrase'
  );
}

function derivePlanningRequested(conversationIntent) {
  if (!conversationIntent) return false;
  return (
    conversationIntent.via === 'mission_planning_turn' ||
    conversationIntent.via === 'mission_plan_edit'
  );
}

function deriveIntentLabel(conversationSubject, conversationIntent) {
  if (
    conversationSubject &&
    conversationSubject.locked &&
    (conversationSubject.subject === 'identity' ||
      conversationSubject.subject === 'reflection' ||
      conversationSubject.subject === 'conversation')
  ) {
    return 'meta_conversation';
  }
  return (conversationIntent && conversationIntent.intent) || THINKING_MODES.INSPECT;
}

function buildOwnerHints(input = {}) {
  return {
    missionCreation: input.missionCreationRequested || false,
    missionCreationReason: input.missionCreationReason || null,
    executionLanguagePresent: input.executionLanguagePresent || false,
    readOnly:
      !input.mutatesMission &&
      !input.executionRequested &&
      !input.planningRequested,
  };
}

/**
 * Classify operator intent exactly once for a turn.
 *
 * @param {object} input
 * @param {string} input.question
 * @param {object} [input.session]
 * @param {object} [input.context]
 * @param {object} [input.mission] — optional active mission for planning detection
 * @param {boolean} [input.resolveMission=true]
 * @returns {Promise<OperatorIntent>}
 */
async function analyzeOperatorIntent(input = {}) {
  resetOperatorIntentAudit();

  const question = normalizeText(input.question);
  const session = input.session || null;
  const context = input.context || (session && session.context) || null;

  let mission = input.mission || null;
  if (!mission && input.resolveMission !== false) {
    try {
      mission = await resolveAcquisitionActiveMission({
        session,
        context,
        question,
        missionEngine: input.missionEngine,
        missionsEnabled: input.missionsEnabled,
        resolverEnabled: input.resolverEnabled,
        acquisitionMissionRuntime: input.acquisitionMissionRuntime,
        runtimeProvider: input.runtimeProvider,
      });
    } catch (_) {
      mission = null;
    }
  }

  let conversationSubject = detectConversationSubject(question, null, session);

  let conversationIntent = attachSpecialists(
    classifyOperatorCognition(question, {
      session,
      context,
      mission,
    })
  );

  const executionLanguagePresent = hasExecutionLanguage(question);
  const creationLanguage = detectMissionExecutionLanguage(question);

  let executionRequested = deriveExecutionRequested(conversationIntent);
  let planningRequested = derivePlanningRequested(conversationIntent);

  let resolvedQuestion = question;
  let continuityApplied = false;

  const continuity = applyConversationalContinuity({
    question,
    session,
    conversationSubject,
    conversationIntent,
  });

  if (continuity.applied) {
    continuityApplied = true;
    conversationSubject = continuity.conversationSubject;
    conversationIntent = continuity.conversationIntent;
    resolvedQuestion = continuity.resolvedQuestion;
    executionRequested = false;
    planningRequested = false;
  }

  const conversationLocked = Boolean(
    conversationSubject && conversationSubject.locked
  );

  const mutatesMission = continuityApplied
    ? false
    : Boolean(conversationIntent && conversationIntent.mutatesMission);

  const operatorIntent = {
    subject: conversationSubject.subject,
    thinkingMode: conversationIntent.thinkingMode,
    intent: deriveIntentLabel(conversationSubject, conversationIntent),
    mutatesMission,
    executionRequested,
    planningRequested,
    confidence: Math.max(
      conversationSubject.confidence || 0,
      conversationIntent.confidence || 0
    ),
    ownerHints: buildOwnerHints({
      missionCreationRequested: creationLanguage.matched,
      missionCreationReason: creationLanguage.reason,
      executionLanguagePresent,
      mutatesMission,
      executionRequested,
      planningRequested,
    }),
    conversationLocked,
    conversationSubject,
    conversationIntent,
    resolvedQuestion,
    continuityApplied,
    executionLanguagePresent,
    missionCreationRequested: creationLanguage.matched,
    missionCreationReason: creationLanguage.reason,
    mission,
  };

  sealOperatorIntent(operatorIntent);
  return operatorIntent;
}

module.exports = {
  analyzeOperatorIntent,
  missionMayOwnTurn,
  deriveExecutionRequested,
  derivePlanningRequested,
  deriveIntentLabel,
};
