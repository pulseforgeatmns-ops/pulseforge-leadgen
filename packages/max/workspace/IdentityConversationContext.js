'use strict';

/**
 * SPEC-149 / SPEC-149A / SPEC-151 — Identity Conversation routing.
 * Answers questions about Max itself — role, capabilities, boundaries, roster.
 * Follow-ups reason over the structured operating model (SPEC-151).
 * Does not receive Blueprint strategy or acquisition recommendations.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const { THINKING_MODES } = require('../operatorCognition/ThinkingModes');
const { createDefaultCapabilityRegistry } = require('../specialistDelegation/CapabilityRegistry');
const askPathTrace = require('./audit/AskPathTrace');
const {
  MAX_ROLE,
  MAX_OWNS,
  OPERATOR_OWNS,
  MAX_DOES_NOT,
  RESPONSIBILITY_BOUNDARIES,
  DELEGATION_RULES,
  SPECIALIST_ROSTER,
  composeWorkspaceIntroduction,
  operatingModeLabel,
  assertIdentityCompliance,
} = require('../identity/MaxIdentity');
const {
  shouldUseOperatingModelReasoning,
  composeIdentityReasoning,
  planOperatingModelQuery,
  reasoningMetadata,
} = require('../identity/IdentityReasoning');
const { getConversationalState } = require('./ConversationalStateMachine');
const {
  isSessionInspectionQuestion,
  formatSessionInspection,
  formatSessionFieldInspection,
  resolveSessionStateField,
  getCurrentState,
} = require('./SessionStateManager');
const { getActiveReasoningContext } = require('./ActiveReasoningContext');

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function capabilitySummary(registry) {
  return registry
    .listCallable()
    .map((entry) => `${entry.specialist}: ${entry.capability}`)
    .slice(0, 6);
}

function classifyIdentityQuestion(question) {
  const q = normalizeText(question).toLowerCase();
  if (isSessionInspectionQuestion(question)) return 'session_inspection';
  if (/\b(?:who are you|tell me about yourself)\b/.test(q)) return 'introduction';
  if (/\bwhen should i ignore\b/.test(q)) return 'failure_modes';
  if (/\bwhen would you disagree\b/.test(q)) return 'operating_model_reasoning';
  if (/\bscout disagrees with paige\b/.test(q) || /\bif scout and paige disagreed\b/.test(q)) {
    return 'operating_model_reasoning';
  }
  if (/\b(?:who ultimately decides|can scout approve|can paige approve|who can approve)\b/.test(q)) {
    return 'operating_model_reasoning';
  }
  if (/\bwhy shouldn'?t scout\b/.test(q) || /\bwhy not merge scout\b/.test(q)) {
    return 'operating_model_reasoning';
  }
  if (/\bwhy (?:does|do) pulseforge separate specialists\b/.test(q)) return 'operating_model_reasoning';
  if (/\bwhy preserve operator authority\b/.test(q)) return 'operating_model_reasoning';
  if (/\bwhat should never belong to you\b/.test(q)) return 'operating_model_reasoning';
  if (/\bwhat decisions require me\b/.test(q)) return 'operating_model_reasoning';
  if (/\bhow do scout and paige depend\b/.test(q)) return 'operating_model_reasoning';
  if (/^why\b/.test(q)) return 'operating_model_reasoning';
  if (/\b(?:different from|vs\.?|versus|compare)\b/.test(q)) return 'operating_model_reasoning';
  if (/\b(?:what assumption|could (?:that|it) (?:assumption )?fail|summarize (?:how )?your reasoning)\b/.test(q)) {
    return 'operating_model_reasoning';
  }
  if (/\b(?:capabilities|what can you do)\b/.test(q)) return 'capabilities';
  if (/\b(?:responsibilit|boundar)\b/.test(q)) return 'boundaries';
  if (/\b(?:specialists?|scout|paige|emmett|riley|cal|vera|rex|sam|roster|team)\b/.test(q)) {
    return 'roster';
  }
  if (/\b(?:delegat|route|routing)\b/.test(q)) return 'delegation';
  if (/\b(?:operating mode|current mode|what mode)\b/.test(q)) return 'operating_mode';
  if (/\b(?:operator|who decides|authority|approval)\b/.test(q)) return 'operator_authority';
  if (/\b(?:decision framework|how do you recommend|recommendation)\b/.test(q)) {
    return 'decision_framework';
  }
  return 'role';
}

function composeIdentityProse(question, session, registry) {
  const kind = classifyIdentityQuestion(question);
  const storedSessionState = getCurrentState(session);
  const mode = operatingModeLabel(session);
  const callable = capabilitySummary(registry);
  const introduction = composeWorkspaceIntroduction(session);
  let prose;

  switch (kind) {
    case 'session_inspection':
      prose = formatSessionFieldInspection(
        storedSessionState,
        resolveSessionStateField(question)
      );
      break;
    case 'introduction':
      prose =
        `${introduction} ${mode} ` +
        'Ask about my capabilities, the specialist roster, operator authority, or how I delegate if you want more detail.';
      break;
    case 'capabilities':
      prose =
        `${MAX_ROLE} I own: ${MAX_OWNS.join(', ')}. ` +
        `Callable capabilities in this workspace: ` +
        `${callable.length ? callable.join('; ') : 'none registered yet'}. ${mode}`;
      break;
    case 'boundaries':
      prose =
        `${MAX_ROLE} Responsibility boundaries: ${RESPONSIBILITY_BOUNDARIES.join(' ')} ` +
        `I do not: ${MAX_DOES_NOT.join('; ')}. ${mode}`;
      break;
    case 'roster':
      prose =
        'Specialists I coordinate — they own domain expertise; I own the business operating layer: ' +
        `${SPECIALIST_ROSTER.map((row) => `${row.name} — ${row.role}`).join(' ')} ${mode}`;
      break;
    case 'delegation':
      prose = `${MAX_ROLE} Delegation rules: ${DELEGATION_RULES.join(' ')} ${mode}`;
      break;
    case 'operator_authority':
      prose =
        `${MAX_ROLE} Only the operator owns: ${OPERATOR_OWNS.join(', ')}. ` +
        'Max never replaces operator judgment. ' +
        `${mode}`;
      break;
    case 'decision_framework':
      prose =
        `${MAX_ROLE} Every recommendation follows: ${[
          'business objective',
          'mission',
          'evidence',
          'reasoning',
          'recommendation',
          'operator decision',
        ].join(' → ')}. ${mode}`;
      break;
    case 'operating_mode':
      prose = formatSessionInspection(storedSessionState);
      break;
    case 'role':
    default:
      prose =
        `${MAX_ROLE} ${RESPONSIBILITY_BOUNDARIES[0]} ${RESPONSIBILITY_BOUNDARIES[2]} ${mode}`;
      break;
  }

  assertIdentityCompliance(prose);
  return prose;
}

function buildIdentityStructured(prose, conversationIntent, conversationSubject, reasoningMeta = null) {
  const reasoning = [
    'SPEC-149A — Identity subject routed before business intelligence.',
    'Response grounded in Max operating-system role, capability registry, and delegation boundaries only.',
  ];
  if (reasoningMeta && reasoningMeta.activeReasoningContext) {
    reasoning.push(
      'SPEC-154 — Active Reasoning Context bound follow-up to the current proposition.',
      `Primary claim: ${reasoningMeta.primaryClaim || 'unknown'}.`
    );
    if (reasoningMeta.reasoningOperatorEngine) {
      reasoning.push(
        'SPEC-156 — Reasoning Operator Engine transformed the proposition before presentation.',
        `Operator: ${reasoningMeta.reasoningOperator || 'unknown'} at depth ${reasoningMeta.reasoningDepth ?? 'unknown'}.`
      );
    }
  } else if (reasoningMeta && reasoningMeta.conceptGraphReasoning) {
    reasoning.push(
      'SPEC-152 — Concept graph reasoning synthesized from relationship traversal.',
      `Reasoning goal: ${reasoningMeta.goal || reasoningMeta.reasoningTarget || 'unknown'}.`
    );
    if (reasoningMeta.concepts && reasoningMeta.concepts.length) {
      reasoning.push(`Active concepts: ${reasoningMeta.concepts.join(', ')}.`);
    }
  } else if (reasoningMeta && reasoningMeta.operatingModelReflection) {
    reasoning.push(
      'SPEC-151 — Operating model reflection synthesized from structured concepts.',
      `Reasoning target: ${reasoningMeta.reasoningTarget || 'unknown'}.`
    );
  }

  return buildStructuredResponse({
    answer: prose,
    reasoning,
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: reasoningMeta && reasoningMeta.operatingModelReflection ? 0.97 : 0.96,
    nextInvestigations: [],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: reasoningMeta && reasoningMeta.conceptGraphReasoning
      ? ['spec_152', 'concept_graph_reasoning']
      : reasoningMeta && reasoningMeta.operatingModelReflection
        ? ['spec_151', 'operating_model_reflection']
        : ['spec_149a', 'identity_conversation'],
    timelineReferences: [],
    relatedEntities: [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: false,
        policy: true,
        knowledge: false,
      },
      evidenceCount: 0,
      asOf: new Date().toISOString(),
      unavailable: ['blueprint_strategy', 'acquisition_recommendations'],
      identityConversation: true,
      businessIntelligenceUsed: false,
      operatingModelReflection: Boolean(reasoningMeta && reasoningMeta.operatingModelReflection),
      conceptGraphReasoning: Boolean(reasoningMeta && reasoningMeta.conceptGraphReasoning),
      activeConcepts: reasoningMeta && reasoningMeta.activeConcepts ? reasoningMeta.activeConcepts : null,
      operatingModelReasoning: reasoningMeta || null,
      activeReasoningContext: Boolean(reasoningMeta && reasoningMeta.activeReasoningContext),
      reasoningOperatorEngine: Boolean(reasoningMeta && reasoningMeta.reasoningOperatorEngine),
      reasoningOperator: reasoningMeta && reasoningMeta.reasoningOperator ? reasoningMeta.reasoningOperator : null,
      reasoningDepth: reasoningMeta && reasoningMeta.reasoningDepth != null ? reasoningMeta.reasoningDepth : null,
      primaryClaim: reasoningMeta && reasoningMeta.primaryClaim ? reasoningMeta.primaryClaim : null,
      conversationSubject: conversationSubject && conversationSubject.subject,
      conversationIntent: conversationIntent && conversationIntent.intent,
      underlyingIntent: conversationIntent && conversationIntent.underlyingIntent,
      readOnlyCognition: true,
    },
  });
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleIdentityTurn(input = {}) {
  const conversationSubject = input.conversationSubject || null;
  if (!conversationSubject || conversationSubject.subject !== 'identity') {
    return null;
  }

  askPathTrace.traceEnter('maybeHandleIdentityTurn', {
    subject: conversationSubject.subject,
    reason: conversationSubject.reason,
    resolvedQuestion: input.resolvedQuestion || null,
  });

  const question = normalizeText(input.question);
  const session = input.session || null;
  const conversationIntent = input.conversationIntent || {
    intent: THINKING_MODES.EXPLAIN,
    thinkingMode: 'reasoning',
    confidence: 0.9,
  };
  const registry =
    input.capabilityRegistry || createDefaultCapabilityRegistry();

  let prose;
  let reasoningMeta = null;
  let answerKind = classifyIdentityQuestion(question);
  const priorState = getConversationalState(session);
  const activeConcepts = priorState && priorState.activeConcepts ? priorState.activeConcepts : null;
  const activeReasoningContext =
    input.activeReasoningContext || getActiveReasoningContext(session);
  const arcFollowUp = input.arcFollowUp || null;

  if (answerKind === 'session_inspection' || answerKind === 'operating_mode') {
    prose = formatSessionFieldInspection(
      getCurrentState(session),
      resolveSessionStateField(question)
    );
    answerKind = 'session_inspection';
  } else if (shouldUseOperatingModelReasoning({
    question,
    resolvedQuestion: input.resolvedQuestion,
    conversationIntent,
    session,
    activeConcepts,
    activeReasoningContext,
    arcFollowUp,
  })) {
    const query = planOperatingModelQuery({
      question,
      resolvedQuestion: input.resolvedQuestion,
      conversationIntent,
      session,
      activeConcepts,
      activeReasoningContext,
      arcFollowUp,
    });
    prose = composeIdentityReasoning({
      question,
      resolvedQuestion: input.resolvedQuestion,
      conversationIntent,
      session,
      activeConcepts,
      activeReasoningContext,
      arcFollowUp,
      operatorIntent: input.operatorIntent || null,
      conversationContract: input.conversationContract || null,
    });
    reasoningMeta = reasoningMetadata({
      ...query,
      question,
      resolvedQuestion: input.resolvedQuestion,
      conversationIntent,
    });
    if (prose) {
      answerKind = 'operating_model_reasoning';
    }
  }

  if (!prose) {
    prose = composeIdentityProse(question, session, registry);
  }

  const structured = buildIdentityStructured(
    prose,
    conversationIntent,
    conversationSubject,
    reasoningMeta
  );

  askPathTrace.traceEarlyReturn('maybeHandleIdentityTurn', reasoningMeta && reasoningMeta.activeReasoningContext
    ? 'active_reasoning_context'
    : reasoningMeta && reasoningMeta.conceptGraphReasoning
    ? 'concept_graph_reasoning'
    : reasoningMeta && reasoningMeta.operatingModelReflection
      ? 'operating_model_reasoning'
      : 'identity_composed');

  return {
    handled: true,
    prose,
    structured,
    reason: reasoningMeta && reasoningMeta.activeReasoningContext
      ? 'active_reasoning_context'
      : reasoningMeta && reasoningMeta.conceptGraphReasoning
      ? 'concept_graph_reasoning'
      : reasoningMeta && reasoningMeta.operatingModelReflection
        ? 'operating_model_reflection'
        : 'identity_conversation',
    answered: { kind: 'identity', identityKind: answerKind },
  };
}

module.exports = {
  MAX_ROLE,
  RESPONSIBILITY_BOUNDARIES,
  DELEGATION_RULES,
  SPECIALIST_ROSTER,
  classifyIdentityQuestion,
  composeIdentityProse,
  maybeHandleIdentityTurn,
};
