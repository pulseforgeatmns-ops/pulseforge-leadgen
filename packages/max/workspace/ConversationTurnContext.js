'use strict';

/**
 * SPEC-149 — Conversation-layer turns (repeat, recall prior turn).
 * Subject = conversation locks out business subsystems.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const { THINKING_MODES } = require('../operatorCognition/ThinkingModes');
const askPathTrace = require('./audit/AskPathTrace');

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function lastMaxMessage(session) {
  const messages = (session && session.messages) || [];
  for (let i = messages.length - 2; i >= 0; i -= 1) {
    const row = messages[i];
    if (row && row.role === 'max' && normalizeText(row.text)) {
      return normalizeText(row.text);
    }
  }
  return null;
}

function composeConversationProse(question, session) {
  const q = normalizeText(question).toLowerCase();
  const prior = lastMaxMessage(session);

  if (/\b(?:repeat|say) (?:that|it)\b/.test(q)) {
    if (!prior) {
      return "I don't have a prior response in this session to repeat yet.";
    }
    return prior;
  }

  if (/\b(?:previous|last|prior) (?:message|question|turn|response|answer)\b/.test(q)) {
    if (!prior) {
      return "There isn't a prior Max response recorded in this session yet.";
    }
    return `My last response was: ${prior}`;
  }

  if (/\bwhat (?:did|do) you (?:remember|recall)\b/.test(q)) {
    const count = ((session && session.messages) || []).filter((m) => m.role === 'operator').length;
    if (!prior) {
      return `I see ${count} operator turn(s) so far, but no completed Max response to recall yet.`;
    }
    return (
      `In this session I remember ${count} operator turn(s). ` +
      `My most recent answer was: ${prior.slice(0, 320)}${prior.length > 320 ? '…' : ''}`
    );
  }

  if (!prior) {
    return "I'm tracking this conversation, but there's no prior Max answer to reference yet.";
  }
  return prior;
}

function buildConversationStructured(prose, conversationIntent, conversationSubject) {
  return buildStructuredResponse({
    answer: prose,
    reasoning: [
      'SPEC-149 — Conversation subject; prior turn recall only.',
      'No Blueprint or mission intelligence invoked.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: 0.9,
    nextInvestigations: [],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: ['spec_149', 'conversation_layer'],
    timelineReferences: [],
    relatedEntities: [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: false,
        memory: true,
        policy: true,
        knowledge: false,
      },
      evidenceCount: 0,
      asOf: new Date().toISOString(),
      unavailable: ['blueprint_strategy'],
      conversationLayer: true,
      businessIntelligenceUsed: false,
      conversationSubject: conversationSubject && conversationSubject.subject,
      conversationIntent: conversationIntent && conversationIntent.intent,
      readOnlyCognition: true,
    },
  });
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleConversationTurn(input = {}) {
  const conversationSubject = input.conversationSubject || null;
  if (!conversationSubject || conversationSubject.subject !== 'conversation') {
    return null;
  }

  askPathTrace.traceEnter('maybeHandleConversationTurn', {
    subject: conversationSubject.subject,
  });

  const question = normalizeText(input.question);
  const session = input.session || null;
  const conversationIntent = input.conversationIntent || {
    intent: THINKING_MODES.INSPECT,
    thinkingMode: 'continuation',
    confidence: 0.85,
  };

  const prose = composeConversationProse(question, session);
  const structured = buildConversationStructured(
    prose,
    conversationIntent,
    conversationSubject
  );

  askPathTrace.traceEarlyReturn('maybeHandleConversationTurn', 'conversation_composed');

  return {
    handled: true,
    prose,
    structured,
    reason: 'conversation_layer',
    answered: { kind: 'conversation' },
  };
}

module.exports = {
  lastMaxMessage,
  composeConversationProse,
  maybeHandleConversationTurn,
};
