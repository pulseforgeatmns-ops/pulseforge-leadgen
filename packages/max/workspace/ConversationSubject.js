'use strict';

/**
 * SPEC-148 — Conversation subject detection.
 * Every turn has a subject: business, mission, conversation, or reasoning.
 * Subject governs routing; reasoning locks ownership to the Reflection subsystem.
 */

const { THINKING_MODES } = require('../operatorCognition/ThinkingModes');
const { BLUEPRINT_TOPIC_RE, MISSION_KEYWORD_RE } = require('./WorkspaceOwnershipResolver');

const CONVERSATION_SUBJECTS = Object.freeze({
  BUSINESS: 'business',
  CONVERSATION: 'conversation',
  MISSION: 'mission',
  REASONING: 'reasoning',
});

const REFLECT_SUBJECT_RES = [
  /\bwhat did you think i (?:was )?(?:ask(?:ing|ed)|mean(?:t|ing)?)\b/i,
  /\bwhy did you answer (?:like )?that\b/i,
  /\bwalk me through (?:your )?(?:reasoning|logic|thought process)\b/i,
  /\bwhy are you waiting\b/i,
  /\bwhat assumptions (?:are you|did you) (?:mak(?:ing|e)|made)\b/i,
  /\bwhat were you trying to (?:accomplish|do)\b/i,
  /\bdid you misunderstand me\b/i,
  /\bwhat pipeline (?:answered|handled|claimed|selected)\b/i,
  /\bwhy did scout (?:run|execute)\b/i,
  /\bwhy didn'?t scout (?:run|execute)\b/i,
  /\bwhy did you recommend\b/i,
  /\bwould you answer differently (?:now|today)\b/i,
  /\bwhat (?:was|is) your (?:reasoning|interpretation)\b/i,
  /\bexplain (?:your )?(?:reasoning|interpretation|assumptions)\b/i,
  /\bwhere are you uncertain\b/i,
  /\bwhat else could you have (?:answered|said|done)\b/i,
  /\bwhat would you improve\b/i,
  /\bi don'?t agree with your (?:earlier )?recommendation\b/i,
  /\bwhat (?:were|are) you (?:trying|attempting) to (?:do|accomplish)\b/i,
];

const CONVERSATION_META_RES = [
  /\b(?:previous|last|prior) (?:message|question|turn|response)\b/i,
  /\bour conversation\b/i,
  /\bwhat (?:did|do) you (?:remember|recall) (?:about )?(?:this|our) conversation\b/i,
];

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

/**
 * @param {string} question
 * @param {object} [conversationIntent]
 * @param {object} [session]
 * @returns {{ subject: string, locked: boolean, via: string, confidence: number }}
 */
function detectConversationSubject(question, conversationIntent = {}, session = null) {
  const q = normalizeText(question);

  if (conversationIntent.intent === THINKING_MODES.REFLECT) {
    return {
      subject: CONVERSATION_SUBJECTS.REASONING,
      locked: true,
      via: 'reflect_intent',
      confidence: conversationIntent.confidence || 0.96,
    };
  }

  if (matchesAny(q, REFLECT_SUBJECT_RES)) {
    return {
      subject: CONVERSATION_SUBJECTS.REASONING,
      locked: true,
      via: 'reflect_subject_phrase',
      confidence: 0.94,
    };
  }

  if (matchesAny(q, CONVERSATION_META_RES)) {
    return {
      subject: CONVERSATION_SUBJECTS.CONVERSATION,
      locked: false,
      via: 'conversation_meta',
      confidence: 0.82,
    };
  }

  const ctx = (session && session.context) || {};
  if (ctx.missionId || ctx.acquisitionMissionId || MISSION_KEYWORD_RE.test(q)) {
    return {
      subject: CONVERSATION_SUBJECTS.MISSION,
      locked: false,
      via: 'mission_context',
      confidence: 0.8,
    };
  }

  if (BLUEPRINT_TOPIC_RE.test(q)) {
    return {
      subject: CONVERSATION_SUBJECTS.BUSINESS,
      locked: false,
      via: 'blueprint_topic',
      confidence: 0.85,
    };
  }

  return {
    subject: CONVERSATION_SUBJECTS.BUSINESS,
    locked: false,
    via: 'default_business',
    confidence: 0.55,
  };
}

module.exports = {
  CONVERSATION_SUBJECTS,
  REFLECT_SUBJECT_RES,
  detectConversationSubject,
};
