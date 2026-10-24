'use strict';

/**
 * SPEC-148 / SPEC-149 — Conversation subject detection.
 * Subject is determined before intent, thinking mode, and owner selection.
 *
 * Subjects: identity, reflection, mission, specialist, business, knowledge, conversation.
 * Locked subjects (identity, reflection, conversation, knowledge) block business pipelines.
 */

const {
  MISSION_KEYWORD_RE,
  BLUEPRINT_TOPIC_RE,
  blocksBusinessSubsystemClaim,
  isSubjectOwnerLocked,
  normalizeSubjectValue: normalizeRoutingSubject,
  LOCKED_CONVERSATION_SUBJECTS,
} = require('./WorkspaceRoutingPatterns');

const CONVERSATION_SUBJECTS = Object.freeze({
  IDENTITY: 'identity',
  REFLECTION: 'reflection',
  MISSION: 'mission',
  SPECIALIST: 'specialist',
  BUSINESS: 'business',
  KNOWLEDGE: 'knowledge',
  CONVERSATION: 'conversation',
  /** @deprecated use REFLECTION */
  REASONING: 'reflection',
});

const LOCKED_SUBJECTS = LOCKED_CONVERSATION_SUBJECTS;

const SPECIALIST_NAMES =
  'scout|paige|emmett|max|riley|sam|link|faye|ivy|cal';

const IDENTITY_SUBJECT_RES = [
  /\b(?:what is|what'?s) (?:your|max'?s?) role\b/i,
  /\bwho are you\b/i,
  /\bwhat do you believe (?:your )?role is\b/i,
  /\bwhat are you\b/i,
  /\btell me about yourself\b/i,
  /\bwhat can you do\b/i,
  /\bwhat are your (?:capabilities|responsibilities|boundaries)\b/i,
  /\bwhat is your (?:purpose|job|function)\b/i,
  /\b(?:describe|explain) (?:yourself|your role)\b/i,
  // SPEC-151 — operating model reflection questions
  /\bwhen should i ignore (?:your )?advice\b/i,
  /\bwhen would you disagree\b/i,
  /\bwhy shouldn'?t scout do (?:your|max'?s?) job\b/i,
  /\bwhy not merge scout into max\b/i,
  /\bwhy (?:does|do) pulseforge separate specialists\b/i,
  /\bwhy preserve operator authority\b/i,
  /\bwhat should never belong to you\b/i,
  /\bwhat decisions require me\b/i,
  /\bhow do you think\b/i,
  /\b(?:continue )?evaluat(?:e|ing) how you think\b/i,
  /\b(?:your )?operating model\b/i,
  /\bhow is (?:that|this|it|max|you) different from\b/i,
  /\b(?:max|you) vs\.?\s+(?:scout|paige|rex|emmett|sam|riley|cal|vera)\b/i,
  /\b(?:scout|paige|rex) vs\.?\s+(?:scout|paige|rex|max)\b/i,
  // SPEC-152 — concept graph authority, conflict, and relationship questions
  /\b(?:who ultimately decides|who decides)\b/i,
  /\bcan (?:scout|paige|max) approve\b/i,
  /\bwho can approve outreach\b/i,
  /\bscout disagrees with paige\b/i,
  /\bif scout and paige disagreed\b/i,
  /\bwhy shouldn'?t scout (?:replace|make)\b/i,
  /\bhow do scout and paige depend\b/i,
  /\bwhat happens if one fails\b/i,
  // SPEC-156 — reasoning operator follow-ups during identity conversations
  /\bwhat assumption(?:s)? (?:is|are|that|this) (?:based|that)/i,
  /\bcould (?:that|it|this) (?:assumption )?fail\b/i,
  /\bif it failed\b/i,
  /\bdoes that change your conclusion\b/i,
  /\bsummarize (?:how )?(?:your )?reasoning\b/i,
];

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
  /\b(?:repeat|say) (?:that|it)(?: again)?\b/i,
  /\b(?:previous|last|prior) (?:message|question|turn|response|answer)\b/i,
  /\bour conversation\b/i,
  /\bwhat (?:did|do) you (?:remember|recall) (?:about )?(?:this|our) conversation\b/i,
];

const KNOWLEDGE_SUBJECT_RES = [
  /\bteach me\b/i,
  /\bexplain embeddings\b/i,
  /\bexplain vectors\b/i,
  /\bwhat (?:is|are) (?:embeddings?|vectors?)\b/i,
  new RegExp(String.raw`\bhow does (?:${SPECIALIST_NAMES}) work\b`, 'i'),
];

const SPECIALIST_SUBJECT_RES = [
  new RegExp(
    String.raw`\bwhy (?:did|does|has|have|is|are|was|were|couldn'?t|didn'?t) (?:${SPECIALIST_NAMES})\b`,
    'i'
  ),
  /\bwhy did scout stop\b/i,
  /\bwhat did scout (?:find|do|attach)\b/i,
];

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

function normalizeSubjectValue(subject) {
  if (subject === 'reasoning') return CONVERSATION_SUBJECTS.REFLECTION;
  return normalizeRoutingSubject(subject);
}

function buildSubjectResult(subject, reason, confidence, locked = false) {
  return {
    subject: normalizeSubjectValue(subject),
    confidence,
    reason,
    locked,
    via: reason,
  };
}

/**
 * @param {string} question
 * @param {object} [conversationIntent] — optional; subject detection is independent (SPEC-149)
 * @param {object} [session]
 * @returns {{ subject: string, confidence: number, reason: string, locked: boolean, via: string }}
 */
function detectConversationSubject(question, conversationIntent = {}, session = null) {
  const q = normalizeText(question);
  void conversationIntent;

  if (matchesAny(q, IDENTITY_SUBJECT_RES)) {
    return buildSubjectResult(
      CONVERSATION_SUBJECTS.IDENTITY,
      'role_question',
      0.97,
      true
    );
  }

  if (matchesAny(q, REFLECT_SUBJECT_RES)) {
    return buildSubjectResult(
      CONVERSATION_SUBJECTS.REFLECTION,
      'reflect_subject_phrase',
      0.94,
      true
    );
  }

  if (matchesAny(q, CONVERSATION_META_RES)) {
    return buildSubjectResult(
      CONVERSATION_SUBJECTS.CONVERSATION,
      'conversation_meta',
      0.88,
      true
    );
  }

  if (matchesAny(q, KNOWLEDGE_SUBJECT_RES)) {
    return buildSubjectResult(
      CONVERSATION_SUBJECTS.KNOWLEDGE,
      'knowledge_question',
      0.9,
      true
    );
  }

  if (matchesAny(q, SPECIALIST_SUBJECT_RES)) {
    return buildSubjectResult(
      CONVERSATION_SUBJECTS.SPECIALIST,
      'specialist_question',
      0.91,
      false
    );
  }

  const ctx = (session && session.context) || {};
  if (ctx.missionId || ctx.acquisitionMissionId || MISSION_KEYWORD_RE.test(q)) {
    return buildSubjectResult(
      CONVERSATION_SUBJECTS.MISSION,
      'mission_context',
      0.8,
      false
    );
  }

  if (BLUEPRINT_TOPIC_RE.test(q)) {
    return buildSubjectResult(
      CONVERSATION_SUBJECTS.BUSINESS,
      'blueprint_topic',
      0.85,
      false
    );
  }

  return buildSubjectResult(
    CONVERSATION_SUBJECTS.BUSINESS,
    'default_business',
    0.55,
    false
  );
}

module.exports = {
  CONVERSATION_SUBJECTS,
  LOCKED_SUBJECTS,
  IDENTITY_SUBJECT_RES,
  REFLECT_SUBJECT_RES,
  CONVERSATION_META_RES,
  KNOWLEDGE_SUBJECT_RES,
  SPECIALIST_SUBJECT_RES,
  detectConversationSubject,
  isSubjectOwnerLocked,
  blocksBusinessSubsystemClaim,
  normalizeSubjectValue,
};
