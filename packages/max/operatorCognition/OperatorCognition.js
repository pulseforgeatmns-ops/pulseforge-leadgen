'use strict';

/**
 * SPEC-146 — Operator Cognition Engine.
 * Classify every operator turn before mission runtime, specialist, or execution logic.
 */

const {
  THINKING_MODES,
  thinkingModeCategory,
  modeMutatesMission,
} = require('./ThinkingModes');
const { isMissionExecutionCommand } = require('../workspace/ExecutionLanguageDetection');
const { isMissionPlanningTurn } = require('../workspace/MissionPlanningTurn');

const SPECIALIST_NAMES = 'scout|paige|emmett|max|riley|sam|link|faye|ivy|cal';

const EXECUTE_RES = [
  /\b(?:approved?|approve(?:d)?)\b/i,
  /\bcontinue\b/i,
  /\bbegin discovery\b/i,
  /\bsend campaign\b/i,
  /\blaunch campaign\b/i,
  /\bexecute\b/i,
  /\bproceed\b/i,
  /\bgo ahead\b/i,
];

const INSPECT_RES = [
  /\bwhere are we\b/i,
  /\bwhat stage are we in\b/i,
  /\bshow me the mission\b/i,
  /\bwhat(?:'s| is) pending\b/i,
  /\bmission status\b/i,
  /\bmission progress\b/i,
  /\bmission workspace\b/i,
  /\bwhat(?:'s| is) the current (?:stage|phase)\b/i,
];

const EXPLAIN_RES = [
  /\bwhy (?:did|does|is|are|was|were|has|have|couldn'?t|didn'?t)\b/i,
  /\bexplain (?:your )?reasoning\b/i,
  /\bwalk me through\b/i,
  /\bexplain (?:why|that|this)\b/i,
  new RegExp(String.raw`\bwhy did (?:${SPECIALIST_NAMES})\b`, 'i'),
  new RegExp(String.raw`\bwhy (?:has|hasn't|did|didn't) (?:${SPECIALIST_NAMES})\b`, 'i'),
];

const CHALLENGE_RES = [
  /\bi disagree\b/i,
  /\bthat doesn'?t seem right\b/i,
  /\bwhy not this company\b/i,
  /\bthat(?:'s| is) (?:wrong|incorrect|not right)\b/i,
  /\bi don'?t (?:agree|buy that)\b/i,
];

const COMPARE_RES = [
  /\bcompare\b/i,
  /\bvs\.?\b/i,
  /\bversus\b/i,
  /\bcommercial vs\b/i,
  /\bapollo vs\b/i,
];

const STRATEGY_RES = [
  /\bshould we pivot\b/i,
  /\bis this market worth pursuing\b/i,
  /\bwhat(?:'s| is) the biggest risk\b/i,
  /\bwhat should we (?:do|focus|prioritize)\b/i,
  /\bstrategic(?:ally)?\b/i,
];

const BRAINSTORM_RES = [
  /\bgive me ideas\b/i,
  /\bwhat else could we do\b/i,
  /\bthink creatively\b/i,
  /\bbrainstorm\b/i,
  /\bother options\b/i,
];

const TEACH_RES = [
  /\bexplain embeddings\b/i,
  /\bteach me vectors\b/i,
  new RegExp(String.raw`\bhow does (?:${SPECIALIST_NAMES}|scout) work\b`, 'i'),
  /\bteach me\b/i,
  /\bwhat (?:is|are) (?:embeddings?|vectors?)\b/i,
];

const EDIT_RES = [
  /\bchange the mission\b/i,
  /\bremove \w+\b/i,
  /\bincrease evidence threshold\b/i,
  /\bedit (?:the )?mission\b/i,
  /\bupdate (?:the )?mission\b/i,
  /\bmodify (?:the )?plan\b/i,
];

const RESUME_RES = [
  /\bresume mission\b/i,
  /\bpick up where we left off\b/i,
  /\bcontinue (?:the )?mission\b/i,
  /\bresume\b/i,
];

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

function buildConversationIntent(mode, via, confidence, extras = {}) {
  return {
    intent: mode,
    confidence,
    mutatesMission: modeMutatesMission(mode),
    thinkingMode: thinkingModeCategory(mode),
    via,
    specialists: extras.specialists || null,
  };
}

/**
 * Classify operator cognition before any mission runtime or execution.
 * @param {string} question
 * @param {object} [input]
 * @returns {import('./types').ConversationIntent}
 */
function classifyOperatorCognition(question, input = {}) {
  const q = String(question || '').replace(/\s+/g, ' ').trim();
  if (!q) {
    return buildConversationIntent(THINKING_MODES.INSPECT, 'empty', 0.5);
  }

  const mission = input.mission || null;

  if (matchesAny(q, RESUME_RES)) {
    return buildConversationIntent(THINKING_MODES.RESUME, 'resume_phrase', 0.93);
  }

  if (matchesAny(q, EDIT_RES)) {
    return buildConversationIntent(THINKING_MODES.EDIT, 'edit_phrase', 0.91);
  }

  if (isMissionExecutionCommand(q)) {
    return buildConversationIntent(THINKING_MODES.EXECUTE, 'execution_command', 0.97);
  }

  if (mission && typeof isMissionPlanningTurn === 'function' && isMissionPlanningTurn(mission, q)) {
    const editLike = EDIT_RES.some((re) => re.test(q));
    return buildConversationIntent(
      editLike ? THINKING_MODES.EDIT : THINKING_MODES.EXECUTE,
      editLike ? 'mission_plan_edit' : 'mission_planning_turn',
      0.94
    );
  }

  if (matchesAny(q, EXECUTE_RES) && /\b(?:approved?|discovery|campaign|send|launch|execute)\b/i.test(q)) {
    return buildConversationIntent(THINKING_MODES.EXECUTE, 'execute_phrase', 0.92);
  }

  if (matchesAny(q, CHALLENGE_RES)) {
    return buildConversationIntent(THINKING_MODES.CHALLENGE, 'challenge_phrase', 0.94);
  }

  if (matchesAny(q, COMPARE_RES)) {
    return buildConversationIntent(THINKING_MODES.COMPARE, 'compare_phrase', 0.93);
  }

  if (matchesAny(q, TEACH_RES)) {
    return buildConversationIntent(THINKING_MODES.TEACH, 'teach_phrase', 0.92);
  }

  if (matchesAny(q, BRAINSTORM_RES)) {
    return buildConversationIntent(THINKING_MODES.BRAINSTORM, 'brainstorm_phrase', 0.9);
  }

  if (matchesAny(q, STRATEGY_RES)) {
    return buildConversationIntent(THINKING_MODES.STRATEGY, 'strategy_phrase', 0.9);
  }

  if (matchesAny(q, INSPECT_RES)) {
    return buildConversationIntent(THINKING_MODES.INSPECT, 'inspect_phrase', 0.95);
  }

  if (matchesAny(q, EXPLAIN_RES)) {
    return buildConversationIntent(THINKING_MODES.EXPLAIN, 'explain_phrase', 0.96);
  }

  if (/\b(?:why|what|how|where|when|show|explain)\b/i.test(q)) {
    return buildConversationIntent(THINKING_MODES.EXPLAIN, 'interrogative_default', 0.72);
  }

  return buildConversationIntent(THINKING_MODES.INSPECT, 'default_read_only', 0.55);
}

function attachSpecialists(conversationIntent) {
  const { selectSpecialists } = require('./SpecialistParticipation');
  return {
    ...conversationIntent,
    specialists: selectSpecialists(conversationIntent),
  };
}

module.exports = {
  classifyOperatorCognition,
  attachSpecialists,
  EXECUTE_RES,
  INSPECT_RES,
  EXPLAIN_RES,
};
