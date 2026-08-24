'use strict';

/**
 * ADR-071 — Session Directive Registry.
 *
 * Canonical vocabulary for operator session configuration. Classification
 * (MessageTypeClassifier) and extraction (SessionStateManager) consume the
 * same registry so a recognized directive is always interpretable.
 */

const {
  OPERATING_MODES,
  EXECUTION_POLICIES,
  REASONING_MODES,
  CONVERSATION_STYLES,
  EVALUATION_MODES,
  normalizeText,
} = require('./SessionState');

/** Minimum independent field-directive hits for classification without scope. */
const SESSION_CONFIGURATION_THRESHOLD = 2;

/**
 * @typedef {object} SessionDirective
 * @property {string} id
 * @property {RegExp[]} aliases
 * @property {'field'|'scope'|'reset'} kind
 * @property {string} [targetField]
 * @property {*} [parsedValue]
 * @property {number} [confidence]
 * @property {boolean} [standalone] — apply without a persistent scope marker
 */

/** @type {SessionDirective[]} */
const SESSION_DIRECTIVES = [
  // --- reset ---
  {
    id: 'session_reset',
    kind: 'reset',
    aliases: [
      /\breset (?:the )?session\b/i,
      /\bclear (?:the )?session (?:state|settings)\b/i,
      /\bstart (?:a )?fresh session\b/i,
      /\breturn to default (?:mode|settings)\b/i,
    ],
    confidence: 0.98,
  },

  // --- scope (persistent markers) ---
  {
    id: 'scope_rest_of_conversation',
    kind: 'scope',
    aliases: [
      /\bfor the rest of (?:this )?conversation\b/i,
      /\bfor the remainder of (?:this )?conversation\b/i,
    ],
    confidence: 0.95,
  },
  {
    id: 'scope_until_otherwise',
    kind: 'scope',
    aliases: [/\buntil i (?:say|tell you) otherwise\b/i],
    confidence: 0.95,
  },
  {
    id: 'scope_rest_of_session',
    kind: 'scope',
    aliases: [
      /\bfor the rest of this session\b/i,
      /\bfor the remainder of this session\b/i,
    ],
    confidence: 0.95,
  },
  {
    id: 'scope_this_session',
    kind: 'scope',
    aliases: [/\bfor this session\b/i, /\bfor today'?s session\b/i],
    confidence: 0.95,
  },
  {
    id: 'scope_today_conversation',
    kind: 'scope',
    aliases: [/\bfor today'?s conversation\b/i],
    confidence: 0.95,
  },
  {
    id: 'scope_during_evaluation',
    kind: 'scope',
    aliases: [/\bduring this evaluation\b/i],
    confidence: 0.95,
  },
  {
    id: 'scope_going_forward',
    kind: 'scope',
    aliases: [/\bgoing forward\b/i],
    confidence: 0.95,
  },
  {
    id: 'scope_until_change',
    kind: 'scope',
    aliases: [/\buntil i change it\b/i],
    confidence: 0.95,
  },

  // --- execution policy (priority order preserved) ---
  {
    id: 'execution_read_only',
    kind: 'field',
    targetField: 'executionPolicy',
    parsedValue: EXECUTION_POLICIES.READ_ONLY,
    standalone: true,
    aliases: [
      /\b(?:don'?t|do not)\s+execute anything\b/i,
      /\b(?:don'?t|do not)\s+execute\b(?!\s*(?:,|launch|approve|print|or mail))/i,
      /\b(?:don'?t|do not)\s+(?:do|perform)\s+anything\b/i,
      /\bdisable execution\b(?:\s+until\b.{0,80})?/i,
      /\bread[\s-]?only(?:\s+mode)?\b/i,
      /\bno execution\b/i,
      /\bexecution disabled\b/i,
    ],
    confidence: 0.96,
  },
  {
    id: 'execution_autonomous',
    kind: 'field',
    targetField: 'executionPolicy',
    parsedValue: EXECUTION_POLICIES.AUTONOMOUS,
    standalone: true,
    aliases: [
      /\bautonomous execution\b/i,
      /\bexecute autonomously\b/i,
      /\b(?:you may|go ahead and) execute without asking\b/i,
    ],
    confidence: 0.94,
  },
  {
    id: 'execution_normal',
    kind: 'field',
    targetField: 'executionPolicy',
    parsedValue: EXECUTION_POLICIES.NORMAL,
    standalone: true,
    aliases: [
      /\b(?:let'?s|go ahead and|time to|ready to)\s+execute\b/i,
      /\bstop theoriz(?:e|ing|y)\b/i,
      /\benough theory\b/i,
      /\b(?:let'?s|go ahead and)\s+(?:run|launch|begin|operate|proceed|approve)\b/i,
      /\benable execution\b/i,
      /\bresume execution\b/i,
    ],
    confidence: 0.92,
  },

  // --- reasoning mode (first match wins; order matters) ---
  {
    id: 'reasoning_analytical_explain_naturally',
    kind: 'field',
    targetField: 'reasoningMode',
    parsedValue: REASONING_MODES.ANALYTICAL,
    standalone: true,
    aliases: [
      /\bexplain (?:your )?reasoning naturally\b/i,
      /\bexplain your reasoning\b.*\bnaturally\b/i,
    ],
    confidence: 0.95,
  },
  {
    id: 'reasoning_analytical',
    kind: 'field',
    targetField: 'reasoningMode',
    parsedValue: REASONING_MODES.ANALYTICAL,
    standalone: true,
    aliases: [
      /\bexplain your reasoning\b/i,
      /\bexplain (?:the )?reasoning\b/i,
      /\bshow your (?:reasoning|work)\b/i,
      /\bwalk me through your reasoning\b/i,
      /\bthink (?:aloud|out loud)\b/i,
    ],
    confidence: 0.93,
  },
  {
    id: 'reasoning_natural',
    kind: 'field',
    targetField: 'reasoningMode',
    parsedValue: REASONING_MODES.NATURAL,
    standalone: true,
    aliases: [
      /\bexplain (?:your reasoning )?naturally\b/i,
      /\banswer naturally\b/i,
      /\btalk naturally\b/i,
      /\bnatural reasoning\b/i,
    ],
    confidence: 0.9,
  },
  {
    id: 'reasoning_concise',
    kind: 'field',
    targetField: 'reasoningMode',
    parsedValue: REASONING_MODES.CONCISE,
    standalone: true,
    aliases: [/\bshort answers?\b/i],
    confidence: 0.9,
  },
  {
    id: 'reasoning_teaching',
    kind: 'field',
    targetField: 'reasoningMode',
    parsedValue: REASONING_MODES.TEACHING,
    standalone: true,
    aliases: [
      /\bteaching reasoning mode\b/i,
      /\bswitch to teaching reasoning mode\b/i,
      /\buse teaching reasoning\b/i,
      /\breason in teaching mode\b/i,
      /\bteach(?:ing)? mode\b/i,
      /\bexplain like (?:i'?m|you'?re) teaching\b/i,
      /\bwalk me through step by step\b/i,
    ],
    confidence: 0.9,
  },

  // --- conversation style ---
  {
    id: 'conversation_concise',
    kind: 'field',
    targetField: 'conversationStyle',
    parsedValue: CONVERSATION_STYLES.CONCISE,
    standalone: true,
    aliases: [
      /\buse concise responses?\b/i,
      /\brespond concisely\b/i,
      /\bkeep your answers? brief\b/i,
      /\bbe concise\b/i,
      /\bkeep (?:it )?brief\b/i,
    ],
    confidence: 0.92,
  },
  {
    id: 'conversation_natural_explicit',
    kind: 'field',
    targetField: 'conversationStyle',
    parsedValue: CONVERSATION_STYLES.NATURAL,
    standalone: true,
    aliases: [
      /\banswer naturally\b/i,
      /\btalk (?:to me )?naturally\b/i,
      /\b(?:stay|keep it)\s+conversational\b/i,
      /\bnatural conversation\b/i,
      /\bexplain (?:your )?reasoning naturally\b/i,
      /\bexplain your reasoning\b.*\bnaturally\b/i,
    ],
    confidence: 0.92,
  },
  {
    id: 'conversation_natural_loose',
    kind: 'field',
    targetField: 'conversationStyle',
    parsedValue: CONVERSATION_STYLES.NATURAL,
    standalone: false,
    aliases: [/\bnaturally\b/i],
    confidence: 0.75,
  },
  {
    id: 'conversation_technical',
    kind: 'field',
    targetField: 'conversationStyle',
    parsedValue: CONVERSATION_STYLES.TECHNICAL,
    standalone: true,
    aliases: [
      /\btechnical (?:mode|detail)\b/i,
      /\bbe technical\b/i,
      /\buse technical language\b/i,
    ],
    confidence: 0.92,
  },
  {
    id: 'conversation_executive',
    kind: 'field',
    targetField: 'conversationStyle',
    parsedValue: CONVERSATION_STYLES.EXECUTIVE,
    standalone: true,
    aliases: [
      /\bexecutive (?:mode|summary)\b/i,
      /\bbe (?:brief and )?executive\b/i,
      /\bhigh[\s-]?level only\b/i,
    ],
    confidence: 0.92,
  },

  // --- operating mode (first match wins) ---
  {
    id: 'operating_business_os',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.BUSINESS_OPERATION,
    standalone: true,
    aliases: [/\boperate as (?:the )?(?:business operating system|max)\b/i],
    confidence: 0.94,
  },
  {
    id: 'operating_business_mode_label',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.BUSINESS_OPERATION,
    standalone: true,
    aliases: [/\bbusiness operation(?:s)? mode\b/i],
    confidence: 0.94,
  },
  {
    id: 'operating_according_to_role',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.BUSINESS_OPERATION,
    standalone: true,
    aliases: [/\boperate (?:according to|in) your role\b/i],
    confidence: 0.94,
  },
  {
    id: 'operating_work_as',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.BUSINESS_OPERATION,
    standalone: true,
    aliases: [/\b(?:work|function|behave) as\b/i],
    confidence: 0.9,
  },
  {
    id: 'operating_treat_as_production',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.BUSINESS_OPERATION,
    standalone: true,
    aliases: [/\btreat .+ as (?:a )?(?:real )?production business\b/i],
    confidence: 0.93,
  },
  {
    id: 'operating_treat_like_production',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.BUSINESS_OPERATION,
    standalone: true,
    aliases: [/\btreat .+ like (?:a )?(?:real )?production business\b/i],
    confidence: 0.93,
  },
  {
    id: 'operating_assume_production',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.BUSINESS_OPERATION,
    standalone: true,
    aliases: [/\bassume .+ is (?:a )?(?:real )?production business\b/i],
    confidence: 0.93,
  },
  {
    id: 'operating_consider_production',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.BUSINESS_OPERATION,
    standalone: true,
    aliases: [/\bconsider .+ (?:a )?(?:real )?production business\b/i],
    confidence: 0.93,
  },
  {
    id: 'operating_evaluate_intent',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.REASONING_EVALUATION,
    standalone: true,
    aliases: [
      /\b(?:i(?:'d| would)? like to|i want to|we'?re|i'?m)\s+evaluat(?:e|ing)\b/i,
    ],
    confidence: 0.93,
  },
  {
    id: 'operating_evaluate_how_operate',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.REASONING_EVALUATION,
    standalone: true,
    aliases: [/\bevaluat(?:e|ing)\b.{0,40}\bhow you operate\b/i],
    confidence: 0.93,
  },
  {
    id: 'operating_evaluate_session',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.REASONING_EVALUATION,
    standalone: true,
    aliases: [/\bfor this session\s+evaluat(?:e|ing)\b/i],
    confidence: 0.93,
  },
  {
    id: 'operating_evaluate_reasoning_bundle',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.REASONING_EVALUATION,
    standalone: true,
    aliases: [
      /\bevaluat(?:e|ing)\b.{0,40}\b(?:reasoning|how you operate|your operating model)\b/i,
      /\bevaluat(?:e|ing)\s+how you operate\b/i,
    ],
    confidence: 0.9,
  },
  {
    id: 'operating_reasoning_evaluation_label',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.REASONING_EVALUATION,
    standalone: true,
    aliases: [/\breasoning evaluation\b/i],
    confidence: 0.9,
  },
  {
    id: 'operating_mission_execution',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.MISSION_EXECUTION,
    standalone: true,
    aliases: [/\bmission execution mode\b/i],
    confidence: 0.9,
  },
  {
    id: 'operating_architecture_review',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.ARCHITECTURE_REVIEW,
    standalone: true,
    aliases: [/\barchitecture review\b/i],
    confidence: 0.9,
  },
  {
    id: 'operating_debugging',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.DEBUGGING,
    standalone: true,
    aliases: [/\bdebug(?:ging)? mode\b/i],
    confidence: 0.9,
  },
  {
    id: 'operating_learning',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.LEARNING,
    standalone: true,
    aliases: [/\blearning mode\b/i],
    confidence: 0.9,
  },
  {
    id: 'operating_planning',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.PLANNING,
    standalone: true,
    aliases: [/\bplanning mode\b/i],
    confidence: 0.9,
  },
  {
    id: 'operating_brainstorming',
    kind: 'field',
    targetField: 'operatingMode',
    parsedValue: OPERATING_MODES.BRAINSTORMING,
    standalone: true,
    aliases: [/\bbrainstorm(?:ing)? mode\b/i],
    confidence: 0.9,
  },

  // --- evaluation mode (first match wins) ---
  {
    id: 'evaluation_max_intent',
    kind: 'field',
    targetField: 'evaluationMode',
    parsedValue: EVALUATION_MODES.MAX,
    standalone: true,
    aliases: [
      /\b(?:we'?re|i'?m|i(?:'d| would)? like to|i want to)\s+evaluat(?:e|ing)\s+max\b/i,
    ],
    confidence: 0.93,
  },
  {
    id: 'evaluation_max',
    kind: 'field',
    targetField: 'evaluationMode',
    parsedValue: EVALUATION_MODES.MAX,
    standalone: true,
    aliases: [
      /\bevaluat(?:e|ing)\s+max\b/i,
      /\bevaluat(?:e|ing)\s+how you operate\b/i,
    ],
    confidence: 0.92,
  },
  {
    id: 'evaluation_scout',
    kind: 'field',
    targetField: 'evaluationMode',
    parsedValue: EVALUATION_MODES.SCOUT,
    standalone: true,
    aliases: [/\bevaluat(?:e|ing)\s+scout\b/i],
    confidence: 0.92,
  },
  {
    id: 'evaluation_mission_runtime',
    kind: 'field',
    targetField: 'evaluationMode',
    parsedValue: EVALUATION_MODES.MISSION_RUNTIME,
    standalone: true,
    aliases: [/\bevaluat(?:e|ing)\s+(?:the )?mission runtime\b/i],
    confidence: 0.92,
  },
  {
    id: 'evaluation_business',
    kind: 'field',
    targetField: 'evaluationMode',
    parsedValue: EVALUATION_MODES.BUSINESS,
    standalone: true,
    aliases: [/\bevaluat(?:e|ing)\s+(?:the )?business\b/i],
    confidence: 0.92,
  },
];

function directiveMatches(text, directive) {
  return directive.aliases.some((alias) => alias.test(text));
}

/**
 * All directives matched in registry order.
 * @param {string} text
 * @returns {SessionDirective[]}
 */
function matchDirectives(text) {
  const q = normalizeText(text);
  if (!q) return [];
  return SESSION_DIRECTIVES.filter((directive) => directiveMatches(q, directive));
}

/**
 * Field directives matched for classification heuristics.
 * @param {string} text
 * @returns {SessionDirective[]}
 */
function matchFieldDirectives(text) {
  return matchDirectives(text).filter((directive) => directive.kind === 'field');
}

function countSettingHits(text) {
  return matchFieldDirectives(text).length;
}

function hasScopeMarker(text) {
  return matchDirectives(text).some((directive) => directive.kind === 'scope');
}

function isResetDirective(text) {
  return matchDirectives(text).some((directive) => directive.kind === 'reset');
}

function isPersistentDirective(text) {
  return hasScopeMarker(text);
}

/**
 * First matched field value per targetField (registry order = priority).
 * @param {string} text
 * @returns {Record<string, *>}
 */
function extractFieldValues(text) {
  const values = {};
  for (const directive of matchFieldDirectives(text)) {
    if (directive.targetField && values[directive.targetField] == null) {
      values[directive.targetField] = directive.parsedValue;
    }
  }
  return values;
}

/**
 * Whether a standalone field directive would apply for this question/field.
 * @param {string} text
 * @param {string} field
 * @returns {boolean}
 */
function hasStandaloneFieldDirective(text, field) {
  const q = normalizeText(text);
  if (!q) return false;
  return matchFieldDirectives(q).some(
    (directive) => directive.targetField === field && directive.standalone === true
  );
}

/**
 * Shared semantic extraction consumed by SessionStateManager.
 * @param {string} text
 * @returns {{
 *   persistent: boolean,
 *   reset: boolean,
 *   executionPolicy: string|null,
 *   reasoningMode: string|null,
 *   conversationStyle: string|null,
 *   operatingMode: string|null,
 *   evaluationMode: string|null,
 *   matchedDirectives: SessionDirective[]
 * }}
 */
function extractDirectiveSignals(text) {
  const q = normalizeText(text);
  if (!q) {
    return {
      persistent: false,
      reset: false,
      executionPolicy: null,
      reasoningMode: null,
      conversationStyle: null,
      operatingMode: null,
      evaluationMode: null,
      matchedDirectives: [],
    };
  }

  const matchedDirectives = matchDirectives(q);
  const fields = extractFieldValues(q);

  return {
    persistent: hasScopeMarker(q),
    reset: isResetDirective(q),
    executionPolicy: fields.executionPolicy ?? null,
    reasoningMode: fields.reasoningMode ?? null,
    conversationStyle: fields.conversationStyle ?? null,
    operatingMode: fields.operatingMode ?? null,
    evaluationMode: fields.evaluationMode ?? null,
    matchedDirectives,
  };
}

function hasStructuredSessionField(signals) {
  return (
    signals.executionPolicy != null ||
    signals.reasoningMode != null ||
    signals.conversationStyle != null ||
    signals.operatingMode != null ||
    signals.evaluationMode != null
  );
}

/**
 * ADR-071 runtime guarantee helper — true when extraction would mutate state.
 * @param {string} text
 * @param {object} [signals]
 * @returns {boolean}
 */
function isInterpretableSessionConfiguration(text, signals = extractDirectiveSignals(text)) {
  if (signals.reset) return true;
  if (!hasStructuredSessionField(signals)) return false;
  if (signals.persistent) return true;
  if (matchFieldDirectives(text).some((directive) => directive.standalone === true)) return true;
  return countSettingHits(text) >= SESSION_CONFIGURATION_THRESHOLD;
}

module.exports = {
  SESSION_DIRECTIVES,
  SESSION_CONFIGURATION_THRESHOLD,
  matchDirectives,
  matchFieldDirectives,
  countSettingHits,
  hasScopeMarker,
  isResetDirective,
  isPersistentDirective,
  extractFieldValues,
  extractDirectiveSignals,
  hasStructuredSessionField,
  hasStandaloneFieldDirective,
  isInterpretableSessionConfiguration,
};
