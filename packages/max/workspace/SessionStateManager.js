'use strict';

/**
 * SPEC-148 — Session State Manager (ADR-068).
 *
 * Pipeline position:
 *   Raw Operator Message → Message Type Classifier (SPEC-149) → Session State Manager → Conversation Contract → …
 *
 * Session State precedes conversation analysis. Persistent operator directives
 * become explicit Session State — never inferred solely from recent prompts.
 */

const askPathTrace = require('./audit/AskPathTrace');
const {
  OPERATING_MODES,
  EXECUTION_POLICIES,
  REASONING_MODES,
  CONVERSATION_STYLES,
  EVALUATION_MODES,
  normalizeText,
  cloneSessionState,
  getSessionState,
  setSessionState,
  appendSessionStateHistory,
  createDefaultSessionState,
  sessionStateBlocksExecution,
} = require('./SessionState');

/** Operator establishes persistent session rules for the remainder of the workspace. */
const PERSISTENT_DIRECTIVE_RES = [
  /\bfor the rest of (?:this )?conversation\b/i,
  /\bfor the remainder of (?:this )?conversation\b/i,
  /\buntil i (?:say|tell you) otherwise\b/i,
  /\bfor the rest of this session\b/i,
  /\bfor the remainder of this session\b/i,
  /\bfor this session\b/i,
  /\bfor today'?s conversation\b/i,
  /\bduring this evaluation\b/i,
  /\bgoing forward\b/i,
  /\buntil i change it\b/i,
];

const SESSION_RESET_RES = [
  /\breset (?:the )?session\b/i,
  /\bclear (?:the )?session (?:state|settings)\b/i,
  /\bstart (?:a )?fresh session\b/i,
  /\breturn to default (?:mode|settings)\b/i,
];

const SESSION_WHY_RES = [
  /\bwhy are you using that(?: operating mode)?\b/i,
  /\bwhy are you using it\b/i,
  /\bwhy (?:are you|is (?:that|this)) (?:using )?(?:that )?(?:operating mode|execution policy|reasoning mode|conversation style|evaluation mode)\b/i,
  /\bwhy is (?:that|this) (?:the )?(?:operating mode|execution policy|reasoning mode)\b/i,
];

/** Operator asks to read stored state — not configure it. */
const SESSION_INSPECTION_INTENT_RES = [
  /\bwhat\b/i,
  /\bhow are you\b/i,
  /\bhow am i\b/i,
  /\bsummarize\b/i,
  /\bshow (?:me )?\b/i,
  /\bis active\b/i,
  /\bare you\b/i,
  /\bcurrent\b/i,
  /\bconfigured\b/i,
  /\bfollowing\b/i,
  /\busing\b/i,
  /\bare we\b/i,
];

const EXECUTION_DISABLED_RES = [
  /\b(?:don'?t|do not)\s+execute anything\b/i,
  /\b(?:don'?t|do not)\s+execute\b(?!\s*(?:,|launch|approve|print|or mail))/i,
  /\b(?:don'?t|do not)\s+(?:do|perform)\s+anything\b/i,
  /\bread[\s-]?only\b/i,
  /\bno execution\b/i,
  /\bexecution disabled\b/i,
];

const EXECUTION_AUTONOMOUS_RES = [
  /\bautonomous execution\b/i,
  /\bexecute autonomously\b/i,
  /\b(?:you may|go ahead and) execute without asking\b/i,
];

const EXECUTION_NORMAL_RES = [
  /\b(?:let'?s|go ahead and|time to|ready to)\s+execute\b/i,
  /\bstop theoriz(?:e|ing|y)\b/i,
  /\benough theory\b/i,
  /\b(?:let'?s|go ahead and)\s+(?:run|launch|begin|operate|proceed|approve)\b/i,
  /\benable execution\b/i,
  /\bresume execution\b/i,
];

const REASONING_ANALYTICAL_RES = [
  /\bexplain your reasoning\b/i,
  /\bexplain (?:the )?reasoning\b/i,
  /\bshow your (?:reasoning|work)\b/i,
  /\bwalk me through your reasoning\b/i,
  /\bthink (?:aloud|out loud)\b/i,
];

const REASONING_NATURAL_RES = [
  /\bexplain (?:your reasoning )?naturally\b/i,
  /\banswer naturally\b/i,
  /\btalk naturally\b/i,
  /\bnatural reasoning\b/i,
];

const REASONING_CONCISE_RES = [
  /\bbe concise\b/i,
  /\bkeep (?:it )?brief\b/i,
  /\bshort answers?\b/i,
];

const REASONING_TEACHING_RES = [
  /\bteach(?:ing)? mode\b/i,
  /\bexplain like (?:i'?m|you'?re) teaching\b/i,
  /\bwalk me through step by step\b/i,
];

const CONVERSATION_NATURAL_RES = [
  /\banswer naturally\b/i,
  /\btalk (?:to me )?naturally\b/i,
  /\b(?:stay|keep it)\s+conversational\b/i,
  /\bnatural conversation\b/i,
];

const CONVERSATION_TECHNICAL_RES = [
  /\btechnical (?:mode|detail)\b/i,
  /\bbe technical\b/i,
  /\buse technical language\b/i,
];

const CONVERSATION_EXECUTIVE_RES = [
  /\bexecutive (?:mode|summary)\b/i,
  /\bbe (?:brief and )?executive\b/i,
  /\bhigh[\s-]?level only\b/i,
];

const OPERATING_MODE_PATTERNS = [
  {
    re: /\boperate as (?:the )?business operating system\b/i,
    mode: OPERATING_MODES.BUSINESS_OPERATION,
  },
  {
    re: /\bbusiness operation(?:s)? mode\b/i,
    mode: OPERATING_MODES.BUSINESS_OPERATION,
  },
  {
    re: /\boperate (?:according to|in) your role\b/i,
    mode: OPERATING_MODES.BUSINESS_OPERATION,
  },
  {
    re: /\b(?:work|function|behave) as\b/i,
    mode: OPERATING_MODES.BUSINESS_OPERATION,
  },
  {
    re: /\btreat .+ as (?:a )?(?:real )?production business\b/i,
    mode: OPERATING_MODES.BUSINESS_OPERATION,
  },
  {
    re: /\btreat .+ like (?:a )?(?:real )?production business\b/i,
    mode: OPERATING_MODES.BUSINESS_OPERATION,
  },
  {
    re: /\bassume .+ is (?:a )?(?:real )?production business\b/i,
    mode: OPERATING_MODES.BUSINESS_OPERATION,
  },
  {
    re: /\bconsider .+ (?:a )?(?:real )?production business\b/i,
    mode: OPERATING_MODES.BUSINESS_OPERATION,
  },
  {
    re: /\b(?:i(?:'d| would)? like to|i want to|we'?re|i'?m)\s+evaluat(?:e|ing)\b/i,
    mode: OPERATING_MODES.REASONING_EVALUATION,
  },
  {
    re: /\bevaluat(?:e|ing)\b.{0,40}\bhow you operate\b/i,
    mode: OPERATING_MODES.REASONING_EVALUATION,
  },
  {
    re: /\bfor this session\s+evaluat(?:e|ing)\b/i,
    mode: OPERATING_MODES.REASONING_EVALUATION,
  },
  {
    re: /\breasoning evaluation\b/i,
    mode: OPERATING_MODES.REASONING_EVALUATION,
  },
  {
    re: /\bmission execution mode\b/i,
    mode: OPERATING_MODES.MISSION_EXECUTION,
  },
  {
    re: /\barchitecture review\b/i,
    mode: OPERATING_MODES.ARCHITECTURE_REVIEW,
  },
  {
    re: /\bdebug(?:ging)? mode\b/i,
    mode: OPERATING_MODES.DEBUGGING,
  },
  {
    re: /\blearning mode\b/i,
    mode: OPERATING_MODES.LEARNING,
  },
  {
    re: /\bplanning mode\b/i,
    mode: OPERATING_MODES.PLANNING,
  },
  {
    re: /\bbrainstorm(?:ing)? mode\b/i,
    mode: OPERATING_MODES.BRAINSTORMING,
  },
];

const EVALUATION_PATTERNS = [
  { re: /\b(?:we'?re|i'?m|i(?:'d| would)? like to|i want to)\s+evaluat(?:e|ing)\s+max\b/i, mode: EVALUATION_MODES.MAX },
  { re: /\bevaluat(?:e|ing)\s+max\b/i, mode: EVALUATION_MODES.MAX },
  { re: /\bevaluat(?:e|ing)\s+scout\b/i, mode: EVALUATION_MODES.SCOUT },
  { re: /\bevaluat(?:e|ing)\s+(?:the )?mission runtime\b/i, mode: EVALUATION_MODES.MISSION_RUNTIME },
  { re: /\bevaluat(?:e|ing)\s+(?:the )?business\b/i, mode: EVALUATION_MODES.BUSINESS },
  { re: /\bevaluat(?:e|ing)\s+how you operate\b/i, mode: EVALUATION_MODES.MAX },
];

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

function isPersistentDirective(text) {
  return matchesAny(text, PERSISTENT_DIRECTIVE_RES);
}

function isSessionResetRequest(text) {
  return matchesAny(text, SESSION_RESET_RES);
}

/**
 * SPEC-150A — registry of every inspectable Session State field.
 * @type {Array<{ key: string, displayName: string, stateKey: string, aliases: RegExp[], formatter: (value: *) => string, isSummary?: boolean }>}
 */
const SESSION_STATE_FIELDS = [];

function registerSessionStateField(field) {
  SESSION_STATE_FIELDS.push(field);
}

function fieldMatchesQuestion(field, question) {
  return field.aliases.some((alias) => alias.test(question));
}

function hasSessionInspectionIntent(question) {
  return matchesAny(question, SESSION_INSPECTION_INTENT_RES);
}

/**
 * Resolve which Session State field an operator question inspects.
 * @param {string} text
 * @returns {object|null}
 */
function resolveSessionStateField(text) {
  const q = normalizeText(text);
  if (!q || matchesAny(q, SESSION_WHY_RES)) return null;
  if (!hasSessionInspectionIntent(q)) return null;

  for (const field of SESSION_STATE_FIELDS) {
    if (fieldMatchesQuestion(field, q)) return field;
  }
  return null;
}

function isSessionInspectionQuestion(text) {
  return resolveSessionStateField(text) != null;
}

function getSessionStateField(key) {
  return SESSION_STATE_FIELDS.find((field) => field.key === key) || null;
}

function detectSessionDirectiveSignals(text) {
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
    };
  }

  const persistent = isPersistentDirective(q);
  const reset = isSessionResetRequest(q);

  let executionPolicy = null;
  if (matchesAny(q, EXECUTION_DISABLED_RES)) {
    executionPolicy = EXECUTION_POLICIES.READ_ONLY;
  } else if (matchesAny(q, EXECUTION_AUTONOMOUS_RES)) {
    executionPolicy = EXECUTION_POLICIES.AUTONOMOUS;
  } else if (matchesAny(q, EXECUTION_NORMAL_RES)) {
    executionPolicy = EXECUTION_POLICIES.NORMAL;
  }

  let reasoningMode = null;
  const explainReasoningNaturally =
    /\bexplain (?:your )?reasoning naturally\b/i.test(q) ||
    /\bexplain your reasoning\b.*\bnaturally\b/i.test(q);
  if (explainReasoningNaturally) {
    reasoningMode = REASONING_MODES.ANALYTICAL;
  } else if (matchesAny(q, REASONING_ANALYTICAL_RES)) {
    reasoningMode = REASONING_MODES.ANALYTICAL;
  } else if (matchesAny(q, REASONING_NATURAL_RES)) {
    reasoningMode = REASONING_MODES.NATURAL;
  } else if (matchesAny(q, REASONING_CONCISE_RES)) {
    reasoningMode = REASONING_MODES.CONCISE;
  } else if (matchesAny(q, REASONING_TEACHING_RES)) {
    reasoningMode = REASONING_MODES.TEACHING;
  }

  let conversationStyle = null;
  if (
    matchesAny(q, CONVERSATION_NATURAL_RES) ||
    explainReasoningNaturally ||
    /\bnaturally\b/i.test(q)
  ) {
    conversationStyle = CONVERSATION_STYLES.NATURAL;
  } else if (matchesAny(q, CONVERSATION_TECHNICAL_RES)) {
    conversationStyle = CONVERSATION_STYLES.TECHNICAL;
  } else if (matchesAny(q, CONVERSATION_EXECUTIVE_RES)) {
    conversationStyle = CONVERSATION_STYLES.EXECUTIVE;
  }

  let operatingMode = null;
  for (const entry of OPERATING_MODE_PATTERNS) {
    if (entry.re.test(q)) {
      operatingMode = entry.mode;
      break;
    }
  }

  let evaluationMode = null;
  for (const entry of EVALUATION_PATTERNS) {
    if (entry.re.test(q)) {
      evaluationMode = entry.mode;
      break;
    }
  }

  return {
    persistent,
    reset,
    executionPolicy,
    reasoningMode,
    conversationStyle,
    operatingMode,
    evaluationMode,
  };
}

function shouldApplyFieldUpdate(signals, fieldPresent) {
  if (!fieldPresent) return false;
  return signals.persistent || signals.reset;
}

function applyFieldUpdate(state, field, value, reason, session, changes) {
  if (value == null || state[field] === value) return;
  const change = {
    field,
    previous: state[field],
    current: value,
    reason,
    timestamp: new Date().toISOString(),
  };
  state[field] = value;
  changes.push(change);
  appendSessionStateHistory(session, change);
}

/**
 * Apply directive signals to session state. Persistent directives update state;
 * non-persistent field signals still apply when bundled with a persistent marker
 * or when they appear as standalone session-setting phrases.
 *
 * @param {object} input
 * @param {string} input.question
 * @param {object|null} [input.priorState]
 * @param {object|null} [input.session]
 * @returns {{ state: object, changed: boolean, changes: object[], reason: string|null }}
 */
function buildSessionState(input = {}) {
  const question = normalizeText(input.question);
  const session = input.session || null;
  const signals = detectSessionDirectiveSignals(question);
  const now = new Date().toISOString();

  if (signals.reset) {
    const state = createDefaultSessionState();
    const changes = [];
    if (session) {
      appendSessionStateHistory(session, {
        field: '*',
        previous: input.priorState || null,
        current: state,
        reason: 'session_reset',
        timestamp: now,
      });
    }
    return { state, changed: true, changes, reason: 'session_reset' };
  }

  const prior = input.priorState
    ? cloneSessionState(input.priorState)
    : createDefaultSessionState();
  const state = prior;
  const changes = [];
  let changed = false;
  let reason = null;

  const applyIf = (field, value, fieldReason, force = false) => {
    if (value == null) return;
    const mayApply =
      force ||
      signals.persistent ||
      shouldApplyStandaloneField(question, field);
    if (!mayApply) return;
    const before = state[field];
    applyFieldUpdate(state, field, value, fieldReason, session, changes);
    if (state[field] !== before) {
      changed = true;
      reason = reason || fieldReason;
    }
  };

  applyIf('executionPolicy', signals.executionPolicy, 'execution_policy');
  applyIf('reasoningMode', signals.reasoningMode, 'reasoning_mode');
  applyIf('conversationStyle', signals.conversationStyle, 'conversation_style');
  applyIf('operatingMode', signals.operatingMode, 'operating_mode');
  applyIf('evaluationMode', signals.evaluationMode, 'evaluation_mode');

  if (changed) {
    state.updatedAt = now;
  }

  return { state, changed, changes, reason };
}

/** Standalone session-setting phrases that update state even without persistent marker. */
function shouldApplyStandaloneField(question, field) {
  const q = normalizeText(question);
  if (!q) return false;
  switch (field) {
    case 'executionPolicy':
      return matchesAny(q, EXECUTION_DISABLED_RES) || matchesAny(q, EXECUTION_NORMAL_RES);
    case 'reasoningMode':
      return (
        matchesAny(q, REASONING_ANALYTICAL_RES) ||
        matchesAny(q, REASONING_NATURAL_RES) ||
        matchesAny(q, REASONING_CONCISE_RES) ||
        matchesAny(q, REASONING_TEACHING_RES)
      );
    case 'conversationStyle':
      return (
        matchesAny(q, CONVERSATION_NATURAL_RES) ||
        matchesAny(q, CONVERSATION_TECHNICAL_RES) ||
        matchesAny(q, CONVERSATION_EXECUTIVE_RES)
      );
    case 'operatingMode':
      return OPERATING_MODE_PATTERNS.some((entry) => entry.re.test(q));
    case 'evaluationMode':
      return EVALUATION_PATTERNS.some((entry) => entry.re.test(q));
    default:
      return false;
  }
}

function formatOperatingModeLabel(mode) {
  if (!mode) return 'Business Operation';
  return String(mode)
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function formatExecutionPolicyLabel(policy) {
  if (!policy) return 'Normal';
  if (policy === EXECUTION_POLICIES.READ_ONLY) return 'Read Only';
  if (policy === EXECUTION_POLICIES.EXECUTION_DISABLED) return 'Execution Disabled';
  if (policy === EXECUTION_POLICIES.AUTONOMOUS) return 'Autonomous';
  if (policy === EXECUTION_POLICIES.OPERATOR_APPROVAL_REQUIRED) {
    return 'Operator Approval Required';
  }
  return 'Normal';
}

function formatReasoningModeLabel(mode) {
  if (!mode) return 'Natural';
  return String(mode)
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function formatConversationStyleLabel(style) {
  if (!style) return 'Natural';
  return String(style).charAt(0).toUpperCase() + String(style).slice(1);
}

function formatEvaluationModeLabel(mode) {
  if (!mode || mode === EVALUATION_MODES.NONE) return 'None';
  if (mode === EVALUATION_MODES.MAX) return 'Max Operating Model';
  if (mode === EVALUATION_MODES.SCOUT) return 'Scout';
  if (mode === EVALUATION_MODES.MISSION_RUNTIME) return 'Mission Runtime';
  if (mode === EVALUATION_MODES.BUSINESS) return 'Business';
  return String(mode);
}

registerSessionStateField({
  key: 'executionPolicy',
  displayName: 'Execution Policy',
  stateKey: 'executionPolicy',
  aliases: [
    /\bexecution policy\b/i,
    /\bexecution mode\b/i,
    /\bare you allowed to execute\b/i,
    /\bexecution settings\b/i,
    /\bcurrent execution policy\b/i,
  ],
  formatter: formatExecutionPolicyLabel,
});
registerSessionStateField({
  key: 'reasoningMode',
  displayName: 'Reasoning Mode',
  stateKey: 'reasoningMode',
  aliases: [
    /\breasoning mode\b/i,
    /\bthinking mode\b/i,
    /\bhow are you reasoning\b/i,
    /\bcurrent reasoning mode\b/i,
  ],
  formatter: formatReasoningModeLabel,
});
registerSessionStateField({
  key: 'conversationStyle',
  displayName: 'Conversation Style',
  stateKey: 'conversationStyle',
  aliases: [
    /\bconversation style\b/i,
    /\bresponse style\b/i,
    /\bcommunication style\b/i,
    /\bhow are you responding\b/i,
  ],
  formatter: formatConversationStyleLabel,
});
registerSessionStateField({
  key: 'evaluationMode',
  displayName: 'Evaluation Mode',
  stateKey: 'evaluationMode',
  aliases: [
    /\bevaluation mode\b/i,
    /\bwhat are we evaluating\b/i,
    /\bevaluation state\b/i,
    /\bcurrent evaluation\b/i,
  ],
  formatter: formatEvaluationModeLabel,
});
registerSessionStateField({
  key: 'operatingMode',
  displayName: 'Operating Mode',
  stateKey: 'operatingMode',
  aliases: [
    /\boperating mode\b/i,
    /\bhow are you operating\b/i,
    /\bcurrent mode\b/i,
    /\bwhat mode\b/i,
    /\b(?:your )?mode\b/i,
  ],
  formatter: formatOperatingModeLabel,
});
registerSessionStateField({
  key: 'summary',
  displayName: 'Session Summary',
  stateKey: 'summary',
  isSummary: true,
  aliases: [
    /\bsession state\b/i,
    /\bcurrent session\b/i,
    /\bsummarize(?:\s+(?:the\s+)?(?:current\s+)?session|\s+your(?:\s+current)?\s+session)\b/i,
    /\bsession summary\b/i,
    /\bhow are you configured\b/i,
    /\b(?:what are your )?current session settings\b/i,
    /\bshow (?:me )?(?:the )?current session\b/i,
    /\bwhat (?:is your )?current session\b/i,
  ],
  formatter: () => '',
});

/**
 * Format one inspectable field or the full session summary.
 * @param {object|null} state
 * @param {object|null} field
 * @returns {string}
 */
function formatSessionFieldInspection(state, field) {
  if (!field || field.isSummary) {
    return formatSessionInspection(state);
  }
  const s = state || createDefaultSessionState();
  const value = field.formatter(s[field.stateKey]);
  return ['Current Session', '', field.displayName, '', value].join('\n');
}

/**
 * Format session state for operator inspection — reads stored state, not inference.
 * @param {object|null} state
 * @returns {string}
 */
function formatSessionInspection(state) {
  const s = state || createDefaultSessionState();
  const lines = [
    'Current Session',
    '',
    'Operating Mode',
    '',
    formatOperatingModeLabel(s.operatingMode),
    '',
    'Execution Policy',
    '',
    formatExecutionPolicyLabel(s.executionPolicy),
    '',
    'Reasoning Mode',
    '',
    formatReasoningModeLabel(s.reasoningMode),
    '',
    'Conversation Style',
    '',
    formatConversationStyleLabel(s.conversationStyle),
    '',
    'Evaluation Mode',
    '',
    formatEvaluationModeLabel(s.evaluationMode),
  ];
  if (s.activeObjective) {
    lines.push('', 'Active Objective', '', String(s.activeObjective));
  }
  return lines.join('\n');
}

/**
 * SPEC-150 / ADR-070 — the shared inspection read interface.
 * Returns stored Session State only. Never infers from mission context or the prompt.
 *
 * Any runtime component that mutates persistent Session State must write through
 * `setSessionState` so this function exposes the same fields.
 *
 * @param {object|null} session
 * @returns {{
 *   operatingMode: string,
 *   executionPolicy: string,
 *   reasoningMode: string,
 *   conversationStyle: string,
 *   evaluationMode: string,
 *   activeObjective: string|null,
 *   sessionStarted: string|null,
 *   lastUpdated: string|null
 * }}
 */
function getCurrentState(session) {
  const stored = getSessionState(session);
  const state =
    stored && typeof stored === 'object'
      ? cloneSessionState(stored)
      : createDefaultSessionState();
  return {
    operatingMode: state.operatingMode,
    executionPolicy: state.executionPolicy,
    reasoningMode: state.reasoningMode,
    conversationStyle: state.conversationStyle,
    evaluationMode: state.evaluationMode,
    activeObjective: state.activeObjective || null,
    sessionStarted: state.sessionStarted || state.createdAt || null,
    lastUpdated: state.lastUpdated || state.updatedAt || null,
  };
}

/**
 * Apply session state execution policy to a conversation contract.
 * Session State precedes and constrains the contract (ADR-068).
 *
 * @param {object|null} sessionState
 * @param {object} contract
 * @returns {object}
 */
function applySessionStateToContract(sessionState, contract) {
  if (!contract || typeof contract !== 'object') return contract;
  if (!sessionState || !sessionStateBlocksExecution(sessionState)) {
    return contract;
  }
  return {
    ...contract,
    executionAllowed: false,
    reasoningMode:
      sessionState.reasoningMode === REASONING_MODES.ANALYTICAL ||
      sessionState.reasoningMode === REASONING_MODES.REFLECTIVE
        ? 'reflection'
        : contract.reasoningMode,
    naturalConversation:
      sessionState.conversationStyle === CONVERSATION_STYLES.NATURAL
        ? true
        : contract.naturalConversation,
    locked: true,
    via: 'session_state',
    sessionExecutionPolicy: sessionState.executionPolicy,
  };
}

/**
 * Resolve session state for this turn. First step in WorkspaceEngine pipeline.
 *
 * @param {object} input
 * @param {string} input.question
 * @param {object} [input.session]
 * @returns {{ state: object, changed: boolean, changes: object[], reason: string|null, priorState: object|null }}
 */
function resolveSessionState(input = {}) {
  askPathTrace.traceEnter('resolveSessionState');
  const question = normalizeText(input.question);
  const session = input.session || null;
  const priorState = getSessionState(session) || null;

  const built = buildSessionState({
    question,
    priorState,
    session,
  });

  const state = built.state;
  setSessionState(session, state);

  askPathTrace.traceBranch('session_state', {
    operatingMode: state.operatingMode,
    executionPolicy: state.executionPolicy,
    reasoningMode: state.reasoningMode,
    conversationStyle: state.conversationStyle,
    evaluationMode: state.evaluationMode,
    changed: built.changed,
    reason: built.reason,
  });

  if (built.changed) {
    askPathTrace.traceBranch('session_state_changed', {
      reason: built.reason,
      changes: built.changes.map((c) => c.field),
    });
  }

  askPathTrace.traceEarlyReturn('resolveSessionState', built.reason || 'resolved');
  return {
    state,
    changed: built.changed,
    changes: built.changes,
    reason: built.reason,
    priorState,
  };
}

module.exports = {
  resolveSessionState,
  buildSessionState,
  detectSessionDirectiveSignals,
  isPersistentDirective,
  isSessionResetRequest,
  isSessionInspectionQuestion,
  resolveSessionStateField,
  SESSION_STATE_FIELDS,
  getSessionStateField,
  formatSessionInspection,
  formatSessionFieldInspection,
  formatOperatingModeLabel,
  formatExecutionPolicyLabel,
  formatReasoningModeLabel,
  formatConversationStyleLabel,
  formatEvaluationModeLabel,
  getCurrentState,
  applySessionStateToContract,
  sessionStateBlocksExecution,
  SESSION_WHY_RES,
};
