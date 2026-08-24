'use strict';

/**
 * SPEC-155 — Conversation Contract types and session helpers (ADR-062).
 *
 * Conversation contracts establish rules of engagement before ownership
 * is resolved. They are first-class runtime objects that constrain
 * ownership, reasoning, execution, and presentation.
 */

const REASONING_MODES = Object.freeze({
  REFLECTION: 'reflection',
  EXPLANATION: 'explanation',
  EXECUTION: 'execution',
  INSPECTION: 'inspection',
  EXPLORATION: 'exploration',
});

const EXPLANATION_DEPTHS = Object.freeze({
  BRIEF: 'brief',
  STANDARD: 'standard',
  DEEP: 'deep',
});

const DEFAULT_CONTRACT = Object.freeze({
  executionAllowed: true,
  reasoningMode: REASONING_MODES.EXPLANATION,
  maintainContext: false,
  naturalConversation: false,
  explanationDepth: EXPLANATION_DEPTHS.STANDARD,
  conversationGoal: null,
  locked: false,
  confidence: 0.5,
  createdAt: null,
});

/** Conversation-level execution bans — not preparation-only workflow negations. */
const EXECUTION_FORBIDDEN_RES = [
  /\b(?:don'?t|do not)\s+execute anything\b/i,
  /\b(?:don'?t|do not)\s+execute\b(?!\s*(?:,|launch|approve|print|or mail))/i,
  /\b(?:don'?t|do not)\s+execute\b.{0,30}\b(?:during|this)\s+(?:conversation|discussion|turn)\b/i,
  /\b(?:don'?t|do not)\s+(?:do|perform)\s+anything\b/i,
  /\bread[\s-]?only\b/i,
  /\bno\s+(?:mutations?|mission updates?)\b/i,
  /\breasoning only\b/i,
  /\b(?:just|only)\s+(?:talk|discuss|converse|explore|think)\b/i,
  /\b(?:i'?m|i am)\s+evaluat(?:e|ing)\s+how you think\b/i,
  /\bunderstand how you think\b/i,
];

/** Desk/canary workflow negations — not conversation contract signals. */
const WORKFLOW_SCOPED_NEGATION_RES = [
  /\bdo not create a mission\b/i,
  /\bdo not launch,\s*execute,\s*approve\b/i,
  /\bdo not print or mail\b/i,
  /\bpreparation-only\b/i,
  /\bdo not include reasoning\b/i,
  /\bproceed with the recommended next preparation-only work order\b/i,
  /\bpacket-content review\b/i,
  /\bcall-script review\b/i,
  /\bfillable (?:verification )?table\b/i,
  /\bcanary summary\b/i,
  /\bwork order:/i,
];

const EXECUTION_ALLOWED_RES = [
  /\b(?:let'?s|go ahead and|time to|ready to)\s+execute\b/i,
  /\bstop theoriz(?:e|ing|y)\b/i,
  /\benough theory\b/i,
  /\b(?:let'?s|go ahead and)\s+(?:run|launch|begin|operate|proceed|approve)\b/i,
  /\bactually\s+approv(?:e|al)\b/i,
  /\bgo ahead and (?:approve|execute|run|proceed)\b/i,
];

const NATURAL_CONVERSATION_RES = [
  /\banswer naturally\b/i,
  /\blet'?s explore\b/i,
  /\bthink out loud\b/i,
  /\bwalk me through\b/i,
  /\bhelp me understand\b/i,
  /\b(?:stay|keep it)\s+conversational\b/i,
  /\btalk (?:to me )?naturally\b/i,
  /\b(?:continue|keep) (?:this )?conversation\b/i,
];

const MAINTAIN_CONTEXT_RES = [
  /\bmaintain (?:the )?(?:context|conversation)\b/i,
  /\bcontinue (?:this )?(?:discussion|conversation|thread)\b/i,
  /\bstay on (?:this )?(?:topic|subject|thread)\b/i,
  /\b(?:don'?t|do not)\s+restart\b/i,
  /\bstay conversational\b/i,
  /\bkeep (?:the )?(?:thread|context|discussion)\b/i,
  /\bmaintain the conversation\b/i,
];

const REFLECTION_MODE_RES = [
  /\b(?:evaluat(?:e|ing)|understand)\s+how you think\b/i,
  /\b(?:discuss|explore|evaluate)\s+(?:your )?reasoning\b/i,
  /\bthink out loud\b/i,
  /\bwalk me through (?:your )?(?:reasoning|thinking)\b/i,
  /\b(?:discuss|explore) your (?:reasoning|thinking|operating model)\b/i,
  /\bhow do you think\b/i,
];

const TOPIC_SWITCH_RES = [
  /\b(?:let'?s|time to)\s+switch (?:topics?|subjects?)\b/i,
  /\b(?:different|new|unrelated)\s+topic\b/i,
  /\bchange (?:the )?(?:subject|topic)\b/i,
  /\bmove on to\b/i,
];

const GOAL_EXTRACTION_RES = [
  {
    re: /\bunderstand (?:max'?s?|your)\s+(?:operating model|reasoning|role|thinking)\b/i,
    goal: "Understand Max's reasoning.",
  },
  {
    re: /\bunderstand scout\b/i,
    goal: "Understand Scout's role.",
  },
  {
    re: /\bevaluat(?:e|ing)\s+(?:reasoning|how you think)\b/i,
    goal: 'Evaluate reasoning.',
  },
  {
    re: /\breview (?:the )?architecture\b/i,
    goal: 'Review architecture.',
  },
  {
    re: /\bdebug mission ownership\b/i,
    goal: 'Debug mission ownership.',
  },
  {
    re: /\bunderstand how you think\b/i,
    goal: "Understand Max's reasoning.",
  },
  {
    re: /\b(?:discuss|explore) your reasoning\b/i,
    goal: "Understand Max's reasoning.",
  },
];

const CONTRACT_LOCK_RES = [
  /\b(?:don'?t|do not)\s+execute\b/i,
  /\bread[\s-]?only\b/i,
  /\breasoning only\b/i,
  /\b(?:don'?t|do not)\s+(?:do|perform)\s+anything\b/i,
  /\b(?:i'?m|i am)\s+evaluat(?:e|ing)\b/i,
  /\bunderstand how you think\b/i,
  /\bmaintain (?:the )?(?:context|conversation)\b/i,
  /\bstay conversational\b/i,
  /\banswer naturally\b/i,
];

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

function cloneContract(contract) {
  if (!contract || typeof contract !== 'object') return null;
  return { ...contract };
}

function getConversationContract(session) {
  if (session && session.conversationContract && typeof session.conversationContract === 'object') {
    return session.conversationContract;
  }
  const ctx = session && session.context && typeof session.context === 'object' ? session.context : null;
  const contract = ctx && ctx.conversationContract;
  if (!contract || typeof contract !== 'object') return null;
  return contract;
}

function setConversationContract(session, contract) {
  if (!session || typeof session !== 'object' || !contract) return;
  session.conversationContract = contract;
  if (session.context && typeof session.context === 'object') {
    session.context.conversationContract = contract;
  }
}

function createDefaultContract() {
  return {
    ...DEFAULT_CONTRACT,
    createdAt: new Date().toISOString(),
  };
}

function extractConversationGoal(text) {
  const q = normalizeText(text);
  for (const entry of GOAL_EXTRACTION_RES) {
    if (entry.re.test(q)) return entry.goal;
  }
  return null;
}

function isWorkflowScopedNegation(text) {
  return matchesAny(text, WORKFLOW_SCOPED_NEGATION_RES);
}

function detectContractSignals(text) {
  const q = normalizeText(text);
  if (!q) {
    return {
      executionForbidden: false,
      executionAllowed: null,
      naturalConversation: false,
      maintainContext: false,
      reflectionMode: false,
      topicSwitch: false,
      conversationGoal: null,
      shouldLock: false,
    };
  }

  let executionForbidden = matchesAny(q, EXECUTION_FORBIDDEN_RES);
  if (executionForbidden && isWorkflowScopedNegation(q)) {
    executionForbidden = false;
  }
  const executionAllowed =
    !executionForbidden && matchesAny(q, EXECUTION_ALLOWED_RES) ? true : null;
  const explicitExecutionAllowed =
    executionForbidden ? false : matchesAny(q, EXECUTION_ALLOWED_RES);

  return {
    executionForbidden,
    executionAllowed: explicitExecutionAllowed ? true : executionForbidden ? false : null,
    naturalConversation: matchesAny(q, NATURAL_CONVERSATION_RES),
    maintainContext: matchesAny(q, MAINTAIN_CONTEXT_RES),
    reflectionMode: matchesAny(q, REFLECTION_MODE_RES),
    topicSwitch: matchesAny(q, TOPIC_SWITCH_RES),
    conversationGoal: extractConversationGoal(q),
    shouldLock: matchesAny(q, CONTRACT_LOCK_RES),
  };
}

/**
 * Merge detected signals into a contract, respecting prior contract immutability
 * within a turn and explicit operator updates across turns.
 *
 * @param {object} input
 * @param {string} input.question
 * @param {object|null} [input.priorContract]
 * @returns {{ contract: object, changed: boolean, reason: string|null }}
 */
function buildConversationContract(input = {}) {
  const question = normalizeText(input.question);
  const prior = input.priorContract ? cloneContract(input.priorContract) : null;
  const signals = detectContractSignals(question);
  const now = new Date().toISOString();

  if (signals.topicSwitch) {
    const contract = {
      executionAllowed: signals.executionAllowed !== null ? signals.executionAllowed : true,
      reasoningMode: signals.reflectionMode
        ? REASONING_MODES.REFLECTION
        : REASONING_MODES.EXPLANATION,
      maintainContext: signals.maintainContext,
      naturalConversation: signals.naturalConversation,
      explanationDepth: EXPLANATION_DEPTHS.STANDARD,
      conversationGoal: signals.conversationGoal,
      locked: signals.shouldLock,
      confidence: signals.shouldLock ? 0.95 : 0.75,
      createdAt: now,
      via: 'topic_switch',
    };
    return { contract, changed: true, reason: 'topic_switch' };
  }

  if (!prior) {
    const contract = {
      executionAllowed:
        signals.executionAllowed !== null ? signals.executionAllowed : DEFAULT_CONTRACT.executionAllowed,
      reasoningMode: signals.reflectionMode
        ? REASONING_MODES.REFLECTION
        : signals.executionAllowed === true
          ? REASONING_MODES.EXECUTION
          : REASONING_MODES.EXPLANATION,
      maintainContext: signals.maintainContext,
      naturalConversation: signals.naturalConversation,
      explanationDepth: EXPLANATION_DEPTHS.STANDARD,
      conversationGoal: signals.conversationGoal,
      locked: signals.shouldLock,
      confidence: signals.shouldLock ? 0.95 : 0.7,
      createdAt: now,
      via: signals.shouldLock ? 'explicit_contract' : 'default',
    };
    return {
      contract,
      changed: signals.shouldLock || signals.executionAllowed === false,
      reason: signals.shouldLock ? 'explicit_contract' : 'default',
    };
  }

  const contract = { ...prior };
  let changed = false;
  let reason = null;

  if (signals.executionAllowed !== null && signals.executionAllowed !== prior.executionAllowed) {
    contract.executionAllowed = signals.executionAllowed;
    contract.reasoningMode = signals.executionAllowed
      ? REASONING_MODES.EXECUTION
      : signals.reflectionMode
        ? REASONING_MODES.REFLECTION
        : prior.reasoningMode;
    if (signals.executionAllowed) {
      contract.locked = false;
    }
    changed = true;
    reason = signals.executionAllowed ? 'execution_enabled' : 'execution_forbidden';
  } else if (signals.executionForbidden && prior.executionAllowed !== false) {
    contract.executionAllowed = false;
    contract.reasoningMode = REASONING_MODES.REFLECTION;
    changed = true;
    reason = 'execution_forbidden';
  }

  if (signals.naturalConversation) {
    contract.naturalConversation = true;
    changed = true;
    reason = reason || 'natural_conversation';
  }

  if (signals.maintainContext) {
    contract.maintainContext = true;
    changed = true;
    reason = reason || 'maintain_context';
  }

  if (signals.reflectionMode && contract.executionAllowed === false) {
    contract.reasoningMode = REASONING_MODES.REFLECTION;
  }

  if (signals.conversationGoal) {
    contract.conversationGoal = signals.conversationGoal;
    changed = true;
    reason = reason || 'goal_set';
  }

  if (signals.shouldLock) {
    contract.locked = true;
    contract.confidence = Math.max(contract.confidence || 0, 0.95);
    changed = true;
    reason = reason || 'contract_locked';
  }

  if (changed) {
    contract.updatedAt = now;
  }

  return { contract, changed, reason };
}

function contractBlocksExecution(contract) {
  return Boolean(contract && contract.executionAllowed === false);
}

function contractRequiresContinuity(contract) {
  return Boolean(contract && contract.maintainContext === true);
}

function contractLocksConversation(contract) {
  return Boolean(contract && contract.locked === true);
}

module.exports = {
  REASONING_MODES,
  EXPLANATION_DEPTHS,
  DEFAULT_CONTRACT,
  getConversationContract,
  setConversationContract,
  createDefaultContract,
  detectContractSignals,
  buildConversationContract,
  extractConversationGoal,
  contractBlocksExecution,
  contractRequiresContinuity,
  contractLocksConversation,
};
