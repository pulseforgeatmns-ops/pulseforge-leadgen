'use strict';

/**
 * SPEC-167 — Operator Objective Resolution Engine (ADR-087).
 *
 * Transforms a free-form executive directive into a structured execution contract
 * before routing begins. Routing uses primaryObjective only; modifiers affect
 * execution and presentation, never routing.
 *
 * Pipeline:
 *   Operator Message → Extract Candidates → Resolve Primary → Supporting →
 *   Execution Modifiers → Conversation Modifiers → Required Capabilities →
 *   Routing Decision → Execution Contract
 */

const askPathTrace = require('./audit/AskPathTrace');
const { normalizeText } = require('./SessionState');
const {
  PRIMARY_OBJECTIVES,
  SUPPORTING_OBJECTIVES,
  EXECUTION_MODIFIERS,
  CONVERSATION_MODIFIERS,
  REQUIRED_CAPABILITIES,
  comparePrimaryObjectivePriority,
} = require('./PrimaryObjective');
const { resolveRoutingDecision } = require('./ObjectiveRoutingMap');
const { buildExecutionContract } = require('./ExecutionContract');
const { resolveCanonicalObjective } = require('./ResolvedObjective');
const { isSessionInspectionQuestion } = require('./SessionStateManager');
const { isExecutionInspectionQuestion } = require('./ExecutionInspectionRegistry');
const { detectConversationSubject, CONVERSATION_SUBJECTS } = require('./ConversationSubject');
const {
  detectSessionDirectiveSignals,
  isSessionResetRequest,
} = require('./SessionStateManager');
const {
  shouldInspectActiveMission,
} = require('./WorkspaceMissionInspection');
const { detectAcquisitionObjective } = require('./AcquisitionObjectiveDetection');
const { splitMessageSegments } = require('./MessageTypeClassifier');
const {
  isSessionConfigurationMessage,
  isMissionCreationMessage,
  isApprovalMessage,
} = require('./MessageTypeClassifier');
const { isMissionExecutionCommand } = require('./ExecutionLanguageDetection');

const MISSION_CREATION_RES = [
  /\bcreate (?:a )?(?:new )?mission\b/i,
  /\bcreate\b.{0,40}\b(?:acquisition\s+)?mission\b/i,
  /\bcreate\s+a\s+(?:new\s+|production\s+)?acquisition\s+mission\b/i,
  /\bstart (?:a )?(?:new )?(?:acquisition )?mission\b/i,
  /\bbegin (?:a )?(?:new )?(?:acquisition )?mission\b/i,
  /\b(?:launch|open|kick off)\s+(?:a )?(?:new )?(?:acquisition )?mission\b/i,
  /\bnew acquisition mission\b/i,
  /\bstart acquisition\b/i,
  /\bbegin campaign\b/i,
  /\bkick off investigation\b/i,
  /\bopen new mission\b/i,
  /\blaunch mission\b/i,
];

const MISSION_EXECUTION_RES = [
  /\b(?:then )?continue (?:the )?(?:acquisition )?mission\b/i,
  /\b(?:then )?resume (?:the )?(?:acquisition )?mission\b/i,
  /\b(?:then )?proceed with (?:the )?(?:acquisition )?mission\b/i,
  /\bcontinue the mission\b/i,
  /\bexecute (?:the )?(?:acquisition )?mission\b/i,
  /\brun (?:the )?(?:acquisition )?mission\b/i,
];

const BUSINESS_INTELLIGENCE_RES = [
  /\bhow is .+ doing\b/i,
  /\bhow are (?:we|they) doing\b/i,
  /\bhow(?:'s| is) .+ performing\b/i,
  /\bwhat(?:'s| is) (?:the )?status of\b/i,
  /\bhow (?:is|are) .+ (?:performing|progressing)\b/i,
  /\bgive me (?:a )?(?:summary|update|status) (?:on|of|for)\b/i,
  /\bwhat(?:'s| is) happening with\b/i,
];

const BUSINESS_DECISION_RES = [
  /\bwhat should (?:happen|we do|i do) next\b/i,
  /\bwhat(?:'s| is) the next (?:step|action|move)\b/i,
  /\bwhat do you recommend (?:we do )?next\b/i,
  /\bhow should we proceed\b/i,
  /\bwhat would you do next\b/i,
  /\bwhat should we do next\b/i,
];

const IDENTITY_RES = [
  /\bwho are you\b/i,
  /\bwhat are you\b/i,
  /\bwhat(?:'s| is) (?:your|max'?s?) role\b/i,
  /\btell me about yourself\b/i,
  /\bwhat can you do\b/i,
];

const REASONING_EXPLANATION_RES = [
  /\bexplain (?:your )?reasoning\b/i,
  /\bshow (?:your )?reasoning\b/i,
  /\bwalk me through\b/i,
  /\bexplain why\b/i,
  /\bshow your work\b/i,
];

const CAPABILITY_RES = Object.freeze({
  [REQUIRED_CAPABILITIES.SCOUT_INTELLIGENCE]: [
    /\bscout intelligence\b/i,
    /\buse scout\b/i,
  ],
  [REQUIRED_CAPABILITIES.OPPORTUNITY_INTELLIGENCE]: [
    /\bopportunity intelligence\b/i,
  ],
  [REQUIRED_CAPABILITIES.OUTCOME_LEARNING]: [
    /\boutcome learning\b/i,
  ],
});

const PRODUCTION_MISSION_RES = [
  /\bproduction acquisition mission\b/i,
  /\bproduction mission\b/i,
];

const PAUSE_ON_APPROVAL_RES = [
  /\bpause only (?:for|when) operator (?:judgment|approval)\b/i,
  /\bpause only when operator approval is required\b/i,
  /\bpause on approval\b/i,
  /\bwait for (?:my )?approval\b/i,
  /\boperator approval required\b/i,
];

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

function isMissionExecutionMessage(text, input = {}) {
  const q = normalizeText(text);
  if (!q) return false;
  if (isSessionConfigurationMessage(q)) return false;
  if (isMissionCreationMessage(q)) return false;
  const hasActiveMission = Boolean(input.hasActiveMission || input.mission);
  if (!hasActiveMission && !isApprovalMessage(q)) return false;
  return isMissionExecutionCommand(q) || isApprovalMessage(q);
}

/**
 * @typedef {object} CandidateObjective
 * @property {string} objective — PRIMARY_OBJECTIVES value
 * @property {number} confidence
 * @property {string[]} evidence
 * @property {string} segment
 * @property {number} segmentIndex
 */

/**
 * @typedef {object} ObjectiveResolution
 * @property {string} primaryObjective
 * @property {string[]} supportingObjectives
 * @property {string[]} executionModifiers
 * @property {string[]} conversationModifiers
 * @property {string[]} requiredCapabilities
 * @property {{ owner: string, pipeline: string, reason: string }} routingDecision
 * @property {number} confidence
 * @property {string[]} evidence
 */

/**
 * @typedef {object} ExecutionContract
 * @property {ObjectiveResolution} objectiveResolution
 * @property {object} executionPolicy
 * @property {object} reasoningPolicy
 * @property {object} conversationPolicy
 * @property {string[]} requiredCapabilities
 */

/**
 * Classify one segment into candidate primary objectives.
 * @param {string} segment
 * @param {number} segmentIndex
 * @param {object} [input]
 * @returns {CandidateObjective[]}
 */
function extractSegmentCandidates(segment, segmentIndex, input = {}) {
  const q = normalizeText(segment);
  if (!q) return [];

  const candidates = [];

  if (isSessionInspectionQuestion(q)) {
    candidates.push({
      objective: PRIMARY_OBJECTIVES.SESSION_INSPECTION,
      confidence: 0.97,
      evidence: ['session_inspection'],
      segment: q,
      segmentIndex,
    });
    return candidates;
  }

  if (isExecutionInspectionQuestion(q)) {
    candidates.push({
      objective: PRIMARY_OBJECTIVES.EXECUTION_INSPECTION,
      confidence: 0.96,
      evidence: ['execution_inspection'],
      segment: q,
      segmentIndex,
    });
    return candidates;
  }

  if (
    isMissionCreationMessage(q) ||
    matchesAny(q, MISSION_CREATION_RES) ||
    detectAcquisitionObjective(q)
  ) {
    candidates.push({
      objective: PRIMARY_OBJECTIVES.MISSION_CREATION,
      confidence: 0.96,
      evidence: ['mission_creation'],
      segment: q,
      segmentIndex,
    });
  }

  const hasActiveMission = Boolean(
    input.hasActiveMission ||
      input.mission ||
      (input.session &&
        input.session.context &&
        (input.session.context.missionId || input.session.context.acquisitionMissionId))
  );

  if (
    (isMissionExecutionMessage(q, input) ||
      matchesAny(q, MISSION_EXECUTION_RES) ||
      (isMissionExecutionCommand(q) && hasActiveMission) ||
      (isApprovalMessage(q) && hasActiveMission)) &&
    !(isSessionConfigurationMessage(q) && !hasActiveMission)
  ) {
    candidates.push({
      objective: PRIMARY_OBJECTIVES.MISSION_EXECUTION,
      confidence: 0.94,
      evidence: ['mission_execution'],
      segment: q,
      segmentIndex,
    });
  }

  if (hasActiveMission && shouldInspectActiveMission(q, true)) {
    candidates.push({
      objective: PRIMARY_OBJECTIVES.MISSION_INSPECTION,
      confidence: 0.9,
      evidence: ['mission_inspection'],
      segment: q,
      segmentIndex,
    });
  }

  if (matchesAny(q, BUSINESS_DECISION_RES)) {
    candidates.push({
      objective: PRIMARY_OBJECTIVES.BUSINESS_DECISION,
      confidence: 0.88,
      evidence: ['business_decision'],
      segment: q,
      segmentIndex,
    });
  }

  if (matchesAny(q, BUSINESS_INTELLIGENCE_RES)) {
    candidates.push({
      objective: PRIMARY_OBJECTIVES.BUSINESS_INTELLIGENCE,
      confidence: 0.9,
      evidence: ['business_intelligence'],
      segment: q,
      segmentIndex,
    });
  }

  if (matchesAny(q, IDENTITY_RES)) {
    candidates.push({
      objective: PRIMARY_OBJECTIVES.IDENTITY,
      confidence: 0.95,
      evidence: ['identity'],
      segment: q,
      segmentIndex,
    });
  }

  const subject = detectConversationSubject(q);
  if (subject.subject === CONVERSATION_SUBJECTS.IDENTITY && !matchesAny(q, IDENTITY_RES)) {
    candidates.push({
      objective: PRIMARY_OBJECTIVES.IDENTITY,
      confidence: subject.confidence || 0.9,
      evidence: ['identity_subject'],
      segment: q,
      segmentIndex,
    });
  }

  if (isSessionConfigurationMessage(q)) {
    candidates.push({
      objective: PRIMARY_OBJECTIVES.WORKSPACE_OPERATION,
      confidence: 0.85,
      evidence: ['workspace_operation'],
      segment: q,
      segmentIndex,
    });
  }

  return candidates;
}

/**
 * @param {string} question
 * @param {object} [input]
 * @returns {CandidateObjective[]}
 */
function extractCandidateObjectives(question, input = {}) {
  const segments = splitMessageSegments(question);
  const all = [];
  segments.forEach((segment, segmentIndex) => {
    all.push(...extractSegmentCandidates(segment, segmentIndex, input));
  });
  if (!all.length && normalizeText(question)) {
    all.push(
      ...extractSegmentCandidates(question, 0, input)
    );
  }
  return all;
}

/**
 * Pick exactly one primary objective from candidates.
 * @param {CandidateObjective[]} candidates
 * @returns {CandidateObjective|null}
 */
function resolvePrimaryObjective(candidates) {
  if (!candidates.length) return null;

  const sorted = [...candidates].sort((a, b) => {
    const priority = comparePrimaryObjectivePriority(a.objective, b.objective);
    if (priority !== 0) return priority;
    return b.confidence - a.confidence;
  });

  return sorted[0];
}

/**
 * @param {CandidateObjective[]} candidates
 * @param {string} primaryObjective
 * @param {string} question
 * @returns {string[]}
 */
function resolveSupportingObjectives(candidates, primaryObjective, question) {
  const supporting = new Set();
  const q = normalizeText(question);

  for (const candidate of candidates) {
    if (candidate.objective === primaryObjective) continue;
    if (candidate.objective === PRIMARY_OBJECTIVES.WORKSPACE_OPERATION) {
      supporting.add(SUPPORTING_OBJECTIVES.SESSION_CONFIGURATION);
      continue;
    }
    if (candidate.objective === PRIMARY_OBJECTIVES.IDENTITY) {
      supporting.add(SUPPORTING_OBJECTIVES.IDENTITY);
      continue;
    }
  }

  if (matchesAny(q, REASONING_EXPLANATION_RES)) {
    supporting.add(SUPPORTING_OBJECTIVES.REASONING_EXPLANATION);
  }

  if (isSessionConfigurationMessage(q) && primaryObjective !== PRIMARY_OBJECTIVES.WORKSPACE_OPERATION) {
    supporting.add(SUPPORTING_OBJECTIVES.SESSION_CONFIGURATION);
  }

  return [...supporting];
}

/**
 * @param {string} question
 * @returns {string[]}
 */
function resolveExecutionModifiers(question) {
  const q = normalizeText(question);
  const modifiers = [];
  const signals = detectSessionDirectiveSignals(q);

  if (signals.executionPolicy === 'autonomous' || /\bexecute autonomously\b/i.test(q)) {
    modifiers.push(EXECUTION_MODIFIERS.AUTONOMOUS);
  }
  if (signals.executionPolicy === 'read_only' || /\bread[\s-]?only\b/i.test(q)) {
    modifiers.push(EXECUTION_MODIFIERS.READ_ONLY);
  }
  if (matchesAny(q, PAUSE_ON_APPROVAL_RES)) {
    modifiers.push(EXECUTION_MODIFIERS.PAUSE_ON_APPROVAL);
  }
  if (/\bsimulation\b/i.test(q)) {
    modifiers.push(EXECUTION_MODIFIERS.SIMULATION);
  }
  if (matchesAny(q, PRODUCTION_MISSION_RES) || /\bproduction\b/i.test(q)) {
    modifiers.push(EXECUTION_MODIFIERS.PRODUCTION);
  }
  if (/\bhuman[\s-]?in[\s-]?the[\s-]?loop\b/i.test(q)) {
    modifiers.push(EXECUTION_MODIFIERS.HUMAN_IN_THE_LOOP);
  }
  if (/\b(?:move )?fast\b/i.test(q) && !/\bfast(?:er)? than\b/i.test(q)) {
    modifiers.push(EXECUTION_MODIFIERS.FAST);
  }
  if (/\bconservative(?:ly)?\b/i.test(q)) {
    modifiers.push(EXECUTION_MODIFIERS.CONSERVATIVE);
  }

  return [...new Set(modifiers)];
}

/**
 * @param {string} question
 * @returns {string[]}
 */
function resolveConversationModifiers(question) {
  const q = normalizeText(question);
  const modifiers = [];
  const signals = detectSessionDirectiveSignals(q);

  if (
    /\bexplain (?:your )?reasoning naturally\b/i.test(q) ||
    (/\bnaturally\b/i.test(q) && /\breasoning\b/i.test(q))
  ) {
    modifiers.push(CONVERSATION_MODIFIERS.NATURAL_REASONING);
    modifiers.push(CONVERSATION_MODIFIERS.SHOW_REASONING);
  } else if (matchesAny(q, REASONING_EXPLANATION_RES)) {
    modifiers.push(CONVERSATION_MODIFIERS.SHOW_REASONING);
  }

  if (signals.conversationStyle === 'concise' || /\bbe concise\b/i.test(q)) {
    modifiers.push(CONVERSATION_MODIFIERS.CONCISE);
  }
  if (signals.conversationStyle === 'natural' || /\bnaturally\b/i.test(q)) {
    modifiers.push(CONVERSATION_MODIFIERS.NATURAL);
  }
  if (/\b(?:be )?verbose\b/i.test(q)) {
    modifiers.push(CONVERSATION_MODIFIERS.VERBOSE);
  }
  if (/\bstep[\s-]?by[\s-]?step\b/i.test(q)) {
    modifiers.push(CONVERSATION_MODIFIERS.STEP_BY_STEP);
  }
  if (signals.reasoningMode === 'teaching' || /\bteaching mode\b/i.test(q)) {
    modifiers.push(CONVERSATION_MODIFIERS.TEACHING_MODE);
  }

  return [...new Set(modifiers)];
}

/**
 * @param {string} question
 * @param {string} primaryObjective
 * @returns {string[]}
 */
function resolveRequiredCapabilities(question, primaryObjective) {
  const q = normalizeText(question);
  const capabilities = [];

  for (const [cap, patterns] of Object.entries(CAPABILITY_RES)) {
    if (matchesAny(q, patterns)) {
      capabilities.push(cap);
    }
  }

  if (
    primaryObjective === PRIMARY_OBJECTIVES.MISSION_CREATION &&
    (matchesAny(q, PRODUCTION_MISSION_RES) || /\bproduction\b/i.test(q))
  ) {
    capabilities.push(
      REQUIRED_CAPABILITIES.SCOUT_INTELLIGENCE,
      REQUIRED_CAPABILITIES.OPPORTUNITY_INTELLIGENCE,
      REQUIRED_CAPABILITIES.OUTCOME_LEARNING
    );
  }

  return [...new Set(capabilities)];
}

/**
 * Resolve operator message into structured objective resolution.
 * @param {object} input
 * @param {string} input.question
 * @param {object} [input.session]
 * @param {boolean} [input.hasActiveMission]
 * @param {object} [input.mission]
 * @returns {ObjectiveResolution}
 */
function resolveOperatorObjective(input = {}) {
  const question = String(input.question || '');
  const q = normalizeText(question);

  if (!q) {
    return {
      primaryObjective: PRIMARY_OBJECTIVES.GENERAL_CONVERSATION,
      supportingObjectives: [],
      executionModifiers: [],
      conversationModifiers: [],
      requiredCapabilities: [],
      routingDecision: resolveRoutingDecision(PRIMARY_OBJECTIVES.GENERAL_CONVERSATION),
      confidence: 0.4,
      evidence: ['empty_message'],
    };
  }

  const candidates = extractCandidateObjectives(question, input);
  let primary = resolvePrimaryObjective(candidates);

  if (!primary) {
    if (isSessionResetRequest(q) || isSessionConfigurationMessage(q)) {
      primary = {
        objective: PRIMARY_OBJECTIVES.WORKSPACE_OPERATION,
        confidence: 0.85,
        evidence: ['workspace_operation_fallback'],
        segment: q,
        segmentIndex: 0,
      };
    } else {
      primary = {
        objective: PRIMARY_OBJECTIVES.GENERAL_CONVERSATION,
        confidence: 0.5,
        evidence: ['unclassified'],
        segment: q,
        segmentIndex: 0,
      };
    }
  }

  const executionModifiers = resolveExecutionModifiers(question);
  const conversationModifiers = resolveConversationModifiers(question);
  const supportingObjectives = resolveSupportingObjectives(
    candidates,
    primary.objective,
    question
  );
  const requiredCapabilities = resolveRequiredCapabilities(question, primary.objective);
  const routingDecision = resolveRoutingDecision(primary.objective);

  const evidence = [...primary.evidence];
  if (executionModifiers.length) evidence.push('execution_modifiers');
  if (conversationModifiers.length) evidence.push('conversation_modifiers');
  if (supportingObjectives.length) evidence.push('supporting_objectives');

  return {
    primaryObjective: primary.objective,
    supportingObjectives,
    executionModifiers,
    conversationModifiers,
    requiredCapabilities,
    routingDecision,
    confidence: primary.confidence,
    evidence,
  };
}

/**
 * Full resolution pipeline — returns ExecutionContract and canonical ResolvedObjective (SPEC-168).
 * @param {object} input
 * @returns {{ objectiveResolution: ObjectiveResolution, executionContract: ExecutionContract, resolvedObjective: import('./ResolvedObjective').ResolvedObjective|null }}
 */
function resolveExecutionContract(input = {}) {
  askPathTrace.traceEnter('resolveExecutionContract');
  const objectiveResolution = resolveOperatorObjective(input);
  const executionContract = buildExecutionContract(objectiveResolution);
  const resolvedObjective = resolveCanonicalObjective({
    question: input.question,
    objectiveResolution,
    executionContract,
    context: input.context || (input.session && input.session.context) || null,
    targetSegment: input.targetSegment,
    resolutions: input.resolutions,
  });

  askPathTrace.traceBranch('objective_resolution', {
    primaryObjective: objectiveResolution.primaryObjective,
    supportingObjectives: objectiveResolution.supportingObjectives,
    executionModifiers: objectiveResolution.executionModifiers,
    conversationModifiers: objectiveResolution.conversationModifiers,
    routingOwner: objectiveResolution.routingDecision.owner,
    confidence: objectiveResolution.confidence,
    canonicalObjective: resolvedObjective.objective || null,
    canonicalReady: resolvedObjective.ready,
  });

  askPathTrace.traceEarlyReturn(
    'resolveExecutionContract',
    objectiveResolution.primaryObjective
  );

  return { objectiveResolution, executionContract, resolvedObjective };
}

function primaryObjectiveBypassesReasoning(primaryObjective) {
  return (
    primaryObjective === PRIMARY_OBJECTIVES.WORKSPACE_OPERATION ||
    primaryObjective === PRIMARY_OBJECTIVES.SESSION_INSPECTION
  );
}

function primaryObjectiveBypassesOwnership(primaryObjective) {
  return (
    primaryObjective === PRIMARY_OBJECTIVES.WORKSPACE_OPERATION ||
    primaryObjective === PRIMARY_OBJECTIVES.SESSION_INSPECTION ||
    primaryObjective === PRIMARY_OBJECTIVES.EXECUTION_INSPECTION
  );
}

function mapPrimaryObjectiveToMessageType(primaryObjective) {
  const { MESSAGE_TYPES } = require('./MessageType');
  switch (primaryObjective) {
    case PRIMARY_OBJECTIVES.MISSION_CREATION:
      return MESSAGE_TYPES.MISSION_CREATION;
    case PRIMARY_OBJECTIVES.MISSION_EXECUTION:
      return MESSAGE_TYPES.MISSION_EXECUTION;
    case PRIMARY_OBJECTIVES.WORKSPACE_OPERATION:
      return MESSAGE_TYPES.SESSION_CONFIGURATION;
    case PRIMARY_OBJECTIVES.SESSION_INSPECTION:
      return MESSAGE_TYPES.SESSION_INSPECTION;
    case PRIMARY_OBJECTIVES.IDENTITY:
      return MESSAGE_TYPES.QUESTION;
    default:
      return null;
  }
}

module.exports = {
  resolveOperatorObjective,
  resolveExecutionContract,
  extractCandidateObjectives,
  extractSegmentCandidates,
  resolvePrimaryObjective,
  resolveSupportingObjectives,
  resolveExecutionModifiers,
  resolveConversationModifiers,
  resolveRequiredCapabilities,
  resolveRoutingDecision: require('./ObjectiveRoutingMap').resolveRoutingDecision,
  primaryObjectiveBypassesReasoning,
  primaryObjectiveBypassesOwnership,
  mapPrimaryObjectiveToMessageType,
  MISSION_CREATION_RES,
  BUSINESS_INTELLIGENCE_RES,
  BUSINESS_DECISION_RES,
};
