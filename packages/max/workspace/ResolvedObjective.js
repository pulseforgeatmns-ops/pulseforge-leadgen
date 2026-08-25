'use strict';

/**
 * SPEC-168 — Canonical Objective Resolution (ADR-087 extension).
 *
 * One operator message resolves to exactly one canonical business objective.
 * Downstream systems consume ResolvedObjective — they never reparse the prompt.
 */

const { normalizeText } = require('./SessionState');
const { splitMessageSegments } = require('./MessageTypeClassifier');
const { detectAcquisitionObjective } = require('./AcquisitionObjectiveDetection');
const { buildExecutionContract } = require('./ExecutionContract');
const { EXECUTION_MODIFIERS, CONVERSATION_MODIFIERS } = require('./PrimaryObjective');
const { EXECUTION_POLICIES } = require('./SessionState');
const { MISSION_TYPES } = require('../../acquisition-mission/StructuredMission');
const {
  analyzeIntent,
  inferSegmentKey,
  inferConstraints,
  expandGeography,
  cleanObjective,
  detectAmbiguities,
  segmentMeta,
  extractCountObjective,
  applyResolutions,
} = require('../../acquisition-mission/MissionPlanner');
const { extractGeography, inferTargetSegmentFromObjective } = require('../../acquisition-mission/MissionNaming');
const { asText } = require('../../acquisition-mission/types');

const MISSION_COMMAND_RES = [
  /\bcreate (?:a )?(?:new )?(?:production )?(?:acquisition )?mission\b/i,
  /\bstart (?:a )?(?:new )?(?:acquisition )?mission\b/i,
  /\bbegin (?:a )?(?:new )?(?:acquisition )?mission\b/i,
  /\b(?:launch|open|kick off)\s+(?:a )?(?:new )?(?:acquisition )?mission\b/i,
  /\bnew acquisition mission\b/i,
  /\bproduction acquisition mission\b/i,
  /\bproduction mission\b/i,
];

const EXECUTION_POLICY_LINE_RES = [
  /\bexecute autonomously\b/i,
  /\boperate autonomously\b/i,
  /\bread[\s-]?only\b/i,
  /\bpause only (?:for|when) operator (?:judgment|approval)\b/i,
  /\bpause on approval\b/i,
  /\bwait for (?:my )?approval\b/i,
  /\boperator approval required\b/i,
  /\bhuman[\s-]?in[\s-]?the[\s-]?loop\b/i,
  /\bsimulation\b/i,
];

const COMMUNICATION_POLICY_LINE_RES = [
  /\bexplain (?:your )?reasoning naturally\b/i,
  /\bexplain (?:your )?reasoning\b/i,
  /\bshow (?:your )?reasoning\b/i,
  /\bexplain reasoning only when material\b/i,
  /\breasoning only when material\b/i,
  /\bbe concise\b/i,
  /\b(?:be )?verbose\b/i,
  /\bstep[\s-]?by[\s-]?step\b/i,
  /\bteaching mode\b/i,
];

const OBJECTIVE_PREFIX_RE = /^(?:objective|goal|target)\s*:\s*/i;

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

function classifyLine(line) {
  const text = normalizeText(line);
  if (!text) return 'ignored';
  if (matchesAny(text, MISSION_COMMAND_RES)) return 'policy';
  if (matchesAny(text, EXECUTION_POLICY_LINE_RES)) return 'policy';
  if (matchesAny(text, COMMUNICATION_POLICY_LINE_RES)) return 'policy';
  if (OBJECTIVE_PREFIX_RE.test(text)) return 'objective';
  if (detectAcquisitionObjective(text)) return 'objective';
  if (/^acquire\s+/i.test(text)) return 'objective';
  if (/\b(?:acquire|win|land|sign|close|get)\s+(?:one|a|an|\d+)/i.test(text)) return 'objective';
  if (/\b(?:recurring|commercial cleaning|short[- ]term rental|str operator)/i.test(text)) return 'objective';
  if (/\bfind\b/i.test(text)) return 'objective';
  if (/\b(?:property managers?|operators?|clients?|customers?|firms?|law firms?)\b/i.test(text)) {
    return 'objective';
  }
  return 'ignored';
}

function classifyMessageLines(question) {
  const segments = splitMessageSegments(question);
  const objectiveLines = [];
  const policyLines = [];
  const ignoredLines = [];

  for (const segment of segments) {
    const kind = classifyLine(segment);
    if (kind === 'objective') {
      objectiveLines.push(segment.replace(OBJECTIVE_PREFIX_RE, '').trim());
    } else if (kind === 'policy') {
      policyLines.push(segment);
    } else {
      ignoredLines.push(segment);
    }
  }

  return { objectiveLines, policyLines, ignoredLines, segments };
}

function inferSubtype(text, segmentKey) {
  const hay = asText(text).toLowerCase();
  if (/\bcommercial cleaning\b/.test(hay) || segmentKey === 'short_term_rental') {
    return 'commercial_cleaning';
  }
  if (/\blaw firm\b/.test(hay) || segmentKey === 'law_firm') return 'law_firm';
  if (/\baccounting\b/.test(hay) || segmentKey === 'accounting') return 'accounting';
  return segmentKey || null;
}

function buildExecutionPolicy(executionContract) {
  const contract = executionContract || {};
  const policy = contract.executionPolicy || {};
  const modifiers = policy.modifiers || [];
  let autonomy = 'normal';
  if (policy.executionPolicy === EXECUTION_POLICIES.AUTONOMOUS) autonomy = 'autonomous';
  else if (policy.executionPolicy === EXECUTION_POLICIES.READ_ONLY) autonomy = 'read_only';
  else if (policy.executionPolicy === EXECUTION_POLICIES.OPERATOR_APPROVAL_REQUIRED) {
    autonomy = 'operator_approval_required';
  }

  let environment = 'standard';
  if (modifiers.includes(EXECUTION_MODIFIERS.PRODUCTION)) environment = 'production';
  if (modifiers.includes(EXECUTION_MODIFIERS.SIMULATION)) environment = 'simulation';

  return { autonomy, environment, modifiers };
}

function buildCommunicationPolicy(executionContract) {
  const contract = executionContract || {};
  const conversation = contract.conversationPolicy || {};
  const modifiers = conversation.modifiers || [];
  let reasoning = 'default';
  if (modifiers.includes(CONVERSATION_MODIFIERS.NATURAL_REASONING)) reasoning = 'natural';
  else if (modifiers.includes(CONVERSATION_MODIFIERS.SHOW_REASONING)) reasoning = 'visible';
  if (/\bmaterial only\b/i.test(JSON.stringify(contract))) reasoning = 'material_only';
  return {
    reasoning,
    conversationStyle: conversation.conversationStyle || null,
    modifiers,
  };
}

function buildEvaluationPolicy(executionContract, objectiveResolution) {
  const primary = objectiveResolution && objectiveResolution.primaryObjective;
  const executiveBehavior =
    primary === 'mission_creation' ||
    primary === 'mission_execution' ||
    primary === 'business_decision';
  return { executiveBehavior };
}

function isBareManchester(text, regionText) {
  const hay = `${asText(regionText)} ${asText(text)}`.toLowerCase();
  if (!/\bmanchester\b/.test(hay)) return false;
  if (/manchester\s+nh|manchester,\s*nh|greater\s+manchester|new hampshire|\bnh\b/.test(hay)) {
    return false;
  }
  if (/manchester\s+uk|manchester,\s*uk|\buk\b|england|united kingdom/.test(hay)) {
    return false;
  }
  return true;
}

function applyContextPrecedence(extracted, context = {}) {
  const next = { ...extracted, geography: { ...(extracted.geography || {}) } };
  if (next.geography.region) return next;

  const { pickByPrecedence } = require('../../acquisition-mission/ContextPrecedence');
  const safeContext = context || {};
  const blueprint = (safeContext && safeContext.blueprint) || {};
  const raw = blueprint.geography || blueprint.region || blueprint.targetMarkets || null;
  let blueprintGeo = null;
  if (raw) {
    blueprintGeo = typeof raw === 'object'
      ? expandGeography(raw.region || '', JSON.stringify(raw))
      : expandGeography(String(raw), String(raw));
  }
  const workspaceGeo = safeContext.workspace && (safeContext.workspace.geography || safeContext.workspace.region);
  const picked = pickByPrecedence([
    blueprintGeo && blueprintGeo.region ? { source: 'blueprint', value: blueprintGeo } : null,
    workspaceGeo
      ? { source: 'workspace', value: typeof workspaceGeo === 'object' ? workspaceGeo : expandGeography(workspaceGeo, workspaceGeo) }
      : null,
  ]);
  if (picked && picked.value) {
    next.contextGeography = { ...picked.value, source: picked.source };
  }
  return next;
}

function inferEvidence(text, opts = {}) {
  const hay = asText(text).toLowerCase();
  if (opts.evidence) return opts.evidence;
  if (/\bhigh(?:er)? confidence\b|\bstrong evidence\b/.test(hay)) {
    return { minimumConfidence: 0.85, minimumBuyingSignals: 3, thresholdLabel: 'high' };
  }
  if (/\blow(?:er)? confidence\b|\blight evidence\b/.test(hay)) {
    return { minimumConfidence: 0.5, minimumBuyingSignals: 1, thresholdLabel: 'low' };
  }
  return { minimumConfidence: 0.7, minimumBuyingSignals: 2, thresholdLabel: 'medium' };
}

function buildSemanticAmbiguities(extracted, text, opts = {}) {
  return detectAmbiguities(extracted, text, opts);
}

function normalizeBusinessObjective(text) {
  let objective = asText(text);
  objective = objective.replace(/\s+/g, ' ').trim();
  if (!objective.endsWith('.')) objective += '.';
  return objective;
}

function buildMissingObjectiveAmbiguity() {
  return {
    field: 'objective',
    question: 'What is the business objective for this mission?',
    choices: [],
    reason: 'Mission creation requires a canonical business objective separate from execution instructions.',
  };
}

/**
 * @typedef {object} ResolvedObjective
 * @property {string} objective
 * @property {string} missionType
 * @property {string|null} subtype
 * @property {string|null} market
 * @property {object} geography
 * @property {object} successCriteria
 * @property {object} executionPolicy
 * @property {object} communicationPolicy
 * @property {object} evaluationPolicy
 * @property {object} extractedFrom
 * @property {object[]} [ambiguities]
 * @property {boolean} ready
 */

/**
 * Resolve operator message into canonical business objective (SPEC-168).
 * @param {object} input
 * @param {string} input.question
 * @param {object} [input.executionContract]
 * @param {object} [input.objectiveResolution]
 * @param {object} [input.context]
 * @param {object} [input.resolutions]
 * @param {string} [input.targetSegment]
 * @returns {ResolvedObjective}
 */
function resolveCanonicalObjective(input = {}) {
  const question = String(input.question || '');
  const executionContract =
    input.executionContract ||
    (input.objectiveResolution ? buildExecutionContract(input.objectiveResolution) : null);
  const objectiveResolution = input.objectiveResolution ||
    (executionContract && executionContract.objectiveResolution) ||
    null;

  const { objectiveLines, policyLines, ignoredLines } = classifyMessageLines(question);
  let businessText = objectiveLines.join(' ').trim();
  if (!businessText && detectAcquisitionObjective(question)) {
    businessText = normalizeText(question);
  }

  const executionPolicy = buildExecutionPolicy(executionContract);
  const communicationPolicy = buildCommunicationPolicy(executionContract);
  const evaluationPolicy = buildEvaluationPolicy(executionContract, objectiveResolution);

  if (!businessText) {
    return {
      objective: '',
      missionType: MISSION_TYPES.ACQUISITION,
      subtype: null,
      market: null,
      geography: { region: null, cities: [] },
      successCriteria: { recurringClients: 1 },
      executionPolicy,
      communicationPolicy,
      evaluationPolicy,
      extractedFrom: { objectiveLines, policyLines, ignoredLines },
      ambiguities: [buildMissingObjectiveAmbiguity()],
      ready: false,
    };
  }

  const text = businessText;
  const intent = analyzeIntent(text, { missionType: input.missionType || input.type });
  const segmentLabel = asText(input.targetSegment) || inferTargetSegmentFromObjective(text);
  let segmentKey = inferSegmentKey(text, segmentLabel);
  const geographyMention = extractGeography(text) || asText(input.geography) || '';
  let extracted = {
    intent,
    segmentKey,
    segmentLabel,
    geography: expandGeography(geographyMention, text),
    constraints: [
      ...inferConstraints(text),
      ...(Array.isArray(input.constraints) ? input.constraints.map(asText).filter(Boolean) : []),
    ],
  };

  if (input.resolutions) {
    extracted = applyResolutions(extracted, input.resolutions);
  }

  extracted = applyContextPrecedence(extracted, input.context || {});

  const bareManchester = isBareManchester(text, extracted.geography && extracted.geography.region);
  if (bareManchester && !(input.resolutions && input.resolutions.geography)) {
    extracted.geography = { region: null, cities: [], mention: 'Manchester' };
  } else if (extracted.contextGeography && extracted.contextGeography.region) {
    extracted.geography = extracted.contextGeography;
    extracted.geographySource = extracted.contextGeography.source || 'blueprint';
  }

  const ambiguities = buildSemanticAmbiguities(extracted, text, {
    context: input.context,
    resolutions: input.resolutions,
  });

  const market = segmentMeta(extracted.segmentKey, extracted.segmentLabel);
  const evidence = inferEvidence(text, input);
  const successTarget = extractCountObjective(text);
  const successType = /recurr/i.test(text) ? 'recurring_clients' : 'customers';

  return {
    objective: normalizeBusinessObjective(text),
    missionType: intent.type || MISSION_TYPES.ACQUISITION,
    subtype: inferSubtype(text, extracted.segmentKey),
    market: extracted.segmentKey || null,
    geography: extracted.geography || { region: null, cities: [] },
    successCriteria: {
      recurringClients: successType === 'recurring_clients' ? successTarget : 0,
      customers: successTarget,
      type: successType,
      target: successTarget,
    },
    constraints: [...new Set(extracted.constraints || [])],
    marketMeta: market,
    evidence,
    executionPolicy,
    communicationPolicy,
    evaluationPolicy,
    extractedFrom: { objectiveLines, policyLines, ignoredLines },
    ambiguities,
    ready: ambiguities.length === 0 && Boolean(normalizeBusinessObjective(text)),
    intent,
    segmentKey: extracted.segmentKey,
    segmentLabel: extracted.segmentLabel,
    geographySource: extracted.geographySource || 'operator',
    provenanceSource: text,
  };
}

function canonicalObjectiveText(resolvedObjective) {
  if (!resolvedObjective) return '';
  return asText(resolvedObjective.objective);
}

module.exports = {
  resolveCanonicalObjective,
  canonicalObjectiveText,
  classifyMessageLines,
  classifyLine,
  buildExecutionPolicy,
  buildCommunicationPolicy,
  buildEvaluationPolicy,
};
