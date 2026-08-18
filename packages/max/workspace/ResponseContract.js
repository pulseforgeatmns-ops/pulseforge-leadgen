'use strict';

/**
 * SPEC-109 / SPEC-111 — Intent-bound response contracts.
 *
 * Operator intent selects the analysis mode, which selects the response
 * structure, before retrieval, grounding, reasoning, or specialist
 * delegation. Evidence fills content. Reasoning may produce a
 * recommendation only when the selected contract allows it.
 *
 * SPEC-110 adds Business Intelligence as a required first section on
 * retrieval, summary, and recommendation contracts. Intelligence is a
 * first-class object; evidence remains attributable and listed after.
 *
 * SPEC-111 adds Diagnosis, Unknown Analysis, Risk, and Progress contracts.
 * Those consume existing BI objects instead of duplicating reasoning.
 *
 * Advice is not a universal response type.
 */

const {
  COGNITIVE_MODES,
  classifyCognitiveMode,
  looksLikeClaimChallenge,
  looksLikeInvestigation,
  looksLikeSummary,
  looksLikeCompletedRetrieval,
} = require('../specialistDelegation/CognitiveMode');
const {
  OPERATOR_INTENTS,
  looksLikeDiagnosis,
  looksLikeUnknownAnalysis,
  looksLikeRisk,
  looksLikeProgress,
} = require('./OperatorIntentRegistry');

const CONTRACT_IDS = Object.freeze({
  RETRIEVAL: 'retrieval',
  SUMMARY: 'summary',
  RECOMMENDATION: 'recommendation',
  DIAGNOSIS: 'diagnosis',
  UNKNOWN_ANALYSIS: 'unknown_analysis',
  RISK: 'risk',
  PROGRESS: 'progress',
  CHALLENGE: 'challenge',
  INVESTIGATION: 'investigation',
});

const SECTION = Object.freeze({
  BUSINESS_INTELLIGENCE: 'business_intelligence',
  VERIFIED_STATE: 'verified_state',
  UNKNOWNS: 'unknowns',
  EVIDENCE: 'evidence',
  OBSERVED_STATE: 'observed_state',
  GOALS: 'goals',
  RECOMMENDATION: 'recommendation',
  CURRENT_STATE: 'current_state',
  REASONING: 'reasoning',
  CONFIDENCE: 'confidence',
  CLAIM_IDENTIFIED: 'claim_identified',
  EVIDENCE_REVIEWED: 'evidence_reviewed',
  REVISION: 'revision',
  UPDATED_RECOMMENDATION: 'updated_recommendation',
  KNOWN: 'known',
  NEED_SPECIALIST: 'need_specialist',
  EXPECTED_OUTPUTS: 'expected_outputs',
  UNSOLICITED_STRATEGY: 'unsolicited_strategy',
  ACQUISITION_RECOMMENDATION: 'acquisition_recommendation',
  UNSUPPORTED_MEMORY_ANSWER: 'unsupported_memory_answer',
  GENERIC_BLUEPRINT_STRATEGY: 'generic_blueprint_strategy',
  SPECULATION: 'speculation',
  BOTTLENECK: 'bottleneck',
  OPERATOR_IMPACT: 'operator_impact',
  EVIDENCE_GAPS: 'evidence_gaps',
  SUGGESTED_INVESTIGATIONS: 'suggested_investigations',
  RISKS: 'risks',
  POTENTIAL_IMPACT: 'potential_impact',
  PROGRESS: 'progress',
  REMAINING_WORK: 'remaining_work',
});

const UNSOLICITED_STRATEGY_RE =
  /\bI(?:'d| would) (?:recommend|start by) (?:proving|building|a focused first campaign|a repeatable)/i;

const ACQUISITION_RECOMMENDATION_RE =
  /\bI(?:'d| would) recommend (?:a focused first campaign|proving a repeatable(?: commercial)? acquisition motion)/i;

const GENERIC_BLUEPRINT_STRATEGY_RE =
  /\bI(?:'d| would) (?:recommend|start by) (?:proving|building) (?:a focused first campaign|a repeatable(?: commercial)? acquisition)/i;

const SPECULATION_RE =
  /\b(probably working|likely converting|acquisition rumors?|I believe the market|should be performing|must be converting)\b/i;

function freezeContract(contract) {
  return Object.freeze({
    ...contract,
    required: Object.freeze([...(contract.required || [])]),
    optional: Object.freeze([...(contract.optional || [])]),
    forbidden: Object.freeze([...(contract.forbidden || [])]),
  });
}

const RetrievalContract = freezeContract({
  id: CONTRACT_IDS.RETRIEVAL,
  label: 'Retrieval',
  required: [SECTION.BUSINESS_INTELLIGENCE, SECTION.VERIFIED_STATE, SECTION.UNKNOWNS],
  optional: [SECTION.EVIDENCE],
  forbidden: [SECTION.UNSOLICITED_STRATEGY, SECTION.ACQUISITION_RECOMMENDATION],
  permitsRecommendation: false,
  recommendationPrimary: false,
  forbidsCieClaim: false,
  forbidsSpecialistDelegation: true,
});

const SummaryContract = freezeContract({
  id: CONTRACT_IDS.SUMMARY,
  label: 'Summary',
  required: [SECTION.BUSINESS_INTELLIGENCE, SECTION.OBSERVED_STATE, SECTION.GOALS, SECTION.UNKNOWNS],
  optional: [SECTION.RECOMMENDATION, SECTION.EVIDENCE],
  forbidden: [],
  permitsRecommendation: true,
  recommendationPrimary: false,
  forbidsCieClaim: true,
  forbidsSpecialistDelegation: true,
});

const RecommendationContract = freezeContract({
  id: CONTRACT_IDS.RECOMMENDATION,
  label: 'Recommendation',
  required: [
    SECTION.BUSINESS_INTELLIGENCE,
    SECTION.CURRENT_STATE,
    SECTION.REASONING,
    SECTION.RECOMMENDATION,
    SECTION.CONFIDENCE,
    SECTION.EVIDENCE,
  ],
  optional: [],
  forbidden: [],
  permitsRecommendation: true,
  recommendationPrimary: true,
  forbidsCieClaim: true,
  forbidsSpecialistDelegation: false,
});

const ChallengeContract = freezeContract({
  id: CONTRACT_IDS.CHALLENGE,
  label: 'Challenge',
  required: [
    SECTION.CLAIM_IDENTIFIED,
    SECTION.EVIDENCE_REVIEWED,
    SECTION.REVISION,
    SECTION.UPDATED_RECOMMENDATION,
  ],
  optional: [],
  forbidden: [],
  permitsRecommendation: true,
  recommendationPrimary: false,
  forbidsCieClaim: true,
  forbidsSpecialistDelegation: true,
});

const InvestigationContract = freezeContract({
  id: CONTRACT_IDS.INVESTIGATION,
  label: 'Investigation',
  required: [SECTION.KNOWN, SECTION.NEED_SPECIALIST, SECTION.EXPECTED_OUTPUTS],
  optional: [],
  forbidden: [SECTION.UNSUPPORTED_MEMORY_ANSWER],
  permitsRecommendation: false,
  recommendationPrimary: false,
  forbidsCieClaim: true,
  forbidsSpecialistDelegation: false,
});

const DiagnosisContract = freezeContract({
  id: CONTRACT_IDS.DIAGNOSIS,
  label: 'Diagnosis',
  required: [SECTION.BOTTLENECK, SECTION.CONFIDENCE, SECTION.EVIDENCE],
  optional: [SECTION.RECOMMENDATION, SECTION.OPERATOR_IMPACT],
  forbidden: [SECTION.GENERIC_BLUEPRINT_STRATEGY, SECTION.UNSOLICITED_STRATEGY],
  permitsRecommendation: true,
  recommendationPrimary: false,
  forbidsCieClaim: true,
  forbidsSpecialistDelegation: true,
  consumesIntelligence: Object.freeze(['bottleneck', 'readiness', 'momentum']),
});

const UnknownAnalysisContract = freezeContract({
  id: CONTRACT_IDS.UNKNOWN_ANALYSIS,
  label: 'Unknown Analysis',
  required: [SECTION.UNKNOWNS, SECTION.OPERATOR_IMPACT, SECTION.EVIDENCE_GAPS],
  optional: [SECTION.SUGGESTED_INVESTIGATIONS],
  forbidden: [SECTION.SPECULATION, SECTION.UNSOLICITED_STRATEGY, SECTION.ACQUISITION_RECOMMENDATION],
  permitsRecommendation: false,
  recommendationPrimary: false,
  forbidsCieClaim: true,
  forbidsSpecialistDelegation: true,
  consumesIntelligence: Object.freeze(['unknown']),
});

const RiskContract = freezeContract({
  id: CONTRACT_IDS.RISK,
  label: 'Risk Assessment',
  required: [SECTION.RISKS, SECTION.EVIDENCE, SECTION.CONFIDENCE, SECTION.POTENTIAL_IMPACT],
  optional: [],
  forbidden: [SECTION.SPECULATION, SECTION.GENERIC_BLUEPRINT_STRATEGY],
  permitsRecommendation: false,
  recommendationPrimary: false,
  forbidsCieClaim: true,
  forbidsSpecialistDelegation: true,
  consumesIntelligence: Object.freeze(['risk']),
});

const ProgressContract = freezeContract({
  id: CONTRACT_IDS.PROGRESS,
  label: 'Progress Review',
  required: [SECTION.PROGRESS, SECTION.REMAINING_WORK, SECTION.CONFIDENCE],
  optional: [SECTION.EVIDENCE],
  forbidden: [SECTION.GENERIC_BLUEPRINT_STRATEGY],
  permitsRecommendation: false,
  recommendationPrimary: false,
  forbidsCieClaim: true,
  forbidsSpecialistDelegation: true,
  consumesIntelligence: Object.freeze(['momentum', 'readiness', 'unknown']),
});

const CONTRACTS = Object.freeze({
  [CONTRACT_IDS.RETRIEVAL]: RetrievalContract,
  [CONTRACT_IDS.SUMMARY]: SummaryContract,
  [CONTRACT_IDS.RECOMMENDATION]: RecommendationContract,
  [CONTRACT_IDS.DIAGNOSIS]: DiagnosisContract,
  [CONTRACT_IDS.UNKNOWN_ANALYSIS]: UnknownAnalysisContract,
  [CONTRACT_IDS.RISK]: RiskContract,
  [CONTRACT_IDS.PROGRESS]: ProgressContract,
  [CONTRACT_IDS.CHALLENGE]: ChallengeContract,
  [CONTRACT_IDS.INVESTIGATION]: InvestigationContract,
});

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function getResponseContract(id) {
  return CONTRACTS[id] || null;
}

function listResponseContracts() {
  return [
    RetrievalContract,
    SummaryContract,
    RecommendationContract,
    DiagnosisContract,
    UnknownAnalysisContract,
    RiskContract,
    ProgressContract,
    ChallengeContract,
    InvestigationContract,
  ];
}

/**
 * Select the response contract from classified intent.
 * Runs after intent classification and before retrieval or delegation.
 *
 * @param {string} question
 * @param {object} [mode]
 * @returns {object|null}
 */
function selectResponseContract(question, mode) {
  const classified =
    mode && mode.kind
      ? mode
      : classifyCognitiveMode(question);
  const q = String(question || '').trim();

  if (!q) return RetrievalContract;

  if (classified.via === 'claim_challenge' || looksLikeClaimChallenge(q)) {
    return ChallengeContract;
  }

  if (
    classified.kind === COGNITIVE_MODES.INVESTIGATION ||
    classified.intent === OPERATOR_INTENTS.INVESTIGATION ||
    looksLikeInvestigation(q)
  ) {
    return InvestigationContract;
  }

  if (
    classified.kind === COGNITIVE_MODES.DIAGNOSIS ||
    classified.intent === OPERATOR_INTENTS.DIAGNOSIS ||
    looksLikeDiagnosis(q)
  ) {
    return DiagnosisContract;
  }

  if (
    classified.kind === COGNITIVE_MODES.UNKNOWN_ANALYSIS ||
    classified.intent === OPERATOR_INTENTS.UNKNOWN_ANALYSIS ||
    looksLikeUnknownAnalysis(q)
  ) {
    return UnknownAnalysisContract;
  }

  if (
    classified.kind === COGNITIVE_MODES.RISK ||
    classified.intent === OPERATOR_INTENTS.RISK ||
    looksLikeRisk(q)
  ) {
    return RiskContract;
  }

  if (
    classified.kind === COGNITIVE_MODES.PROGRESS ||
    classified.intent === OPERATOR_INTENTS.PROGRESS ||
    looksLikeProgress(q)
  ) {
    return ProgressContract;
  }

  if (classified.kind === COGNITIVE_MODES.RECOMMENDATION || classified.via === 'recommendation') {
    return RecommendationContract;
  }

  if (classified.via === 'summary' || classified.intent === OPERATOR_INTENTS.SUMMARY || looksLikeSummary(q)) {
    return SummaryContract;
  }

  if (
    classified.kind === COGNITIVE_MODES.RETRIEVAL ||
    classified.via === 'operating_evidence' ||
    classified.via === 'retrieval' ||
    looksLikeCompletedRetrieval(q)
  ) {
    return RetrievalContract;
  }

  if (classified.kind === COGNITIVE_MODES.EXPLANATION || classified.kind === COGNITIVE_MODES.REFLECTION) {
    return RetrievalContract;
  }

  if (classified.kind === COGNITIVE_MODES.EXECUTION) {
    return null;
  }

  // AUDIT-001 — unknown / planning intent fails toward Retrieval, never
  // Recommendation or a Blueprint Advisory fallback.
  return RetrievalContract;
}

function mayIncludeRecommendation(contract, extras = {}) {
  if (!contract) return extras.operatorAsked === true;
  if (extras.operatorAsked === true) return true;
  if (contract.id === CONTRACT_IDS.CHALLENGE) return true;
  if (contract.permitsRecommendation !== true) return false;
  if (contract.recommendationPrimary === true) return true;
  return extras.includeOptionalRecommendation === true || extras.recommend === true;
}

function heading(title, body) {
  const content = present(body);
  if (!content) return '';
  return `${title}\n${content}`;
}

function joinSections(parts) {
  return (parts || []).map((part) => present(part)).filter(Boolean).join('\n\n');
}

function firstRecommendationIndex(prose) {
  const text = String(prose || '');
  const match = text.match(
    /(?:^|\n)\s*(?:RECOMMENDATION(?:S)?:|I'd recommend|I would recommend)/i
  );
  return match ? match.index : -1;
}

function stripUnsolicitedAdvice(prose) {
  const text = String(prose || '');
  const idx = firstRecommendationIndex(text);
  if (idx < 0) return text.trim();
  return text.slice(0, idx).trim();
}

function containsForbidden(prose, contract) {
  if (!contract || !prose) return [];
  const hits = [];
  const forbidden = contract.forbidden || [];
  if (forbidden.includes(SECTION.UNSOLICITED_STRATEGY) && UNSOLICITED_STRATEGY_RE.test(prose)) {
    hits.push(SECTION.UNSOLICITED_STRATEGY);
  }
  if (
    forbidden.includes(SECTION.ACQUISITION_RECOMMENDATION) &&
    ACQUISITION_RECOMMENDATION_RE.test(prose)
  ) {
    hits.push(SECTION.ACQUISITION_RECOMMENDATION);
  }
  if (
    forbidden.includes(SECTION.UNSUPPORTED_MEMORY_ANSWER) &&
    /I'd start by proving a repeatable|I'd recommend a focused first campaign/i.test(prose) &&
    !/\bNeed specialist\??:/i.test(prose)
  ) {
    hits.push(SECTION.UNSUPPORTED_MEMORY_ANSWER);
  }
  if (
    forbidden.includes(SECTION.GENERIC_BLUEPRINT_STRATEGY) &&
    GENERIC_BLUEPRINT_STRATEGY_RE.test(prose)
  ) {
    hits.push(SECTION.GENERIC_BLUEPRINT_STRATEGY);
  }
  if (forbidden.includes(SECTION.SPECULATION) && SPECULATION_RE.test(prose)) {
    hits.push(SECTION.SPECULATION);
  }
  return hits;
}

function enforceContract(prose, contract, extras = {}) {
  let next = String(prose || '').trim();
  if (!contract) return next;
  if (!mayIncludeRecommendation(contract, extras)) {
    next = stripUnsolicitedAdvice(next);
  }
  return next;
}

function composeRetrievalProse(sections = {}, extras = {}) {
  const verifiedTitle = extras.completedRecently ? 'Recently completed' : 'What I can verify';
  const unknownTitle = extras.completedRecently ? 'Unknown' : 'What I cannot verify';
  const parts = [
    heading('Business Intelligence', sections.businessIntelligence),
    heading(verifiedTitle, sections.verifiedState || sections.observedState),
    heading(unknownTitle, sections.unknowns),
  ];
  if (sections.evidence) {
    parts.push(heading('Evidence', sections.evidence));
  }
  if (sections.nextInvestigation) {
    parts.push(heading('What I would need to investigate', sections.nextInvestigation));
  }
  if (mayIncludeRecommendation(RetrievalContract, extras) && sections.recommendation) {
    parts.push(heading('Recommendation', sections.recommendation));
  }
  return joinSections(parts);
}

function composeSummaryProse(sections = {}, extras = {}) {
  const parts = [
    heading('Business Intelligence', sections.businessIntelligence),
    heading('Observed operating state', sections.observedState || sections.verifiedState),
    heading('Goals', sections.goals),
    heading('Unknowns', sections.unknowns),
  ];
  if (mayIncludeRecommendation(SummaryContract, { ...extras, includeOptionalRecommendation: true }) && sections.recommendation) {
    parts.push(heading('Recommendation', sections.recommendation));
  }
  if (sections.evidence) {
    parts.push(heading('Evidence', sections.evidence));
  }
  return joinSections(parts);
}

function composeRecommendationProse(sections = {}, extras = {}) {
  const rec = present(sections.recommendation);
  const recLooksComplete = /^RECOMMENDATION\b/i.test(rec);
  const parts = [heading('Business Intelligence', sections.businessIntelligence)];
  if (recLooksComplete) {
    parts.push(rec);
    if (sections.currentState && !/\bCurrent state\b|WHAT'S ALREADY IN MOTION/i.test(rec)) {
      parts.push(heading('Current state', sections.currentState));
    }
    if (sections.confidence && !/\bConfidence\b/i.test(rec)) {
      parts.push(heading('Confidence', sections.confidence));
    }
  } else if (extras.recommendationPrimary === false) {
    parts.push(
      heading('Current state', sections.currentState),
      heading('Reasoning', sections.reasoning),
      heading('Recommendation', rec),
      heading('Confidence', sections.confidence)
    );
  } else {
    parts.push(
      heading('Recommendation', rec),
      heading('Current state', sections.currentState),
      heading('Reasoning', sections.reasoning),
      heading('Confidence', sections.confidence)
    );
  }
  if (sections.evidence && !/\nEvidence\n/i.test(rec)) {
    parts.push(heading('Supporting Evidence', sections.evidence));
  }
  return joinSections(parts);
}

function composeChallengeProse(sections = {}) {
  return joinSections([
    heading('Claim identified', sections.claimIdentified),
    heading('Evidence reviewed', sections.evidenceReviewed),
    heading('Revision', sections.revision),
    heading('Updated recommendation', sections.updatedRecommendation),
  ]);
}

function composeInvestigationProse(sections = {}) {
  return joinSections([
    heading('Known', sections.known),
    heading('Need specialist?', sections.needSpecialist),
    heading('Expected outputs', sections.expectedOutputs),
  ]);
}

function composeDiagnosisProse(sections = {}, extras = {}) {
  const parts = [
    heading('Current bottleneck', sections.bottleneck),
    heading('Supporting evidence', sections.evidence),
    heading('Confidence', sections.confidence),
    heading('Operator impact', sections.operatorImpact || sections.impact),
  ];
  if (mayIncludeRecommendation(DiagnosisContract, extras) && sections.recommendation) {
    parts.push(heading('Recommendation', sections.recommendation));
  }
  return joinSections(parts);
}

function composeUnknownAnalysisProse(sections = {}) {
  return joinSections([
    heading('Critical unknowns', sections.unknowns || sections.criticalUnknowns),
    heading('Evidence gaps', sections.evidenceGaps),
    heading('Why they matter', sections.operatorImpact || sections.impact),
    heading('Suggested investigations', sections.suggestedInvestigations),
  ]);
}

function composeRiskProse(sections = {}) {
  return joinSections([
    heading('Risks', sections.risks),
    heading('Evidence', sections.evidence),
    heading('Confidence', sections.confidence),
    heading('Potential impact', sections.potentialImpact || sections.operatorImpact || sections.impact),
  ]);
}

function composeProgressProse(sections = {}) {
  return joinSections([
    heading('Progress', sections.progress),
    heading('Remaining work', sections.remainingWork),
    heading('Confidence', sections.confidence),
    sections.evidence ? heading('Evidence', sections.evidence) : '',
  ]);
}

/**
 * Compose operator-facing prose from a selected contract and filled sections.
 * Answer first. Advise second — and only when permitted.
 */
function composeAccordingToContract(contract, sections = {}, extras = {}) {
  if (!contract) return present(sections.prose);
  let prose = '';
  if (contract.id === CONTRACT_IDS.RETRIEVAL) {
    prose = composeRetrievalProse(sections, extras);
  } else if (contract.id === CONTRACT_IDS.SUMMARY) {
    prose = composeSummaryProse(sections, extras);
  } else if (contract.id === CONTRACT_IDS.RECOMMENDATION) {
    prose = composeRecommendationProse(sections, extras);
  } else if (contract.id === CONTRACT_IDS.DIAGNOSIS) {
    prose = composeDiagnosisProse(sections, extras);
  } else if (contract.id === CONTRACT_IDS.UNKNOWN_ANALYSIS) {
    prose = composeUnknownAnalysisProse(sections);
  } else if (contract.id === CONTRACT_IDS.RISK) {
    prose = composeRiskProse(sections);
  } else if (contract.id === CONTRACT_IDS.PROGRESS) {
    prose = composeProgressProse(sections);
  } else if (contract.id === CONTRACT_IDS.CHALLENGE) {
    prose = composeChallengeProse(sections);
  } else if (contract.id === CONTRACT_IDS.INVESTIGATION) {
    prose = composeInvestigationProse(sections);
  } else if (sections.prose) {
    prose = present(sections.prose);
  }
  return enforceContract(prose, contract, extras);
}

function attachContractMetadata(structured, contract, extras = {}) {
  if (!structured || !contract) return structured;
  const metadata = structured.metadata && typeof structured.metadata === 'object'
    ? structured.metadata
    : {};
  metadata.responseContract = contract.id;
  metadata.intentBoundResponse = true;
  if (extras.analysisMode) {
    metadata.analysisMode = extras.analysisMode;
  }
  if (extras.intent) {
    metadata.operatorIntent = extras.intent;
  }
  if (extras.businessIntelligence) {
    metadata.businessIntelligence = extras.businessIntelligence;
  }
  structured.metadata = metadata;
  return structured;
}

module.exports = {
  CONTRACT_IDS,
  SECTION,
  CONTRACTS,
  RetrievalContract,
  SummaryContract,
  RecommendationContract,
  DiagnosisContract,
  UnknownAnalysisContract,
  RiskContract,
  ProgressContract,
  ChallengeContract,
  InvestigationContract,
  looksLikeSummary,
  looksLikeCompletedRetrieval,
  getResponseContract,
  listResponseContracts,
  selectResponseContract,
  mayIncludeRecommendation,
  composeAccordingToContract,
  composeRetrievalProse,
  composeSummaryProse,
  composeRecommendationProse,
  composeDiagnosisProse,
  composeUnknownAnalysisProse,
  composeRiskProse,
  composeProgressProse,
  composeChallengeProse,
  composeInvestigationProse,
  enforceContract,
  containsForbidden,
  stripUnsolicitedAdvice,
  attachContractMetadata,
};
