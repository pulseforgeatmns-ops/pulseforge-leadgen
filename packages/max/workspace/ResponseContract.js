'use strict';

/**
 * SPEC-109 — Intent-bound response contracts.
 *
 * Operator intent selects the response structure before retrieval, grounding,
 * reasoning, or specialist delegation. Evidence fills content. Reasoning may
 * produce a recommendation only when the selected contract allows it.
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

const CONTRACT_IDS = Object.freeze({
  RETRIEVAL: 'retrieval',
  SUMMARY: 'summary',
  RECOMMENDATION: 'recommendation',
  CHALLENGE: 'challenge',
  INVESTIGATION: 'investigation',
});

const SECTION = Object.freeze({
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
});

const UNSOLICITED_STRATEGY_RE =
  /\bI(?:'d| would) (?:recommend|start by) (?:proving|building|a focused first campaign|a repeatable)/i;

const ACQUISITION_RECOMMENDATION_RE =
  /\bI(?:'d| would) recommend (?:a focused first campaign|proving a repeatable(?: commercial)? acquisition motion)/i;

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
  required: [SECTION.VERIFIED_STATE, SECTION.UNKNOWNS],
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
  required: [SECTION.OBSERVED_STATE, SECTION.GOALS, SECTION.UNKNOWNS],
  optional: [SECTION.RECOMMENDATION],
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
  forbidsCieClaim: false,
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

const CONTRACTS = Object.freeze({
  [CONTRACT_IDS.RETRIEVAL]: RetrievalContract,
  [CONTRACT_IDS.SUMMARY]: SummaryContract,
  [CONTRACT_IDS.RECOMMENDATION]: RecommendationContract,
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

  if (classified.kind === COGNITIVE_MODES.INVESTIGATION || looksLikeInvestigation(q)) {
    return InvestigationContract;
  }

  if (classified.kind === COGNITIVE_MODES.RECOMMENDATION || classified.via === 'recommendation') {
    return RecommendationContract;
  }

  if (classified.via === 'summary' || looksLikeSummary(q)) {
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

  return null;
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
    heading('Observed operating state', sections.observedState || sections.verifiedState),
    heading('Goals', sections.goals),
    heading('Unknowns', sections.unknowns),
  ];
  if (mayIncludeRecommendation(SummaryContract, { ...extras, includeOptionalRecommendation: true }) && sections.recommendation) {
    parts.push(heading('Recommendation', sections.recommendation));
  }
  return joinSections(parts);
}

function composeRecommendationProse(sections = {}, extras = {}) {
  const rec = present(sections.recommendation);
  const parts = extras.recommendationPrimary === false
    ? [
        heading('Current state', sections.currentState),
        heading('Reasoning', sections.reasoning),
        heading('Recommendation', rec),
        heading('Confidence', sections.confidence),
        heading('Evidence', sections.evidence),
      ]
    : [
        heading('Recommendation', rec),
        heading('Current state', sections.currentState),
        heading('Reasoning', sections.reasoning),
        heading('Confidence', sections.confidence),
        heading('Evidence', sections.evidence),
      ];
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
  } else if (contract.id === CONTRACT_IDS.CHALLENGE) {
    prose = composeChallengeProse(sections);
  } else if (contract.id === CONTRACT_IDS.INVESTIGATION) {
    prose = composeInvestigationProse(sections);
  } else if (sections.prose) {
    prose = present(sections.prose);
  }
  return enforceContract(prose, contract, extras);
}

function attachContractMetadata(structured, contract) {
  if (!structured || !contract) return structured;
  const metadata = structured.metadata && typeof structured.metadata === 'object'
    ? structured.metadata
    : {};
  metadata.responseContract = contract.id;
  metadata.intentBoundResponse = true;
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
  composeChallengeProse,
  composeInvestigationProse,
  enforceContract,
  containsForbidden,
  stripUnsolicitedAdvice,
  attachContractMetadata,
};
