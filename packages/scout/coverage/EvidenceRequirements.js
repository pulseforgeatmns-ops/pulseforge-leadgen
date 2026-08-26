'use strict';

/**
 * SPEC-177 — Evidence Requirements.
 * Investigative questions declare the evidence they require.
 * Providers satisfy evidence — not questions.
 */

const INVESTIGATIVE_EVIDENCE = Object.freeze({
  IDENTITY: 'identity',
  PORTFOLIO: 'portfolio_evidence',
  DECISION_MAKERS: 'decision_makers',
  GROWTH: 'growth_signals',
  CLEANING: 'cleaning_signals',
  REVIEWS: 'reviews',
  LICENSING: 'licensing',
  SOCIAL: 'social',
  CONTACT: 'contact_path',
  BUYING: 'buying_signals',
});

const INVESTIGATIVE_QUESTIONS = Object.freeze({
  BUSINESS_EXISTS: 'business_exists',
  MANAGES_STRS: 'manages_strs',
  BUYING_DECISIONS: 'buying_decisions',
  GROWING: 'growing',
  OUTSOURCES_CLEANING: 'outsources_cleaning',
  VENDOR_INSTABILITY: 'vendor_instability',
  GEOGRAPHIC_FIT: 'geographic_fit',
  BUSINESS_FIT: 'business_fit',
});

/** Question → required evidence types. */
const QUESTION_TO_EVIDENCE = Object.freeze({
  [INVESTIGATIVE_QUESTIONS.BUSINESS_EXISTS]: [INVESTIGATIVE_EVIDENCE.IDENTITY],
  [INVESTIGATIVE_QUESTIONS.MANAGES_STRS]: [INVESTIGATIVE_EVIDENCE.PORTFOLIO],
  [INVESTIGATIVE_QUESTIONS.BUYING_DECISIONS]: [INVESTIGATIVE_EVIDENCE.DECISION_MAKERS],
  [INVESTIGATIVE_QUESTIONS.GROWING]: [INVESTIGATIVE_EVIDENCE.GROWTH],
  [INVESTIGATIVE_QUESTIONS.OUTSOURCES_CLEANING]: [INVESTIGATIVE_EVIDENCE.CLEANING],
  [INVESTIGATIVE_QUESTIONS.VENDOR_INSTABILITY]: [
    INVESTIGATIVE_EVIDENCE.REVIEWS,
    INVESTIGATIVE_EVIDENCE.GROWTH,
  ],
  [INVESTIGATIVE_QUESTIONS.GEOGRAPHIC_FIT]: [INVESTIGATIVE_EVIDENCE.IDENTITY],
  [INVESTIGATIVE_QUESTIONS.BUSINESS_FIT]: [
    INVESTIGATIVE_EVIDENCE.IDENTITY,
    INVESTIGATIVE_EVIDENCE.REVIEWS,
  ],
});

/** Evidence that would increase confidence when found. */
const CONFIDENCE_INCREASING = Object.freeze({
  [INVESTIGATIVE_EVIDENCE.IDENTITY]: [
    'place_id',
    'government_registry',
    'review_history',
    'phone',
  ],
  [INVESTIGATIVE_EVIDENCE.PORTFOLIO]: ['listing_count', 'property_count', 'registry_filings'],
  [INVESTIGATIVE_EVIDENCE.DECISION_MAKERS]: ['owner_title', 'operations_manager', 'linkedin_role'],
  [INVESTIGATIVE_EVIDENCE.GROWTH]: ['hiring', 'expansion', 'new_locations'],
  [INVESTIGATIVE_EVIDENCE.CLEANING]: ['vendor_mention', 'hiring_cleaners', 'turnover_pain'],
  [INVESTIGATIVE_EVIDENCE.REVIEWS]: ['positive_reviews', 'service_mentions'],
});

/** Evidence that would reduce confidence when found. */
const CONFIDENCE_DECREASING = Object.freeze({
  [INVESTIGATIVE_EVIDENCE.IDENTITY]: ['duplicate_entity', 'closed_business'],
  [INVESTIGATIVE_EVIDENCE.PORTFOLIO]: ['single_property_only', 'residential_only'],
  [INVESTIGATIVE_EVIDENCE.DECISION_MAKERS]: ['no_contact_path', 'corporate_only'],
  [INVESTIGATIVE_EVIDENCE.CLEANING]: ['in_house_cleaning_team', 'existing_vendor_locked'],
  [INVESTIGATIVE_EVIDENCE.GROWTH]: ['declining_reviews', 'closed_locations'],
});

/** Hypothesis gap → investigative questions. */
const GAP_TO_QUESTIONS = Object.freeze({
  geographic_fit: [INVESTIGATIVE_QUESTIONS.BUSINESS_EXISTS, INVESTIGATIVE_QUESTIONS.GEOGRAPHIC_FIT],
  business_fit: [INVESTIGATIVE_QUESTIONS.BUSINESS_EXISTS, INVESTIGATIVE_QUESTIONS.BUSINESS_FIT],
  decision_maker: [INVESTIGATIVE_QUESTIONS.BUYING_DECISIONS],
  portfolio_size: [INVESTIGATIVE_QUESTIONS.MANAGES_STRS],
  cleaning_responsibility: [INVESTIGATIVE_QUESTIONS.OUTSOURCES_CLEANING],
  buying_signals: [INVESTIGATIVE_QUESTIONS.GROWING, INVESTIGATIVE_QUESTIONS.VENDOR_INSTABILITY],
  contact_path: [INVESTIGATIVE_QUESTIONS.BUYING_DECISIONS],
  vendor_relationship: [INVESTIGATIVE_QUESTIONS.VENDOR_INSTABILITY],
  company_size: [INVESTIGATIVE_QUESTIONS.GROWING],
  ownership: [INVESTIGATIVE_QUESTIONS.BUYING_DECISIONS],
});

const DEFAULT_INVESTIGATION_QUESTIONS = Object.freeze([
  INVESTIGATIVE_QUESTIONS.BUSINESS_EXISTS,
  INVESTIGATIVE_QUESTIONS.BUSINESS_FIT,
  INVESTIGATIVE_QUESTIONS.BUYING_DECISIONS,
]);

function buildInvestigationQuestion(partial = {}) {
  return {
    id: partial.id || partial.question || `q-${Date.now()}`,
    question: partial.question || partial.id || '',
    text: partial.text || partial.label || '',
    requiredEvidence: Array.isArray(partial.requiredEvidence) ? partial.requiredEvidence : [],
    increasingEvidence: Array.isArray(partial.increasingEvidence) ? partial.increasingEvidence : [],
    decreasingEvidence: Array.isArray(partial.decreasingEvidence) ? partial.decreasingEvidence : [],
    hypothesisId: partial.hypothesisId || null,
    satisfied: partial.satisfied === true,
    confidence: partial.confidence != null ? Number(partial.confidence) : null,
  };
}

function buildEvidenceRequirement(partial = {}) {
  return {
    evidenceType: partial.evidenceType || partial.type || '',
    questionIds: Array.isArray(partial.questionIds) ? partial.questionIds : [],
    required: partial.required !== false,
    satisfied: partial.satisfied === true,
    confidence: partial.confidence != null ? Number(partial.confidence) : 0,
    sources: Array.isArray(partial.sources) ? partial.sources : [],
  };
}

/**
 * Derive investigative questions for a single hypothesis from its gap.
 * Generic fallback applies only when the hypothesis has no gap.
 * @param {object} hypothesis
 * @param {object} [marketDefinition]
 * @returns {object[]}
 */
function deriveQuestionsForHypothesis(hypothesis = {}, marketDefinition = {}) {
  const gapQuestions =
    hypothesis.gap && GAP_TO_QUESTIONS[hypothesis.gap]
      ? GAP_TO_QUESTIONS[hypothesis.gap]
      : hypothesis.gap
        ? []
        : DEFAULT_INVESTIGATION_QUESTIONS;

  return gapQuestions.map((qId) => {
    const requiredEvidence = QUESTION_TO_EVIDENCE[qId] || [];
    const increasingEvidence = requiredEvidence.flatMap((ev) => CONFIDENCE_INCREASING[ev] || []);
    const decreasingEvidence = requiredEvidence.flatMap((ev) => CONFIDENCE_DECREASING[ev] || []);

    return buildInvestigationQuestion({
      id: `${hypothesis.id}:${qId}`,
      question: qId,
      text: questionLabel(qId, hypothesis, marketDefinition),
      requiredEvidence,
      increasingEvidence,
      decreasingEvidence,
      hypothesisId: hypothesis.id,
    });
  });
}

/**
 * Derive investigative questions from business hypotheses.
 * @param {object[]} hypotheses
 * @param {object} [marketDefinition]
 * @returns {object[]}
 */
function deriveQuestionsFromHypotheses(hypotheses = [], marketDefinition = {}) {
  const questions = [];
  const seen = new Set();

  for (const hyp of hypotheses) {
    for (const question of deriveQuestionsForHypothesis(hyp, marketDefinition)) {
      const key = question.id;
      if (seen.has(key)) continue;
      seen.add(key);
      questions.push(question);
    }
  }

  // Identity is always the first question when not already present.
  if (!questions.some((q) => q.question === INVESTIGATIVE_QUESTIONS.BUSINESS_EXISTS)) {
    questions.unshift(
      buildInvestigationQuestion({
        id: 'identity:initial',
        question: INVESTIGATIVE_QUESTIONS.BUSINESS_EXISTS,
        text: 'Does this business exist?',
        requiredEvidence: [INVESTIGATIVE_EVIDENCE.IDENTITY],
        increasingEvidence: CONFIDENCE_INCREASING[INVESTIGATIVE_EVIDENCE.IDENTITY],
        decreasingEvidence: CONFIDENCE_DECREASING[INVESTIGATIVE_EVIDENCE.IDENTITY],
      })
    );
  }

  return questions;
}

function questionLabel(questionId, hypothesis = {}, marketDefinition = {}) {
  const labels = {
    [INVESTIGATIVE_QUESTIONS.BUSINESS_EXISTS]: 'Does this business exist?',
    [INVESTIGATIVE_QUESTIONS.MANAGES_STRS]: 'Does it manage STRs?',
    [INVESTIGATIVE_QUESTIONS.BUYING_DECISIONS]: 'Who makes buying decisions?',
    [INVESTIGATIVE_QUESTIONS.GROWING]: 'Are they growing?',
    [INVESTIGATIVE_QUESTIONS.OUTSOURCES_CLEANING]: 'Do they outsource cleaning?',
    [INVESTIGATIVE_QUESTIONS.VENDOR_INSTABILITY]: 'Is there vendor instability?',
    [INVESTIGATIVE_QUESTIONS.GEOGRAPHIC_FIT]: `Is this business in ${marketDefinition.geography || 'target geography'}?`,
    [INVESTIGATIVE_QUESTIONS.BUSINESS_FIT]: `Does this business fit: ${hypothesis.text || 'target ICP'}?`,
  };
  return labels[questionId] || questionId;
}

/**
 * Build evidence requirements from investigative questions.
 * @param {object[]} questions
 * @returns {object[]}
 */
function buildEvidenceRequirementsFromQuestions(questions = []) {
  const byType = new Map();

  for (const q of questions) {
    for (const ev of q.requiredEvidence || []) {
      const existing = byType.get(ev) || {
        evidenceType: ev,
        questionIds: [],
        required: true,
        satisfied: false,
        confidence: 0,
        sources: [],
      };
      if (!existing.questionIds.includes(q.id)) {
        existing.questionIds.push(q.id);
      }
      byType.set(ev, existing);
    }
  }

  return [...byType.values()].map((row) => buildEvidenceRequirement(row));
}

/**
 * Check whether collected evidence satisfies a requirement.
 * @param {object} requirement
 * @param {object[]} collectedEvidence
 * @returns {boolean}
 */
function evidenceRequirementSatisfied(requirement, collectedEvidence = []) {
  const norm = String(requirement.evidenceType || '').toLowerCase();
  return collectedEvidence.some((row) => {
    const types = [
      row.evidenceType,
      row.type,
      row.kind,
      ...(row.evidenceProduced || []),
    ]
      .filter(Boolean)
      .map((v) => String(v).toLowerCase());
    return types.some((t) => t === norm || t.includes(norm.replace(/_/g, '')));
  });
}

/**
 * Compute outstanding evidence from requirements and collection.
 * @param {object[]} requirements
 * @param {object[]} collectedEvidence
 * @returns {object[]}
 */
function computeOutstandingEvidence(requirements = [], collectedEvidence = []) {
  return requirements.filter(
    (req) => !evidenceRequirementSatisfied(req, collectedEvidence)
  );
}

/**
 * Compute satisfied evidence from requirements and collection.
 * @param {object[]} requirements
 * @param {object[]} collectedEvidence
 * @returns {object[]}
 */
function computeSatisfiedEvidence(requirements = [], collectedEvidence = []) {
  return requirements
    .filter((req) => evidenceRequirementSatisfied(req, collectedEvidence))
    .map((req) => ({ ...req, satisfied: true }));
}

module.exports = {
  INVESTIGATIVE_EVIDENCE,
  INVESTIGATIVE_QUESTIONS,
  QUESTION_TO_EVIDENCE,
  CONFIDENCE_INCREASING,
  CONFIDENCE_DECREASING,
  GAP_TO_QUESTIONS,
  DEFAULT_INVESTIGATION_QUESTIONS,
  buildInvestigationQuestion,
  buildEvidenceRequirement,
  deriveQuestionsForHypothesis,
  deriveQuestionsFromHypotheses,
  buildEvidenceRequirementsFromQuestions,
  evidenceRequirementSatisfied,
  computeOutstandingEvidence,
  computeSatisfiedEvidence,
};
