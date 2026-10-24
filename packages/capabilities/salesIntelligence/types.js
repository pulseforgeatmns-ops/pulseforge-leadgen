'use strict';

/**
 * Sales Intelligence types (SPEC-048 / ADR-032).
 * Structured sell reasoning — no channel prose.
 */

const CONFIDENCE_LABEL = Object.freeze({
  HIGH: 'High',
  MEDIUM: 'Medium',
  LOW: 'Low',
});

const BUYER_TYPES = Object.freeze({
  RELATIONSHIP_DRIVEN: 'Relationship Driven',
  OPERATIONS_FOCUSED: 'Operations Focused',
  COST_SENSITIVE: 'Cost Sensitive',
  GROWTH_ORIENTED: 'Growth Oriented',
  COMPLIANCE_DRIVEN: 'Compliance Driven',
});

const GATE_REASONS = Object.freeze({
  WRONG_INDUSTRY: 'wrong_industry',
  WRONG_BUYER: 'wrong_buyer',
  UNSUPPORTED_PERSONALIZATION: 'unsupported_personalization',
  HALLUCINATED_FACTS: 'hallucinated_facts',
  GENERIC_VALUE_PROPOSITION: 'generic_value_proposition',
  LOW_REASONING_CONFIDENCE: 'low_reasoning_confidence',
  PROSPECT_AFTER_ANCHOR: 'prospect_after_anchor',
  REPEATED_PHRASES: 'repeated_phrases',
});

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildBuyingSignal(partial = {}) {
  return {
    signal: String(partial.signal || partial.title || '').trim(),
    confidence: normalizeConfidenceLabel(partial.confidence),
    confidenceScore: clamp01(partial.confidenceScore),
    evidence: String(partial.evidence || partial.description || '').trim(),
    source: String(partial.source || 'unknown').trim(),
    evidenceRefs: asStringList(partial.evidenceRefs),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildPersonalizationClaim(partial = {}) {
  const verified = partial.verified === true;
  return {
    claim: String(partial.claim || '').trim(),
    evidenceRef: String(partial.evidenceRef || '').trim(),
    verified,
    source: String(partial.source || '').trim(),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildMessagingStrategy(partial = {}) {
  return {
    opening_focus: String(partial.opening_focus || partial.openingFocus || '').trim(),
    avoid: asStringList(partial.avoid),
    social_proof: asStringList(partial.social_proof || partial.socialProof),
    cta: String(partial.cta || '').trim(),
    tone: asStringList(partial.tone),
    positioning: String(partial.positioning || '').trim(),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildGateRejection(partial = {}) {
  return {
    reason: String(partial.reason || '').trim(),
    evidence: String(partial.evidence || '').trim(),
    regenerationRecommendation: String(
      partial.regenerationRecommendation || partial.recommendation || ''
    ).trim(),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildOperatorConfidence(partial = {}) {
  const dims = {
    industryAccuracy: score10(partial.industryAccuracy),
    buyerRelevance: score10(partial.buyerRelevance),
    evidenceUse: score10(partial.evidenceUse),
    specificity: score10(partial.specificity),
    naturalness: score10(partial.naturalness),
    salesJudgment: score10(partial.salesJudgment),
  };
  const values = Object.values(dims);
  const avg =
    values.reduce((a, b) => a + b, 0) / (values.length || 1);
  const overall =
    partial.overall != null
      ? Math.round(Number(partial.overall))
      : Math.round(avg * 10);
  return {
    ...dims,
    overall: Math.max(0, Math.min(100, overall)),
    passed: partial.passed !== false && overall >= 70,
    editInstinct: Boolean(partial.editInstinct),
    notes: asStringList(partial.notes),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildSalesIntelligenceProfile(partial = {}) {
  const confidenceLabel = normalizeConfidenceLabel(partial.confidence);
  const confidenceScore =
    partial.confidenceScore != null
      ? clamp01(partial.confidenceScore)
      : labelToScore(confidenceLabel);
  const claims = Array.isArray(partial.personalization_claims)
    ? partial.personalization_claims.map(buildPersonalizationClaim)
    : Array.isArray(partial.personalizationClaims)
      ? partial.personalizationClaims.map(buildPersonalizationClaim)
      : [];
  const gates = Array.isArray(partial.gateRejections)
    ? partial.gateRejections.map(buildGateRejection)
    : [];
  const sendable =
    partial.sendable === false
      ? false
      : gates.length === 0 && confidenceScore >= 0.45;

  return {
    prospectId:
      partial.prospectId != null
        ? String(partial.prospectId)
        : partial.id != null
          ? String(partial.id)
          : null,
    company: String(partial.company || '').trim(),
    industry: String(partial.industry || '').trim(),
    decision_maker: String(
      partial.decision_maker || partial.decisionMaker || ''
    ).trim(),
    decision_maker_confidence: normalizeConfidenceLabel(
      partial.decision_maker_confidence || partial.decisionMakerConfidence
    ),
    buyer_type: String(partial.buyer_type || partial.buyerType || '').trim(),
    primary_pain: String(
      partial.primary_pain || partial.primaryPain || ''
    ).trim(),
    secondary_pain: String(
      partial.secondary_pain || partial.secondaryPain || ''
    ).trim(),
    business_goal: String(
      partial.business_goal || partial.businessGoal || ''
    ).trim(),
    risk_if_unchanged: String(
      partial.risk_if_unchanged || partial.riskIfUnchanged || ''
    ).trim(),
    anchor_advantage: asStringList(
      partial.anchor_advantage || partial.anchorAdvantage
    ),
    recommended_angle: String(
      partial.recommended_angle || partial.recommendedAngle || ''
    ).trim(),
    call_to_action: String(
      partial.call_to_action || partial.callToAction || ''
    ).trim(),
    buying_signals: Array.isArray(partial.buying_signals)
      ? partial.buying_signals.map(buildBuyingSignal)
      : Array.isArray(partial.buyingSignals)
        ? partial.buyingSignals.map(buildBuyingSignal)
        : [],
    messaging_strategy: buildMessagingStrategy(
      partial.messaging_strategy || partial.messagingStrategy || {}
    ),
    personalization_claims: claims,
    confidence: confidenceLabel,
    confidenceScore,
    sendable,
    evidenceRefs: asStringList(partial.evidenceRefs),
    gateRejections: gates,
    businessIntelligenceProfileId:
      partial.businessIntelligenceProfileId != null
        ? String(partial.businessIntelligenceProfileId)
        : null,
    operatorConfidence:
      partial.operatorConfidence && typeof partial.operatorConfidence === 'object'
        ? buildOperatorConfidence(partial.operatorConfidence)
        : null,
    derivedAt: partial.derivedAt || new Date().toISOString(),
    runtimeVersion: partial.runtimeVersion || 'sales-intelligence@1.0.0',
  };
}

function normalizeConfidenceLabel(value) {
  if (value == null || value === '') return CONFIDENCE_LABEL.LOW;
  if (typeof value === 'number') {
    if (value >= 0.75) return CONFIDENCE_LABEL.HIGH;
    if (value >= 0.5) return CONFIDENCE_LABEL.MEDIUM;
    return CONFIDENCE_LABEL.LOW;
  }
  const s = String(value).trim().toLowerCase();
  if (s === 'high' || s === 'h') return CONFIDENCE_LABEL.HIGH;
  if (s === 'medium' || s === 'med' || s === 'm') return CONFIDENCE_LABEL.MEDIUM;
  return CONFIDENCE_LABEL.LOW;
}

function labelToScore(label) {
  if (label === CONFIDENCE_LABEL.HIGH) return 0.85;
  if (label === CONFIDENCE_LABEL.MEDIUM) return 0.6;
  return 0.35;
}

function clamp01(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(1, x));
}

function score10(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 5;
  return Math.max(0, Math.min(10, Math.round(x)));
}

function asStringList(value) {
  if (!Array.isArray(value)) return [];
  return value.map((v) => String(v).trim()).filter(Boolean);
}

module.exports = {
  CONFIDENCE_LABEL,
  BUYER_TYPES,
  GATE_REASONS,
  buildBuyingSignal,
  buildPersonalizationClaim,
  buildMessagingStrategy,
  buildGateRejection,
  buildOperatorConfidence,
  buildSalesIntelligenceProfile,
  normalizeConfidenceLabel,
  labelToScore,
};
