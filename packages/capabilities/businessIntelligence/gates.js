'use strict';

/**
 * Business Intelligence quality gates (SPEC-053).
 * Required questions must be answered or marked uncertain — never guessed silently.
 */

const {
  CONFIDENCE_LABEL,
  buildBusinessIntelligenceProfile,
} = require('./types');

const GATE_REASONS = Object.freeze({
  MISSING_REVENUE_MODEL: 'missing_revenue_model',
  MISSING_GROWTH_CONSTRAINT: 'missing_growth_constraint',
  MISSING_OPERATIONAL_PRESSURE: 'missing_operational_pressure',
  MISSING_PROBLEM_OWNER: 'missing_problem_owner',
  MISSING_BUYING_URGENCY: 'missing_buying_urgency',
  LOW_REASONING_CONFIDENCE: 'low_reasoning_confidence',
});

/**
 * Apply quality gates; append uncertainty for unanswered required questions.
 * @param {object} profile
 * @returns {object}
 */
function applyBusinessIntelligenceGates(profile) {
  const uncertainty = [...(profile.uncertainty || [])];
  const answers = profile.qualityAnswers || {};
  const gateRejections = [];

  const required = [
    {
      key: 'howTheyMakeMoney',
      reason: GATE_REASONS.MISSING_REVENUE_MODEL,
      label: 'How they make money',
      fallbackField: profile.revenue_model,
    },
    {
      key: 'growthConstraints',
      reason: GATE_REASONS.MISSING_GROWTH_CONSTRAINT,
      label: 'What constrains growth',
      fallbackField: (profile.operational_constraints || [])[0],
    },
    {
      key: 'operationalPressures',
      reason: GATE_REASONS.MISSING_OPERATIONAL_PRESSURE,
      label: 'Operational pressures',
      fallbackField: (profile.operational_constraints || [])[1],
    },
    {
      key: 'problemOwner',
      reason: GATE_REASONS.MISSING_PROBLEM_OWNER,
      label: 'Who owns the problem',
      fallbackField: (profile.decision_makers || [])[0],
    },
    {
      key: 'whyBuyNow',
      reason: GATE_REASONS.MISSING_BUYING_URGENCY,
      label: 'Why buy now',
      fallbackField: (profile.buying_triggers || [])[0],
    },
  ];

  const nextAnswers = { ...answers };
  for (const req of required) {
    const value = String(nextAnswers[req.key] || req.fallbackField || '').trim();
    if (!value) {
      uncertainty.push(`Uncertain: ${req.label}`);
      gateRejections.push({
        reason: req.reason,
        evidence: `No confident answer for "${req.label}"`,
        regenerationRecommendation:
          'Attach industry / signals / decision-maker evidence before claiming certainty',
      });
      nextAnswers[req.key] = `Uncertain — ${req.label.toLowerCase()} not evidenced`;
    } else {
      nextAnswers[req.key] = value;
    }
  }

  let confidenceScore =
    profile.confidenceScore != null ? Number(profile.confidenceScore) : 0.35;
  if (gateRejections.length) {
    confidenceScore = Math.min(confidenceScore, 0.45);
  }
  if (gateRejections.length >= 3) {
    confidenceScore = Math.min(confidenceScore, 0.3);
  }
  if (confidenceScore < 0.45) {
    gateRejections.push({
      reason: GATE_REASONS.LOW_REASONING_CONFIDENCE,
      evidence: `confidenceScore=${confidenceScore}`,
      regenerationRecommendation:
        'Collect Level-1 facts (industry, website, signals) before raising confidence',
    });
  }

  const uniqueUncertainty = [...new Set(uncertainty.filter(Boolean))];

  return buildBusinessIntelligenceProfile({
    ...profile,
    qualityAnswers: nextAnswers,
    uncertainty: uniqueUncertainty,
    confidenceScore,
    confidence:
      confidenceScore >= 0.75
        ? CONFIDENCE_LABEL.HIGH
        : confidenceScore >= 0.5
          ? CONFIDENCE_LABEL.MEDIUM
          : CONFIDENCE_LABEL.LOW,
    gateRejections,
  });
}

/**
 * True when all five required questions have non-uncertain answers.
 * @param {object} profile
 */
function answersQualityGates(profile) {
  const a = (profile && profile.qualityAnswers) || {};
  const keys = [
    'howTheyMakeMoney',
    'growthConstraints',
    'operationalPressures',
    'problemOwner',
    'whyBuyNow',
  ];
  return keys.every((k) => {
    const v = String(a[k] || '').trim();
    return v && !/^uncertain/i.test(v);
  });
}

module.exports = {
  GATE_REASONS,
  applyBusinessIntelligenceGates,
  answersQualityGates,
};
