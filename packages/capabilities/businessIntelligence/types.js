'use strict';

/**
 * Business Intelligence types (SPEC-053 / ADR-037).
 * Analytical business reasoning — not descriptive company directories.
 */

const CONFIDENCE_LABEL = Object.freeze({
  HIGH: 'High',
  MEDIUM: 'Medium',
  LOW: 'Low',
});

const REASONING_LEVELS = Object.freeze({
  FACTS: 1,
  BUSINESS_MODEL: 2,
  OPERATIONAL_MODEL: 3,
  BUYING_PSYCHOLOGY: 4,
  SALES_INPUT: 5,
});

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildQualityAnswers(partial = {}) {
  return {
    howTheyMakeMoney: String(
      partial.howTheyMakeMoney || partial.revenue || ''
    ).trim(),
    growthConstraints: String(
      partial.growthConstraints || partial.constraints || ''
    ).trim(),
    operationalPressures: String(
      partial.operationalPressures || partial.pressures || ''
    ).trim(),
    problemOwner: String(
      partial.problemOwner || partial.owner || ''
    ).trim(),
    whyBuyNow: String(partial.whyBuyNow || partial.urgency || '').trim(),
    operationalLeverage: String(
      partial.operationalLeverage || partial.leverage || ''
    ).trim(),
    outcomesThatMatter: String(
      partial.outcomesThatMatter || partial.outcomes || ''
    ).trim(),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildReasoningLayers(partial = {}) {
  return {
    level1_facts: asStringList(partial.level1_facts || partial.facts),
    level2_business_model: String(
      partial.level2_business_model || partial.businessModel || ''
    ).trim(),
    level3_operational_model: asStringList(
      partial.level3_operational_model || partial.operational
    ),
    level4_buying_psychology: asStringList(
      partial.level4_buying_psychology || partial.psychology
    ),
    level5_sales_input: asStringList(
      partial.level5_sales_input || partial.salesInput
    ),
  };
}

/**
 * @param {object} [partial]
 * @returns {object} BusinessIntelligenceProfile
 */
function buildBusinessIntelligenceProfile(partial = {}) {
  const confidenceLabel = normalizeConfidenceLabel(partial.confidence);
  const confidenceScore =
    partial.confidenceScore != null
      ? clamp01(partial.confidenceScore)
      : labelToScore(confidenceLabel);
  const qualityAnswers = buildQualityAnswers(
    partial.qualityAnswers || partial.answers || {}
  );
  const uncertainty = asStringList(partial.uncertainty);

  return {
    prospectId:
      partial.prospectId != null
        ? String(partial.prospectId)
        : partial.id != null
          ? String(partial.id)
          : null,
    company: String(partial.company || '').trim(),
    industry: String(partial.industry || '').trim(),
    business_model: String(
      partial.business_model || partial.businessModel || ''
    ).trim(),
    revenue_model: String(
      partial.revenue_model || partial.revenueModel || ''
    ).trim(),
    primary_customers: String(
      partial.primary_customers || partial.primaryCustomers || ''
    ).trim(),
    growth_strategy: String(
      partial.growth_strategy || partial.growthStrategy || ''
    ).trim(),
    competitive_position: String(
      partial.competitive_position || partial.competitivePosition || ''
    ).trim(),
    operational_constraints: asStringList(
      partial.operational_constraints || partial.operationalConstraints
    ),
    likely_kpis: asStringList(partial.likely_kpis || partial.likelyKpis),
    cost_drivers: asStringList(partial.cost_drivers || partial.costDrivers),
    risk_factors: asStringList(partial.risk_factors || partial.riskFactors),
    buying_triggers: asStringList(
      partial.buying_triggers || partial.buyingTriggers
    ),
    decision_makers: asStringList(
      partial.decision_makers || partial.decisionMakers
    ),
    vendor_landscape: String(
      partial.vendor_landscape || partial.vendorLandscape || ''
    ).trim(),
    seasonality: String(partial.seasonality || '').trim(),
    expansion_signals: asStringList(
      partial.expansion_signals || partial.expansionSignals
    ),
    service_angle: String(
      partial.service_angle || partial.cleaning_angle || partial.cleaningAngle || ''
    ).trim(),
    qualityAnswers,
    reasoningLayers: buildReasoningLayers(
      partial.reasoningLayers || partial.layers || {}
    ),
    uncertainty,
    confidence: confidenceLabel,
    confidenceScore,
    evidenceRefs: asStringList(partial.evidenceRefs),
    gateRejections: Array.isArray(partial.gateRejections)
      ? partial.gateRejections.map((g) => ({
          reason: String((g && g.reason) || '').trim(),
          evidence: String((g && g.evidence) || '').trim(),
          regenerationRecommendation: String(
            (g &&
              (g.regenerationRecommendation || g.recommendation)) ||
              ''
          ).trim(),
        }))
      : [],
    derivedAt: partial.derivedAt || new Date().toISOString(),
    runtimeVersion: partial.runtimeVersion || 'business-intelligence@1.0.0',
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

function asStringList(value) {
  if (value == null || value === '') return [];
  if (!Array.isArray(value)) {
    const s = String(value).trim();
    return s ? [s] : [];
  }
  return value.map((v) => String(v).trim()).filter(Boolean);
}

module.exports = {
  CONFIDENCE_LABEL,
  REASONING_LEVELS,
  buildQualityAnswers,
  buildReasoningLayers,
  buildBusinessIntelligenceProfile,
  normalizeConfidenceLabel,
  labelToScore,
};
