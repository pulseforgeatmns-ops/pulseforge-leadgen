'use strict';

/**
 * SPEC-141 — Scout Intelligence Pipeline types.
 * Scout investigates markets; providers are instrumentation.
 */

const INTELLIGENCE_STAGES = Object.freeze({
  MARKET_UNDERSTANDING: 'market_understanding',
  INVESTIGATION_PLANNING: 'investigation_planning',
  EVIDENCE_PLANNING: 'evidence_planning',
  PROVIDER_STRATEGY: 'provider_strategy',
  CANDIDATE_DISCOVERY: 'candidate_universe_discovery',
  EVIDENCE_COLLECTION: 'evidence_collection',
  QUALIFICATION: 'qualification',
  OPPORTUNITY_RANKING: 'opportunity_ranking',
  MARKET_COVERAGE: 'market_coverage',
});

const EVIDENCE_REQUIREMENTS = Object.freeze({
  CANDIDATE_UNIVERSE: 'candidate_universe',
  DECISION_MAKERS: 'decision_makers',
  PROPERTY_COUNT: 'property_count',
  CONTACT_PATH: 'contact_path',
  BUYING_SIGNALS: 'buying_signals',
  BUSINESS_MATURITY: 'business_maturity',
  EXISTING_VENDORS: 'existing_vendors',
  CLEANING_RESPONSIBILITY: 'cleaning_responsibility',
  GEOGRAPHIC_COVERAGE: 'geographic_coverage',
});

const RANKING_FACTORS = Object.freeze({
  REVENUE_POTENTIAL: 'revenue_potential',
  EASE_OF_ACCESS: 'ease_of_access',
  BUYING_SIGNALS: 'buying_signals',
  RELATIONSHIP_PROBABILITY: 'relationship_probability',
  GEOGRAPHIC_FIT: 'geographic_fit',
  EVIDENCE_CONFIDENCE: 'evidence_confidence',
  STRATEGIC_VALUE: 'strategic_value',
});

const QUALIFICATION_OUTCOMES = Object.freeze({
  QUALIFIED: 'qualified',
  OUT: 'out',
  WATCH: 'watch',
});

const COST_TIERS = Object.freeze({
  FREE: 'free',
  CACHED: 'cached',
  LOCAL: 'local',
  PAID: 'paid',
});

const SCOUT_INTELLIGENCE_EVENTS = Object.freeze({
  STARTED: 'SCOUT_INTELLIGENCE_STARTED',
  STAGE: 'SCOUT_INTELLIGENCE_STAGE',
  COMPLETED: 'SCOUT_INTELLIGENCE_COMPLETED',
});

function buildStageResult(stage, partial = {}) {
  return {
    stage,
    startedAt: partial.startedAt || new Date().toISOString(),
    completedAt: partial.completedAt || new Date().toISOString(),
    output: partial.output || null,
    error: partial.error || null,
  };
}

function buildIntelligenceResult(partial = {}) {
  return {
    outcome: partial.outcome || 'completed',
    stages: Array.isArray(partial.stages) ? partial.stages : [],
    marketDefinition: partial.marketDefinition || null,
    investigationPlan: partial.investigationPlan || null,
    evidencePlan: partial.evidencePlan || null,
    providerStrategy: partial.providerStrategy || null,
    candidateUniverse: partial.candidateUniverse || null,
    evidenceByCandidate: partial.evidenceByCandidate || [],
    qualified: Array.isArray(partial.qualified) ? partial.qualified : [],
    rankedOpportunities: Array.isArray(partial.rankedOpportunities) ? partial.rankedOpportunities : [],
    coverage: partial.coverage || null,
    report: partial.report || null,
    intelligenceResult: partial.intelligenceResult || null,
  };
}

module.exports = {
  INTELLIGENCE_STAGES,
  EVIDENCE_REQUIREMENTS,
  RANKING_FACTORS,
  QUALIFICATION_OUTCOMES,
  COST_TIERS,
  SCOUT_INTELLIGENCE_EVENTS,
  buildStageResult,
  buildIntelligenceResult,
};
