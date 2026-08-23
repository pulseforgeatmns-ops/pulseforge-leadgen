'use strict';

/**
 * SPEC-141 — Scout Intelligence Pipeline public exports.
 */

const {
  INTELLIGENCE_STAGES,
  EVIDENCE_REQUIREMENTS,
  RANKING_FACTORS,
  QUALIFICATION_OUTCOMES,
  COST_TIERS,
  SCOUT_INTELLIGENCE_EVENTS,
  buildStageResult,
  buildIntelligenceResult,
} = require('./types');

const {
  EVIDENCE_CAPABILITIES,
  DEFAULT_PROVIDERS,
  createDefaultProviderRegistry,
  createProviderCapabilityRegistry,
} = require('./ProviderCapabilityRegistry');

const { buildMarketDefinition, buildDelegationFromMission } = require('./MarketUnderstanding');
const { buildEvidencePlan } = require('./EvidencePlanning');
const { buildProviderStrategy } = require('./ProviderStrategy');
const { discoverCandidateUniverse } = require('./CandidateDiscovery');
const { collectEvidence } = require('./EvidenceCollection');
const { fuseCandidateEvidence, normalizeEvidenceItem } = require('./EvidenceFusion');
const { qualifyCandidates } = require('./Qualification');
const { rankOpportunities } = require('./OpportunityRanking');
const { analyzeMarketCoverage } = require('./MarketCoverage');
const { buildIntelligenceReport } = require('./IntelligenceReport');
const { runIntelligencePipeline } = require('./Pipeline');
const {
  emitIntelligenceStarted,
  emitIntelligenceStage,
  emitIntelligenceCompleted,
  listIntelligenceLog,
  clearIntelligenceLog,
} = require('./observability');

module.exports = {
  INTELLIGENCE_STAGES,
  EVIDENCE_REQUIREMENTS,
  RANKING_FACTORS,
  QUALIFICATION_OUTCOMES,
  COST_TIERS,
  SCOUT_INTELLIGENCE_EVENTS,
  EVIDENCE_CAPABILITIES,
  DEFAULT_PROVIDERS,
  buildStageResult,
  buildIntelligenceResult,
  createDefaultProviderRegistry,
  createProviderCapabilityRegistry,
  buildMarketDefinition,
  buildDelegationFromMission,
  buildEvidencePlan,
  buildProviderStrategy,
  discoverCandidateUniverse,
  collectEvidence,
  fuseCandidateEvidence,
  normalizeEvidenceItem,
  qualifyCandidates,
  rankOpportunities,
  analyzeMarketCoverage,
  buildIntelligenceReport,
  runIntelligencePipeline,
  emitIntelligenceStarted,
  emitIntelligenceStage,
  emitIntelligenceCompleted,
  listIntelligenceLog,
  clearIntelligenceLog,
};
