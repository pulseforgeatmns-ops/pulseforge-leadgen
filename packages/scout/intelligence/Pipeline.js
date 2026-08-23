'use strict';

/**
 * SPEC-141 — Scout Intelligence Pipeline orchestrator.
 *
 * Mission → Market Understanding → Evidence Planning → Provider Strategy →
 * Candidate Universe Discovery → Evidence Collection → Qualification →
 * Opportunity Ranking → Market Coverage → Mission Intelligence Report
 */

const { INTELLIGENCE_STAGES, buildStageResult, buildIntelligenceResult } = require('./types');
const { buildMarketDefinition, buildDelegationFromMission } = require('./MarketUnderstanding');
const { buildEvidencePlan } = require('./EvidencePlanning');
const { buildProviderStrategy } = require('./ProviderStrategy');
const { discoverCandidateUniverse } = require('./CandidateDiscovery');
const { collectEvidence } = require('./EvidenceCollection');
const { qualifyCandidates } = require('./Qualification');
const { rankOpportunities } = require('./OpportunityRanking');
const { analyzeMarketCoverage } = require('./MarketCoverage');
const { buildIntelligenceReport } = require('./IntelligenceReport');
const { runScoutAcquisitionIntelligence } = require('../../max/scoutAcquisition/ScoutAdapter');
const {
  emitIntelligenceStarted,
  emitIntelligenceStage,
  emitIntelligenceCompleted,
} = require('./observability');

/**
 * Run the full Scout Intelligence Pipeline.
 *
 * @param {object} input
 * @param {object} [input.mission]
 * @param {object} [input.delegation]
 * @param {object} [input.scoutPayload]
 * @param {object} [input.opts]
 * @returns {Promise<object>}
 */
async function runIntelligencePipeline(input = {}) {
  const opts = input.opts || {};
  const stages = [];
  const missionId = input.mission && input.mission.id;

  emitIntelligenceStarted({ missionId, tenantId: input.mission && input.mission.tenantId });

  // Stage 1 — Market Understanding
  emitIntelligenceStage(INTELLIGENCE_STAGES.MARKET_UNDERSTANDING, { missionId });
  const marketDefinition = buildMarketDefinition(input);
  stages.push(
    buildStageResult(INTELLIGENCE_STAGES.MARKET_UNDERSTANDING, {
      output: marketDefinition,
    })
  );

  if (!marketDefinition.valid) {
    const report = buildIntelligenceReport({
      marketDefinition,
      coverage: {
        estimatedUniverse: 0,
        investigated: 0,
        qualified: 0,
        strong: 0,
        immediate: 0,
        coveragePct: 0,
        confidence: 0,
        finished: false,
      },
      ranking: { rankedOpportunities: [] },
      evidenceCollection: {},
      providerStrategy: {},
    });

    emitIntelligenceCompleted({ missionId, outcome: 'blocked' });
    return buildIntelligenceResult({
      outcome: 'blocked',
      stages,
      marketDefinition,
      report,
    });
  }

  const delegation =
    input.delegation || buildDelegationFromMission(input.mission || {}, input.scoutPayload || {});

  // Stage 2 — Evidence Planning
  emitIntelligenceStage(INTELLIGENCE_STAGES.EVIDENCE_PLANNING, { missionId });
  const evidencePlan = buildEvidencePlan(marketDefinition, opts);
  stages.push(
    buildStageResult(INTELLIGENCE_STAGES.EVIDENCE_PLANNING, { output: evidencePlan })
  );

  // Stage 3 — Provider Strategy
  emitIntelligenceStage(INTELLIGENCE_STAGES.PROVIDER_STRATEGY, { missionId });
  const providerStrategy = buildProviderStrategy(evidencePlan, opts);
  stages.push(
    buildStageResult(INTELLIGENCE_STAGES.PROVIDER_STRATEGY, { output: providerStrategy })
  );

  // Stage 4 — Candidate Universe Discovery
  emitIntelligenceStage(INTELLIGENCE_STAGES.CANDIDATE_DISCOVERY, { missionId });
  const candidateUniverse = await discoverCandidateUniverse({
    marketDefinition,
    delegation,
    mission: input.mission,
    scoutPayload: input.scoutPayload,
    opts,
  });
  stages.push(
    buildStageResult(INTELLIGENCE_STAGES.CANDIDATE_DISCOVERY, {
      output: {
        discovered: candidateUniverse.discovered,
        coverage: candidateUniverse.coverage,
      },
    })
  );

  // Stage 5 — Evidence Collection
  emitIntelligenceStage(INTELLIGENCE_STAGES.EVIDENCE_COLLECTION, { missionId });
  const evidenceCollection = collectEvidence({
    candidateUniverse,
    providerStrategy,
  });
  stages.push(
    buildStageResult(INTELLIGENCE_STAGES.EVIDENCE_COLLECTION, {
      output: {
        withEvidence: evidenceCollection.withEvidence,
        avgConfidence: evidenceCollection.avgConfidence,
      },
    })
  );

  // Stage 6 — Qualification
  emitIntelligenceStage(INTELLIGENCE_STAGES.QUALIFICATION, { missionId });
  const qualification = await qualifyCandidates({
    marketDefinition,
    candidateUniverse,
    evidenceCollection,
    opts,
  });
  stages.push(
    buildStageResult(INTELLIGENCE_STAGES.QUALIFICATION, {
      output: {
        qualified: qualification.qualifiedCount,
        watch: qualification.watchCount,
        rejected: qualification.rejectedCount,
      },
    })
  );

  // Stage 7 — Opportunity Ranking
  emitIntelligenceStage(INTELLIGENCE_STAGES.OPPORTUNITY_RANKING, { missionId });
  const ranking = rankOpportunities({
    qualification,
    evidenceCollection,
    candidateUniverse,
  });
  stages.push(
    buildStageResult(INTELLIGENCE_STAGES.OPPORTUNITY_RANKING, {
      output: {
        ranked: ranking.rankedOpportunities.length,
        strong: ranking.strong,
        immediate: ranking.immediate,
      },
    })
  );

  // Stage 8 — Market Coverage
  emitIntelligenceStage(INTELLIGENCE_STAGES.MARKET_COVERAGE, { missionId });
  const coverage = analyzeMarketCoverage({
    candidateUniverse,
    qualification,
    ranking,
    evidenceCollection,
  });
  stages.push(
    buildStageResult(INTELLIGENCE_STAGES.MARKET_COVERAGE, { output: coverage })
  );

  const report = buildIntelligenceReport({
    marketDefinition,
    coverage,
    ranking,
    evidenceCollection,
    providerStrategy,
  });

  // Delegate to existing acquisition intelligence for AMO-compatible payload
  let intelligenceResult = null;
  if (opts.runAcquisitionIntelligence !== false) {
    try {
      intelligenceResult = await runScoutAcquisitionIntelligence(delegation, {
        ...opts,
        missionId: opts.amoMissionId || opts.missionId || missionId,
      });
    } catch {
      // Best-effort — pipeline report is authoritative for SPEC-141
    }
  }

  emitIntelligenceCompleted({
    missionId,
    outcome: 'completed',
    qualified: coverage.qualified,
    confidence: coverage.confidence,
  });

  return buildIntelligenceResult({
    outcome: candidateUniverse.discovered > 0 ? 'completed' : 'partial',
    stages,
    marketDefinition,
    evidencePlan,
    providerStrategy,
    candidateUniverse,
    evidenceByCandidate: evidenceCollection.evidenceByCandidate,
    qualified: qualification.qualified,
    rankedOpportunities: ranking.rankedOpportunities,
    coverage,
    report,
    intelligenceResult,
  });
}

module.exports = {
  runIntelligencePipeline,
};
