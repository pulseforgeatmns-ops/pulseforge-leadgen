'use strict';

/**
 * SPEC-141 + SPEC-142 — Scout.investigate() entry point.
 * Scout investigates markets through a hypothesis-driven evidence loop (SPEC-142),
 * reusing SPEC-141 provider capabilities, fusion, and qualification.
 */

const { runInvestigationEngine } = require('./investigation/InvestigationLoop');
const { buildDelegationFromMission } = require('./intelligence/MarketUnderstanding');
const { buildDiscoveryResult, DISCOVERY_OUTCOMES } = require('./types');

/**
 * Canonical Scout investigation contract.
 * Runs the SPEC-142 hypothesis-driven investigation engine.
 *
 * @param {object} input
 * @param {object} input.mission
 * @param {object} [input.missionEngine]
 * @param {object} [input.scoutPayload]
 * @param {object} [input.opts]
 * @returns {Promise<object>}
 */
async function investigate(input) {
  const { mission, scoutPayload = {}, opts = {} } = input;
  if (!mission) {
    throw new Error('Scout.investigate requires mission');
  }

  const engineResult = await runInvestigationEngine({
    mission,
    scoutPayload,
    delegation: input.delegation,
    opts: {
      ...opts,
      amoMissionId: opts.amoMissionId || opts.missionId,
    },
  });

  const report = engineResult.report || {};
  const legacyReport = report.kind === 'investigation_report' ? report : null;

  const outcome =
    engineResult.outcome === 'blocked'
      ? DISCOVERY_OUTCOMES.BLOCKED
      : engineResult.outcome === 'partial'
        ? DISCOVERY_OUTCOMES.PARTIAL
        : DISCOVERY_OUTCOMES.COMPLETED;

  const rankedOpportunities = (engineResult.ranking && engineResult.ranking.rankedOpportunities) || [];

  return {
    ...buildDiscoveryResult({
      outcome,
      strategy: 'Evidence-Driven Investigation Engine',
      phases: (engineResult.iterations || []).map((it) => ({
        phase: it.phase,
        iteration: it.iteration,
        result: {
          overallConfidence: it.overallConfidence,
          missing: it.missing,
          claimsCount: it.claimsCount,
          nextStep: it.nextStep,
        },
      })),
      prospectCount: report.qualified || (engineResult.qualification && engineResult.qualification.qualifiedCount) || 0,
      companies: (engineResult.candidateUniverse && engineResult.candidateUniverse.candidates) || [],
      confidence: engineResult.overallConfidence || report.missionIntelligence?.overallConfidence,
      recommendations: (report.recommendations || []).map(
        (r) => `${r.rank}. ${r.name} — ${(r.reasons && r.reasons.join(', ')) || r.claim}`
      ),
    }),
    investigation: engineResult,
    investigationReport: report,
    investigationGraph: engineResult.graph,
    hypotheses: engineResult.hypotheses,
    claims: engineResult.claims,
    missingEvidence: engineResult.missingEvidence,
    report: legacyReport,
    rankedOpportunities,
    coverage: {
      estimatedUniverse: engineResult.candidateUniverse?.estimatedMarket,
      investigated: (engineResult.candidateUniverse?.candidates || []).length,
      qualified: engineResult.qualification?.qualifiedCount || 0,
      confidence: engineResult.overallConfidence,
      finished: engineResult.completionReason === 'confidence_threshold_reached',
    },
    marketDefinition: engineResult.marketDefinition,
    evidencePlan: engineResult.evidencePlan,
    providerStrategy: engineResult.providerStrategy,
    opportunities: rankedOpportunities,
    startingPoint: engineResult.startingPoint || null,
    memoryLoaded: engineResult.memoryLoaded === true,
    memoryPersist: engineResult.memoryPersist || null,
  };
}

module.exports = {
  investigate,
  buildDelegationFromMission,
};
