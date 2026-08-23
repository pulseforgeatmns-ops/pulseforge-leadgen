'use strict';

/**
 * SPEC-141 — Scout.investigate() entry point.
 * Scout investigates markets; providers are instrumentation.
 */

const { runIntelligencePipeline } = require('./intelligence/Pipeline');
const { buildDelegationFromMission } = require('./intelligence/MarketUnderstanding');
const { buildDiscoveryResult, DISCOVERY_OUTCOMES } = require('./types');

/**
 * Canonical Scout investigation contract (SPEC-141).
 * Runs the 8-stage intelligence pipeline and returns a Mission Intelligence Report.
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

  const pipelineResult = await runIntelligencePipeline({
    mission,
    scoutPayload,
    opts: {
      ...opts,
      amoMissionId: opts.amoMissionId || opts.missionId,
    },
  });

  const report = pipelineResult.report || {};
  const intelligenceResult = pipelineResult.intelligenceResult;
  const opportunities =
    intelligenceResult &&
    intelligenceResult.payload &&
    intelligenceResult.payload.opportunities;

  const outcome =
    pipelineResult.outcome === 'blocked'
      ? DISCOVERY_OUTCOMES.BLOCKED
      : pipelineResult.outcome === 'partial'
        ? DISCOVERY_OUTCOMES.PARTIAL
        : DISCOVERY_OUTCOMES.COMPLETED;

  return {
    ...buildDiscoveryResult({
      outcome,
      strategy: 'Intelligence Pipeline',
      phases: pipelineResult.stages.map((s) => ({
        phase: s.stage,
        startedAt: s.startedAt,
        completedAt: s.completedAt,
        result: s.output,
      })),
      prospectCount: report.qualified || 0,
      companies: (pipelineResult.candidateUniverse && pipelineResult.candidateUniverse.candidates) || [],
      confidence: report.confidence,
      recommendations: (report.immediateOpportunities || []).map(
        (o) => `${o.rank}. ${o.name} — ${o.reasons && o.reasons.join(', ')}`
      ),
    }),
    intelligence: pipelineResult,
    report,
    rankedOpportunities: pipelineResult.rankedOpportunities || [],
    coverage: pipelineResult.coverage,
    marketDefinition: pipelineResult.marketDefinition,
    evidencePlan: pipelineResult.evidencePlan,
    providerStrategy: pipelineResult.providerStrategy,
    intelligenceResult,
    opportunities: opportunities || pipelineResult.rankedOpportunities,
  };
}

module.exports = {
  investigate,
  buildDelegationFromMission,
};
