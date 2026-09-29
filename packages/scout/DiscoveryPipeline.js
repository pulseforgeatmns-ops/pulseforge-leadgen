'use strict';

/**
 * SPEC-154 — Unified Discovery Pipeline.
 *
 * Every discovery request executes the same investigative reasoning engine:
 *   Understand Market → Estimate Universe → Build Investigation Plan →
 *   Execute Coverage Plan → Measure Coverage → Determine Sufficiency →
 *   Produce Intelligence Report
 *
 * No code path may bypass CoverageEngine.
 */

const { buildMarketDefinition, buildDelegationFromMission } = require('./intelligence/MarketUnderstanding');
const { createHypothesisInvestigationPlan } = require('./coverage/HypothesisInvestigationPlanner');
const {
  INVESTIGATION_MODES,
  resolveInvestigationMode,
  buildEntityInvestigationPlan,
  buildInvestigationContinuationContext,
} = require('./investigation/EntityInvestigationContinuation');
const { loadRepository } = require('../max/scoutAcquisition/ExistingIntelligence');
const { assessExistingSufficiency } = require('../max/scoutAcquisition/CandidateUniverse');
const { defaultDiscoveryAdapters } = require('../max/scoutAcquisition/DiscoveryAdapters');
const {
  evaluateDiscoveryCapability,
  buildCapabilityBlockedResult,
  CAPABILITY_BLOCKER_CODE,
} = require('./coverage/DiscoveryCapabilityGate');
const { runScoutAcquisitionIntelligence } = require('../max/scoutAcquisition/ScoutAdapter');
const {
  buildDiscoveryPlan,
  computeDiscoveryConfidence,
  buildDiscoveryReport,
  canConcludeEmptyUniverse,
  discoveryStatusFromCoverage,
} = require('./coverage/DiscoveryCoverageEngine');
const { DISCOVERY_PIPELINE_STAGES, DISCOVERY_OUTCOMES } = require('./types');
const {
  estimateCandidateUniverse,
  reviseCandidateUniverseEstimate,
  computeCoverageFromEstimate,
  extractExpectedValue,
  estimateUniverseFromPlan,
} = require('./universe/CandidateUniverseEstimate');
const { runInvestigativeReasoningLoop } = require('./investigation/InvestigativeReasoningLoop');
const { mergeIntoDiscoveryReport } = require('./investigation/MissionIntelligenceReport');
const { buildExplainabilityGraph, serializeGraph } = require('./explainability/ExplainabilityGraph');
const {
  prepareInvestigationWithMemory,
  persistDiscoveryKnowledge,
  recallMarketMemoryForInvestigation,
  buildMarketChangesSection,
  detectMarketChanges,
} = require('./memory');

function nowIso() {
  return new Date().toISOString();
}

function buildStage(stage, partial = {}) {
  return {
    stage,
    startedAt: partial.startedAt || nowIso(),
    completedAt: partial.completedAt || nowIso(),
    output: partial.output != null ? partial.output : null,
    error: partial.error || null,
  };
}

function computeCoveragePct(coverageMetrics, universeEstimate, investigated = null) {
  const marketCoverage = computeCoverageFromEstimate(investigated, universeEstimate);
  if (marketCoverage != null) return marketCoverage;
  return null;
}

function mapIntelligenceStatus(status) {
  if (status === 'blocked') return DISCOVERY_OUTCOMES.BLOCKED;
  if (status === 'partial') return DISCOVERY_OUTCOMES.PARTIAL;
  return DISCOVERY_OUTCOMES.COMPLETED;
}

function buildPipelineResult(partial = {}) {
  return {
    outcome: partial.outcome || DISCOVERY_OUTCOMES.BLOCKED,
    stages: Array.isArray(partial.stages) ? partial.stages : [],
    marketDefinition: partial.marketDefinition || null,
    universeEstimate: partial.universeEstimate != null ? partial.universeEstimate : null,
    coveragePlan: partial.coveragePlan || null,
    coverageMetrics: partial.coverageMetrics || null,
    coveragePct: partial.coveragePct != null ? partial.coveragePct : null,
    intelligenceReport: partial.intelligenceReport || null,
    investigationState: partial.investigationState || null,
    missionIntelligenceReport: partial.missionIntelligenceReport || null,
    emptyMarketDecision: partial.emptyMarketDecision === true,
    confidence: partial.confidence != null ? partial.confidence : null,
    discoveryConfidence: partial.discoveryConfidence || null,
    sufficiency: partial.sufficiency || null,
    gapAnalysis: partial.gapAnalysis || null,
    existingIntelligence: partial.existingIntelligence || null,
    intelligenceResult: partial.intelligenceResult || null,
    qualifiedCount: partial.qualifiedCount != null ? Number(partial.qualifiedCount) : 0,
    blockReason: partial.blockReason || null,
    capabilityBlocked: partial.capabilityBlocked === true,
    capabilityEvaluation: partial.capabilityEvaluation || null,
    blockerCode: partial.blockerCode || null,
    coverageEngineUsed: partial.coverageEngineUsed !== false,
    marketMemoryRecall: partial.marketMemoryRecall || null,
    marketMemoryPersist: partial.marketMemoryPersist || null,
    memoryLoaded: partial.memoryLoaded === true,
    explainabilityGraph: partial.explainabilityGraph || null,
    investigationPlan: partial.investigationPlan || null,
  };
}

/**
 * Run the unified discovery pipeline (SPEC-154).
 *
 * @param {object} input
 * @param {object} [input.mission]
 * @param {object} [input.delegation]
 * @param {object} [input.scoutPayload]
 * @param {object} [input.opts]
 * @returns {Promise<object>}
 */
async function runDiscoveryPipeline(input = {}) {
  const opts = input.opts || {};
  const mission = input.mission || {};
  const stages = [];
  const tenantId = String(
    (input.delegation && input.delegation.tenantId) ||
      mission.tenantId ||
      mission.clientId ||
      opts.tenantId ||
      ''
  );

  // ── SPEC-161: Recall Market Memory before investigation ────────
  let loadedMemory = null;
  let marketMemoryRecall = null;
  if (opts.persistMemory !== false && tenantId) {
    const preDelegation =
      input.delegation || buildDelegationFromMission(mission, input.scoutPayload || {});
    const preMarket = buildMarketDefinition(input);
    loadedMemory = await prepareInvestigationWithMemory({
      tenantId,
      mission,
      marketDefinition: preMarket,
      opts: { store: opts.memoryStore || opts.store },
    });
    marketMemoryRecall = recallMarketMemoryForInvestigation({
      priorMarketMemory: loadedMemory.memory?.market || null,
      memory: loadedMemory.memory,
    });
    opts.memory = loadedMemory.memory;
    opts.investigationMemory = loadedMemory.memory;
    opts.marketMemoryRecall = marketMemoryRecall;
  }

  // ── Stage 1: Understand Market ─────────────────────────────────
  const marketStageStarted = nowIso();

  // Always consult existing intelligence before concluding market is unusable.
  let existing = { companies: [], people: [] };
  let existingError = null;
  try {
    const preDelegation =
      input.delegation || buildDelegationFromMission(mission, input.scoutPayload || {});
    existing = await loadRepository({
      authorizedTenantId: tenantId,
      tenantId,
      targetContext: preDelegation.targetContext,
      businessContext: preDelegation.businessContext,
      loadCompanies: opts.loadCompanies,
      companies: opts.companies,
      people: opts.people,
    });
  } catch (err) {
    existingError = err.message || String(err);
  }

  const existingIntelligence = {
    companyCount: (existing.companies || []).length,
    prospectCount: (existing.people || []).length,
    consulted: true,
    error: existingError,
  };

  let marketDefinition = buildMarketDefinition(input);
  if (marketMemoryRecall?.loaded && marketMemoryRecall.marketUnderstanding?.dominantTerminology?.length) {
    marketDefinition = {
      ...marketDefinition,
      terminology: [
        ...new Set([
          ...(marketMemoryRecall.marketUnderstanding.dominantTerminology || []),
          ...(marketDefinition.terminology || []),
        ]),
      ],
      priorMarketMemory: true,
    };
  }
  stages.push(
    buildStage(DISCOVERY_PIPELINE_STAGES.UNDERSTAND_MARKET, {
      startedAt: marketStageStarted,
      output: marketDefinition,
    })
  );

  if (!marketDefinition.valid) {
    return buildPipelineResult({
      outcome: DISCOVERY_OUTCOMES.BLOCKED,
      stages,
      marketDefinition,
      existingIntelligence,
      blockReason: marketDefinition.invalidReason || 'Invalid market definition',
      intelligenceReport: buildDiscoveryReport({
        coverage: {},
        candidateUniverse: [],
        qualifiedCount: 0,
      }),
      emptyMarketDecision: false,
      confidence: 0,
    });
  }

  const delegation =
    input.delegation || buildDelegationFromMission(mission, input.scoutPayload || {});
  const searchDefinition = marketDefinition.searchDefinition;

  const gapAnalysis = assessExistingSufficiency(existing, searchDefinition, opts);

  const adapters =
    (opts.discoveryAdapters) ||
    defaultDiscoveryAdapters({
      discover: opts.discover,
      enablePlaces: opts.enablePlaces,
      placesProvider: opts.placesProvider,
      apiKey: opts.apiKey,
      fetchImpl: opts.fetchImpl,
      companies: opts.companies,
    });

  // ── Stage 2: Estimate Universe ─────────────────────────────────
  const universeStageStarted = nowIso();
  let coveragePlan = null;
  try {
    coveragePlan = buildDiscoveryPlan(searchDefinition, { adapters });
  } catch (err) {
    return buildPipelineResult({
      outcome: DISCOVERY_OUTCOMES.BLOCKED,
      stages,
      marketDefinition,
      gapAnalysis,
      existingIntelligence,
      blockReason: err.message || String(err),
      coverageEngineUsed: true,
    });
  }

  const universeEstimate = estimateCandidateUniverse({
    discoveryPlan: coveragePlan,
    existingIntelligence,
    gapAnalysis,
    marketDefinition,
    memory: opts.memory || opts.investigationMemory || {},
    historicalMissions: opts.historicalMissions || [],
  });
  stages.push(
    buildStage(DISCOVERY_PIPELINE_STAGES.ESTIMATE_UNIVERSE, {
      startedAt: universeStageStarted,
      output: {
        ...universeEstimate,
        basis: 'multi_signal_estimation',
        planTotals: coveragePlan.totals || null,
      },
    })
  );

  // ── SPEC-175: Capability Evaluation before external discovery ──
  const capabilityStageStarted = nowIso();
  const capabilityEvaluation = evaluateDiscoveryCapability({
    adapters,
    coveragePlan,
    requireExternalDiscovery: opts.requireExternalDiscovery !== false,
    discover: opts.discover,
    enablePlaces: opts.enablePlaces,
    placesProvider: opts.placesProvider,
    apiKey: opts.apiKey,
    fetchImpl: opts.fetchImpl,
  });
  stages.push(
    buildStage('capability_evaluation', {
      startedAt: capabilityStageStarted,
      output: {
        enoughSensors: capabilityEvaluation.enoughSensors,
        canExecute: capabilityEvaluation.canExecute,
        registry: capabilityEvaluation.registry,
        operationalProviders: capabilityEvaluation.operationalProviders,
      },
    })
  );

  if (!capabilityEvaluation.canExecute) {
    const blocked = buildCapabilityBlockedResult(capabilityEvaluation, {
      coveragePlan,
      existingIntelligence,
      gapAnalysis,
    });
    return buildPipelineResult({
      outcome: blocked.outcome,
      stages,
      marketDefinition,
      universeEstimate,
      coveragePlan,
      gapAnalysis,
      existingIntelligence,
      blockReason: blocked.blockReason,
      capabilityBlocked: true,
      capabilityEvaluation: blocked.capabilityEvaluation,
      blockerCode: blocked.blockerCode,
      intelligenceReport: buildDiscoveryReport({
        coverage: {},
        candidateUniverse: [],
        qualifiedCount: 0,
      }),
      emptyMarketDecision: false,
      confidence: 0,
      coverageEngineUsed: true,
    });
  }

  // ── Stage 3: Build Investigation Plan ──────────────────────────
  const planStageStarted = nowIso();
  const investigationMode = resolveInvestigationMode({
    priorPayload: opts.priorDiscoveryPayload || {},
    opts,
  });
  let investigationPlan;
  if (investigationMode === INVESTIGATION_MODES.ENTITY_CONTINUATION) {
    investigationPlan = buildEntityInvestigationPlan({
      mission,
      marketDefinition,
      priorPayload: opts.priorDiscoveryPayload || {},
      opts: {
        ...opts,
        estimatedMarket: extractExpectedValue(universeEstimate),
        universeEstimate,
      },
    });
  } else {
    investigationPlan = createHypothesisInvestigationPlan({
      mission,
      marketDefinition,
      opts: {
        ...opts,
        estimatedMarket: extractExpectedValue(universeEstimate),
        universeEstimate,
      },
    });
  }
  stages.push(
    buildStage(DISCOVERY_PIPELINE_STAGES.BUILD_INVESTIGATION_PLAN, {
      startedAt: planStageStarted,
      output: {
        coveragePlan,
        investigationPlan,
        investigationMode,
      },
    })
  );

  // ── Stages 4–7: Execute coverage, measure, sufficiency, report ─
  // runScoutAcquisitionIntelligence always routes through CoverageEngine
  // via constructCandidateUniverse({ useCoverageEngine: true }).
  const executeStarted = nowIso();
  let intelligenceResult;
  try {
    intelligenceResult = await runScoutAcquisitionIntelligence(delegation, {
      ...opts,
      mode: opts.mode || 'completed',
      missionId: opts.amoMissionId || opts.missionId || mission.id,
      tenantId,
      useCoverageEngine: true,
      marketDefinition,
      mission,
      investigationPlan,
      investigationMode,
      entityInvestigationContinuation:
        investigationMode === INVESTIGATION_MODES.ENTITY_CONTINUATION,
      preservedCandidates: opts.preservedCandidates || [],
    });
  } catch (err) {
    stages.push(
      buildStage(DISCOVERY_PIPELINE_STAGES.EXECUTE_COVERAGE_PLAN, {
        startedAt: executeStarted,
        error: err.message || String(err),
      })
    );
    return buildPipelineResult({
      outcome: DISCOVERY_OUTCOMES.FAILED,
      stages,
      marketDefinition,
      universeEstimate,
      coveragePlan,
      gapAnalysis,
      existingIntelligence,
      blockReason: err.message || String(err),
      coverageEngineUsed: true,
    });
  }

  const payload = (intelligenceResult && intelligenceResult.payload) || {};
  const investigation = payload.investigation || {};
  const coverageMetrics =
    payload.coverageMetrics ||
    (payload.discoveryReport && payload.discoveryReport.coverageMetrics) ||
    investigation.coverageMetrics ||
    null;
  const candidateUniverse =
    payload.candidateUniverse ||
    investigation.candidateUniverse ||
    [];
  const qualifiedCount =
    payload.qualifiedCount != null
      ? Number(payload.qualifiedCount)
      : Array.isArray(payload.opportunities) && Array.isArray(payload.fitCandidates)
        ? payload.opportunities.length +
          payload.fitCandidates.length +
          (Array.isArray(payload.watchCandidates)
            ? payload.watchCandidates.filter((row) => row.qualified === true).length
            : 0)
        : Array.isArray(payload.opportunities)
          ? payload.opportunities.length
          : investigation.qualifiedCount != null
            ? Number(investigation.qualifiedCount)
            : 0;

  stages.push(
    buildStage(DISCOVERY_PIPELINE_STAGES.EXECUTE_COVERAGE_PLAN, {
      startedAt: executeStarted,
      output: {
        discoveryRan: investigation.discoveryRan !== false,
        candidatesDiscovered: investigation.candidatesDiscovered || candidateUniverse.length,
        sourceTypesChecked: investigation.sourceTypesChecked || [],
        coverageEngineUsed: true,
      },
    })
  );

  const investigatedCount = candidateUniverse.filter((row) => row.dedupeStatus !== 'duplicate').length;
  let revisedUniverseEstimate = reviseCandidateUniverseEstimate(universeEstimate, {
    investigated: investigatedCount,
    discovered: investigatedCount,
    coverageMetrics,
    coverageComplete: Boolean(coverageMetrics && coverageMetrics.complete),
  });
  const coveragePct = computeCoverageFromEstimate(investigatedCount, revisedUniverseEstimate);
  stages.push(
    buildStage(DISCOVERY_PIPELINE_STAGES.MEASURE_COVERAGE, {
      output: {
        coverageMetrics,
        coveragePct,
        investigated: investigatedCount,
        estimatedUniverse: extractExpectedValue(revisedUniverseEstimate),
        universeEstimate: revisedUniverseEstimate,
        discoveryStatus: discoveryStatusFromCoverage(coverageMetrics),
      },
    })
  );

  const sufficiency = {
    sufficient: gapAnalysis.sufficient,
    coverageComplete: Boolean(coverageMetrics && coverageMetrics.complete),
    canConcludeEmpty: canConcludeEmptyUniverse(coverageMetrics, qualifiedCount),
    gapAnalysis,
  };
  stages.push(
    buildStage(DISCOVERY_PIPELINE_STAGES.DETERMINE_SUFFICIENCY, {
      output: sufficiency,
    })
  );

  const discoveryConfidence =
    payload.discoveryConfidence ||
    computeDiscoveryConfidence({
      coverage: coverageMetrics,
      candidateUniverse,
      searchSuccess:
        coverageMetrics && coverageMetrics.searches && coverageMetrics.searches.planned
          ? (coverageMetrics.searches.executed || 0) / coverageMetrics.searches.planned
          : qualifiedCount > 0
            ? 0.6
            : 0.3,
      evidenceQuality: existingIntelligence.companyCount ? 0.65 : 0.35,
    });

  // SPEC-159 — Investigative Reasoning Loop (ADR-079: understanding before recommendation)
  let investigationState = null;
  let missionIntelligenceReport = null;
  if (opts.useInvestigativeReasoningLoop !== false) {
    const reasoningResult = await runInvestigativeReasoningLoop({
      mission,
      tenantId,
      marketDefinition,
      universeEstimate: revisedUniverseEstimate,
      existingIntelligence,
      memory: opts.memory || opts.investigationMemory || {},
      priorOutcomeLearnings: opts.priorOutcomeLearnings || [],
      investigationState:
        payload.investigationState ||
        (payload.investigation && payload.investigation.investigationState) ||
        null,
      candidateInvestigation: payload.candidateInvestigation || null,
      coverageResult: {
        candidates: candidateUniverse,
        searchHypotheses:
          payload.searchHypotheses ||
          (payload.investigationReport && payload.investigationReport.hypotheses) ||
          [],
        coverage: coverageMetrics,
        revisedMarketDefinition:
          payload.revisedMarketDefinition ||
          (payload.discoveryReport && payload.discoveryReport.revisedMarketDefinition) ||
          marketDefinition,
      },
      opts,
    });
    investigationState = reasoningResult.state;
    missionIntelligenceReport = reasoningResult.report;
    if (reasoningResult.learningInfluence?.length) {
      missionIntelligenceReport = {
        ...missionIntelligenceReport,
        priorLearningInfluence: reasoningResult.learningInfluence,
      };
    }
    if (reasoningResult.state?.marketDefinition?.revised) {
      marketDefinition = reasoningResult.state.marketDefinition;
    }
    if (reasoningResult.state?.universeEstimate) {
      revisedUniverseEstimate = reasoningResult.state.universeEstimate;
    }
  }

  if (missionIntelligenceReport && marketMemoryRecall?.loaded) {
    const marketChanges = detectMarketChanges(marketMemoryRecall.marketMemory, {
      entities: missionIntelligenceReport.businessUnderstanding?.items?.map((item) => ({
        entityId: item.entity,
        name: item.entity,
        currentUnderstanding: {
          assertions: item.assertions || [],
          confidence: item.confidence || 0,
        },
        buyingSignalHistory: [],
      })) || [],
      confidence: investigationState?.confidence || discoveryConfidence?.overall || 0,
      marketUnderstanding: {
        dominantTerminology: marketDefinition.terminology || [],
      },
    });
    missionIntelligenceReport = {
      ...missionIntelligenceReport,
      ...buildMarketChangesSection(marketChanges, {
        outstandingUnknowns: missionIntelligenceReport.remainingUnknowns || [],
      }),
    };
  }

  let intelligenceReport =
    payload.discoveryReport ||
    buildDiscoveryReport({
      coverage: coverageMetrics || {},
      candidateUniverse,
      qualifiedCount,
      discoveryConfidence,
      universeEstimate: revisedUniverseEstimate,
      investigated: investigatedCount,
      coveragePct,
      marketDefinition,
      revisedMarketDefinition: marketDefinition.revised ? marketDefinition : undefined,
    });

  if (missionIntelligenceReport) {
    intelligenceReport = mergeIntoDiscoveryReport(intelligenceReport, missionIntelligenceReport);
  }

  let explainabilityGraph = null;
  if (investigationState || missionIntelligenceReport) {
    explainabilityGraph = buildExplainabilityGraph({
      mission,
      investigationState,
      plan: investigationPlan,
      missionIntelligenceReport,
    });
  }

  stages.push(
    buildStage(DISCOVERY_PIPELINE_STAGES.PRODUCE_INTELLIGENCE_REPORT, {
      output: intelligenceReport,
      explainabilityGraph: explainabilityGraph ? serializeGraph(explainabilityGraph) : null,
    })
  );

  const emptyMarketDecision = canConcludeEmptyUniverse(coverageMetrics, qualifiedCount);
  const outcome = mapIntelligenceStatus(intelligenceResult.status);
  const pipelineConfidence =
    (investigationState && investigationState.confidence) ||
    (discoveryConfidence.overall != null ? discoveryConfidence.overall : intelligenceResult.confidence);

  const pipelineResult = buildPipelineResult({
    outcome,
    stages,
    marketDefinition,
    universeEstimate: revisedUniverseEstimate,
    initialUniverseEstimate: universeEstimate,
    coveragePlan,
    coverageMetrics,
    coveragePct,
    intelligenceReport,
    investigationState,
    missionIntelligenceReport,
    emptyMarketDecision,
    confidence: pipelineConfidence,
    discoveryConfidence,
    sufficiency,
    gapAnalysis,
    existingIntelligence,
    intelligenceResult,
    qualifiedCount,
    blockReason: intelligenceResult.summary || null,
    coverageEngineUsed: true,
    marketMemoryRecall,
    memoryLoaded: Boolean(marketMemoryRecall?.loaded),
    explainabilityGraph: explainabilityGraph ? serializeGraph(explainabilityGraph) : null,
    investigationPlan,
    learningInfluence:
      (missionIntelligenceReport && missionIntelligenceReport.priorLearningInfluence) || [],
    priorOutcomeLearnings: opts.priorOutcomeLearnings || [],
  });

  let marketMemoryPersist = null;
  if (opts.persistMemory !== false && tenantId) {
    marketMemoryPersist = await persistDiscoveryKnowledge(pipelineResult, {
      tenantId,
      missionId: opts.amoMissionId || opts.missionId || mission.id,
      mission,
      priorMarketMemory: marketMemoryRecall?.marketMemory || loadedMemory?.memory?.market || null,
      completedAt: nowIso(),
      store: opts.memoryStore || opts.store,
      opts,
    });
    pipelineResult.marketMemoryPersist = marketMemoryPersist;
  }

  return pipelineResult;
}

module.exports = {
  runDiscoveryPipeline,
  estimateUniverseFromPlan,
  computeCoveragePct,
  buildPipelineResult,
  estimateCandidateUniverse,
  reviseCandidateUniverseEstimate,
  computeCoverageFromEstimate,
};
