'use strict';

/**
 * SPEC-123 + SPEC-154 — Unified Scout Discovery.
 *
 * Canonical contract: Scout.discover()
 * All discovery executes DiscoveryPipeline (CoverageEngine mandatory).
 * Investigation is internal implementation — not a separate operator concept.
 */

const { BUILTIN_IDS } = require('../capabilities/types');
const { MISSION_STATUS } = require('../mission-engine/types');
const {
  buildDiscoveryExecutionReport,
  emitDiscoveryAuditEvents,
} = require('../mission-engine/discoveryExecutionReport');
const ScoutDiscoveryAudit = require('../mission-engine/ScoutDiscoveryAudit');
const { selectDiscoveryStrategy, buildDelegationFromMission } = require('./Discovery.helpers');
const { runDiscoveryPipeline } = require('./DiscoveryPipeline');
const {
  DISCOVERY_PHASES,
  DISCOVERY_PIPELINE_STAGES,
  DISCOVERY_OUTCOMES,
  buildDiscoveryResult,
} = require('./types');
const {
  emitDiscoveryStarted,
  emitDiscoveryPhase,
  emitGapAnalysis,
  emitExternalDiscovery,
  emitVerification,
  emitEnrichment,
  emitRanking,
  emitDiscoveryCompleted,
} = require('./observability');
const {
  resolveScoutDiscoveryRuntimePolicy,
  assertMissionRuntimeBoundary,
  RUNTIME_OWNERS,
} = require('../acquisition-mission/MissionRuntimeOwnership');

function findDiscoveryStepIndex(mission) {
  const steps = (mission.plan && mission.plan.steps) || [];
  return steps.findIndex(
    (s) =>
      s.stageId === 'prospect_discovery' ||
      s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY ||
      s.capabilityId === 'prospect_discovery'
  );
}

function mapPipelineStrategy(gapAnalysis, existing) {
  return selectDiscoveryStrategy(gapAnalysis, existing);
}

function pipelineStagesToLegacyPhases(pipelineStages = []) {
  const legacy = [];
  const hasStage = (name) => pipelineStages.some((row) => row.stage === name);

  for (const row of pipelineStages) {
    if (row.stage === DISCOVERY_PIPELINE_STAGES.UNDERSTAND_MARKET) {
      legacy.push({
        phase: DISCOVERY_PHASES.EXISTING_INTELLIGENCE,
        startedAt: row.startedAt,
        completedAt: row.completedAt,
        result: { marketDefinition: row.output, consulted: true },
      });
    }
    if (row.stage === DISCOVERY_PIPELINE_STAGES.DETERMINE_SUFFICIENCY) {
      legacy.push({
        phase: DISCOVERY_PHASES.GAP_ANALYSIS,
        startedAt: row.startedAt,
        completedAt: row.completedAt,
        result: row.output,
      });
    }
    if (row.stage === DISCOVERY_PIPELINE_STAGES.EXECUTE_COVERAGE_PLAN) {
      legacy.push({
        phase: DISCOVERY_PHASES.EXTERNAL_DISCOVERY,
        startedAt: row.startedAt,
        completedAt: row.completedAt,
        result: { executed: !row.error, coverageEngineUsed: true, ...(row.output || {}) },
      });
    }
    if (row.stage === DISCOVERY_PIPELINE_STAGES.PRODUCE_INTELLIGENCE_REPORT) {
      legacy.push(
        { phase: DISCOVERY_PHASES.VERIFICATION, completedAt: row.completedAt },
        { phase: DISCOVERY_PHASES.ENRICHMENT, completedAt: row.completedAt },
        { phase: DISCOVERY_PHASES.RANKING, completedAt: row.completedAt, result: row.output }
      );
    }
  }

  if (!hasStage(DISCOVERY_PIPELINE_STAGES.DETERMINE_SUFFICIENCY)) {
    legacy.push({
      phase: DISCOVERY_PHASES.GAP_ANALYSIS,
      completedAt: new Date().toISOString(),
    });
  }
  if (!hasStage(DISCOVERY_PIPELINE_STAGES.EXECUTE_COVERAGE_PLAN)) {
    legacy.push({
      phase: DISCOVERY_PHASES.EXTERNAL_DISCOVERY,
      completedAt: new Date().toISOString(),
      result: { executed: false, skipped: true, coverageEngineUsed: true },
    });
  }

  legacy.push({
    phase: DISCOVERY_PHASES.MISSION_UPDATE,
    completedAt: new Date().toISOString(),
  });
  return legacy;
}

/**
 * Sync mission store with pipeline results — replaces prospect_discovery capability path.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function syncMissionFromPipeline(input) {
  const { mission, missionEngine, pipelineResult, operatorId } = input;
  if (!missionEngine || !mission) return mission;

  const discoveryIdx = findDiscoveryStepIndex(mission);
  const steps = (mission.plan && mission.plan.steps) || [];
  const prospectCount = pipelineResult.qualifiedCount || 0;
  const stepResult = {
    capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
    stageId: 'prospect_discovery',
    status: 'completed',
    outcome: pipelineResult.outcome,
    result: {
      status: pipelineResult.outcome === DISCOVERY_OUTCOMES.BLOCKED ? 'blocked' : 'completed',
      outputs: {
        prospectCount,
        prospects: [],
        companies: (pipelineResult.intelligenceResult &&
          pipelineResult.intelligenceResult.payload &&
          pipelineResult.intelligenceResult.payload.companies) ||
          [],
        summary: {
          discovered: prospectCount,
          coveragePct: pipelineResult.coveragePct,
        },
        discoveryProfile: pipelineResult.marketDefinition &&
          pipelineResult.marketDefinition.searchDefinition,
      },
      errors: pipelineResult.blockReason ? [pipelineResult.blockReason] : [],
    },
  };

  const updatedSteps =
    discoveryIdx >= 0
      ? steps.map((s, idx) =>
          idx === discoveryIdx
            ? { ...s, status: 'completed', error: undefined }
            : s
        )
      : steps;

  const deliverables = {
    ...(mission.deliverables || {}),
    stepResults: [
      ...((mission.deliverables && mission.deliverables.stepResults) || []).filter(
        (s) =>
          s.capabilityId !== BUILTIN_IDS.PROSPECT_DISCOVERY &&
          s.capabilityId !== 'prospect_discovery'
      ),
      stepResult,
    ],
  };

  const nextStatus =
    mission.status === MISSION_STATUS.PLANNING || mission.status === MISSION_STATUS.REQUESTED
      ? MISSION_STATUS.EXECUTING
      : mission.status;

  return missionEngine.store.update({
    id: mission.id,
    status: nextStatus,
    plan: discoveryIdx >= 0 ? { ...mission.plan, steps: updatedSteps } : mission.plan,
    deliverables,
    review: null,
    progress: {
      ...(mission.progress || {}),
      currentStage: 'Discovery',
      currentCapabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
    },
    ...(operatorId ? { lastOperatorId: operatorId } : {}),
  });
}

/**
 * Canonical Scout Discovery contract (SPEC-123 / SPEC-154).
 *
 * @param {object} input
 * @param {object} input.mission
 * @param {import('../mission-engine/MissionEngine').MissionEngine} [input.missionEngine]
 * @param {object} [input.scoutPayload]
 * @param {string} [input.operatorId]
 * @param {string} [input.message]
 * @param {object} [input.opts]
 * @returns {Promise<object>}
 */
async function discover(input) {
  const { mission, missionEngine, scoutPayload = {}, operatorId, message, opts = {} } = input;
  if (!mission) {
    throw new Error('Scout.discover requires mission');
  }

  const missionId = mission.id;
  const tenantId = String(mission.tenantId || mission.clientId || '');
  const delegation = opts.delegation || buildDelegationFromMission(mission, scoutPayload);

  emitDiscoveryStarted({
    missionId,
    objective: scoutPayload.objective || mission.objectiveText || mission.title,
    tenantId,
  });

  const pipelineResult = await runDiscoveryPipeline({
    mission,
    delegation,
    scoutPayload,
    opts: {
      ...opts,
      amoMissionId: opts.amoMissionId || opts.missionId,
      tenantId,
    },
  });

  const phases = pipelineStagesToLegacyPhases(pipelineResult.stages);
  for (const phaseRow of phases) {
    emitDiscoveryPhase(phaseRow.phase, { missionId, result: phaseRow.result || null });
  }

  const strategy = mapPipelineStrategy(
    pipelineResult.gapAnalysis || { sufficient: false, shouldDiscoverGap: true, freshCount: 0 },
    {
      companies: new Array((pipelineResult.existingIntelligence || {}).companyCount || 0),
    }
  );

  emitGapAnalysis({
    missionId,
    strategy,
    existingCompanyCount: (pipelineResult.existingIntelligence || {}).companyCount || 0,
    existingProspectCount: (pipelineResult.existingIntelligence || {}).prospectCount || 0,
    freshCount: (pipelineResult.gapAnalysis || {}).freshCount || 0,
    relevantCount: (pipelineResult.gapAnalysis || {}).relevantCount || 0,
    shouldDiscoverGap: (pipelineResult.gapAnalysis || {}).shouldDiscoverGap,
    sufficient: (pipelineResult.gapAnalysis || {}).sufficient,
  });

  emitExternalDiscovery({
    missionId,
    strategy,
    skipped: false,
    coverageEngineUsed: true,
  });
  emitVerification({ missionId, strategy, source: 'discovery_pipeline' });
  emitEnrichment({ missionId, strategy, source: 'discovery_pipeline' });
  emitRanking({ missionId, strategy, source: 'discovery_pipeline' });

  const runtimePolicy = resolveScoutDiscoveryRuntimePolicy({ mission, missionEngine, opts });

  if (runtimePolicy.amoOwned && missionEngine) {
    assertMissionRuntimeBoundary({
      mission,
      missionEngine,
      expectedOwner: RUNTIME_OWNERS.AMO,
      operation: 'sync Scout discovery into Mission Engine',
    });
  }

  let updatedMission = mission;
  if (runtimePolicy.syncToMissionEngine) {
    updatedMission = await syncMissionFromPipeline({
      mission,
      missionEngine,
      pipelineResult,
      operatorId,
    });
  }

  const scoutDiscoveryMeta = {
    strategy,
    gapAnalysis: pipelineResult.gapAnalysis,
    existingIntelligence: pipelineResult.existingIntelligence,
    intelligenceResult: pipelineResult.intelligenceResult,
    pipeline: {
      stages: pipelineResult.stages,
      marketDefinition: pipelineResult.marketDefinition,
      universeEstimate: pipelineResult.universeEstimate,
      coveragePlan: pipelineResult.coveragePlan,
      coveragePct: pipelineResult.coveragePct,
      emptyMarketDecision: pipelineResult.emptyMarketDecision,
      confidence: pipelineResult.confidence,
    },
    phases,
    capabilityPath: 'scout.discover',
    scoutAcquisitionPathInvoked: Boolean(pipelineResult.intelligenceResult),
    coverageEngineUsed: true,
  };

  const discoveryReport = buildDiscoveryExecutionReport(
    updatedMission,
    scoutPayload,
    scoutDiscoveryMeta
  );
  discoveryReport.missionStatus = updatedMission.status;
  discoveryReport.pipeline = scoutDiscoveryMeta.pipeline;
  discoveryReport.coveragePct = pipelineResult.coveragePct;
  discoveryReport.emptyMarketDecision = pipelineResult.emptyMarketDecision;
  discoveryReport.intelligenceReport = pipelineResult.intelligenceReport;

  emitDiscoveryAuditEvents(discoveryReport, ScoutDiscoveryAudit);
  ScoutDiscoveryAudit.logMissionDiscoveryResponse({
    missionId: discoveryReport.missionId,
    discoveryStrategy: discoveryReport.discoveryStrategy,
    evidenceSources: discoveryReport.evidenceSources,
    outcome: discoveryReport.outcome,
    blockReason: discoveryReport.blockReason,
    operatorResponseKind: 'mission_execution_outcome',
  });

  // ADR-089 — AMO-owned discovery commits through TME; legacy attach is for non-AMO bridges only.
  if (runtimePolicy.attachViaLegacyFacade) {
    try {
      const { attachScoutDiscovery } = require('../../services/acquisitionMission');
      await attachScoutDiscovery(
        {
          missionId: opts.amoMissionId || opts.missionId,
          tenantId,
          clientId: mission.clientId,
        },
        pipelineResult.intelligenceResult ||
          { payload: { companies: [], prospects: [] } },
        opts
      );
    } catch {
      // AMO attach is best-effort when no AMO mission exists
    }
  }

  emitDiscoveryCompleted({
    missionId,
    outcome: discoveryReport.outcome,
    strategy: discoveryReport.discoveryStrategy,
    prospectCount: discoveryReport.prospectCount,
    blockReason: discoveryReport.blockReason,
  });

  return {
    ...buildDiscoveryResult({
      outcome: pipelineResult.outcome,
      strategy,
      blockReason: pipelineResult.blockReason || discoveryReport.blockReason,
      phases,
      gapAnalysis: pipelineResult.gapAnalysis,
      existingIntelligence: pipelineResult.existingIntelligence,
      prospectCount: pipelineResult.qualifiedCount || discoveryReport.prospectCount,
      companies:
        (pipelineResult.intelligenceResult &&
          pipelineResult.intelligenceResult.payload &&
          pipelineResult.intelligenceResult.payload.companies) ||
        [],
      confidence: pipelineResult.confidence,
      mission: updatedMission,
      discoveryReport,
    }),
    pipeline: pipelineResult,
    intelligenceResult: pipelineResult.intelligenceResult,
    marketDefinition: pipelineResult.marketDefinition,
    universeEstimate: pipelineResult.universeEstimate,
    coveragePlan: pipelineResult.coveragePlan,
    coveragePct: pipelineResult.coveragePct,
    intelligenceReport: pipelineResult.intelligenceReport,
    emptyMarketDecision: pipelineResult.emptyMarketDecision,
  };
}

module.exports = {
  discover,
  buildDelegationFromMission,
  selectDiscoveryStrategy,
  findDiscoveryStepIndex,
  syncMissionFromPipeline,
  runDiscoveryPipeline,
};
