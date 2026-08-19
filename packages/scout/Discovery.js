'use strict';

/**
 * SPEC-123 — Unified Scout Discovery pipeline.
 *
 * Canonical contract: Scout.discover()
 * Internal phases: Retrieve → Gap Analysis → External Discovery →
 *   Verification → Enrichment → Ranking → Mission Update
 *
 * runScoutAcquisitionIntelligence() and prospect_discovery() are internal
 * implementation details — never operator-visible.
 */

const { BUILTIN_IDS } = require('../capabilities/types');
const { MISSION_STATUS, REVIEW_ACTIONS } = require('../mission-engine/types');
const {
  buildDiscoveryExecutionReport,
  emitDiscoveryAuditEvents,
} = require('../mission-engine/discoveryExecutionReport');
const ScoutDiscoveryAudit = require('../mission-engine/ScoutDiscoveryAudit');
const { loadRepository } = require('../max/scoutAcquisition/ExistingIntelligence');
const { assessExistingSufficiency } = require('../max/scoutAcquisition/CandidateUniverse');
const { buildAcquisitionSearchDefinition } = require('../max/scoutAcquisition/SearchDefinition');
const { runScoutAcquisitionIntelligence } = require('../max/scoutAcquisition/ScoutAdapter');
const {
  DISCOVERY_PHASES,
  DISCOVERY_STRATEGIES,
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

/**
 * Map mission + scoutPayload to acquisition delegation for internal intelligence.
 * @param {object} mission
 * @param {object} [scoutPayload]
 * @returns {object}
 */
function buildDelegationFromMission(mission, scoutPayload = {}) {
  const constraints = mission.constraints || {};
  const plan = (mission.plan && mission.plan.missionPlan) || mission.missionPlan || {};
  return {
    tenantId: String(mission.tenantId || mission.clientId || scoutPayload.tenantId || ''),
    targetContext: {
      geography:
        scoutPayload.geography ||
        constraints.locationHint ||
        (plan.geography && plan.geography.label) ||
        null,
      segments: constraints.vertical ? [constraints.vertical] : [],
      businessType: constraints.vertical || constraints.industry || null,
    },
    businessContext: {
      serviceGeography: scoutPayload.geography || constraints.locationHint || null,
      preferredSegments: constraints.vertical ? [constraints.vertical] : [],
      operatorDirection: scoutPayload.operatorMessage || null,
    },
  };
}

/**
 * Select discovery strategy from gap analysis — internal optimization only.
 * @param {object} gapAnalysis
 * @param {object} existing
 * @returns {string}
 */
function selectDiscoveryStrategy(gapAnalysis, existing) {
  const existingCount = ((existing && existing.companies) || []).length;
  const freshCount = gapAnalysis.freshCount || 0;

  if (existingCount === 0) {
    return DISCOVERY_STRATEGIES.EXTERNAL_HEAVY;
  }
  if (freshCount > 0 && gapAnalysis.shouldDiscoverGap) {
    return DISCOVERY_STRATEGIES.HYBRID;
  }
  if (freshCount > 0 && !gapAnalysis.shouldDiscoverGap) {
    return DISCOVERY_STRATEGIES.RETRIEVE_ONLY;
  }
  if (existingCount > 0 && freshCount === 0) {
    return DISCOVERY_STRATEGIES.VERIFICATION_ONLY;
  }
  return DISCOVERY_STRATEGIES.HYBRID;
}

/**
 * Execute prospect_discovery via MissionExecutor (internal implementation detail).
 * @param {object} input
 * @returns {Promise<object>}
 */
async function executeProspectDiscovery(input) {
  const { mission, missionEngine, discoveryIdx } = input;
  const missionId = mission.id;
  const steps = (mission.plan && mission.plan.steps) || [];

  if (
    mission.status === MISSION_STATUS.PLANNING ||
    mission.status === MISSION_STATUS.REQUESTED
  ) {
    return missionEngine.executor.execute(missionId);
  }

  if (discoveryIdx >= 0) {
    const resetSteps = steps.map((s, idx) => {
      if (idx >= discoveryIdx) {
        return { ...s, status: 'queued', error: undefined };
      }
      return s.status === 'completed'
        ? s
        : { ...s, status: 'completed', error: undefined };
    });

    await missionEngine.store.update({
      id: missionId,
      status: MISSION_STATUS.EXECUTING,
      plan: { ...mission.plan, steps: resetSteps },
      review: null,
    });

    return missionEngine.executor.execute(missionId);
  }

  return missionEngine.review({
    missionId,
    action: REVIEW_ACTIONS.RUN_AGAIN,
    actor: input.operatorId || null,
  });
}

function findDiscoveryStepIndex(mission) {
  const steps = (mission.plan && mission.plan.steps) || [];
  return steps.findIndex(
    (s) =>
      s.stageId === 'prospect_discovery' ||
      s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY ||
      s.capabilityId === 'prospect_discovery'
  );
}

/**
 * Canonical Scout Discovery contract (SPEC-123).
 *
 * @param {object} input
 * @param {object} input.mission
 * @param {import('../mission-engine/MissionEngine').MissionEngine} input.missionEngine
 * @param {object} [input.scoutPayload]
 * @param {string} [input.operatorId]
 * @param {string} [input.message]
 * @param {object} [input.opts]
 * @returns {Promise<object>}
 */
async function discover(input) {
  const { mission, missionEngine, scoutPayload = {}, operatorId, message, opts = {} } = input;
  if (!mission || !missionEngine) {
    throw new Error('Scout.discover requires mission and missionEngine');
  }

  const missionId = mission.id;
  const tenantId = String(mission.tenantId || mission.clientId || '');
  const phases = [];

  emitDiscoveryStarted({
    missionId,
    objective: scoutPayload.objective || mission.objectiveText || mission.title,
    tenantId,
  });

  // ── Phase 1: Existing Intelligence ─────────────────────────────
  emitDiscoveryPhase(DISCOVERY_PHASES.EXISTING_INTELLIGENCE, { missionId });
  phases.push({ phase: DISCOVERY_PHASES.EXISTING_INTELLIGENCE, startedAt: new Date().toISOString() });

  let existing = { companies: [], people: [] };
  let existingError = null;
  const delegation = buildDelegationFromMission(mission, scoutPayload);

  try {
    existing = await loadRepository({
      authorizedTenantId: tenantId,
      tenantId,
      targetContext: delegation.targetContext,
      businessContext: delegation.businessContext,
      loadCompanies: opts.loadCompanies,
      companies: opts.companies,
      people: opts.people,
    });
  } catch (err) {
    existingError = err.message || String(err);
  }

  const existingCompanyCount = (existing.companies || []).length;
  const existingProspectCount = (existing.people || []).length;
  phases[phases.length - 1].completedAt = new Date().toISOString();
  phases[phases.length - 1].result = {
    companies: existingCompanyCount,
    prospects: existingProspectCount,
    error: existingError,
  };

  // ── Phase 2: Gap Analysis ──────────────────────────────────────
  emitDiscoveryPhase(DISCOVERY_PHASES.GAP_ANALYSIS, { missionId });
  phases.push({ phase: DISCOVERY_PHASES.GAP_ANALYSIS, startedAt: new Date().toISOString() });

  const searchDefinition = buildAcquisitionSearchDefinition({ delegation, tenantId });
  const gapAnalysis = assessExistingSufficiency(existing, searchDefinition, opts);
  const strategy = selectDiscoveryStrategy(gapAnalysis, existing);

  emitGapAnalysis({
    missionId,
    strategy,
    existingCompanyCount,
    existingProspectCount,
    freshCount: gapAnalysis.freshCount,
    relevantCount: gapAnalysis.relevantCount,
    shouldDiscoverGap: gapAnalysis.shouldDiscoverGap,
    sufficient: gapAnalysis.sufficient,
  });

  phases[phases.length - 1].completedAt = new Date().toISOString();
  phases[phases.length - 1].result = { strategy, ...gapAnalysis };

  // ── Phase 3: External Discovery (demand-driven) ────────────────
  let updatedMission = mission;
  let intelligenceResult = null;
  const discoveryIdx = findDiscoveryStepIndex(mission);
  const skipExternal =
    strategy === DISCOVERY_STRATEGIES.RETRIEVE_ONLY &&
    gapAnalysis.sufficient &&
    existingCompanyCount > 0;

  if (!skipExternal) {
    emitDiscoveryPhase(DISCOVERY_PHASES.EXTERNAL_DISCOVERY, { missionId, strategy });
    emitExternalDiscovery({ missionId, strategy, skipped: false });
    phases.push({
      phase: DISCOVERY_PHASES.EXTERNAL_DISCOVERY,
      startedAt: new Date().toISOString(),
    });

    updatedMission = await executeProspectDiscovery({
      mission,
      missionEngine,
      discoveryIdx,
      operatorId,
    });

    phases[phases.length - 1].completedAt = new Date().toISOString();
    phases[phases.length - 1].result = { executed: true, capability: 'prospect_discovery' };

    // Hybrid intelligence enrichment (internal — SPEC-100A path)
    if (
      strategy === DISCOVERY_STRATEGIES.HYBRID ||
      strategy === DISCOVERY_STRATEGIES.VERIFICATION_ONLY
    ) {
      try {
        intelligenceResult = await runScoutAcquisitionIntelligence(delegation, {
          ...opts,
          mode: opts.mode || 'completed',
          missionId: opts.amoMissionId || opts.missionId,
        });
      } catch {
        // Intelligence enrichment is best-effort; external discovery is primary
      }
    }
  } else {
    emitExternalDiscovery({ missionId, strategy, skipped: true, reason: 'Existing intelligence sufficient' });
    phases.push({
      phase: DISCOVERY_PHASES.EXTERNAL_DISCOVERY,
      startedAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      result: { executed: false, skipped: true },
    });
  }

  // ── Phases 4-6: Verification, Enrichment, Ranking ──────────────
  // prospect_discovery handles these internally; emit observability markers.
  emitVerification({ missionId, strategy, source: 'prospect_discovery' });
  emitEnrichment({ missionId, strategy, source: 'prospect_discovery' });
  emitRanking({ missionId, strategy, source: 'prospect_discovery' });
  phases.push(
    { phase: DISCOVERY_PHASES.VERIFICATION, completedAt: new Date().toISOString() },
    { phase: DISCOVERY_PHASES.ENRICHMENT, completedAt: new Date().toISOString() },
    { phase: DISCOVERY_PHASES.RANKING, completedAt: new Date().toISOString() }
  );

  // ── Phase 7: Mission Update ──────────────────────────────────────
  emitDiscoveryPhase(DISCOVERY_PHASES.MISSION_UPDATE, { missionId });
  phases.push({ phase: DISCOVERY_PHASES.MISSION_UPDATE, startedAt: new Date().toISOString() });

  const scoutDiscoveryMeta = {
    strategy,
    gapAnalysis,
    existingIntelligence: {
      companyCount: existingCompanyCount,
      prospectCount: existingProspectCount,
      consulted: true,
      error: existingError,
    },
    intelligenceResult,
    phases,
    capabilityPath: 'scout.discover',
    scoutAcquisitionPathInvoked: Boolean(intelligenceResult),
  };

  const discoveryReport = buildDiscoveryExecutionReport(
    updatedMission,
    scoutPayload,
    scoutDiscoveryMeta
  );
  discoveryReport.missionStatus = updatedMission.status;

  emitDiscoveryAuditEvents(discoveryReport, ScoutDiscoveryAudit);
  ScoutDiscoveryAudit.logMissionDiscoveryResponse({
    missionId: discoveryReport.missionId,
    discoveryStrategy: discoveryReport.discoveryStrategy,
    evidenceSources: discoveryReport.evidenceSources,
    outcome: discoveryReport.outcome,
    blockReason: discoveryReport.blockReason,
    operatorResponseKind: 'mission_execution_outcome',
  });

  // AMO mission update (best-effort)
  if (opts.attachScoutDiscovery !== false && (opts.amoMissionId || opts.missionId)) {
    try {
      const { attachScoutDiscovery } = require('../../services/acquisitionMission');
      await attachScoutDiscovery(
        {
          missionId: opts.amoMissionId || opts.missionId,
          tenantId,
          clientId: mission.clientId,
        },
        intelligenceResult || { payload: { companies: existing.companies, prospects: existing.people } },
        opts
      );
    } catch {
      // AMO attach is best-effort when no AMO mission exists
    }
  }

  phases[phases.length - 1].completedAt = new Date().toISOString();
  phases[phases.length - 1].result = {
    outcome: discoveryReport.outcome,
    prospectCount: discoveryReport.prospectCount,
  };

  emitDiscoveryCompleted({
    missionId,
    outcome: discoveryReport.outcome,
    strategy: discoveryReport.discoveryStrategy,
    prospectCount: discoveryReport.prospectCount,
    blockReason: discoveryReport.blockReason,
  });

  return buildDiscoveryResult({
    outcome: discoveryReport.outcome,
    strategy: scoutDiscoveryMeta.strategy || discoveryReport.discoveryStrategy,
    blockReason: discoveryReport.blockReason,
    phases,
    gapAnalysis,
    existingIntelligence: scoutDiscoveryMeta.existingIntelligence,
    prospectCount: discoveryReport.prospectCount,
    mission: updatedMission,
    discoveryReport,
  });
}

module.exports = {
  discover,
  buildDelegationFromMission,
  selectDiscoveryStrategy,
  executeProspectDiscovery,
  findDiscoveryStepIndex,
};
