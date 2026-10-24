'use strict';
const { withResearchProviderScope, assertResearchProviderAllowed, isResearchProviderControlError, rethrowResearchProviderControl } = require('../../../utils/anchorResearchProviderPause');

/**
 * Canonical Scout discovery execution via SEC (SPEC-077).
 *
 * Mission → TME → buildExecutionInput → executeSpecialist('scout') → Scout.discover()
 */

const amo = require('../../acquisition-mission');
const { Scout } = require('../../scout');
const {
  SPECIALISTS,
  buildExecutionInput,
  executeSpecialist,
  createExecutionResult,
  EXECUTION_STATUSES,
  fromScoutLegacyOutput,
} = amo;
const { buildMissionExecutionContext } = require('../../acquisition-mission/MissionExecutionContext');
const { scoutDelegationFromMission } = require('../../acquisition-mission/SpecialistInputs');
const {
  buildInvestigationContinuationContext,
  extractPayloadFromDiscoveryContribution,
} = require('../../scout/investigation/EntityInvestigationContinuation');
const {
  mapSecPriorLearningToOutcomeLearnings,
} = require('../../scout/investigation/PriorLearningInfluence');
const {
  buildScoutDiscoveryArtifact,
  assertScoutEvidenceHandoff,
} = require('../../scout/adapters/ScoutDiscoveryArtifact');
const { normalizeScoutDiscoveryPayload } = require('../../acquisition-mission/DiscoveryPayload');
const { findLatestScoutDiscovery } = require('./MaxPrioritizationExecutor');

function mapScoutIntelligenceToDiscoveryPayload(result = {}, opts = {}) {
  const artifact = buildScoutDiscoveryArtifact(result, {
    missionObjective: opts.missionObjective,
    approvalConsumed: true,
  });
  const payload = normalizeScoutDiscoveryPayload(result, {
    ...opts,
    discoveryArtifact: artifact,
  });
  assertScoutEvidenceHandoff(artifact, payload);
  return payload;
}

function buildScoutDiscoverOpts(mission, executionInput = {}, opts = {}) {
  const delegation = scoutDelegationFromMission(mission);
  const executionContext =
    executionInput.executionContext ||
    buildMissionExecutionContext({
      engine: opts.engine,
      mission,
      tenantId: delegation.tenantId,
      transactionId: executionInput.transactionId || opts.transactionId,
      pool: opts.pool,
    });

  if (opts.executionRequest) {
    executionContext.executionRequest = opts.executionRequest;
  }

  const priorLearning = executionInput.memoryContext?.priorLearning || [];
  const priorOutcomeLearnings = mapSecPriorLearningToOutcomeLearnings(priorLearning);

  let scoutOpts = {
    ...opts,
    delegation,
    executionContext,
    mode: opts.scoutMode || 'completed',
    missionId: mission.id,
    amoMissionId: mission.id,
    runtimeOwner: 'amo',
    attachScoutDiscovery: false,
    tenantId: delegation.tenantId,
    companies: opts.scoutCompanies,
    people: opts.scoutPeople,
    discover: opts.discover,
    enablePlaces: opts.enablePlaces,
    placesProvider: opts.placesProvider,
    allowFixtureFallback: opts.allowFixtureFallback,
    priorOutcomeLearnings,
    priorLearningRetrievalWarning:
      executionInput.memoryContext?.priorLearningRetrievalWarning || null,
    // SPEC-143 memory store only. AMO engine.store is the mission/outcome-learning
    // store and does not implement loadForMarket — never inject it here.
    store: opts.memoryStore || opts.store,
    memoryStore: opts.memoryStore || opts.store,
  };

  if (opts.investigationContinuation === true && opts.engine) {
    const snapshot = opts.engine.inspect(mission.id, { tenantId: delegation.tenantId });
    const priorDiscovery = findLatestScoutDiscovery(snapshot.contributions || [], mission);
    const priorPayload = extractPayloadFromDiscoveryContribution(priorDiscovery || {});
    const continuation = buildInvestigationContinuationContext({
      priorPayload,
      opts: {
        ...opts,
        investigationContinuation: true,
        question: opts.question,
      },
    });
    scoutOpts = {
      ...scoutOpts,
      investigationContinuation: true,
      investigationMode: continuation.investigationMode,
      priorDiscoveryPayload: continuation.priorDiscoveryPayload,
      preservedCandidates: continuation.preservedCandidates,
      entityInvestigationContinuation:
        continuation.investigationMode === 'entity_continuation',
    };
  }

  return scoutOpts;
}

function normalizeConfidenceForSec(value) {
  if (value && typeof value === 'object' && value.overall != null) {
    return value;
  }
  const overall = Number(value);
  if (Number.isFinite(overall)) {
    return { overall, evidence: overall, fit: overall, completeness: overall };
  }
  return { overall: 0.5, evidence: 0.5, fit: 0.5, completeness: 0.5 };
}

function buildScoutExecutionResult({
  scoutResult,
  discoveryPayload,
  transactionId,
  priorLearningRetrievalWarning,
}) {
  const learningInfluence =
    scoutResult.learningInfluence ||
    (scoutResult.pipeline && scoutResult.pipeline.learningInfluence) ||
    (discoveryPayload.missionIntelligenceReport &&
      discoveryPayload.missionIntelligenceReport.priorLearningInfluence) ||
    [];

  const blocked =
    discoveryPayload.blocked === true || /blocked/i.test(String(discoveryPayload.outcome || ''));
  const status = blocked ? EXECUTION_STATUSES.BLOCKED : EXECUTION_STATUSES.SUCCESS;
  const unknowns = (discoveryPayload.unknowns || []).map((row) =>
    typeof row === 'string' ? { unknown: row, reason: 'Discovery unknown.' } : row
  );

  if (priorLearningRetrievalWarning) {
    unknowns.push({
      unknown: 'Prior OutcomeLearning retrieval',
      reason: priorLearningRetrievalWarning,
    });
  }

  const legacy = fromScoutLegacyOutput(
    { ...scoutResult, discoveryPayload, payload: discoveryPayload },
    { specialist: SPECIALISTS.SCOUT, transactionId }
  );

  return createExecutionResult({
    ...legacy,
    status,
    learningInfluence,
    unknowns: unknowns.length ? unknowns : legacy.unknowns,
    explainability: {
      ...(legacy.explainability || {}),
      learningInfluence,
    },
  });
}

async function runScoutDiscovery(executionInput = {}, opts = {}) {
  const mission =
    executionInput.mission ||
    (executionInput.executionContext && executionInput.executionContext.mission) ||
    null;
  if (!mission) {
    return createExecutionResult({
      specialist: SPECIALISTS.SCOUT,
      transactionId: executionInput.transactionId,
      status: EXECUTION_STATUSES.BLOCKED,
      reason: 'Mission is required for Scout discovery.',
      requiredPrecondition: 'mission',
    });
  }

  const scoutOpts = buildScoutDiscoverOpts(mission, executionInput, opts);
  const discoverImpl = typeof opts.discoverImpl === 'function' ? opts.discoverImpl : Scout.discover.bind(Scout);
  const inventoryService = require('../../../services/acquisitionMissionInventory');
  const existing = typeof opts.discoverImpl === 'function' ? null
    : (await inventoryService.discoverGovernedInventory(mission, opts)
      || await inventoryService.discoverKnowledgeInventory(mission, opts));
  const scoutResult = existing || await discoverImpl({
    mission,
    missionEngine: null,
    scoutPayload: {},
    operatorId: opts.operatorId,
    opts: scoutOpts,
  });

  const discoveryPayload = mapScoutIntelligenceToDiscoveryPayload(scoutResult, {
    missionObjective: mission.objective,
  });

  return buildScoutExecutionResult({
    scoutResult,
    discoveryPayload,
    transactionId: executionInput.transactionId,
    priorLearningRetrievalWarning:
      executionInput.memoryContext?.priorLearningRetrievalWarning || null,
  });
}

const existingCrmRunners = new WeakSet();
function createExistingCrmScoutRunner(scoutResult) {
  // No caller-supplied function is registered. This closure can only normalize
  // already-materialized CRM evidence; its identity cannot be forged by JSON.
  if (scoutResult?.payload?.source !== 'explicit_approved_existing_crm'
    || !Array.isArray(scoutResult.payload.fitCandidates) || scoutResult.payload.fitCandidates.length < 1
    || scoutResult.payload.fitCandidates.length > 5) {
    throw Object.assign(new Error('existing_crm_discovery_required'), { code: 'existing_crm_discovery_required' });
  }
  const data = JSON.parse(JSON.stringify(scoutResult));
  const runner = (mission, opts) => runExistingCrmScoutForAmoMission(mission, data, opts);
  existingCrmRunners.add(runner);
  return runner;
}
async function runScoutForAmoMission(mission, opts = {}) {
  const runner = opts.runScout;
  if (existingCrmRunners.has(runner)) return runner(mission, opts);
  return withResearchProviderScope(mission.tenantId ?? mission.clientId, () => {
    if (mission.clientId != null) assertResearchProviderAllowed('research', mission.clientId);
    return runScoutForAmoMissionCanonical(mission, opts);
  });
}

// A separate data-only adapter, not a skip-pause flag. This function never
// evaluates discovery/provider callbacks or inventory fallbacks. Its caller is
// the locked, explicitly authorized existing-CRM preparation bridge.
async function runExistingCrmScoutForAmoMission(mission, scoutResult, opts = {}) {
  if (String(mission?.tenantId) !== '10' || String(mission?.clientId) !== '10'
    || mission.crmPreparation?.preparationOnly !== true || mission.crmPreparation?.providerCalls !== 0
    || scoutResult?.payload?.source !== 'explicit_approved_existing_crm') {
    throw Object.assign(new Error('existing_crm_discovery_required'), { code: 'existing_crm_discovery_required' });
  }
  const data = JSON.parse(JSON.stringify(scoutResult));
  const canonical = Object.fromEntries(['engine', 'pool', 'contributions', 'operatorId', 'transactionId', 'executionRequest']
    .filter(key => Object.hasOwn(opts, key)).map(key => [key, opts[key]]));
  return runScoutForAmoMissionCanonical(mission, { ...canonical, allowFixtureFallback: false, treatErrorsAsBlocked: false }, data);
}

async function runScoutForAmoMissionCanonical(mission, opts = {}, existingCrmResult) {
  if (typeof opts.runScout === 'function') {
    return opts.runScout(mission, opts);
  }

  const tenantId = String(mission.tenantId || mission.clientId || '');
  const contributions =
    opts.contributions ||
    (opts.engine && typeof opts.engine.inspect === 'function'
      ? (opts.engine.inspect(mission.id, { tenantId }).contributions || [])
      : []);

  const executionContext = buildMissionExecutionContext({
    engine: opts.engine,
    mission,
    tenantId,
    transactionId: opts.transactionId,
    pool: opts.pool,
  });
  if (opts.executionRequest) {
    executionContext.executionRequest = opts.executionRequest;
  }

  const input = buildExecutionInput({
    mission,
    specialist: SPECIALISTS.SCOUT,
    contributions,
    transactionId: opts.transactionId,
    executionContext,
    observations: opts.observations || [],
    store: opts.engine?.store,
  });

  try {
    let providerControlError = null;
    const executionResult = await executeSpecialist({
      mission,
      specialist: SPECIALISTS.SCOUT,
      contributions,
      transactionId: opts.transactionId,
      store: opts.engine?.store,
      run: async (secInput) => {
        try {
          if (existingCrmResult !== undefined) {
            return buildScoutExecutionResult({ scoutResult: existingCrmResult,
              discoveryPayload: mapScoutIntelligenceToDiscoveryPayload(existingCrmResult, { missionObjective: mission.objective }),
              transactionId: secInput.transactionId,
              priorLearningRetrievalWarning: secInput.memoryContext?.priorLearningRetrievalWarning || null });
          }
          return await runScoutDiscovery({ ...secInput, mission }, opts);
        }
        catch (error) {
          if (isResearchProviderControlError(error)) providerControlError = error;
          throw error;
        }
      },
      treatErrorsAsBlocked: opts.treatErrorsAsBlocked !== false,
    });

    // SEC intentionally converts ordinary exceptions to blocked/failed results.
    // A provider control must survive either mode before any fixture fallback.
    if (providerControlError) throw providerControlError;
    const controlCode = executionResult.blocked?.requiredPrecondition;
    if (controlCode) rethrowResearchProviderControl(Object.assign(new Error(String(controlCode)), { code: controlCode }));

    const discoveryPayload =
      executionResult.contributions && Object.keys(executionResult.contributions).length
        ? executionResult.contributions
        : null;

    if (
      opts.allowFixtureFallback === true &&
      discoveryPayload &&
      (discoveryPayload.blocked || discoveryPayload.qualifiedCount <= 0)
    ) {
      const fixture = opts.fixtureScoutDiscoveryResult;
      if (typeof fixture === 'function') return fixture();
    }

    if (executionResult.status === EXECUTION_STATUSES.BLOCKED
      || executionResult.status === EXECUTION_STATUSES.FAILED) {
      const blockedReason = executionResult.blocked?.reason;
      const auditReason = executionResult.audit?.reason;
      const specificReason =
        (blockedReason && !/^execution blocked\.?$/i.test(String(blockedReason).trim()))
          ? blockedReason
          : auditReason ||
            executionResult.blocked?.recommendedAction ||
            executionResult.recommendations?.[0]?.text ||
            'Scout discovery blocked.';
      const err = new Error(specificReason);
      err.code = executionResult.blocked?.requiredPrecondition || 'scout_blocked';
      if (opts.allowFixtureFallback === true && typeof opts.fixtureScoutDiscoveryResult === 'function') {
        return opts.fixtureScoutDiscoveryResult();
      }
      throw err;
    }

    const scoutResult = {
      status: executionResult.status === EXECUTION_STATUSES.SUCCESS ? 'completed' : executionResult.status,
      confidence: normalizeConfidenceForSec(executionResult.confidence).overall,
      payload: discoveryPayload,
      intelligenceResult: discoveryPayload
        ? { payload: discoveryPayload, status: 'completed' }
        : null,
      learningInfluence: executionResult.explainability?.learningInfluence || [],
      executionResult,
    };

    return scoutResult;
  } catch (err) {
    rethrowResearchProviderControl(err);
    if (opts.allowFixtureFallback === true && typeof opts.fixtureScoutDiscoveryResult === 'function') {
      return opts.fixtureScoutDiscoveryResult();
    }
    throw err;
  }
}

module.exports = {
  runScoutForAmoMission,
  createExistingCrmScoutRunner,
  runScoutDiscovery,
  buildScoutDiscoverOpts,
  buildScoutExecutionResult,
  mapScoutIntelligenceToDiscoveryPayload,
};
