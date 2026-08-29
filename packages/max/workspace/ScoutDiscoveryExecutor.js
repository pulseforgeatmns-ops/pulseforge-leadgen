'use strict';

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
    store: opts.engine?.store || opts.store,
    memoryStore: opts.memoryStore || opts.engine?.store || opts.store,
  };

  if (opts.investigationContinuation === true && opts.engine) {
    const snapshot = opts.engine.inspect(mission.id, { tenantId: delegation.tenantId });
    const priorDiscovery = findLatestScoutDiscovery(snapshot.contributions || []);
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
  const scoutResult = await discoverImpl({
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

async function runScoutForAmoMission(mission, opts = {}) {
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
    const executionResult = await executeSpecialist({
      mission,
      specialist: SPECIALISTS.SCOUT,
      contributions,
      transactionId: opts.transactionId,
      store: opts.engine?.store,
      run: (secInput) => runScoutDiscovery({ ...secInput, mission }, opts),
      treatErrorsAsBlocked: opts.treatErrorsAsBlocked !== false,
    });

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
      const err = new Error(
        executionResult.blocked?.reason ||
          executionResult.audit?.reason ||
          'Scout discovery blocked.'
      );
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
    if (opts.allowFixtureFallback === true && typeof opts.fixtureScoutDiscoveryResult === 'function') {
      return opts.fixtureScoutDiscoveryResult();
    }
    throw err;
  }
}

module.exports = {
  runScoutForAmoMission,
  runScoutDiscovery,
  buildScoutDiscoverOpts,
  buildScoutExecutionResult,
  mapScoutIntelligenceToDiscoveryPayload,
};
