'use strict';

const { isValidCallerEvent, clusterIdForEvent, gatherIndependentConvergence } = require('./observationTriggers');
const { CALL_EVENT_TYPES } = require('../features/convergence');
const { DEFAULT_RESEARCH_CONFIG } = require('../config/defaultConfig');
const { wilsonInterval } = require('./wilsonInterval');
const { evaluateCohortLayers, evaluateExecutionDelaySensitivity } = require('./cohortEvaluation');
const {
  simulateShuffledOutcomeLabels,
  simulateShuffledConvergenceSecondCallers,
} = require('./negativeControls');
const { documentKnownCohort001Coupling, scanBackfillModulesForLeakage } = require('./leakageAudit');
const { VALIDATION_COHORT_001_ID, VALIDATION_COHORT_002_ID } = require('../acquisition/candidateTypes');

const CALLER_EVENT_PROVENANCE = Object.freeze({
  REAL_PROVIDER: 'REAL_PROVIDER',
  HISTORICAL_FIXTURE: 'HISTORICAL_FIXTURE',
  PROCEDURAL_GENERATED: 'PROCEDURAL_GENERATED',
  MANUAL: 'MANUAL',
  UNKNOWN: 'UNKNOWN',
});

const CLUSTER_RELATIONSHIP = Object.freeze({
  KNOWN_INDEPENDENT: 'known-independent',
  KNOWN_CORRELATED: 'known-correlated',
  UNKNOWN: 'unknown',
});

const FIXTURE_TOKEN_SET = new Set([
  '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump',
  'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump',
  '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump',
]);

function classifyEventProvenance(event) {
  const p = event.provenance || {};
  if (p.backfill === 'callerBackfill') return CALLER_EVENT_PROVENANCE.PROCEDURAL_GENERATED;
  if (FIXTURE_TOKEN_SET.has(event.tokenAddress) && !p.backfill) {
    return CALLER_EVENT_PROVENANCE.HISTORICAL_FIXTURE;
  }
  if (event.sourceType === 'telegram' && event.sourceId?.startsWith('src-front-runners')) {
    return CALLER_EVENT_PROVENANCE.REAL_PROVIDER;
  }
  if (p.manual === true) return CALLER_EVENT_PROVENANCE.MANUAL;
  return CALLER_EVENT_PROVENANCE.UNKNOWN;
}

function classifyTimestampProvenance(event) {
  const p = event.provenance || {};
  if (p.backfill === 'callerBackfill') {
    return { origin: 'anchor_offset_minutes', derivedFromOutcome: false, note: 'callerBackfill anchor + fixed offsets' };
  }
  if (FIXTURE_TOKEN_SET.has(event.tokenAddress)) {
    return { origin: 'historical_fixture_timeline', derivedFromOutcome: false };
  }
  return { origin: 'stored_event_occurred_at', derivedFromOutcome: false };
}

function clusterRelationshipForPair(store, clusterA, clusterB) {
  if (!clusterA || !clusterB || clusterA === clusterB) {
    return CLUSTER_RELATIONSHIP.KNOWN_CORRELATED;
  }
  const a = store.clusters?.get?.(clusterA);
  const b = store.clusters?.get?.(clusterB);
  const relatedA = a?.metadata?.relatedClusterIds || [];
  const relatedB = b?.metadata?.relatedClusterIds || [];
  if (relatedA.includes(clusterB) || relatedB.includes(clusterA)) {
    return CLUSTER_RELATIONSHIP.KNOWN_CORRELATED;
  }
  const basisA = a?.metadata?.relationshipBasis;
  const basisB = b?.metadata?.relationshipBasis;
  if (basisA === 'acquisition_seed' && basisB === 'acquisition_seed' && clusterA !== clusterB) {
    return CLUSTER_RELATIONSHIP.KNOWN_INDEPENDENT;
  }
  if (a?.clusterType === 'promotional_network' && b?.clusterType === 'unknown') {
    return CLUSTER_RELATIONSHIP.KNOWN_INDEPENDENT;
  }
  if (a?.clusterType === 'unknown' && b?.clusterType === 'unknown') {
    return CLUSTER_RELATIONSHIP.UNKNOWN;
  }
  if (clusterA.startsWith('cluster-proc-') && clusterB.startsWith('cluster-proc-')) {
    return CLUSTER_RELATIONSHIP.KNOWN_INDEPENDENT;
  }
  if (clusterA.startsWith('cluster-holdout-') && clusterB.startsWith('cluster-holdout-')) {
    return CLUSTER_RELATIONSHIP.KNOWN_INDEPENDENT;
  }
  return CLUSTER_RELATIONSHIP.UNKNOWN;
}

function gatherCallerChain(store, tokenAddress, evaluatedAt) {
  const evaluatedMs = new Date(evaluatedAt).getTime();
  const events = store
    .getEventsForToken(tokenAddress, { maxOccurredAt: evaluatedAt })
    .filter(
      e =>
        CALL_EVENT_TYPES.has(e.eventType) &&
        isValidCallerEvent(e) &&
        e.occurredAt.getTime() <= evaluatedMs
    )
    .sort((a, b) => a.occurredAt - b.occurredAt || a.id.localeCompare(b.id));

  const seenClusters = new Set();
  const chain = [];
  for (const e of events) {
    const clusterId = clusterIdForEvent(store, e);
    chain.push({ event: e, clusterId, sourceId: e.sourceId });
    seenClusters.add(clusterId);
  }

  const first = chain[0] || null;
  let second = null;
  if (first) {
    for (let i = 1; i < chain.length; i += 1) {
      if (chain[i].clusterId !== first.clusterId) {
        second = chain[i];
        break;
      }
    }
  }

  const conv = gatherIndependentConvergence(store, tokenAddress, evaluatedAt, DEFAULT_RESEARCH_CONFIG);
  const triggered = conv.independentClusterCount >= DEFAULT_RESEARCH_CONFIG.minIndependentClusters;

  return { chain, first, second, conv, triggered };
}

function outcomeForLayer(store, tokenAddress, observationType, delaySeconds) {
  const obs = store.researchObservations.find(
    o => o.tokenAddress === tokenAddress && o.observationType === observationType
  );
  if (!obs) return { label: null, status: 'NO_OBSERVATION' };
  const outcome = store.researchObservationOutcomes.find(
    o => o.observationId === obs.id && o.executionDelaySeconds === delaySeconds
  );
  if (!outcome) return { label: null, status: 'NO_OUTCOME' };
  if (!outcome.label) return { label: null, status: outcome.dataAvailability || 'UNLABELED' };
  return { label: outcome.label, status: 'RESOLVED' };
}

/**
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
 * @param {string} cohortId
 * @param {number} [delaySeconds]
 */
function buildConvergenceAuditRows(store, cohortId, delaySeconds = 60) {
  const members = store.getCohortMembers(cohortId);
  const rows = [];

  for (const member of members) {
    const selectionCategory =
      member.provenance?.selectionCategory || member.provenance?.category || 'unknown';

    const firstObs = store.researchObservations.find(
      o => o.tokenAddress === member.tokenAddress && o.observationType === 'FIRST_CALLER'
    );
    const convObs = store.researchObservations.find(
      o => o.tokenAddress === member.tokenAddress && o.observationType === 'INDEPENDENT_CONVERGENCE'
    );

    const evalAt =
      convObs?.occurredAt ||
      firstObs?.occurredAt ||
      member.provenance?.researchAnchor ||
      new Date();

    const { first, second, conv, triggered } = gatherCallerChain(
      store,
      member.tokenAddress,
      evalAt
    );

    const deltaMinutes =
      first && second
        ? (second.event.occurredAt.getTime() - first.event.occurredAt.getTime()) / 60000
        : null;

    const clusterPairRelationship =
      first && second
        ? clusterRelationshipForPair(store, first.clusterId, second.clusterId)
        : null;

    const marketOutcome = outcomeForLayer(
      store,
      member.tokenAddress,
      'INDEPENDENT_CONVERGENCE',
      delaySeconds
    );
    const firstCallerOutcome = outcomeForLayer(
      store,
      member.tokenAddress,
      'FIRST_CALLER',
      delaySeconds
    );

    rows.push({
      token: member.tokenAddress,
      selectionCategory,
      firstCaller: first?.sourceId || null,
      firstCallerSource: first?.event.sourceType || null,
      firstCallerCluster: first?.clusterId || null,
      firstCallerTimestamp: first?.event.occurredAt?.toISOString() || null,
      secondQualifyingCaller: second?.sourceId || null,
      secondCallerSource: second?.event.sourceType || null,
      secondCallerCluster: second?.clusterId || null,
      secondCallerTimestamp: second?.event.occurredAt?.toISOString() || null,
      deltaMinutesBetweenCallers: deltaMinutes,
      independentConvergenceTriggered: triggered,
      convergenceTimestamp: convObs?.occurredAt?.toISOString() || null,
      callerEventProvenance: {
        first: first ? classifyEventProvenance(first.event) : null,
        second: second ? classifyEventProvenance(second.event) : null,
      },
      sourceClusterProvenance: {
        first: first?.event.provenance || null,
        second: second?.event.provenance || null,
        clusterRelationship: clusterPairRelationship,
      },
      timestampProvenance: {
        first: first ? classifyTimestampProvenance(first.event) : null,
        second: second ? classifyTimestampProvenance(second.event) : null,
      },
      marketOutcomeConvergence: marketOutcome.label || marketOutcome.status,
      marketOutcomeFirstCaller: firstCallerOutcome.label || firstCallerOutcome.status,
      convergenceClusterIds: conv.clusterIds,
    });
  }

  return rows;
}

function summarizeCallerProvenance(auditRows) {
  const counts = Object.fromEntries(Object.values(CALLER_EVENT_PROVENANCE).map(k => [k, 0]));
  for (const row of auditRows) {
    if (row.secondQualifyingCaller && row.callerEventProvenance.second) {
      counts[row.callerEventProvenance.second] =
        (counts[row.callerEventProvenance.second] || 0) + 1;
    }
  }
  return counts;
}

function layerPrecisionWithWilson(evaluation, observationType) {
  const layer = evaluation.layers.find(l => l.observationType === observationType);
  if (!layer) return null;
  const wi = wilsonInterval(layer.PASS, layer.denominators.resolvedN);
  return {
    observationType,
    N: layer.denominators.resolvedN,
    PASS: layer.PASS,
    FAIL: layer.FAIL,
    UNRESOLVED: layer.UNRESOLVED,
    precision: layer.precision,
    medianMfe: layer.medianMfe,
    medianMae: layer.medianMae,
    wilson95: wi,
    triggeredN: layer.denominators.observationTriggeredN,
    cohortN: layer.denominators.cohortN,
  };
}

function convergenceRate(evaluation) {
  const conv = evaluation.layers.find(l => l.observationType === 'INDEPENDENT_CONVERGENCE');
  if (!conv) return null;
  return {
    cohortN: conv.denominators.cohortN,
    triggeredN: conv.denominators.observationTriggeredN,
    rate: conv.denominators.cohortN
      ? conv.denominators.observationTriggeredN / conv.denominators.cohortN
      : null,
  };
}

function crossCohortComparison(eval001, eval002) {
  const pick = (ev, type) => {
    const layer = ev.layers.find(l => l.observationType === type);
    return layer || null;
  };
  const conv1 = pick(eval001, 'INDEPENDENT_CONVERGENCE');
  const conv2 = pick(eval002, 'INDEPENDENT_CONVERGENCE');
  const fc1 = pick(eval001, 'FIRST_CALLER');
  const fc2 = pick(eval002, 'FIRST_CALLER');

  return {
    FIRST_CALLER_precision: { cohort001: fc1?.precision ?? null, cohort002: fc2?.precision ?? null },
    CONVERGENCE_precision: { cohort001: conv1?.precision ?? null, cohort002: conv2?.precision ?? null },
    CONVERGENCE_N: {
      cohort001: conv1?.denominators?.resolvedN ?? 0,
      cohort002: conv2?.denominators?.resolvedN ?? 0,
    },
    CONVERGENCE_medianMFE: { cohort001: conv1?.medianMfe ?? null, cohort002: conv2?.medianMfe ?? null },
    CONVERGENCE_medianMAE: { cohort001: conv1?.medianMae ?? null, cohort002: conv2?.medianMae ?? null },
    CONVERGENCE_rate: {
      cohort001: conv1?.denominators?.cohortN
        ? conv1.denominators.observationTriggeredN / conv1.denominators.cohortN
        : null,
      cohort002: conv2?.denominators?.cohortN
        ? conv2.denominators.observationTriggeredN / conv2.denominators.cohortN
        : null,
    },
  };
}

function classifyAuditResult({
  leakageScan,
  knownCoupling,
  holdoutConvPrecision,
  holdoutConvN,
  shuffleOutcomes,
}) {
  const v1Coupling = knownCoupling?.proceduralCatalogV1;
  const backfillClean = leakageScan?.clean === true;

  if (v1Coupling || !backfillClean) {
    return {
      classification: 'INVALID',
      rationale:
        'Identified acquisition coupling between selectionCategory and evidence generation (cohort 001 procedural catalog) and/or backfill leakage scan failures.',
    };
  }

  if (shuffleOutcomes?.observed?.precision === 1 && !shuffleOutcomes?.summary?.destroysPerfectRelationship) {
    return {
      classification: 'INVALID',
      rationale: 'Outcome shuffle control failed to break perfect convergence association.',
    };
  }

  if (holdoutConvN >= 10 && holdoutConvPrecision != null && holdoutConvPrecision >= 0.75) {
    return {
      classification: 'REPLICATED',
      rationale: 'Holdout cohort shows substantial independent-convergence precision with adequate N.',
    };
  }

  if (holdoutConvN >= 5 && holdoutConvPrecision != null && holdoutConvPrecision > 0.55) {
    return {
      classification: 'PRELIMINARY',
      rationale: 'Holdout shows directional convergence improvement but sample remains small.',
    };
  }

  return {
    classification: 'UNSUPPORTED',
    rationale: 'No backfill leakage after hardening, but holdout does not reproduce cohort 001 convergence advantage.',
  };
}

/**
 * Full SIGNAL-V1-004 audit artifact (does not mutate store).
 *
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
 * @param {object} [options]
 */
function runConvergenceIntegrityAudit(store, options = {}) {
  const delay = options.executionDelaySeconds ?? 60;
  const cohort001Id = options.cohort001Id || VALIDATION_COHORT_001_ID;
  const cohort002Id = options.cohort002Id || VALIDATION_COHORT_002_ID;

  const eval001 = evaluateCohortLayers(store, cohort001Id, delay);
  const eval002 = store.getCohortMembers(cohort002Id)?.length
    ? evaluateCohortLayers(store, cohort002Id, delay)
    : null;

  const auditTable001 = buildConvergenceAuditRows(store, cohort001Id, delay);
  const provenanceCounts = summarizeCallerProvenance(
    auditTable001.filter(r => r.independentConvergenceTriggered)
  );

  const convLabeled = auditTable001
    .filter(r => r.independentConvergenceTriggered)
    .map(r => ({
      tokenAddress: r.token,
      label: r.marketOutcomeConvergence === 'PASS' || r.marketOutcomeConvergence === 'FAIL'
        ? r.marketOutcomeConvergence
        : null,
    }))
    .filter(r => r.label);

  const cohortOutcomePool = auditTable001
    .map(r =>
      r.marketOutcomeFirstCaller === 'PASS' || r.marketOutcomeFirstCaller === 'FAIL'
        ? r.marketOutcomeFirstCaller
        : null
    )
    .filter(Boolean);

  const shuffleOutcomes = simulateShuffledOutcomeLabels(convLabeled, cohortOutcomePool);
  const shuffleConvergence = simulateShuffledConvergenceSecondCallers(auditTable001);

  const clusterSummary = { 'known-independent': 0, 'known-correlated': 0, unknown: 0 };
  for (const row of auditTable001.filter(r => r.independentConvergenceTriggered)) {
    const rel = row.sourceClusterProvenance.clusterRelationship;
    if (rel === CLUSTER_RELATIONSHIP.KNOWN_INDEPENDENT) clusterSummary['known-independent'] += 1;
    else if (rel === CLUSTER_RELATIONSHIP.KNOWN_CORRELATED) clusterSummary['known-correlated'] += 1;
    else clusterSummary.unknown += 1;
  }

  const holdoutLayer = eval002
    ? layerPrecisionWithWilson(eval002, 'INDEPENDENT_CONVERGENCE')
    : null;

  const knownCoupling = documentKnownCohort001Coupling();
  const leakageScan = scanBackfillModulesForLeakage();

  const auditResult = classifyAuditResult({
    leakageScan,
    knownCoupling,
    holdoutConvPrecision: holdoutLayer?.precision ?? null,
    holdoutConvN: holdoutLayer?.N ?? 0,
    shuffleOutcomes,
  });

  const failures002 = eval002
    ? auditTable002Failures(store, cohort002Id, delay)
    : { convergenceFails: [], falseNegatives: [] };

  return {
    spec: 'SIGNAL-V1-004',
    generatedAt: new Date().toISOString(),
    frozenConvergenceDefinition: {
      minIndependentClusters: DEFAULT_RESEARCH_CONFIG.minIndependentClusters,
      windowMinutes: DEFAULT_RESEARCH_CONFIG.independentConvergenceWindowMinutes,
    },
    cohort001: {
      id: cohort001Id,
      auditTable: auditTable001,
      layerMetrics: {
        FIRST_CALLER: layerPrecisionWithWilson(eval001, 'FIRST_CALLER'),
        INDEPENDENT_CONVERGENCE: layerPrecisionWithWilson(eval001, 'INDEPENDENT_CONVERGENCE'),
      },
      convergenceRate: convergenceRate(eval001),
      executionDelaySensitivity: evaluateExecutionDelaySensitivity(store, cohort001Id),
      secondCallerProvenanceCounts: provenanceCounts,
      clusterRelationshipSummary: clusterSummary,
    },
    cohort002: eval002
      ? {
          id: cohort002Id,
          layerMetrics: {
            FIRST_CALLER: layerPrecisionWithWilson(eval002, 'FIRST_CALLER'),
            INDEPENDENT_CONVERGENCE: holdoutLayer,
          },
          convergenceRate: convergenceRate(eval002),
          executionDelaySensitivity: evaluateExecutionDelaySensitivity(store, cohort002Id),
          failureAnalysis: failures002,
        }
      : null,
    crossCohort: eval002 ? crossCohortComparison(eval001, eval002) : null,
    negativeControls: {
      shuffledOutcomes: shuffleOutcomes,
      shuffledConvergence: shuffleConvergence,
    },
    leakageAudit: {
      backfillScan: leakageScan,
      knownCohort001Coupling: knownCoupling,
    },
    auditResult,
  };
}

function auditTable002Failures(store, cohortId, delaySeconds) {
  const rows = buildConvergenceAuditRows(store, cohortId, delaySeconds);
  const convergenceFails = rows.filter(
    r => r.independentConvergenceTriggered && r.marketOutcomeConvergence === 'FAIL'
  );
  const falseNegatives = rows.filter(
    r =>
      !r.independentConvergenceTriggered &&
      (r.marketOutcomeFirstCaller === 'PASS' || r.selectionCategory === 'stronger')
  );
  return { convergenceFails, falseNegatives };
}

module.exports = {
  runConvergenceIntegrityAudit,
  buildConvergenceAuditRows,
  classifyEventProvenance,
  clusterRelationshipForPair,
  CALLER_EVENT_PROVENANCE,
  CLUSTER_RELATIONSHIP,
  layerPrecisionWithWilson,
  crossCohortComparison,
  classifyAuditResult,
};
