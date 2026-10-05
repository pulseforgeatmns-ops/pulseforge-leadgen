'use strict';

const { wilsonInterval } = require('./wilsonInterval');
const { median } = require('./sourcePerformanceEngine');
const { gatherIndependentConvergence, gatherDistinctClusterConvergence } = require('./observationTriggers');
const { getClusterPairRelationship, CLUSTER_RELATIONSHIP } = require('./clusterRelationships');
const { DEFAULT_RESEARCH_CONFIG } = require('../config/defaultConfig');
const { buildConvergenceAuditRows } = require('./convergenceIntegrityAudit');

function buildEmpiricalExtendedReport(store, cohortId, evaluation, delaySeconds = 60) {
  const members = store.getCohortMembers(cohortId);
  const firstLayer = evaluation.layers.find(l => l.observationType === 'FIRST_CALLER');
  const convLayer = evaluation.layers.find(l => l.observationType === 'INDEPENDENT_CONVERGENCE');

  const firstWilson = wilsonInterval(firstLayer?.PASS ?? 0, firstLayer?.denominators?.resolvedN ?? 0);
  const convWilson = wilsonInterval(convLayer?.PASS ?? 0, convLayer?.denominators?.resolvedN ?? 0);

  const firstPrecision = firstLayer?.precision ?? null;
  const convPrecision = convLayer?.precision ?? null;
  const absoluteLift =
    firstPrecision != null && convPrecision != null ? convPrecision - firstPrecision : null;
  const relativeLift =
    firstPrecision != null && firstPrecision > 0 && convPrecision != null
      ? convPrecision / firstPrecision
      : null;

  const coverageBias = summarizeCoverageBias(store, members);
  const timeToConvergence = summarizeTimeToConvergence(store, members, delaySeconds);
  const falsePositives = listConvergenceFalsePositives(store, cohortId, delaySeconds);
  const missedRunners = listMissedRunners(store, cohortId, delaySeconds);

  const firstCallerBaseRate = {
    label: 'P(PASS | FIRST_CALLER)',
    precision: firstPrecision,
    wilson95: firstWilson,
    resolvedN: firstLayer?.denominators?.resolvedN ?? 0,
    note: 'Natural sample base rate (not outcome-balanced).',
  };

  const effectSize = {
    absoluteLiftPP: absoluteLift != null ? absoluteLift * 100 : null,
    relativeLift,
    convergencePrecision: convPrecision,
    firstCallerPrecision: firstPrecision,
    note: 'No fabricated statistical significance; Wilson intervals reported separately per layer.',
  };

  const evidenceStrength = classifyEvidenceStrength({
    cohortN: members.length,
    convResolvedN: convLayer?.denominators?.resolvedN ?? 0,
    proceduralEventCount: evaluation.contamination?.proceduralEventCount ?? 0,
  });

  const exploratoryDistinctUnknown = buildExploratoryDistinctUnknownReport(store, members);

  return {
    firstCallerBaseRate,
    strictProvenIndependentConvergence: {
      rate:
        members.length && convLayer
          ? convLayer.denominators.observationTriggeredN / members.length
          : null,
      triggeredN: convLayer?.denominators?.observationTriggeredN ?? 0,
      precision: convPrecision,
      wilson95: convWilson,
      resolvedN: convLayer?.denominators?.resolvedN ?? 0,
    },
    effectSize,
    maeComparison: {
      firstCallerMedianMae: firstLayer?.medianMae ?? null,
      convergenceMedianMae: convLayer?.medianMae ?? null,
      convergenceReducesMae:
        firstLayer?.medianMae != null && convLayer?.medianMae != null
          ? convLayer.medianMae > firstLayer.medianMae
          : null,
    },
    coverageBias,
    timeToConvergence,
    falsePositives,
    missedRunners,
    exploratoryDistinctUnknown,
    evidenceStrength,
    finalQuestions: answerFinalQuestions({
      firstCallerBaseRate,
      convLayer,
      effectSize,
      maeComparison: {
        firstCallerMedianMae: firstLayer?.medianMae ?? null,
        convergenceMedianMae: convLayer?.medianMae ?? null,
      },
      coverageBias,
      missedRunners,
      evidenceStrength,
      delaySensitivity: evaluation.executionDelaySensitivity,
    }),
  };
}

function summarizeCoverageBias(store, members) {
  let oneCaller = 0;
  let twoPlusCallers = 0;
  let provenIndependentTwoPlus = 0;
  let unknownRelationship = 0;

  for (const member of members) {
    const events = store
      .getEventsForToken(member.tokenAddress)
      .filter(e => e.eventType === 'CALL' || e.eventType === 'TOKEN_MENTION');
    const callers = events.length;
    if (callers <= 1) oneCaller += 1;
    else twoPlusCallers += 1;

    const strict = gatherIndependentConvergence(
      store,
      member.tokenAddress,
      member.provenance?.earliestKnownCallAt || new Date(),
      { ...DEFAULT_RESEARCH_CONFIG, strictClusterIndependence: true }
    );
    if (strict.provenIndependentClusterCount >= 2) provenIndependentTwoPlus += 1;

    const exploratory = gatherDistinctClusterConvergence(
      store,
      member.tokenAddress,
      member.provenance?.earliestKnownCallAt || new Date(),
      DEFAULT_RESEARCH_CONFIG
    );
    if (exploratory.clusterIds.length >= 2) {
      const rel = getClusterPairRelationship(
        store,
        exploratory.clusterIds[0],
        exploratory.clusterIds[1]
      );
      if (rel === CLUSTER_RELATIONSHIP.UNKNOWN) unknownRelationship += 1;
    }
  }

  return {
    cohortN: members.length,
    tokensWithOneCaller: oneCaller,
    tokensWithTwoPlusCallers: twoPlusCallers,
    tokensWithProvenIndependentTwoPlusClusters: provenIndependentTwoPlus,
    tokensWithUnknownClusterRelationship: unknownRelationship,
  };
}

function summarizeTimeToConvergence(store, members, delaySeconds) {
  const durations = [];
  const buckets = { '<=5m': 0, '<=15m': 0, '<=30m': 0 };

  for (const member of members) {
    const obs = store.researchObservations.find(
      o =>
        o.tokenAddress === member.tokenAddress &&
        o.observationType === 'INDEPENDENT_CONVERGENCE'
    );
    if (!obs) continue;
    const d = obs.metadata?.convergenceDurationMinutes;
    if (d == null) continue;
    durations.push(d);
    if (d <= 5) buckets['<=5m'] += 1;
    if (d <= 15) buckets['<=15m'] += 1;
    if (d <= 30) buckets['<=30m'] += 1;
  }

  durations.sort((a, b) => a - b);
  return {
    medianMinutes: median(durations),
    p25Minutes: percentile(durations, 0.25),
    p75Minutes: percentile(durations, 0.75),
    buckets,
    n: durations.length,
  };
}

function percentile(sorted, p) {
  if (!sorted.length) return null;
  const idx = (sorted.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

function listConvergenceFalsePositives(store, cohortId, delaySeconds) {
  const rows = buildConvergenceAuditRows(store, cohortId, delaySeconds);
  return rows
    .filter(r => r.independentConvergenceTriggered && r.marketOutcomeConvergence === 'FAIL')
    .map(r => ({
      tokenAddress: r.token,
      timeline: {
        firstCaller: r.firstCallerTimestamp,
        secondCaller: r.secondCallerTimestamp,
        deltaMinutes: r.deltaMinutesBetweenCallers,
      },
      maeMfe: {
        convergenceOutcome: r.marketOutcomeConvergence,
        firstCallerOutcome: r.marketOutcomeFirstCaller,
      },
      provenance: r.callerEventProvenance,
      clusterRelationship: r.sourceClusterProvenance?.clusterRelationship,
    }));
}

function listMissedRunners(store, cohortId, delaySeconds) {
  const rows = buildConvergenceAuditRows(store, cohortId, delaySeconds);
  return rows
    .filter(
      r =>
        !r.independentConvergenceTriggered &&
        r.marketOutcomeFirstCaller === 'PASS'
    )
    .map(r => ({
      tokenAddress: r.token,
      empiricalCallerCount: store.getEventsForToken(r.token).filter(e => e.eventType === 'CALL')
        .length,
      clusterRelationship: r.sourceClusterProvenance?.clusterRelationship,
      secondCallerAfter30m:
        r.deltaMinutesBetweenCallers != null ? r.deltaMinutesBetweenCallers > 30 : null,
      marketOutcomeFirstCaller: r.marketOutcomeFirstCaller,
    }));
}

function buildExploratoryDistinctUnknownReport(store, members) {
  let distinctUnknownPairs = 0;
  for (const member of members) {
    const conv = gatherDistinctClusterConvergence(
      store,
      member.tokenAddress,
      member.provenance?.earliestKnownCallAt || new Date(),
      DEFAULT_RESEARCH_CONFIG
    );
    if (conv.clusterIds.length < 2) continue;
    const rel = getClusterPairRelationship(store, conv.clusterIds[0], conv.clusterIds[1]);
    if (rel === CLUSTER_RELATIONSHIP.UNKNOWN) distinctUnknownPairs += 1;
  }
  return {
    label: 'Distinct sources with UNKNOWN independence (exploratory — not proven independent convergence)',
    count: distinctUnknownPairs,
  };
}

function classifyEvidenceStrength({ cohortN, convResolvedN, proceduralEventCount }) {
  if (proceduralEventCount > 0) {
    return { classification: 'NO EVIDENCE', rationale: 'Procedural contamination detected.' };
  }
  if (cohortN < 10 || convResolvedN < 5) {
    return {
      classification: 'NO EVIDENCE',
      rationale: 'Sample size and/or resolved convergence outcomes too small for inference.',
    };
  }
  if (cohortN < 30 || convResolvedN < 10) {
    return {
      classification: 'DIRECTIONAL',
      rationale: 'Low N empirical sample — directional only.',
    };
  }
  if (cohortN < 50) {
    return {
      classification: 'PRELIMINARY',
      rationale: 'Adequate for preliminary empirical read; not yet replicated.',
    };
  }
  return {
    classification: 'PRELIMINARY',
    rationale: 'Meets target N but single empirical cohort — not REPLICATED without holdout.',
  };
}

function answerFinalQuestions(ctx) {
  return {
    A_firstCallerPassBaseRate: ctx.firstCallerBaseRate.precision,
    B_strictProvenIndependentConvergenceRate: ctx.convLayer
      ? ctx.convLayer.denominators.observationTriggeredN /
        (ctx.convLayer.denominators.cohortN || 1)
      : null,
    C_passGivenIndependentConvergence: ctx.convLayer?.precision ?? null,
    D_lift: ctx.effectSize,
    E_convergenceReducesMae:
      ctx.maeComparison.convergenceMedianMae != null &&
      ctx.maeComparison.firstCallerMedianMae != null
        ? ctx.maeComparison.convergenceMedianMae > ctx.maeComparison.firstCallerMedianMae
        : null,
    F_executionDelaySensitivity: ctx.delaySensitivity,
    G_winningTokensMissedByConvergenceRule: ctx.missedRunners.length,
    H_evidenceStrength: ctx.evidenceStrength,
  };
}

module.exports = {
  buildEmpiricalExtendedReport,
  classifyEvidenceStrength,
};
