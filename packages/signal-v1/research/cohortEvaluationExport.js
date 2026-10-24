'use strict';

const {
  FEATURE_VERSION,
  STRATEGY_VERSION,
  RESEARCH_DEFINITION_VERSION,
  SOURCE_PERFORMANCE_VERSION,
} = require('../types');
const { evaluateCohortLayers, evaluateExecutionDelaySensitivity } = require('./cohortEvaluation');
const {
  VALIDATION_COHORT_001_SELECTION_VERSION,
  VALIDATION_COHORT_003_ID,
} = require('../acquisition/candidateTypes');
const { resolveCohortDataClass, RESEARCH_DATA_CLASS } = require('./dataClass');
const { buildContaminationReport } = require('./empiricalProvenance');
const { PROVIDER_ID: GECKO_PROVIDER_ID } = require('../providers/GeckoTerminalMarketDataProvider');
const { historicalCallerCatalogProvider } = require('../acquisition/providers/historicalCallerCatalogProvider');

/**
 * @param {object} store
 * @param {string} cohortId
 * @param {object} [options]
 */
function buildCohortEvaluationArtifact(store, cohortId, options = {}) {
  const primaryDelay = options.executionDelaySeconds ?? 60;
  const cohort =
    store.researchCohorts?.get?.(cohortId) ||
    [...(store.researchCohorts?.values?.() || [])].find(c => c.id === cohortId) ||
    null;
  const dataClass = resolveCohortDataClass(cohortId, cohort);
  const evaluation = evaluateCohortLayers(store, cohortId, primaryDelay, options);
  const delaySensitivity =
    evaluation.executionDelaySensitivity ||
    evaluateExecutionDelaySensitivity(store, cohortId);

  const members = store.getCohortMembers(cohortId);
  const provenanceCounts =
    dataClass === RESEARCH_DATA_CLASS.EMPIRICAL
      ? buildContaminationReport(
          store,
          members.map(m => m.tokenAddress)
        )
      : evaluation.contamination || null;

  const artifact = {
    cohortId,
    dataClass,
    cohortVersion: cohort?.metadata?.phase || cohortId,
    selectionVersion: cohort?.selectionVersion || cohort?.metadata?.selectionVersion || null,
    selectionRule: cohort?.metadata?.selectionRule || cohort?.metadata?.selectionBreakdown?.procedure || null,
    featureVersion: FEATURE_VERSION,
    researchDefinitionVersion: RESEARCH_DEFINITION_VERSION,
    strategyVersion: STRATEGY_VERSION,
    sourcePerformanceVersion: SOURCE_PERFORMANCE_VERSION,
    providerVersions: options.providerVersions || defaultProviderVersions(cohortId, dataClass),
    generatedAt: new Date().toISOString(),
    frozenAt: cohort?.frozenAt || null,
    cohortMembership: members.map(m => ({
      tokenAddress: m.tokenAddress,
      inclusionReason: m.inclusionReason,
      provenance: m.provenance,
    })),
    provenanceCounts,
    coverage: cohort?.metadata?.coveragePeriod || null,
    primaryExecutionDelaySeconds: primaryDelay,
    evaluation,
    executionDelaySensitivity: delaySensitivity,
    selectionVersionDefault: VALIDATION_COHORT_001_SELECTION_VERSION,
  };

  if (dataClass === RESEARCH_DATA_CLASS.EMPIRICAL) {
    artifact.empiricalReport = evaluation.empiricalReport || null;
    artifact.finalQuestions = evaluation.empiricalReport?.finalQuestions || null;
  }

  return artifact;
}

function defaultProviderVersions(cohortId, dataClass) {
  if (dataClass === RESEARCH_DATA_CLASS.EMPIRICAL || cohortId === VALIDATION_COHORT_003_ID) {
    return {
      market: GECKO_PROVIDER_ID,
      caller: historicalCallerCatalogProvider.providerId,
      callerVersion: historicalCallerCatalogProvider.providerVersion,
    };
  }
  return {
    market: 'fixture-acquisition',
    candidateAcquisition: 'procedural-public-research-catalog-v1',
  };
}

module.exports = {
  buildCohortEvaluationArtifact,
};
