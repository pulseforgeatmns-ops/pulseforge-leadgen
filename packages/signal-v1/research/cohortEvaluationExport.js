'use strict';

const {
  FEATURE_VERSION,
  STRATEGY_VERSION,
  RESEARCH_DEFINITION_VERSION,
  SOURCE_PERFORMANCE_VERSION,
} = require('../types');
const { evaluateCohortLayers, evaluateExecutionDelaySensitivity } = require('./cohortEvaluation');
const { VALIDATION_COHORT_001_SELECTION_VERSION } = require('../acquisition/candidateTypes');

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
  const evaluation = evaluateCohortLayers(store, cohortId, primaryDelay, options);
  const delaySensitivity = evaluateExecutionDelaySensitivity(store, cohortId);

  return {
    cohortId,
    cohortVersion: cohort?.metadata?.phase || cohortId,
    selectionVersion: cohort?.selectionVersion || cohort?.metadata?.selectionVersion || null,
    featureVersion: FEATURE_VERSION,
    researchDefinitionVersion: RESEARCH_DEFINITION_VERSION,
    strategyVersion: STRATEGY_VERSION,
    sourcePerformanceVersion: SOURCE_PERFORMANCE_VERSION,
    providerVersions: options.providerVersions || {
      market: options.marketProviderId || 'fixture-acquisition',
      candidateAcquisition: 'procedural-public-research-catalog-v1',
    },
    generatedAt: new Date().toISOString(),
    frozenAt: cohort?.frozenAt || null,
    primaryExecutionDelaySeconds: primaryDelay,
    evaluation,
    executionDelaySensitivity: delaySensitivity,
    selectionVersionDefault: VALIDATION_COHORT_001_SELECTION_VERSION,
  };
}

module.exports = {
  buildCohortEvaluationArtifact,
};
