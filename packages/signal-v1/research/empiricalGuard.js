'use strict';

const { CALL_EVENT_TYPES } = require('../features/convergence');
const { isEmpiricalDataClass, resolveCohortDataClass } = require('./dataClass');
const {
  classifyCallerEventProvenance,
  classifyMarketObservationProvenance,
  isAllowedEmpiricalCallerClass,
  isAllowedEmpiricalMarketClass,
  EVIDENCE_CLASS,
  buildContaminationReport,
} = require('./empiricalProvenance');

class EmpiricalValidationError extends Error {
  constructor(message, details) {
    super(message);
    this.name = 'EmpiricalValidationError';
    this.details = details;
  }
}

/**
 * Hard fail-closed gate for EMPIRICAL cohort evaluation.
 *
 * @param {object} store
 * @param {string} cohortId
 * @param {object} [options]
 */
function assertEmpiricalCohort(store, cohortId, options = {}) {
  const cohort =
    store.researchCohorts?.get?.(cohortId) ||
    [...(store.researchCohorts?.values?.() || [])].find(c => c.id === cohortId);
  const dataClass = resolveCohortDataClass(cohortId, cohort);
  if (!isEmpiricalDataClass(dataClass)) {
    return { dataClass, skipped: true };
  }

  const members = store.getCohortMembers(cohortId);
  const tokenAddresses = members.map(m => m.tokenAddress);
  const contamination = buildContaminationReport(store, tokenAddresses);
  const violations = [];

  if (contamination.proceduralEventCount > 0) {
    violations.push({
      code: 'PROCEDURAL_CALLER_EVIDENCE',
      count: contamination.proceduralEventCount,
    });
  }
  if (contamination.proceduralMarketObservationCount > 0) {
    violations.push({
      code: 'PROCEDURAL_MARKET_EVIDENCE',
      count: contamination.proceduralMarketObservationCount,
    });
  }

  for (const tokenAddress of tokenAddresses) {
    const evidenceIds = collectPerformanceEvidenceEventIds(store, tokenAddress);
    for (const eventId of evidenceIds) {
      const event = store.events.find(e => e.id === eventId);
      if (!event) continue;
      const cls = classifyCallerEventProvenance(event);
      if (!isAllowedEmpiricalCallerClass(cls)) {
        violations.push({
          code: 'UNVERIFIED_CALLER_EVIDENCE_IN_FEATURES',
          tokenAddress,
          eventId,
          evidenceClass: cls,
        });
      }
    }
  }

  if (options.requireFrozen !== false && !cohort?.frozenAt) {
    violations.push({ code: 'COHORT_NOT_FROZEN' });
  }

  if (violations.length) {
    throw new EmpiricalValidationError('EMPIRICAL cohort validation failed (fail-closed)', {
      cohortId,
      dataClass,
      contamination,
      violations,
    });
  }

  return { dataClass, contamination, ok: true };
}

function collectPerformanceEvidenceEventIds(store, tokenAddress) {
  const ids = new Set();
  for (const obs of store.researchObservations.filter(o => o.tokenAddress === tokenAddress)) {
    if (!['FIRST_CALLER', 'INDEPENDENT_CONVERGENCE'].includes(obs.observationType)) continue;
    for (const eid of obs.evidenceEventIds || []) ids.add(eid);
    for (const eid of obs.triggerEventIds || []) ids.add(eid);
  }
  return [...ids];
}

/**
 * Block accidental procedural provider → empirical report at acquisition time.
 *
 * @param {ResearchDataClass} dataClass
 * @param {string} providerId
 */
function assertProviderAllowedForDataClass(dataClass, providerId) {
  if (!isEmpiricalDataClass(dataClass)) return;
  const blocked = [
    'procedural-public-research-catalog-v1',
    'procedural-holdout-catalog-v2',
    'proceduralCandidateProvider',
    'proceduralCandidateProviderV2',
  ];
  if (blocked.some(b => providerId.includes(b) || providerId === b)) {
    throw new EmpiricalValidationError(
      `Provider ${providerId} cannot feed EMPIRICAL cohort evaluation`,
      { providerId, dataClass }
    );
  }
}

module.exports = {
  EmpiricalValidationError,
  assertEmpiricalCohort,
  assertProviderAllowedForDataClass,
  EVIDENCE_CLASS,
  buildContaminationReport,
};
