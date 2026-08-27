'use strict';

/**
 * SPEC-141 Stage 6 — Qualification.
 * SPEC-194: delegates market qualification and readiness to ProspectEvaluation (ADR-101).
 */

const {
  attachFitToClassified,
  enrichPeopleSafe,
} = require('../../max/scoutAcquisition/FitEvaluation');
const { QUALIFICATION_OUTCOMES } = require('./types');
const {
  OPPORTUNITY_CLASSES,
  QUALIFICATION_STATUSES,
  READINESS_STATES,
} = require('../../max/scoutAcquisition/Types');
const { businessFitQualifiedCount } = require('../../max/scoutAcquisition/ProspectEvaluation');

function mapOutcomeFromEvaluation(evaluation) {
  if (!evaluation) return QUALIFICATION_OUTCOMES.OUT;
  if (evaluation.qualification.status === QUALIFICATION_STATUSES.QUALIFIED) {
    return QUALIFICATION_OUTCOMES.QUALIFIED;
  }
  if (evaluation.qualification.status === QUALIFICATION_STATUSES.UNCERTAIN) {
    return QUALIFICATION_OUTCOMES.WATCH;
  }
  return QUALIFICATION_OUTCOMES.OUT;
}

function buildChecksFromEvaluation(evaluation, working, attached) {
  return {
    meetsIcp: evaluation.qualification.status !== QUALIFICATION_STATUSES.NOT_QUALIFIED,
    hasDecisionMaker: Boolean(
      (working.people || []).some((p) =>
        /\b(owner|principal|partner|operations|office manager|director|president|founder)\b/i.test(
          String(p.jobTitle || '')
        )
      )
    ),
    hasContactPath: Boolean(
      working.email ||
        working.phone ||
        (working.people || []).some((p) => p.email || p.phone)
    ),
    hasBuyingSignals: (evaluation.readiness.signals || []).some((s) =>
      ['portfolio_growth', 'expansion', 'hiring', 'new_location', 'operational_change'].includes(
        String(s.type || '').toLowerCase()
      )
    ),
    evidenceSufficient: Boolean(
      (working.evidenceRefs && working.evidenceRefs.length) ||
        evaluation.readiness.status === READINESS_STATES.READY
    ),
    basicFit: attached.fit && attached.fit.basicFit === true,
    qualificationStatus: evaluation.qualification.status,
    readinessStatus: evaluation.readiness.status,
  };
}

/**
 * Qualify candidates based on multi-dimensional prospect evaluation (SPEC-194).
 *
 * @param {object} input
 * @returns {Promise<object>}
 */
async function qualifyCandidates(input = {}) {
  const marketDefinition = input.marketDefinition;
  const searchDefinition = marketDefinition.searchDefinition;
  const candidates = input.candidateUniverse.candidates || input.candidateUniverse.resolved || [];
  const evidenceByCandidate = input.evidenceCollection.evidenceByCandidate || [];
  const now = input.opts && input.opts.now != null ? Number(input.opts.now) : Date.now();

  const evidenceMap = new Map(
    evidenceByCandidate.map((e) => [String(e.candidateId), e])
  );

  const qualified = [];
  const rejected = [];
  const watch = [];
  const enrichedCandidates = [];
  const prospectEvaluations = [];

  for (const candidate of candidates) {
    let working = { ...candidate };
    if (input.opts && input.opts.enrichPeople) {
      const enriched = await enrichPeopleSafe(working, input.opts.enrichPeople);
      working = { ...working, people: enriched.people || working.people || [] };
    }

    const classifiedSeed = {
      companyId: working.id,
      name: working.name,
      signals: working.signals || [],
      observations: [],
      unknowns: [],
      evidenceRefs: [],
    };

    const attached = attachFitToClassified(classifiedSeed, working, searchDefinition, now);
    const next = attached.classified;
    const evaluation = attached.evaluation;
    working = { ...working, ...next, opportunityClass: next.classification, evaluation };
    prospectEvaluations.push(evaluation);

    const evidence = evidenceMap.get(String(working.id)) || { confidence: 0.2, evidence: [] };
    const icpScore = working.icpScore != null ? Number(working.icpScore) : null;
    const fitScore = attached.fit && attached.fit.score != null ? Number(attached.fit.score) : null;
    const fitClass = next.classification;
    const outcome = mapOutcomeFromEvaluation(evaluation);
    const checks = buildChecksFromEvaluation(evaluation, working, attached);

    const row = {
      candidateId: working.id,
      name: working.name,
      outcome,
      checks,
      fitClass,
      icpScore,
      fitScore,
      evidenceConfidence: evidence.confidence,
      signals: working.signals || [],
      reasons: evaluation.qualification.reasons || [],
      evaluation,
      investigation: evaluation.investigation,
    };

    enrichedCandidates.push(working);

    if (outcome === QUALIFICATION_OUTCOMES.QUALIFIED) qualified.push(row);
    else if (outcome === QUALIFICATION_OUTCOMES.WATCH) watch.push(row);
    else rejected.push(row);
  }

  return {
    qualified,
    watch,
    rejected,
    enrichedCandidates,
    prospectEvaluations,
    qualifiedCount: businessFitQualifiedCount(prospectEvaluations),
    watchCount: watch.length,
    rejectedCount: rejected.length,
  };
}

module.exports = {
  mapOutcomeFromEvaluation,
  qualifyCandidates,
};
