'use strict';

/**
 * SPEC-141 Stage 6 — Qualification.
 * Reason about whether each candidate meets ICP and buying criteria.
 */

const {
  attachFitToClassified,
  enrichPeopleSafe,
} = require('../../max/scoutAcquisition/FitEvaluation');
const { QUALIFICATION_OUTCOMES } = require('./types');
const { OPPORTUNITY_CLASSES } = require('../../max/scoutAcquisition/Types');

function hasDecisionMaker(candidate) {
  const people = candidate.people || [];
  return people.some(
    (p) =>
      p.decisionMaker === true ||
      /\b(owner|principal|partner|operations|office manager|director|president|founder)\b/i.test(
        String(p.jobTitle || '')
      )
  );
}

function hasContactPath(candidate) {
  return Boolean(
    candidate.email ||
      candidate.phone ||
      (candidate.people || []).some((p) => p.email || p.phone)
  );
}

function hasBuyingSignals(candidate) {
  const signals = candidate.signals || [];
  return signals.some((s) =>
    ['portfolio_growth', 'expansion', 'hiring', 'new_location', 'operational_change'].includes(
      String(s.type || s.kind || '').toLowerCase()
    )
  );
}

/**
 * Qualify candidates based on fit evaluation and evidence.
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
    working = { ...working, ...next, opportunityClass: next.classification };

    const evidence = evidenceMap.get(String(working.id)) || { confidence: 0.2, evidence: [] };
    const icpScore = working.icpScore != null ? Number(working.icpScore) : null;
    const fitScore = attached.fit && attached.fit.score != null ? Number(attached.fit.score) : null;
    const fitClass = next.classification;

    const checks = {
      meetsIcp: icpScore == null ? fitClass !== OPPORTUNITY_CLASSES.REJECTED : icpScore >= 70,
      hasDecisionMaker: hasDecisionMaker(working),
      hasContactPath: hasContactPath(working),
      hasBuyingSignals: hasBuyingSignals(working),
      evidenceSufficient: evidence.sufficient === true || evidence.confidence >= 0.45,
      basicFit: attached.fit && attached.fit.basicFit === true,
    };

    const passCount = Object.values(checks).filter(Boolean).length;
    const outcome =
      checks.meetsIcp && checks.basicFit && passCount >= 3
        ? QUALIFICATION_OUTCOMES.QUALIFIED
        : checks.basicFit || passCount >= 2
          ? QUALIFICATION_OUTCOMES.WATCH
          : QUALIFICATION_OUTCOMES.OUT;

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
      reasons: buildQualificationReasons(checks, outcome),
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
    qualifiedCount: qualified.length,
    watchCount: watch.length,
    rejectedCount: rejected.length,
  };
}

function buildQualificationReasons(checks, outcome) {
  const reasons = [];
  if (checks.meetsIcp) reasons.push('Meets ICP threshold');
  if (checks.hasDecisionMaker) reasons.push('Identifiable decision-maker');
  if (checks.hasContactPath) reasons.push('Reachable contact path');
  if (checks.hasBuyingSignals) reasons.push('Buying signals present');
  if (checks.evidenceSufficient) reasons.push('Sufficient evidence confidence');
  if (outcome === QUALIFICATION_OUTCOMES.OUT && !checks.basicFit) {
    reasons.push('Does not meet basic fit criteria');
  }
  return reasons;
}

module.exports = {
  hasDecisionMaker,
  hasContactPath,
  hasBuyingSignals,
  qualifyCandidates,
};
