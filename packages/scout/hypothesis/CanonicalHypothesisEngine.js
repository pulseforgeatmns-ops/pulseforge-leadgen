'use strict';

/**
 * SPEC-179 / ADR-094 — Canonical Hypothesis Engine.
 *
 * One engine produces typed, immutable hypothesis reasoning objects from a
 * canonical MarketDefinition. Business, terminology, and search-strategy
 * hypotheses are facets of a single cognitive step — not separate orchestrators.
 */

const { generateHypotheses } = require('../investigation/HypothesisGeneration');
const { generateInitialSearchHypotheses } = require('../investigation/SearchHypothesisEngine');
const {
  resolveMarketHypothesis,
  resolveMarketHypothesisBySegmentKey,
  expandSearchStrategies,
  normalizeHypothesisKey,
} = require('./MarketHypothesisRegistry');
const { resolveSegmentKey } = require('../intelligence/MarketDefinition');
const { deriveQuestionsForHypothesis } = require('../coverage/EvidenceRequirements');

const HYPOTHESIS_KIND = Object.freeze({
  BUSINESS: 'business',
  TERMINOLOGY: 'terminology',
  SEARCH_STRATEGY: 'search_strategy',
});

const HYPOTHESIS_STATUS = Object.freeze({
  OPEN: 'open',
  CONFIRMED: 'confirmed',
  REJECTED: 'rejected',
  INCONCLUSIVE: 'inconclusive',
});

const INVESTIGATION_STATUS = Object.freeze({
  PENDING: 'pending',
  IN_PROGRESS: 'in_progress',
  COMPLETE: 'complete',
});

function computeUncertainty(confidence) {
  if (confidence == null) return 1;
  return Math.max(0, Math.min(1, 1 - confidence));
}

function freezeCanonicalHypothesis(hypothesis) {
  return Object.freeze({
    ...hypothesis,
    requiredEvidence: Object.freeze(hypothesis.requiredEvidence.slice()),
    searchTerms: Object.freeze(hypothesis.searchTerms.slice()),
    supportingEvidence: Object.freeze(hypothesis.supportingEvidence.slice()),
    contradictoryEvidence: Object.freeze(hypothesis.contradictoryEvidence.slice()),
    generatedQuestions: Object.freeze(hypothesis.generatedQuestions.slice()),
    metadata: hypothesis.metadata ? Object.freeze({ ...hypothesis.metadata }) : null,
  });
}

function buildCanonicalHypothesis(partial = {}) {
  const confidence = partial.confidence != null ? partial.confidence : null;
  const uncertainty =
    partial.uncertainty != null ? partial.uncertainty : computeUncertainty(confidence);

  return freezeCanonicalHypothesis({
    id: partial.id || `ch-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    kind: partial.kind || HYPOTHESIS_KIND.BUSINESS,
    text: partial.text || '',
    status: partial.status || HYPOTHESIS_STATUS.OPEN,
    rationale: partial.rationale || '',
    confidence,
    uncertainty,
    gap: partial.gap != null ? partial.gap : null,
    requiredEvidence: Array.isArray(partial.requiredEvidence) ? partial.requiredEvidence.slice() : [],
    searchTerms: Array.isArray(partial.searchTerms) ? partial.searchTerms.slice() : [],
    supportingEvidence: Array.isArray(partial.supportingEvidence)
      ? partial.supportingEvidence.slice()
      : [],
    contradictoryEvidence: Array.isArray(partial.contradictoryEvidence)
      ? partial.contradictoryEvidence.slice()
      : [],
    investigationStatus: partial.investigationStatus || INVESTIGATION_STATUS.PENDING,
    generatedQuestions: Array.isArray(partial.generatedQuestions)
      ? partial.generatedQuestions.slice()
      : [],
    parentId: partial.parentId || null,
    metadata: partial.metadata || null,
  });
}

function businessHypothesesToCanonical(hypotheses = [], marketDefinition = {}) {
  return hypotheses.map((hyp) => {
    const plannerShape = {
      id: hyp.id,
      text: hyp.text,
      gap: hyp.gap || null,
      requiredEvidence: hyp.requiredEvidence || hyp.missingEvidence || [],
    };
    const generatedQuestions = deriveQuestionsForHypothesis(plannerShape, marketDefinition);

    return buildCanonicalHypothesis({
      id: hyp.id,
      kind: HYPOTHESIS_KIND.BUSINESS,
      text: hyp.text,
      status: hyp.status || HYPOTHESIS_STATUS.OPEN,
      requiredEvidence: hyp.requiredEvidence || hyp.missingEvidence || [],
      gap: hyp.gap || null,
      rationale: hyp.rationale || 'Business hypothesis from ICP evidence gap analysis.',
      parentId: hyp.parentId || null,
      confidence: hyp.confidence,
      uncertainty: hyp.uncertainty != null ? hyp.uncertainty : computeUncertainty(hyp.confidence),
      supportingEvidence: hyp.supportingEvidence || hyp.collectedEvidence || [],
      contradictoryEvidence: hyp.contradictoryEvidence || [],
      investigationStatus: hyp.investigationStatus || INVESTIGATION_STATUS.PENDING,
      generatedQuestions,
      metadata: { entityId: hyp.entityId || null, minConfidence: hyp.minConfidence },
    });
  });
}

function terminologyHypothesesToCanonical(hypotheses = []) {
  return hypotheses.map((hyp) =>
    buildCanonicalHypothesis({
      id: hyp.id,
      kind: HYPOTHESIS_KIND.TERMINOLOGY,
      text: hyp.text,
      status: hyp.status || HYPOTHESIS_STATUS.OPEN,
      searchTerms: hyp.searchTerms || [],
      rationale: hyp.rationale || 'Terminology hypothesis from market self-description analysis.',
      parentId: hyp.parentId || hyp.spawnedFrom || null,
      confidence: hyp.confidence,
      uncertainty: computeUncertainty(hyp.confidence),
      investigationStatus: INVESTIGATION_STATUS.PENDING,
      metadata: { spawnedFrom: hyp.spawnedFrom || null, evidence: hyp.evidence || null },
    })
  );
}

function resolveHypothesisForKey(key) {
  return resolveMarketHypothesisBySegmentKey(key) || resolveMarketHypothesis(key);
}

function searchStrategyHypothesesToCanonical(marketDefinition = {}, opts = {}) {
  const segmentKey =
    marketDefinition.segmentKey ||
    resolveSegmentKey(marketDefinition.segments || []) ||
    normalizeHypothesisKey(marketDefinition.market || marketDefinition.segment || '');

  const verticalKeys = [
    segmentKey,
    ...(marketDefinition.segments || []).map(normalizeHypothesisKey),
  ].filter(Boolean);

  const seen = new Set();
  const canonical = [];

  for (const key of verticalKeys) {
    const hypothesis = resolveHypothesisForKey(key);
    if (!hypothesis || seen.has(hypothesis.id)) continue;
    seen.add(hypothesis.id);

    const geo = {
      city:
        (marketDefinition.geography && marketDefinition.geography.label) ||
        marketDefinition.geography ||
        '',
      state: (marketDefinition.geography && marketDefinition.geography.state) || '',
    };

    const workloads = expandSearchStrategies(hypothesis, geo, opts);
    const searchTerms = [...new Set(workloads.map((row) => row.query).filter(Boolean))];

    canonical.push(
      buildCanonicalHypothesis({
        id: `ss-${hypothesis.id}`,
        kind: HYPOTHESIS_KIND.SEARCH_STRATEGY,
        text: hypothesis.statement,
        searchTerms,
        rationale: `Search strategy registry for ${hypothesis.id}.`,
        investigationStatus: INVESTIGATION_STATUS.PENDING,
        metadata: {
          hypothesisId: hypothesis.id,
          buyerRole: hypothesis.buyerRole,
          segmentKey: hypothesis.segmentKey,
          workloads,
        },
      })
    );
  }

  return canonical;
}

/**
 * Generate the unified canonical hypothesis set from a MarketDefinition.
 * @param {object} marketDefinition
 * @param {object} [mission]
 * @param {object} [opts]
 * @returns {{ hypotheses: object[], business: object[], terminology: object[], searchStrategies: object[] }}
 */
function generateCanonicalHypotheses(marketDefinition = {}, mission = {}, opts = {}) {
  const includeBusiness = opts.includeBusiness !== false;
  const includeTerminology = opts.includeTerminology !== false;
  const includeSearchStrategies = opts.includeSearchStrategies !== false;

  const business = includeBusiness
    ? businessHypothesesToCanonical(generateHypotheses(marketDefinition, mission, opts), marketDefinition)
    : [];

  const terminology = includeTerminology
    ? terminologyHypothesesToCanonical(generateInitialSearchHypotheses(marketDefinition, opts))
    : [];

  const searchStrategies = includeSearchStrategies
    ? searchStrategyHypothesesToCanonical(marketDefinition, opts)
    : [];

  return Object.freeze({
    hypotheses: Object.freeze([...business, ...terminology, ...searchStrategies]),
    business: Object.freeze(business),
    terminology: Object.freeze(terminology),
    searchStrategies: Object.freeze(searchStrategies),
  });
}

/**
 * Project canonical hypotheses back to business-hypothesis shape for legacy planners.
 * @param {object[]} canonicalHypotheses
 * @returns {object[]}
 */
function businessHypothesesForPlanner(canonicalHypotheses = []) {
  return canonicalHypotheses
    .filter((hyp) => hyp.kind === HYPOTHESIS_KIND.BUSINESS)
    .map((hyp) => ({
      id: hyp.id,
      text: hyp.text,
      requiredEvidence: [...hyp.requiredEvidence],
      missingEvidence: [...hyp.requiredEvidence],
      gap: hyp.gap,
      confidence: hyp.confidence,
      uncertainty: hyp.uncertainty,
      rationale: hyp.rationale,
      supportingEvidence: [...hyp.supportingEvidence],
      contradictoryEvidence: [...hyp.contradictoryEvidence],
      investigationStatus: hyp.investigationStatus,
      generatedQuestions: [...hyp.generatedQuestions],
    }));
}

module.exports = {
  HYPOTHESIS_KIND,
  HYPOTHESIS_STATUS,
  INVESTIGATION_STATUS,
  buildCanonicalHypothesis,
  freezeCanonicalHypothesis,
  generateCanonicalHypotheses,
  businessHypothesesForPlanner,
};
