'use strict';

/**
 * SPEC-179 / EPIC-001 — Canonical Hypothesis Engine.
 *
 * One engine produces typed hypothesis nodes from a canonical MarketDefinition.
 * Business, terminology, and search-strategy hypotheses are facets of a single
 * cognitive step — not separate orchestrators.
 */

const { generateHypotheses } = require('../investigation/HypothesisGeneration');
const { generateInitialSearchHypotheses } = require('../investigation/SearchHypothesisEngine');
const {
  resolveMarketHypothesis,
  expandSearchStrategies,
  normalizeHypothesisKey,
} = require('./MarketHypothesisRegistry');
const { resolveSegmentKey } = require('../intelligence/MarketDefinition');

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

function buildCanonicalHypothesis(partial = {}) {
  return {
    id: partial.id || `ch-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    kind: partial.kind || HYPOTHESIS_KIND.BUSINESS,
    text: partial.text || '',
    status: partial.status || HYPOTHESIS_STATUS.OPEN,
    requiredEvidence: Array.isArray(partial.requiredEvidence) ? partial.requiredEvidence.slice() : [],
    searchTerms: Array.isArray(partial.searchTerms) ? partial.searchTerms.slice() : [],
    gap: partial.gap != null ? partial.gap : null,
    rationale: partial.rationale || '',
    parentId: partial.parentId || null,
    confidence: partial.confidence != null ? partial.confidence : null,
    metadata: partial.metadata || null,
  };
}

function businessHypothesesToCanonical(hypotheses = []) {
  return hypotheses.map((hyp) =>
    buildCanonicalHypothesis({
      id: hyp.id,
      kind: HYPOTHESIS_KIND.BUSINESS,
      text: hyp.text,
      status: hyp.status || HYPOTHESIS_STATUS.OPEN,
      requiredEvidence: hyp.requiredEvidence || hyp.missingEvidence || [],
      gap: hyp.gap || null,
      rationale: hyp.rationale || 'Business hypothesis from ICP evidence gap analysis.',
      parentId: hyp.parentId || null,
      confidence: hyp.confidence,
      metadata: { entityId: hyp.entityId || null, minConfidence: hyp.minConfidence },
    })
  );
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
      metadata: { spawnedFrom: hyp.spawnedFrom || null, evidence: hyp.evidence || null },
    })
  );
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
    const hypothesis = resolveMarketHypothesis(key);
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
    ? businessHypothesesToCanonical(generateHypotheses(marketDefinition, mission, opts))
    : [];

  const terminology = includeTerminology
    ? terminologyHypothesesToCanonical(
        generateInitialSearchHypotheses(marketDefinition, opts)
      )
    : [];

  const searchStrategies = includeSearchStrategies
    ? searchStrategyHypothesesToCanonical(marketDefinition, opts)
    : [];

  return {
    hypotheses: [...business, ...terminology, ...searchStrategies],
    business,
    terminology,
    searchStrategies,
  };
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
      requiredEvidence: hyp.requiredEvidence,
      missingEvidence: hyp.requiredEvidence,
      gap: hyp.gap,
      confidence: hyp.confidence,
      rationale: hyp.rationale,
    }));
}

module.exports = {
  HYPOTHESIS_KIND,
  HYPOTHESIS_STATUS,
  buildCanonicalHypothesis,
  generateCanonicalHypotheses,
  businessHypothesesForPlanner,
};
