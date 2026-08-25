'use strict';

/**
 * SPEC-158 — Search Hypothesis Engine.
 * Scout reasons about how markets describe themselves — not just operator language.
 */

const { asText } = require('../../max/scoutAcquisition/Types');
const { resolveSegmentKey } = require('../intelligence/MarketDefinition');

const HYPOTHESIS_STATUS = Object.freeze({
  OPEN: 'open',
  CONFIRMED: 'confirmed',
  REJECTED: 'rejected',
  INCONCLUSIVE: 'inconclusive',
});

const TERMINOLOGY_HYPOTHESIS_TEMPLATES = Object.freeze({
  short_term_rental: Object.freeze([
    {
      text: 'Operators use Vacation Rental terminology rather than short-term rental language.',
      searchTerms: ['Vacation Rental Management', 'Vacation Rental'],
      rationale: 'Markets often self-describe as vacation rental operators, not STR.',
    },
    {
      text: 'Operators advertise as Property Managers.',
      searchTerms: ['Property Manager', 'Property Management'],
      rationale: 'Many STR operators register as property management companies.',
    },
    {
      text: 'Operators are known through Airbnb listings rather than business names.',
      searchTerms: ['Airbnb Host', 'Airbnb Property Manager'],
      rationale: 'Platform-native terminology may surface operators missed by generic STR queries.',
    },
    {
      text: 'Operators serve executive or corporate housing markets.',
      searchTerms: ['Executive Stay', 'Corporate Housing', 'Executive Housing'],
      rationale: 'Adjacent hospitality segments overlap with STR operations.',
    },
  ]),
  property_management: Object.freeze([
    {
      text: 'Operators use Property Management terminology exclusively.',
      searchTerms: ['Property Management', 'Property Manager'],
      rationale: 'Primary market self-description.',
    },
    {
      text: 'Operators manage vacation or short-term rental portfolios.',
      searchTerms: ['Vacation Property Manager', 'Vacation Rental Management'],
      rationale: 'STR overlap within property management.',
    },
  ]),
  law_firm: Object.freeze([
    {
      text: 'Firms describe themselves as attorneys rather than law firms.',
      searchTerms: ['Attorney', 'Law Practice'],
      rationale: 'Solo and small firms often use attorney-first language.',
    },
  ]),
  accounting: Object.freeze([
    {
      text: 'Practices use CPA terminology rather than accounting firm.',
      searchTerms: ['CPA', 'Certified Public Accountant'],
      rationale: 'Professional credential is primary market identifier.',
    },
  ]),
});

const UNIVERSAL_SEARCH_HYPOTHESES = Object.freeze([
  {
    text: 'Market uses adjacent-industry terminology for the same buyer.',
    searchTerms: [],
    rationale: 'When primary terminology underperforms, adjacent market language may surface targets.',
    deriveFromAdjacent: true,
  },
]);

const DEFAULT_RESULT_THRESHOLD = 8;

function buildSearchHypothesis(partial = {}) {
  return {
    id: partial.id || `sh-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    kind: 'terminology_search',
    text: partial.text || '',
    searchTerms: Array.isArray(partial.searchTerms) ? partial.searchTerms.slice() : [],
    rationale: partial.rationale || '',
    status: partial.status || HYPOTHESIS_STATUS.OPEN,
    confidence: partial.confidence != null ? partial.confidence : null,
    evidence: partial.evidence || null,
    parentId: partial.parentId || null,
    spawnedFrom: partial.spawnedFrom || null,
  };
}

function templatesForMarket(marketDefinition = {}) {
  const key = marketDefinition.segmentKey || resolveSegmentKey(marketDefinition.segments || []);
  const segmentTemplates = TERMINOLOGY_HYPOTHESIS_TEMPLATES[key] || [];
  return [...segmentTemplates, ...UNIVERSAL_SEARCH_HYPOTHESES];
}

/**
 * Generate initial search hypotheses from market definition.
 * @param {object} marketDefinition
 * @param {object} [opts]
 * @returns {object[]}
 */
function generateInitialSearchHypotheses(marketDefinition = {}, opts = {}) {
  const templates = templatesForMarket(marketDefinition);
  const hypotheses = [];
  const seen = new Set();

  for (const tpl of templates) {
    let searchTerms = tpl.searchTerms.slice();
    if (tpl.deriveFromAdjacent && marketDefinition.adjacentMarkets) {
      searchTerms = marketDefinition.adjacentMarkets.slice(0, 2);
    }
    if (!searchTerms.length) continue;

    const key = `${tpl.text}|${searchTerms.join(',')}`;
    if (seen.has(key)) continue;
    seen.add(key);

    hypotheses.push(
      buildSearchHypothesis({
        id: `sh-init-${hypotheses.length + 1}`,
        text: tpl.text,
        searchTerms,
        rationale: tpl.rationale,
      })
    );
  }

  if (!hypotheses.length && marketDefinition.terminology) {
    hypotheses.push(
      buildSearchHypothesis({
        id: 'sh-init-1',
        text: `Operators use ${marketDefinition.operatorSegment || marketDefinition.terminology[0]} terminology.`,
        searchTerms: marketDefinition.terminology.slice(0, 4),
        rationale: 'Derived from semantic market definition terminology.',
      })
    );
  }

  if (opts.terminologyLearning && opts.terminologyLearning.length) {
    const ranked = opts.terminologyLearning.slice().sort((a, b) => (b.performance || 0) - (a.performance || 0));
    for (const entry of ranked.slice(0, 2)) {
      if (!entry.terminology) continue;
      hypotheses.unshift(
        buildSearchHypothesis({
          id: `sh-learn-${hypotheses.length + 1}`,
          text: `Prior missions found "${entry.terminology}" performed better in ${entry.geography || 'this market'}.`,
          searchTerms: [entry.terminology],
          rationale: entry.reason || 'Terminology learning from prior investigations.',
        })
      );
    }
  }

  return hypotheses;
}

/**
 * Evaluate a hypothesis branch from search results.
 * @param {object} hypothesis
 * @param {object} results
 * @param {object} [opts]
 * @returns {object}
 */
function evaluateHypothesisBranch(hypothesis, results = {}, opts = {}) {
  const threshold = opts.resultThreshold != null ? opts.resultThreshold : DEFAULT_RESULT_THRESHOLD;
  const resultCount = results.resultCount != null ? Number(results.resultCount) : 0;
  const uniqueCandidates = results.uniqueCandidates != null ? Number(results.uniqueCandidates) : resultCount;

  let status = HYPOTHESIS_STATUS.REJECTED;
  let confidence = 0.1;

  if (uniqueCandidates >= threshold) {
    status = HYPOTHESIS_STATUS.CONFIRMED;
    confidence = Math.min(0.95, 0.5 + uniqueCandidates / (threshold * 3));
  } else if (uniqueCandidates > 0) {
    status = HYPOTHESIS_STATUS.INCONCLUSIVE;
    confidence = Math.min(0.6, 0.2 + uniqueCandidates / threshold);
  }

  return {
    ...hypothesis,
    status,
    confidence: Number(confidence.toFixed(2)),
    evidence: {
      resultCount,
      uniqueCandidates,
      threshold,
      searchTerms: hypothesis.searchTerms,
      dominantConcept: results.dominantConcept || null,
    },
  };
}

/**
 * Generate follow-up hypotheses when prior branches underperform.
 * @param {object} marketDefinition
 * @param {object[]} attemptedHypotheses
 * @param {object} [opts]
 * @returns {object[]}
 */
function generateFollowUpHypotheses(marketDefinition = {}, attemptedHypotheses = [], opts = {}) {
  const attemptedTerms = new Set();
  const attemptedTexts = new Set();
  for (const hyp of attemptedHypotheses) {
    attemptedTexts.add(hyp.text);
    for (const term of hyp.searchTerms || []) attemptedTerms.add(term.toLowerCase());
  }

  const followUps = [];
  const templates = templatesForMarket(marketDefinition);

  for (const tpl of templates) {
    if (attemptedTexts.has(tpl.text)) continue;
    const unusedTerms = tpl.searchTerms.filter((t) => !attemptedTerms.has(t.toLowerCase()));
    if (!unusedTerms.length) continue;

    followUps.push(
      buildSearchHypothesis({
        id: `sh-follow-${followUps.length + 1}`,
        text: tpl.text,
        searchTerms: unusedTerms,
        rationale: tpl.rationale,
        spawnedFrom: 'insufficient_results',
        parentId: attemptedHypotheses[attemptedHypotheses.length - 1]?.id || null,
      })
    );
  }

  for (const adjacent of marketDefinition.adjacentMarkets || []) {
    const term = asText(adjacent);
    if (!term || attemptedTerms.has(term.toLowerCase())) continue;
    followUps.push(
      buildSearchHypothesis({
        id: `sh-adj-${followUps.length + 1}`,
        text: `Maybe targets are found through adjacent market "${term}".`,
        searchTerms: [term],
        rationale: 'Adjacent market hypothesis spawned from failed primary terminology.',
        spawnedFrom: 'adjacent_market',
        parentId: attemptedHypotheses[attemptedHypotheses.length - 1]?.id || null,
      })
    );
  }

  const maxFollowUps = opts.maxFollowUps != null ? opts.maxFollowUps : 3;
  return followUps.slice(0, maxFollowUps);
}

/**
 * Determine if investigation should continue spawning hypotheses.
 * @param {object[]} evaluatedHypotheses
 * @param {number} totalCandidates
 * @param {object} [opts]
 * @returns {boolean}
 */
function shouldContinueHypothesisInvestigation(evaluatedHypotheses = [], totalCandidates = 0, opts = {}) {
  const threshold = opts.resultThreshold != null ? opts.resultThreshold : DEFAULT_RESULT_THRESHOLD;
  if (totalCandidates >= threshold) return false;

  const confirmed = evaluatedHypotheses.some((h) => h.status === HYPOTHESIS_STATUS.CONFIRMED);
  if (confirmed && totalCandidates >= threshold / 2) return false;

  const maxHypotheses = opts.maxHypotheses != null ? opts.maxHypotheses : 6;
  return evaluatedHypotheses.length < maxHypotheses;
}

/**
 * Infer terminology revision from hypothesis evaluation results.
 * @param {object[]} evaluatedHypotheses
 * @returns {object|null}
 */
function inferTerminologyRevision(evaluatedHypotheses = []) {
  const confirmed = evaluatedHypotheses
    .filter((h) => h.status === HYPOTHESIS_STATUS.CONFIRMED)
    .sort((a, b) => (b.confidence || 0) - (a.confidence || 0));

  if (!confirmed.length) return null;

  const best = confirmed[0];
  const dominantTerminology = best.searchTerms && best.searchTerms[0];
  if (!dominantTerminology) return null;

  return {
    reason: `Evidence supports: ${best.text}`,
    dominantTerminology,
    addedTerminology: best.searchTerms || [],
    promoteToCustomerType: dominantTerminology,
  };
}

module.exports = {
  HYPOTHESIS_STATUS,
  TERMINOLOGY_HYPOTHESIS_TEMPLATES,
  DEFAULT_RESULT_THRESHOLD,
  buildSearchHypothesis,
  generateInitialSearchHypotheses,
  evaluateHypothesisBranch,
  generateFollowUpHypotheses,
  shouldContinueHypothesisInvestigation,
  inferTerminologyRevision,
};
