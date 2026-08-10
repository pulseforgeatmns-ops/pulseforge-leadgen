'use strict';

/**
 * Max Synthesis Layer — ArtifactSynthesisContext.
 *
 * Provides Growth / Campaign (and future) artifact renderers with
 * normalized phrase-safe values only. Raw prior paragraphs remain on
 * `evidence` for citation — never for mid-sentence embedding.
 */

const {
  normalizeBusinessFacts,
  asEmbeddablePhrase,
  containsRawPromptFragment,
  findRawPromptFragments,
} = require('./BusinessFactNormalizer');

/**
 * @param {object} opts
 * @param {object} [opts.context] — campaign / growth planning context
 * @param {object} [opts.normalizedFacts]
 * @param {object} [opts.priorCriteriaPreview]
 * @param {object} [opts.priorCampaignPreview]
 * @param {object} [opts.priorGrowthDirection]
 * @param {object} [opts.slots]
 * @param {object} [opts.answers]
 * @param {object} [opts.priorArtifact] — generic prior artifact fields
 * @returns {object} synthesis context
 */
function buildArtifactSynthesisContext(opts = {}) {
  const facts = normalizeBusinessFacts({
    context: opts.context || {},
    normalizedFacts: opts.normalizedFacts || null,
    priorCriteriaPreview: opts.priorCriteriaPreview || null,
    priorCampaignPreview: opts.priorCampaignPreview || null,
    priorArtifact: opts.priorArtifact || opts.priorGrowthDirection || null,
    slots: opts.slots || {},
    answers: opts.answers || null,
    targetSegment: opts.targetSegment,
    targetSubtype: opts.targetSubtype,
    marketBound: opts.marketBound,
    campaignObjective: opts.campaignObjective,
    serviceMix: opts.serviceMix,
    idealCustomers: opts.idealCustomers,
    avoidCustomers: opts.avoidCustomers,
    proofAssets: opts.proofAssets,
    validationSignals: opts.validationSignals,
    disqualifyingSignals: opts.disqualifyingSignals,
  });

  const phrases = facts.phrases;

  function phrase(key, fallback = '') {
    if (phrases && phrases[key] != null && phrases[key] !== '') {
      return phrases[key];
    }
    if (facts[key] != null && facts[key] !== '') {
      if (Array.isArray(facts[key])) return facts[key].join(', ');
      return String(facts[key]);
    }
    return fallback;
  }

  /**
   * Embed a normalized phrase into a wrapper template.
   * Rejects attempts to insert raw instruction paragraphs.
   */
  function embed(wrapperTemplate, phraseKeyOrValue) {
    const value =
      typeof phraseKeyOrValue === 'string' && phrases[phraseKeyOrValue] != null
        ? phrases[phraseKeyOrValue]
        : typeof phraseKeyOrValue === 'string' && facts[phraseKeyOrValue] != null
          ? Array.isArray(facts[phraseKeyOrValue])
            ? facts[phraseKeyOrValue].join(', ')
            : String(facts[phraseKeyOrValue])
          : asEmbeddablePhrase(phraseKeyOrValue);
    const safe = asEmbeddablePhrase(value);
    const rendered = String(wrapperTemplate || '').replace(/\{\}/g, safe);
    if (containsRawPromptFragment(rendered)) {
      // Fall back to safe phrase alone rather than emit banned stitching.
      return safe;
    }
    return rendered;
  }

  return Object.freeze({
    facts,
    phrases,
    evidence: facts.evidence,
    phrase,
    embed,
    containsRawPromptFragment,
    findRawPromptFragments,
    /** Renderers must use phrases — raw prior fields are evidence only. */
    rawDisplayAllowed: false,
  });
}

/**
 * Short display name for Anchor Cleaning → Anchor (shared by renderers).
 */
function shortBusinessName(name) {
  const s = String(name || '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!s) return 'the business';
  return s.replace(/\s+Cleaning$/i, '') || s;
}

module.exports = {
  buildArtifactSynthesisContext,
  shortBusinessName,
};
