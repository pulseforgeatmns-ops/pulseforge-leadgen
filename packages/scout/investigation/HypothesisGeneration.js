'use strict';

/**
 * SPEC-142 — Hypothesis Generation.
 * Before collecting evidence Scout creates hypotheses about what must be true.
 */

const { buildHypothesis, EVIDENCE_GAPS, DEFAULT_CONFIDENCE_THRESHOLD } = require('./types');

const SEGMENT_HYPOTHESES = Object.freeze({
  property_management: [
    {
      text: 'Company manages multiple rental or STR properties.',
      requiredEvidence: ['website', 'property_portfolio', 'county_records', 'linkedin'],
      gap: EVIDENCE_GAPS.PORTFOLIO_SIZE,
    },
    {
      text: 'Company outsources or would outsource cleaning services.',
      requiredEvidence: ['website', 'reviews', 'hiring_activity', 'vendor_references'],
      gap: EVIDENCE_GAPS.CLEANING_RESPONSIBILITY,
    },
    {
      text: 'A reachable decision maker exists for cleaning vendor decisions.',
      requiredEvidence: ['website', 'linkedin', 'contacts'],
      gap: EVIDENCE_GAPS.DECISION_MAKER,
    },
  ],
  short_term_rental: [
    {
      text: 'Property manager owns or manages multiple STR properties.',
      requiredEvidence: ['website', 'property_portfolio', 'county_records', 'linkedin'],
      gap: EVIDENCE_GAPS.PORTFOLIO_SIZE,
    },
    {
      text: 'Turnover cleaning is outsourced or a pain point.',
      requiredEvidence: ['website', 'reviews', 'hiring_activity'],
      gap: EVIDENCE_GAPS.CLEANING_RESPONSIBILITY,
    },
  ],
  law_firm: [
    {
      text: 'Law firm is a single-tenant professional office suitable for commercial cleaning.',
      requiredEvidence: ['website', 'address', 'company_size'],
      gap: EVIDENCE_GAPS.BUSINESS_FIT,
    },
    {
      text: 'A reachable office manager or partner handles vendor decisions.',
      requiredEvidence: ['website', 'linkedin', 'contacts'],
      gap: EVIDENCE_GAPS.DECISION_MAKER,
    },
  ],
  accounting: [
    {
      text: 'Accounting practice is a single-tenant professional office suitable for commercial cleaning.',
      requiredEvidence: ['website', 'address', 'company_size'],
      gap: EVIDENCE_GAPS.BUSINESS_FIT,
    },
    {
      text: 'A reachable office manager or partner handles vendor decisions.',
      requiredEvidence: ['website', 'linkedin', 'contacts'],
      gap: EVIDENCE_GAPS.DECISION_MAKER,
    },
  ],
});

const UNIVERSAL_HYPOTHESES = Object.freeze([
  {
    text: 'Company fits the target ICP for this mission.',
    requiredEvidence: ['website', 'business_fit', 'geographic_fit'],
    gap: EVIDENCE_GAPS.BUSINESS_FIT,
  },
  {
    text: 'Company shows timing or buying signals for outreach.',
    requiredEvidence: ['news', 'linkedin', 'hiring_activity', 'reviews'],
    gap: EVIDENCE_GAPS.BUYING_SIGNALS,
  },
  {
    text: 'A viable contact path exists for outreach.',
    requiredEvidence: ['website', 'contacts', 'emails', 'phone'],
    gap: EVIDENCE_GAPS.CONTACT_PATH,
  },
]);

function normalizeSegmentKey(segment) {
  return String(segment || '')
    .toLowerCase()
    .replace(/\s+/g, '_');
}

/**
 * Generate hypotheses from market definition and mission.
 * @param {object} marketDefinition
 * @param {object} [mission]
 * @param {object} [opts]
 * @returns {object[]}
 */
function generateHypotheses(marketDefinition, mission = {}, opts = {}) {
  const threshold = opts.confidenceThreshold || DEFAULT_CONFIDENCE_THRESHOLD;
  const segments = marketDefinition.segments || [];
  if (marketDefinition.segment && !segments.length) segments.push(marketDefinition.segment);

  const templates = [];
  const seenTexts = new Set();

  for (const segment of segments) {
    const key = normalizeSegmentKey(segment);
    for (const tpl of SEGMENT_HYPOTHESES[key] || []) {
      if (seenTexts.has(tpl.text)) continue;
      seenTexts.add(tpl.text);
      templates.push(tpl);
    }
  }

  for (const tpl of UNIVERSAL_HYPOTHESES) {
    if (seenTexts.has(tpl.text)) continue;
    seenTexts.add(tpl.text);
    templates.push(tpl);
  }

  return templates.map((tpl, idx) =>
    buildHypothesis({
      id: `hyp-${idx + 1}`,
      text: tpl.text,
      requiredEvidence: tpl.requiredEvidence.slice(),
      missingEvidence: tpl.requiredEvidence.slice(),
      gap: tpl.gap || null,
      rationale: tpl.rationale || `Evidence gap "${tpl.gap}" must be resolved for this segment hypothesis.`,
      minConfidence: threshold,
      confidence: null,
    })
  );
}

/**
 * Attach per-candidate hypotheses (entity-scoped copies).
 * @param {object[]} baseHypotheses
 * @param {object} candidate
 * @returns {object[]}
 */
function generateCandidateHypotheses(baseHypotheses, candidate) {
  return baseHypotheses.map((hyp) =>
    buildHypothesis({
      ...hyp,
      id: `${hyp.id}:${candidate.id}`,
      entityId: candidate.id,
      text: `${candidate.name}: ${hyp.text}`,
    })
  );
}

module.exports = {
  SEGMENT_HYPOTHESES,
  UNIVERSAL_HYPOTHESES,
  generateHypotheses,
  generateCandidateHypotheses,
};
