'use strict';

/**
 * SPEC-142 — Missing Evidence.
 * Scout knows exactly why confidence is low and what to search for next.
 */

const { EVIDENCE_GAPS } = require('./types');

const GAP_TO_EVIDENCE_TYPES = Object.freeze({
  [EVIDENCE_GAPS.DECISION_MAKER]: ['website', 'linkedin', 'contacts', 'people'],
  [EVIDENCE_GAPS.PORTFOLIO_SIZE]: ['website', 'property_portfolio', 'county_records', 'linkedin'],
  [EVIDENCE_GAPS.CLEANING_RESPONSIBILITY]: ['website', 'reviews', 'hiring_activity', 'vendor_references'],
  [EVIDENCE_GAPS.CONTACT_PATH]: ['website', 'contacts', 'emails', 'phone'],
  [EVIDENCE_GAPS.BUYING_SIGNALS]: ['news', 'linkedin', 'hiring_activity', 'reviews'],
  [EVIDENCE_GAPS.BUSINESS_FIT]: ['website', 'business_fit', 'address', 'company_size'],
  [EVIDENCE_GAPS.GEOGRAPHIC_FIT]: ['address', 'website', 'geographic_fit'],
  [EVIDENCE_GAPS.VENDOR_RELATIONSHIP]: ['website', 'reviews', 'vendor_references'],
  [EVIDENCE_GAPS.COMPANY_SIZE]: ['website', 'linkedin', 'company_size'],
  [EVIDENCE_GAPS.OWNERSHIP]: ['website', 'linkedin', 'ownership'],
});

function normalizeEvidenceType(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
}

function evidenceSatisfiesGap(gap, collectedTypes) {
  const required = GAP_TO_EVIDENCE_TYPES[gap] || [gap];
  const normalized = new Set(collectedTypes.map(normalizeEvidenceType));
  return required.some((req) => normalized.has(normalizeEvidenceType(req)));
}

/**
 * Determine missing evidence for a hypothesis given collected evidence.
 * @param {object} hypothesis
 * @param {object[]} collectedEvidence
 * @returns {object}
 */
function determineHypothesisMissing(hypothesis, collectedEvidence = []) {
  const collectedTypes = collectedEvidence.map(
    (e) => e.evidenceType || e.type || e.source || e.kind
  );
  const missing = (hypothesis.requiredEvidence || []).filter((req) => {
    const norm = normalizeEvidenceType(req);
    return !collectedTypes.some((c) => normalizeEvidenceType(c) === norm || normalizeEvidenceType(c).includes(norm));
  });

  return {
    hypothesisId: hypothesis.id,
    entityId: hypothesis.entityId,
    currentConfidence: hypothesis.confidence,
    missing,
    collected: collectedTypes,
  };
}

/**
 * Aggregate missing evidence across all hypotheses and claims.
 * @param {object} input
 * @returns {object}
 */
function determineMissingEvidence(input = {}) {
  const hypotheses = input.hypotheses || [];
  const claims = input.claims || [];
  const gapSet = new Set();
  const byHypothesis = [];

  for (const hyp of hypotheses) {
    const result = determineHypothesisMissing(hyp, hyp.collectedEvidence || []);
    byHypothesis.push(result);
    for (const m of result.missing) gapSet.add(m);
    if (hyp.status === 'open' && hyp.confidence != null && hyp.confidence < hyp.minConfidence) {
      const gapKeys = Object.keys(GAP_TO_EVIDENCE_TYPES);
      for (const gap of gapKeys) {
        if (!evidenceSatisfiesGap(gap, result.collected)) gapSet.add(gap);
      }
    }
  }

  for (const claim of claims) {
    for (const m of claim.missingEvidence || []) gapSet.add(m);
  }

  const missing = [...gapSet];
  const avgConfidence =
    claims.length > 0
      ? Number((claims.reduce((s, c) => s + (c.confidence || 0), 0) / claims.length).toFixed(2))
      : hypotheses.length > 0 && hypotheses.some((h) => h.confidence != null)
        ? Number(
            (
              hypotheses.filter((h) => h.confidence != null).reduce((s, h) => s + h.confidence, 0) /
              hypotheses.filter((h) => h.confidence != null).length
            ).toFixed(2)
          )
        : 0;

  return {
    currentConfidence: avgConfidence,
    missing,
    byHypothesis,
    gapCount: missing.length,
  };
}

/**
 * Map evidence types to investigation gaps.
 * @param {string} evidenceType
 * @returns {string[]}
 */
function gapsForEvidenceType(evidenceType) {
  const norm = normalizeEvidenceType(evidenceType);
  const gaps = [];
  for (const [gap, types] of Object.entries(GAP_TO_EVIDENCE_TYPES)) {
    if (types.some((t) => normalizeEvidenceType(t) === norm)) gaps.push(gap);
  }
  return gaps;
}

module.exports = {
  GAP_TO_EVIDENCE_TYPES,
  evidenceSatisfiesGap,
  determineHypothesisMissing,
  determineMissingEvidence,
  gapsForEvidenceType,
};
