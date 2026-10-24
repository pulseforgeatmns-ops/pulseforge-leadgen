'use strict';

/**
 * Paige future-outreach evidence context (SPEC-WEB-001).
 * Read-only structured facts — Paige remains copy owner.
 */

const { PROHIBITED_CLAIM_PATTERNS } = require('../packages/capabilities/websiteOpportunityIntelligence/types');

function sanitizeForPaige(text) {
  const value = String(text || '');
  for (const pattern of PROHIBITED_CLAIM_PATTERNS) {
    if (pattern.test(value)) {
      throw new Error(`Prohibited claim cannot be passed to Paige: ${pattern}`);
    }
  }
  return value;
}

function buildPaigeWebEvidenceContext(assessment) {
  const payload = assessment.payload || assessment;
  const verified = (payload.assessment?.verified_findings || payload.evidence_refs || [])
    .filter((f) => f.evidence_class === 'MEASURED' || f.evidence_class === 'OBSERVED')
    .slice(0, 8)
    .map((f) => ({
      evidence_class: f.evidence_class,
      summary: sanitizeForPaige(f.summary),
      source: f.source,
      ref: f.ref || f.id,
    }));

  return {
    domain: payload.domain,
    opportunity_score: payload.opportunity_score,
    recommended_action: payload.recommended_action,
    commercial_diagnosis: payload.commercial_diagnosis,
    opportunity_economics: payload.economics,
    supported_findings_for_copy: verified,
    recommended_remediation: (payload.assessment?.recommended_remediation || []).map(sanitizeForPaige),
    usage_note: 'Future outreach must reference only supported_findings_for_copy. Estimates are not willingness-to-pay.',
  };
}

module.exports = {
  buildPaigeWebEvidenceContext,
  sanitizeForPaige,
};
