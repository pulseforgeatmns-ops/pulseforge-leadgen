'use strict';

/**
 * Paige copy context for Studio Substral (SPEC-SUBSTRAL-PF-001).
 * Read-only doctrine injection — Paige remains copy owner.
 */

const { buildPaigeWebEvidenceContext } = require('./paigeWebEvidenceContext');
const {
  SUBSTRAL_BRAND_VOICE,
  SUBSTRAL_MISSION_OBJECTIVE,
} = require('./studioSubstralTenant');

function buildPaigeStudioSubstralContext(clientConfig = {}, assessment = null) {
  const evidence = assessment ? buildPaigeWebEvidenceContext(assessment) : null;
  return {
    brand: 'Studio Substral',
    domain: 'studiosubstral.com',
    mission_objective: SUBSTRAL_MISSION_OBJECTIVE,
    voice_rules: {
      contractions: true,
      plain_language: true,
      short_sentences: true,
      evidence_specific_opening: true,
      no_generic_agency_language: true,
      no_manufactured_urgency: true,
      no_unsupported_performance_claims: true,
      no_redesign_assumption: true,
    },
    primary_cta: 'paid website assessment',
    forbidden_ctas: ['redesign consultation', 'free strategy call', 'website makeover'],
    brand_voice: clientConfig.brand_voice || SUBSTRAL_BRAND_VOICE,
    never_say: clientConfig.never_say || null,
    lead_with: clientConfig.lead_with || 'Evidence-first website assessment',
    supported_web_evidence: evidence,
    usage_note: evidence
      ? 'Reference only supported_findings_for_copy. Sell getting the decision right — diagnosis before design.'
      : 'No website findings yet. Do not imply audit results or prescribe redesign.',
  };
}

module.exports = {
  buildPaigeStudioSubstralContext,
};
