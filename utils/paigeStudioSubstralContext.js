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
const { buildPaigeFirstTouchDoctrineContext } = require('./paigeStudioSubstralOutboundDoctrine');

function buildPaigeStudioSubstralContext(clientConfig = {}, assessment = null, scoutIntelligence = null) {
  const evidence = assessment ? buildPaigeWebEvidenceContext(assessment) : null;
  const scout = scoutIntelligence?.studio_scout_intelligence
    || scoutIntelligence?.intelligence
    || scoutIntelligence;
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
    first_touch_outbound: buildPaigeFirstTouchDoctrineContext(),
    supported_web_evidence: evidence,
    scout_prospect_intelligence: scout?.company_name ? {
      why_they_fit: scout.why_they_fit,
      website_issues_observed: scout.website_issues_observed || [],
      business_strength_signals: scout.business_strength_signals || [],
      recommended_outreach_angle: scout.recommended_outreach_angle,
      confidence: scout.confidence,
      first_message_notes: scout.first_message_notes,
      studio_fit_score: scout.studio_fit_score,
    } : null,
    usage_note: scout?.recommended_outreach_angle
      ? 'Draft outreach ONLY from scout_prospect_intelligence — do not invent issues or angles. Reference supported_web_evidence when citing site facts.'
      : evidence
        ? 'Reference only supported_findings_for_copy. Sell getting the decision right — diagnosis before design.'
        : 'No website findings yet. Do not imply audit results or prescribe redesign.',
  };
}

module.exports = {
  buildPaigeStudioSubstralContext,
};
