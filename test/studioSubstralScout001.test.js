'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  STUDIO_MIN_FIT_SCORE,
  STUDIO_PRIORITY_THRESHOLD,
  STUDIO_SCOUT_INSTRUCTION,
  STUDIO_SUBSTRAL_SCOUT_PLAN,
  mapIndustryToStudioCategory,
  isGenericOutreachAngle,
  buildStudioProspectIntelligence,
  evaluateStudioSubstralScoutProspect,
} = require('../services/studioSubstralScoutIntelligence');
const { scoreScoutProspect } = require('../utils/studioSubstralMaxPrioritization');
const { buildPaigeStudioSubstralContext } = require('../utils/paigeStudioSubstralContext');
const { SCORE_COMPONENTS, DIAGNOSIS_CLASS } = require('../packages/capabilities/websiteOpportunityIntelligence/types');

function woiComponents(overrides = {}) {
  return {
    [SCORE_COMPONENTS.WEBSITE_DEFICIENCY]: { score: overrides.deficiency ?? 16, max: 25 },
    [SCORE_COMPONENTS.COMMERCIAL_VALUE]: { score: overrides.commercial ?? 18, max: 25 },
    [SCORE_COMPONENTS.BUYING_SIGNALS]: { score: overrides.buying ?? 10, max: 20 },
    [SCORE_COMPONENTS.CONTACTABILITY]: { score: overrides.contact ?? 10, max: 15 },
    [SCORE_COMPONENTS.PROJECT_ECONOMICS]: { score: overrides.economics ?? 8, max: 15 },
  };
}

function sampleAssessment(components) {
  return {
    score_components: components,
    confidence: 0.72,
    recommended_action: 'AUDIT_WORTH_REVIEWING',
    commercial_diagnosis: { diagnosis_class: DIAGNOSIS_CLASS.REDESIGN_CANDIDATE },
    evidence_refs: [
      { category: 'conversion', summary: 'No obvious conversion path detected on homepage', evidence_class: 'OBSERVED' },
      { category: 'performance', summary: 'Homepage fetch exceeded 4s during audit', evidence_class: 'MEASURED' },
    ],
  };
}

describe('SPEC-STUDIO-SCOUT-001', () => {
  it('exposes scout instruction and Manchester NH batch plan mix', () => {
    assert.match(STUDIO_SCOUT_INSTRUCTION, /Studio Substral Opportunity Intelligence/);
    assert.equal(STUDIO_SUBSTRAL_SCOUT_PLAN.state, 'NH');
    assert.ok(STUDIO_SUBSTRAL_SCOUT_PLAN.cities.includes('Manchester'));
    const mix = STUDIO_SUBSTRAL_SCOUT_PLAN.batch_mix;
    assert.equal(mix.professional_services, 10);
    assert.equal(mix.property_and_home_services, 7);
    assert.equal(mix.medical_wellness_and_aesthetics, 5);
    assert.equal(mix.b2b_service_firms, 3);
  });

  it('maps verticals to studio categories', () => {
    assert.equal(mapIndustryToStudioCategory('law_firm'), 'professional_services');
    assert.equal(mapIndustryToStudioCategory('hvac'), 'property_and_home_services');
    assert.equal(mapIndustryToStudioCategory('dental'), 'medical_wellness_and_aesthetics');
    assert.equal(mapIndustryToStudioCategory('msp'), 'b2b_service_firms');
  });

  it('rejects generic outreach angles', () => {
    assert.equal(isGenericOutreachAngle('Your website could use an update.'), true);
    assert.equal(isGenericOutreachAngle('Harbor Law shows stronger reviews than the homepage trust cues suggest.'), false);
  });

  it('scores and accepts a credible mismatch prospect', () => {
    const lead = {
      company: 'Harbor Law Group',
      url: 'https://harborlaw.example',
      address: 'Manchester, NH',
      phone: '603-555-0100',
      email: 'info@harborlaw.example',
      google_rating: 4.6,
      google_review_count: 42,
    };
    const { intelligence, outreachStatus } = evaluateStudioSubstralScoutProspect({
      lead,
      assessment: sampleAssessment(woiComponents()),
      vertical: 'law_firm',
    });
    assert.equal(intelligence.accepted, true);
    assert.ok(intelligence.studio_fit_score >= STUDIO_MIN_FIT_SCORE);
    assert.ok(intelligence.recommended_outreach_angle.length > 40);
    assert.equal(isGenericOutreachAngle(intelligence.recommended_outreach_angle), false);
    assert.deepEqual(Object.keys(intelligence.score_breakdown).sort(), [
      'business_value_fit',
      'contactability',
      'outreach_angle_quality',
      'proof_gap',
      'website_pain',
    ]);
    if (intelligence.studio_fit_score >= STUDIO_PRIORITY_THRESHOLD) {
      assert.equal(outreachStatus, 'review_needed');
    }
  });

  it('rejects healthy-site prospects with reason', () => {
    const intel = buildStudioProspectIntelligence({
      lead: { company: 'Polished Co', url: 'https://polished.example', google_review_count: 2 },
      assessment: {
        score_components: woiComponents({ deficiency: 3, commercial: 20 }),
        commercial_diagnosis: { diagnosis_class: DIAGNOSIS_CLASS.HEALTHY_SITE },
        confidence: 0.8,
      },
      vertical: 'accounting',
    });
    assert.equal(intel.accepted, false);
    assert.match(intel.reject_reason, /strong|healthy|proof|qualification|Score/i);
  });

  it('Max and Paige consume structured scout output', () => {
    const intel = buildStudioProspectIntelligence({
      lead: {
        company: 'Summit HVAC',
        url: 'https://summithvac.example',
        google_review_count: 18,
        google_rating: 4.4,
        email: 'service@summithvac.example',
      },
      assessment: sampleAssessment(woiComponents({ deficiency: 14 })),
      vertical: 'hvac',
    });
    const maxRow = scoreScoutProspect({
      studio_fit_score: intel.studio_fit_score,
      studio_scout_intelligence: intel,
      studio_outreach_status: 'new',
      studio_confidence: intel.confidence,
    });
    assert.ok(maxRow.max_priority_score > 0);
    assert.ok(maxRow.max_questions.why_worth_assessing);

    const paige = buildPaigeStudioSubstralContext({}, null, intel);
    assert.ok(paige.scout_prospect_intelligence);
    assert.equal(paige.scout_prospect_intelligence.recommended_outreach_angle, intel.recommended_outreach_angle);
    assert.match(paige.usage_note, /scout_prospect_intelligence/);
  });
});
