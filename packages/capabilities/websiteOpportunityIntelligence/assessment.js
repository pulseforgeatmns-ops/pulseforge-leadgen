'use strict';

const { buildAssessmentOutput } = require('./types');
const { enforceEvidenceIntegrity, topFindings } = require('./evidence');
const { assertConservativeLanguage } = require('./diagnosis');

function buildWebsiteOpportunityAssessment({
  business,
  audit,
  businessFindings,
  findings,
  commercial_diagnosis,
  scoring,
  economics,
  recommendation,
}) {
  const allFindings = [...findings, ...(businessFindings || [])];
  const verified = topFindings(allFindings, 20);

  const executive = [];
  for (const f of topFindings(allFindings, 3)) {
    executive.push(`${f.summary} (${f.evidence_class})`);
  }
  if (!executive.length) executive.push('Insufficient verified evidence for executive summary');

  const commercialImplications = (commercial_diagnosis.commercially_important || [])
    .slice(0, 5)
    .map((line) => assertConservativeLanguage(
      line.includes('may') || line.includes('measured') ? line : `${line}. Impact depends on how customers use the site.`
    ));

  const remediation = commercial_diagnosis.advise_first || [];
  const acquisitionRecommendation = {
    action: recommendation.action,
    why: recommendation.why,
    worth_acquiring: commercial_diagnosis.worth_acquiring,
  };

  const assessment = {
    artifact_type: 'website_opportunity_assessment',
    prospect: {
      business_name: business.business_name,
      domain: audit.domain,
      industry: business.industry,
      location: business.location,
      business_evidence: {
        google_rating: business.google_rating,
        google_review_count: business.google_review_count,
        contact_name: business.contact_name,
        email: business.email,
        phone: business.phone,
      },
    },
    executive_diagnosis: executive.join(' '),
    verified_findings: verified,
    commercial_implications: commercialImplications,
    recommended_remediation: remediation,
    opportunity_economics: economics,
    opportunity_score: {
      total: scoring.opportunity_score,
      components: scoring.score_components,
      confidence: scoring.confidence,
    },
    acquisition_recommendation: acquisitionRecommendation,
    created_at: new Date().toISOString(),
  };

  return enforceEvidenceIntegrity(
    buildAssessmentOutput({
      domain: audit.domain,
      audited_at: audit.audited_at,
      technical_evidence: audit.technical_evidence,
      business_evidence: { business, findings: businessFindings },
      commercial_diagnosis,
      opportunity_score: scoring.opportunity_score,
      score_components: scoring.score_components,
      confidence: scoring.confidence,
      recommended_action: recommendation.action,
      economics,
      assessment,
    }),
    allFindings
  );
}

module.exports = {
  buildWebsiteOpportunityAssessment,
};
