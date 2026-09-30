'use strict';

/**
 * Max prioritization for Studio Substral opportunities — evidence strength,
 * commercial relevance, contact quality, decision context, and mission fit.
 */

const { prioritizeWebOpportunities } = require('./webOpportunityMaxPrioritization');
const { summarizeEvidenceStrength } = require('./studioSubstralLayers');
const { ASSESSMENT_STAGE } = require('./studioSubstralAssessmentWorkflow');

function scoreAssessmentRequest(row = {}) {
  const summary = row.evidence_summary || {};
  const sixLayer = row.six_layer_findings || {};
  const strength = summarizeEvidenceStrength(sixLayer);
  let score = 0;
  score += strength.measured * 8;
  score += strength.observed * 5;
  score += strength.inferred * 1;
  if (row.decision_context) score += 12;
  if (row.prospect_id) score += 10;
  if (row.stage === ASSESSMENT_STAGE.REQUESTED) score += 15;
  if (summary.intake_only) score += 8;
  return {
    max_priority_score: score,
    prioritization_factors: {
      evidence_strength: strength,
      has_decision_context: Boolean(row.decision_context),
      contact_linked: Boolean(row.prospect_id),
      stage: row.stage,
      mission_fit: 'paid_website_assessment',
      known_gaps: strength.total === 0 ? ['no_six_layer_evidence_yet'] : [],
    },
    max_questions: {
      why_worth_assessing: row.decision_context
        ? 'Prospect stated a real decision context with the request.'
        : 'Inbound paid-assessment intent — confirm commercial relevance before outreach.',
      evidence_we_have: strength.total
        ? `${strength.measured} measured, ${strength.observed} observed findings across six layers.`
        : 'Intake only — no live-site intelligence attached yet.',
      evidence_we_lack: strength.total === 0
        ? 'Six-layer website intelligence has not been collected for this domain.'
        : strength.inferred > 0 && strength.measured === 0
          ? 'Mostly inferred/unknown evidence — treat conclusions as provisional.'
          : null,
      next_action: row.recommended_next_action,
    },
  };
}

function scoreScoutProspect(row = {}) {
  const intel = row.studio_scout_intelligence || {};
  let score = Number(row.studio_fit_score || intel.studio_fit_score || 0) * 1.2;
  if (row.studio_confidence === 'high' || intel.confidence === 'high') score += 12;
  else if (row.studio_confidence === 'medium' || intel.confidence === 'medium') score += 6;
  if (row.studio_outreach_status === 'review_needed') score += 8;
  if (row.recommended_outreach_angle || intel.recommended_outreach_angle) score += 5;
  const breakdown = intel.score_breakdown || {};
  score += (breakdown.website_pain || 0) * 0.15;
  score += (breakdown.proof_gap || 0) * 0.2;
  return {
    max_priority_score: Math.round(score),
    prioritization_factors: {
      studio_fit_score: row.studio_fit_score || intel.studio_fit_score,
      studio_category: row.studio_category || intel.studio_category,
      confidence: row.studio_confidence || intel.confidence,
      outreach_status: row.studio_outreach_status,
      source: 'studio_substral_scout',
    },
    max_questions: {
      why_worth_assessing: intel.why_they_fit || row.proof_gap_summary || 'Scout-qualified mismatch between business strength and site presentation.',
      evidence_we_have: (intel.website_issues_observed || []).length
        ? (intel.website_issues_observed || []).slice(0, 3).join('; ')
        : row.website_pain_summary || 'Studio Scout intelligence on file — review structured payload.',
      evidence_we_lack: intel.confidence === 'low'
        ? 'Low confidence — verify decision-maker and site issues before outreach.'
        : null,
      next_action: (row.studio_fit_score || 0) >= 80
        ? 'Manual priority outreach review (Jake)'
        : 'Review Scout angle before Paige drafts first message',
    },
  };
}

function prioritizeStudioSubstralOpportunities({
  assessments = [],
  assessmentRequests = [],
  scoutProspects = [],
} = {}) {
  const rankedWeb = prioritizeWebOpportunities(assessments);
  const rankedRequests = assessmentRequests
    .map((row) => ({ ...row, ...scoreAssessmentRequest(row) }))
    .sort((a, b) => b.max_priority_score - a.max_priority_score);
  const rankedScout = scoutProspects
    .map((row) => ({ ...row, ...scoreScoutProspect(row) }))
    .sort((a, b) => b.max_priority_score - a.max_priority_score);

  return {
    top_website_opportunities: rankedWeb.slice(0, 10),
    assessment_requests: rankedRequests,
    scout_prospects: rankedScout,
    top_scout_prospects: rankedScout.slice(0, 10),
    combined: [...rankedWeb, ...rankedRequests, ...rankedScout]
      .sort((a, b) => (b.max_priority_score || 0) - (a.max_priority_score || 0)),
  };
}

module.exports = {
  prioritizeStudioSubstralOpportunities,
  scoreAssessmentRequest,
  scoreScoutProspect,
};
