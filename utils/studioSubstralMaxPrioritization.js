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

function prioritizeStudioSubstralOpportunities({ assessments = [], assessmentRequests = [] } = {}) {
  const rankedWeb = prioritizeWebOpportunities(assessments);
  const rankedRequests = assessmentRequests
    .map((row) => ({ ...row, ...scoreAssessmentRequest(row) }))
    .sort((a, b) => b.max_priority_score - a.max_priority_score);

  return {
    top_website_opportunities: rankedWeb.slice(0, 10),
    assessment_requests: rankedRequests,
    combined: [...rankedWeb, ...rankedRequests]
      .sort((a, b) => (b.max_priority_score || 0) - (a.max_priority_score || 0)),
  };
}

module.exports = {
  prioritizeStudioSubstralOpportunities,
  scoreAssessmentRequest,
};
