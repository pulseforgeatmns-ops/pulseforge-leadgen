'use strict';

const {
  ensureStudioSubstralMission,
  findStudioSubstralClient,
} = require('../utils/studioSubstralTenant');
const { listAssessmentOpportunities } = require('./studioSubstralPersistence');
const { listAssessmentsForClient } = require('./websiteOpportunityPersistence');
const { prioritizeStudioSubstralOpportunities } = require('../utils/studioSubstralMaxPrioritization');
const { evaluateStudioSubstralOutboundReadiness } = require('../utils/studioSubstralOutboundGovernance');
const { ASSESSMENT_STAGE } = require('../utils/studioSubstralAssessmentWorkflow');

async function buildStudioSubstralOperatorSnapshot(db, clientId) {
  const client = await findStudioSubstralClient(db);
  if (!client || Number(client.id) !== Number(clientId)) {
    const err = new Error('Studio Substral tenant scope required');
    err.status = 403;
    throw err;
  }

  const { missionId, mission } = await ensureStudioSubstralMission(db);
  const assessmentRequests = await listAssessmentOpportunities(db, clientId, { limit: 25 });
  const assessments = await listAssessmentsForClient(db, clientId, { limit: 50 });
  const prioritized = prioritizeStudioSubstralOpportunities({ assessments, assessmentRequests });
  const outbound = await evaluateStudioSubstralOutboundReadiness(db, clientId);

  const pendingIntake = assessmentRequests.filter((row) => row.stage === ASSESSMENT_STAGE.REQUESTED);

  return {
    tenant: {
      id: client.id,
      slug: client.slug,
      name: client.name,
      domain: 'studiosubstral.com',
      scoring_profile: client.scoring_profile,
    },
    mission: {
      id: missionId,
      objective: mission?.objective,
      constraints: mission?.constraints,
      active: true,
    },
    assessment_requests: assessmentRequests.map((row) => ({
      id: row.id,
      domain: row.domain,
      stage: row.stage,
      decision_context: row.decision_context,
      recommended_next_action: row.recommended_next_action,
      agent_action_id: row.agent_action_id,
      created_at: row.created_at,
      updated_at: row.updated_at,
    })),
    top_website_opportunities: prioritized.top_website_opportunities.slice(0, 5),
    evidence_summary: {
      pending_intake_count: pendingIntake.length,
      woi_assessment_count: assessments.length,
    },
    outbound_governance: outbound,
    prioritized: prioritized.combined.slice(0, 8),
  };
}

module.exports = {
  buildStudioSubstralOperatorSnapshot,
};
