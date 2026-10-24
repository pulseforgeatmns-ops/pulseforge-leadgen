'use strict';

const {
  CAPABILITY_ID,
  CAPABILITY_VERSION,
  EVIDENCE_CLASS,
  RECOMMENDED_ACTIONS,
  SCORE_COMPONENTS,
  SCORE_MAX,
  DEFAULT_ECONOMICS_CONFIG,
  WEB_EVENT_TYPES,
  buildFinding,
  buildAssessmentOutput,
} = require('./types');
const {
  createWebsiteOpportunityIntelligenceCapability,
} = require('./WebsiteOpportunityIntelligence');
const { runDeterministicAudit, normalizeDomain } = require('./audit/deterministicAudit');
const {
  computeOpportunityScore,
  recommendAction,
  scoreWebsiteDeficiency,
  scoreBuyingSignals,
  scoreProjectEconomics,
} = require('./scoring');
const { computeProjectEconomics } = require('./economics');
const { gatherBusinessEvidence } = require('./businessEvidence');
const { buildCommercialDiagnosis } = require('./diagnosis');
const { buildWebsiteOpportunityAssessment } = require('./assessment');
const { mergeFindings, enforceEvidenceIntegrity, topFindings, partitionEvidence } = require('./evidence');
const {
  buildInferredFindings,
  assertInferredIntegrity,
  isDuplicateOfSource,
} = require('./inference');
const {
  evaluateCohortAdmission,
  assembleStratifiedCohort,
} = require('./discoveryAdmission');
const { emitWebEvent, buildWebEvent } = require('./observability');

async function assessWebsiteOpportunity(input = {}, deps = {}) {
  const cap = createWebsiteOpportunityIntelligenceCapability(deps);
  const context = {
    clientId: input.client_id || input.clientId,
    tenantId: input.tenant_id || input.tenantId || input.client_id,
    missionId: input.mission_id,
    inputs: input,
  };
  const result = await cap.execute(context, deps.runtime || {});
  if (result.status !== 'completed') {
    throw new Error(result.errors?.[0]?.message || 'Website opportunity assessment failed');
  }
  return result.outputs;
}

module.exports = {
  CAPABILITY_ID,
  CAPABILITY_VERSION,
  EVIDENCE_CLASS,
  RECOMMENDED_ACTIONS,
  SCORE_COMPONENTS,
  SCORE_MAX,
  DEFAULT_ECONOMICS_CONFIG,
  WEB_EVENT_TYPES,
  buildFinding,
  buildAssessmentOutput,
  createWebsiteOpportunityIntelligenceCapability,
  assessWebsiteOpportunity,
  runDeterministicAudit,
  normalizeDomain,
  computeOpportunityScore,
  recommendAction,
  scoreWebsiteDeficiency,
  scoreBuyingSignals,
  scoreProjectEconomics,
  computeProjectEconomics,
  gatherBusinessEvidence,
  buildCommercialDiagnosis,
  buildWebsiteOpportunityAssessment,
  mergeFindings,
  enforceEvidenceIntegrity,
  topFindings,
  partitionEvidence,
  buildInferredFindings,
  assertInferredIntegrity,
  isDuplicateOfSource,
  evaluateCohortAdmission,
  assembleStratifiedCohort,
  emitWebEvent,
  buildWebEvent,
};
