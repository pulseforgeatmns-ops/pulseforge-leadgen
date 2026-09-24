'use strict';

/**
 * SPEC-WEB-001 — Scout integration for web_design scoring profile.
 * Discovery + deterministic audit + assessment persistence. NO outbound.
 */

const {
  assessWebsiteOpportunity,
  normalizeDomain,
  RECOMMENDED_ACTIONS,
} = require('../packages/capabilities/websiteOpportunityIntelligence');
const {
  saveWebsiteOpportunityAssessment,
  insertWebsiteOpportunityEvent,
  ensureWebsiteOpportunitySchema,
} = require('./websiteOpportunityPersistence');
const { WEB_EVENT_TYPES } = require('../packages/capabilities/websiteOpportunityIntelligence/types');

async function assessDiscoveredBusiness(input, deps = {}) {
  const pool = deps.pool;
  const clientId = input.client_id || input.clientId;
  const domain = normalizeDomain(input.domain || input.url);
  if (!domain) {
    return { skipped: true, reason: 'missing_domain' };
  }

  await ensureWebsiteOpportunitySchema(pool);

  const onEvent = async (event) => {
    if (pool) await insertWebsiteOpportunityEvent(pool, event);
  };

  const assessment = await assessWebsiteOpportunity({
    client_id: clientId,
    mission_id: input.mission_id,
    prospect_id: input.prospect_id,
    domain,
    business: input,
    skipPuppeteer: input.skipPuppeteer ?? deps.skipPuppeteer ?? true,
    skipPageSpeed: input.skipPageSpeed ?? deps.skipPageSpeed ?? false,
  }, {
    pool,
    onEvent,
    fixtureAudit: deps.fixtureAudit,
    skipPuppeteer: input.skipPuppeteer ?? deps.skipPuppeteer ?? true,
  });

  if (pool && clientId) {
    await saveWebsiteOpportunityAssessment(pool, {
      client_id: clientId,
      mission_id: input.mission_id || null,
      prospect_id: input.prospect_id || null,
      cohort_tag: input.cohort_tag || null,
      business_name: input.company || input.business_name || domain,
      domain,
      industry: input.industry || input.vertical || null,
      location: input.location || input.address || null,
      payload: assessment,
      opportunity_score: assessment.opportunity_score,
      confidence: assessment.confidence,
      recommended_action: assessment.recommended_action,
      score_components: assessment.score_components,
      economics: assessment.economics,
    });
  }

  return {
    skipped: false,
    domain,
    assessment,
    recommended_action: assessment.recommended_action,
    opportunity_score: assessment.opportunity_score,
  };
}

function mapOpportunityScoreToIcp(opportunityScore, recommendedAction) {
  if (recommendedAction === RECOMMENDED_ACTIONS.DO_NOT_PURSUE) {
    return Math.min(opportunityScore || 0, 55);
  }
  return opportunityScore || 0;
}

function isWebDesignProfile(scoringProfile) {
  return scoringProfile === 'web_design';
}

module.exports = {
  assessDiscoveredBusiness,
  mapOpportunityScoreToIcp,
  isWebDesignProfile,
  WEB_EVENT_TYPES,
};
