'use strict';

/**
 * SPEC-074 — Max read-only consumer for Prospect Operating Brief.
 * Max may inspect the synthesis. Max must not execute from it.
 */

function defaultBriefService() {
  return require('../../../services/prospectOperatingBrief');
}

/**
 * @param {object} context
 * @returns {Promise<object>}
 */
async function getProspectOperatingBrief(context = {}) {
  const briefService = context.briefService || defaultBriefService();
  const options = {
    companyId: context.companyId,
    prospectId: context.prospectId,
    opportunityId: context.opportunityId,
    contactId: context.contactId,
    relationshipInteractionId:
      context.relationshipInteractionId || context.interactionId,
    clientId: context.clientId,
    days: context.days,
    includeMarketContext: context.includeMarketContext,
    includeRelationshipContext: context.includeRelationshipContext,
    pool: context.pool,
    store: context.store,
    loadCompanySnapshot: context.loadCompanySnapshot,
    relationshipService: context.relationshipService,
    marketBriefingService: context.marketBriefingService,
  };

  const brief = await briefService.getProspectOperatingBrief(options);
  return {
    ...brief,
    ok: brief && brief.ok !== false,
    kind: 'prospect_operating_brief',
    isEvidence: false,
    source: 'SPEC-074',
    inspectionOnly: true,
    autonomousExecution: false,
  };
}

module.exports = {
  getProspectOperatingBrief,
};
