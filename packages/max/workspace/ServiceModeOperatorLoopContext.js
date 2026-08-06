'use strict';

/**
 * SPEC-075 — Max read-only consumer for Service Mode Operator Loop.
 * Max may inspect the manual action queue. Max must not execute from it.
 */

function defaultLoopService() {
  return require('../../../services/serviceModeOperatorLoop');
}

/**
 * @param {object} context
 * @returns {Promise<object>}
 */
async function getServiceModeOperatorLoop(context = {}) {
  const loopService = context.loopService || defaultLoopService();
  const options = {
    days: context.days,
    limit: context.limit,
    companyId: context.companyId,
    prospectId: context.prospectId,
    opportunityId: context.opportunityId,
    relationshipInteractionId:
      context.relationshipInteractionId || context.interactionId,
    clientId: context.clientId,
    includeMarketContext: context.includeMarketContext,
    pool: context.pool,
    store: context.store,
    loadCompanySnapshot: context.loadCompanySnapshot,
    relationshipService: context.relationshipService,
    marketBriefingService: context.marketBriefingService,
    getProspectOperatingBrief: context.getProspectOperatingBrief,
    briefService: context.briefService,
  };

  const loop = await loopService.getServiceModeOperatorLoop(options);
  return {
    ...loop,
    ok: loop && loop.ok !== false,
    kind: 'service_mode_operator_loop',
    isEvidence: false,
    source: 'SPEC-075',
    inspectionOnly: true,
    autonomousExecution: false,
  };
}

module.exports = {
  getServiceModeOperatorLoop,
};
