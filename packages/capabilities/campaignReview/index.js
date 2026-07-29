'use strict';

/**
 * Campaign Review Workspace Capability (SPEC-034 / ADR-021).
 */

const types = require('./types');
const validate = require('./validate');
const assemble = require('./assemble');
const actions = require('./actions');
const {
  InMemoryCampaignReviewStore,
  createInMemoryCampaignReviewStore,
} = require('./CampaignReviewStore');
const {
  createCampaignReviewCapability,
} = require('./CampaignReviewWorkspace');

module.exports = {
  ...types,
  ...validate,
  ...assemble,
  ...actions,
  InMemoryCampaignReviewStore,
  createInMemoryCampaignReviewStore,
  createCampaignReviewCapability,
};

// Re-export precondition helpers for tests / diagnostics
module.exports.inspectCampaignReviewPreconditions =
  require('./preconditions').inspectCampaignReviewPreconditions;
module.exports.diagnoseCampaignReviewCanRun =
  require('./preconditions').diagnoseCampaignReviewCanRun;
module.exports.toCanRunError = require('./preconditions').toCanRunError;
