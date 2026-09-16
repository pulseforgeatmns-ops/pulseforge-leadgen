'use strict';

const {
  CAPABILITY_ID,
  CAPABILITY_FAMILY,
  CAPABILITY_VERSION,
  ARTIFACT_TYPE,
  APPROVAL_STATES,
  buildProvenance,
  buildSocialContentArtifact,
} = require('./types');
const {
  createSocialContentCapability,
  assertTenantScope,
  buildDraftRows,
} = require('./SocialContent');
const {
  createSocialContentApprovalService,
  DECISIONS,
  normalizeDecision,
} = require('./SocialContentApproval');
const {
  createSocialContentPublishCapability,
} = require('./SocialContentPublish');
const { buildPendingCommentMirror } = require('../contentPublication/artifactMirror');
const {
  PAIGE_SOCIAL_PUBLISH_CHANNELS,
  isPaigeSocialPublishChannel,
} = require('./channels');
const {
  createInMemorySocialContentStore,
  createPostgresSocialContentStore,
  ensureSocialContentArtifactsTable,
} = require('./SocialContentStore');

module.exports = {
  CAPABILITY_ID,
  CAPABILITY_FAMILY,
  CAPABILITY_VERSION,
  ARTIFACT_TYPE,
  APPROVAL_STATES,
  DECISIONS,
  PAIGE_SOCIAL_PUBLISH_CHANNELS,
  buildProvenance,
  buildSocialContentArtifact,
  buildPendingCommentMirror,
  createSocialContentCapability,
  createSocialContentApprovalService,
  createSocialContentPublishCapability,
  normalizeDecision,
  assertTenantScope,
  buildDraftRows,
  isPaigeSocialPublishChannel,
  createInMemorySocialContentStore,
  createPostgresSocialContentStore,
  ensureSocialContentArtifactsTable,
};
