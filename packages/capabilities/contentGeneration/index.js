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
  buildProvenance,
  buildSocialContentArtifact,
  createSocialContentCapability,
  assertTenantScope,
  buildDraftRows,
  createInMemorySocialContentStore,
  createPostgresSocialContentStore,
  ensureSocialContentArtifactsTable,
};
