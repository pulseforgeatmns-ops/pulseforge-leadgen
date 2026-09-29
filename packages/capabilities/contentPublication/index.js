'use strict';

const {
  ADAPTER_VERSION,
  buildPublishSuccess,
  buildPublishFailure,
} = require('./types');
const { buildPendingCommentMirror } = require('./artifactMirror');
const { PLATFORM_ENV_KEYS, resolveEnvCredentials } = require('./credentialResolver');
const { PlatformAdapterRegistry } = require('./PlatformAdapterRegistry');
const { createPublicationService } = require('./PublicationService');
const {
  createLegacyPlatformAdapter,
  createLegacyPlatformAdapters,
  createDefaultAdapterRegistry,
} = require('./adapters/LegacyPlatformAdapters');

module.exports = {
  ADAPTER_VERSION,
  buildPublishSuccess,
  buildPublishFailure,
  buildPendingCommentMirror,
  PLATFORM_ENV_KEYS,
  resolveEnvCredentials,
  PlatformAdapterRegistry,
  createPublicationService,
  createLegacyPlatformAdapter,
  createLegacyPlatformAdapters,
  createDefaultAdapterRegistry,
};
