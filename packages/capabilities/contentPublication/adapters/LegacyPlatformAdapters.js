'use strict';

const { ADAPTER_VERSION, buildPublishSuccess, buildPublishFailure } = require('../types');
const { buildPendingCommentMirror } = require('../artifactMirror');
const { PLATFORM_ENV_KEYS } = require('../credentialResolver');

function normalizeLegacyResult(platform, result) {
  if (result && result.success === true) {
    return buildPublishSuccess({
      externalPlatform: platform,
      externalAccountId: result.externalAccountId,
      externalPostId: result.externalPostId,
      externalUrl: result.externalUrl,
      raw: result.raw || {},
    });
  }
  if (result && result.success === false) {
    return buildPublishFailure({
      externalPlatform: platform,
      errorCode: result.errorCode,
      errorMessage: result.errorMessage,
      retryable: result.retryable,
      raw: result.raw || {},
    });
  }
  // Legacy void return — treat as success for backward compatibility during migration
  return buildPublishSuccess({ externalPlatform: platform });
}

function createCredentialValidator(platform) {
  return function validateCredentials(credentials = {}) {
    const keys = PLATFORM_ENV_KEYS[platform] || [];
    const missing = keys.filter((key) => {
      if (platform === 'linkedin_page' && key === 'BUFFER_ACCESS_TOKEN') {
        return !credentials.bufferAccessToken;
      }
      if (platform === 'linkedin_personal') {
        if (key === 'BUFFER_ACCESS_TOKEN') return !credentials.bufferAccessToken;
        if (key === 'BUFFER_LINKEDIN_PERSONAL_ID') return !credentials.bufferChannelId;
      }
      if (platform === 'facebook_page') {
        if (key === 'FACEBOOK_PAGE_ID') return !credentials.pageId;
        if (key === 'FACEBOOK_PAGE_ACCESS_TOKEN') return !credentials.pageAccessToken;
      }
      if (platform === 'blog') {
        if (key === 'GITHUB_TOKEN') return !credentials.githubToken;
        if (key === 'GITHUB_REPO') return !credentials.githubRepo;
      }
      if (platform === 'google_business') {
        if (key === 'GOOGLE_CLIENT_ID') return !credentials.googleClientId;
        if (key === 'GOOGLE_CLIENT_SECRET') return !credentials.googleClientSecret;
        if (key === 'GOOGLE_REFRESH_TOKEN') return !credentials.googleRefreshToken;
      }
      return false;
    });
    if (missing.length) {
      return { ok: false, reason: 'credentials_missing' };
    }
    return { ok: true };
  };
}

function createLegacyPlatformAdapter(platform, publishFn) {
  return {
    platform,
    version: ADAPTER_VERSION,
    validateCredentials: createCredentialValidator(platform),
    async publish({ artifact, credentials, correlationId }) {
      void credentials;
      void correlationId;
      const mirror = buildPendingCommentMirror(artifact);
      if (!mirror.id) {
        return buildPublishFailure({
          externalPlatform: platform,
          errorCode: 'pending_comment_mirror_missing',
          errorMessage: 'pending_comment_mirror_missing',
          retryable: false,
        });
      }
      try {
        const result = await publishFn(mirror);
        return normalizeLegacyResult(platform, result);
      } catch (err) {
        return buildPublishFailure({
          externalPlatform: platform,
          errorCode: 'publish_failed',
          errorMessage: err.message || 'publish_failed',
          retryable: true,
        });
      }
    },
  };
}

/**
 * @param {object} [deps]
 * @returns {object[]}
 */
function createLegacyPlatformAdapters(deps = {}) {
  const publishers = deps.publishers || require('../../../../utils/publishPipeline');
  const publishBlogPost = deps.publishBlogPost || require('../../../../utils/blogPublisher').publishBlogPost;

  return [
    createLegacyPlatformAdapter('blog', publishBlogPost),
    createLegacyPlatformAdapter('google_business', publishers.publishToGoogleBusiness),
    createLegacyPlatformAdapter('facebook_page', publishers.publishToFacebookPage),
    createLegacyPlatformAdapter('linkedin_page', publishers.publishToLinkedInPage),
    createLegacyPlatformAdapter('linkedin_personal', publishers.publishToLinkedInPersonal),
  ];
}

function createDefaultAdapterRegistry(deps = {}) {
  const { PlatformAdapterRegistry } = require('../PlatformAdapterRegistry');
  const registry = deps.registry || new PlatformAdapterRegistry();
  for (const adapter of createLegacyPlatformAdapters(deps)) {
    registry.register(adapter);
  }
  return registry;
}

module.exports = {
  createLegacyPlatformAdapter,
  createLegacyPlatformAdapters,
  createDefaultAdapterRegistry,
  normalizeLegacyResult,
};
