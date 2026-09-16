'use strict';

/**
 * Phase 1 credential resolver — env-global fallback with tenant/client logging.
 * Per-client platform accounts deferred to SPEC-260.
 */

const PLATFORM_ENV_KEYS = Object.freeze({
  blog: ['GITHUB_TOKEN', 'GITHUB_REPO'],
  google_business: ['GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET', 'GOOGLE_REFRESH_TOKEN'],
  facebook_page: ['FACEBOOK_PAGE_ID', 'FACEBOOK_PAGE_ACCESS_TOKEN'],
  linkedin_page: ['BUFFER_ACCESS_TOKEN'],
  linkedin_personal: ['BUFFER_ACCESS_TOKEN', 'BUFFER_LINKEDIN_PERSONAL_ID'],
});

function resolveEnvCredentials({ tenantId, clientId, platform }) {
  const keys = PLATFORM_ENV_KEYS[platform] || [];
  const missing = keys.filter((key) => !process.env[key]);
  if (missing.length) {
    return {
      ok: false,
      reason: 'credentials_missing',
      missing,
      tenantId,
      clientId,
      platform,
    };
  }

  const credentials = {
    tenantId,
    clientId,
    platform,
    source: 'env_global',
  };

  if (platform === 'blog') {
    credentials.githubToken = process.env.GITHUB_TOKEN;
    credentials.githubRepo = process.env.GITHUB_REPO;
  }
  if (platform === 'google_business') {
    credentials.googleClientId = process.env.GOOGLE_CLIENT_ID;
    credentials.googleClientSecret = process.env.GOOGLE_CLIENT_SECRET;
    credentials.googleRefreshToken = process.env.GOOGLE_REFRESH_TOKEN;
    credentials.gbpAccountId = process.env.GBP_ACCOUNT_ID || null;
    credentials.gbpLocationId = process.env.GBP_LOCATION_ID || null;
  }
  if (platform === 'facebook_page') {
    credentials.pageId = process.env.FACEBOOK_PAGE_ID;
    credentials.pageAccessToken = process.env.FACEBOOK_PAGE_ACCESS_TOKEN;
  }
  if (platform === 'linkedin_page') {
    credentials.bufferAccessToken = process.env.BUFFER_ACCESS_TOKEN;
    credentials.bufferChannelId = process.env.BUFFER_CHANNEL_ID || '69dc4fd9031bfa423cf9941c';
  }
  if (platform === 'linkedin_personal') {
    credentials.bufferAccessToken = process.env.BUFFER_ACCESS_TOKEN;
    credentials.bufferChannelId = process.env.BUFFER_LINKEDIN_PERSONAL_ID;
  }

  return { ok: true, credentials };
}

module.exports = {
  PLATFORM_ENV_KEYS,
  resolveEnvCredentials,
};
