'use strict';

/**
 * Isolated Google OAuth access-token resolver for tenant mailbox IMAP (XOAUTH2).
 * Refresh tokens are read from env via secret refs; access tokens stay in-memory only.
 */

const crypto = require('crypto');

/** Required for Gmail IMAP/POP/SMTP XOAUTH2 — not gmail.readonly (REST API only). */
const GOOGLE_MAIL_IMAP_SCOPE = 'https://mail.google.com/';

const TOKEN_REFRESH_WINDOW_MS = 5 * 60 * 1000;
const accessTokenCache = new Map();

function cacheKey(refreshToken) {
  return crypto.createHash('sha256').update(String(refreshToken || '')).digest('hex');
}

function loadGoogleOAuthClient(env = process.env) {
  const clientId = clean(env.GOOGLE_CLIENT_ID);
  const clientSecret = clean(env.GOOGLE_CLIENT_SECRET);
  if (!clientId || !clientSecret) {
    throw new Error('Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET for Google mailbox OAuth.');
  }
  return { clientId, clientSecret };
}

function clean(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function tokenExpiresSoon(entry) {
  const expiry = Number(entry?.expiryMs || 0);
  if (!expiry) return true;
  return expiry <= Date.now() + TOKEN_REFRESH_WINDOW_MS;
}

async function refreshGoogleAccessToken({ clientId, clientSecret, refreshToken }) {
  if (!refreshToken) {
    throw new Error('Google mailbox OAuth refresh token is missing.');
  }
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      refresh_token: refreshToken,
      grant_type: 'refresh_token',
    }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const details = data.error_description || data.error || res.statusText;
    const err = new Error(`Google mailbox OAuth token refresh failed: ${details}`);
    err.code = 'google_oauth_refresh_failed';
    throw err;
  }
  if (!data.access_token) {
    const err = new Error('Google mailbox OAuth token refresh returned no access_token.');
    err.code = 'google_oauth_refresh_failed';
    throw err;
  }
  return {
    accessToken: data.access_token,
    expiryMs: Date.now() + Number(data.expires_in || 3600) * 1000,
    tokenType: data.token_type || 'Bearer',
    scope: data.scope || GOOGLE_MAIL_IMAP_SCOPE,
  };
}

/**
 * Resolve a short-lived access token for ImapFlow XOAUTH2.
 * Tokens are cached in-process only; never written to disk or logs.
 */
async function getGoogleMailboxAccessToken({
  refreshToken,
  env = process.env,
  forceRefresh = false,
} = {}) {
  const token = clean(refreshToken);
  if (!token) {
    const err = new Error('Google mailbox OAuth refresh token is missing.');
    err.code = 'google_oauth_refresh_missing';
    throw err;
  }
  const key = cacheKey(token);
  const cached = accessTokenCache.get(key);
  if (!forceRefresh && cached && !tokenExpiresSoon(cached)) {
    return cached.accessToken;
  }
  const { clientId, clientSecret } = loadGoogleOAuthClient(env);
  const fresh = await refreshGoogleAccessToken({ clientId, clientSecret, refreshToken: token });
  accessTokenCache.set(key, fresh);
  return fresh.accessToken;
}

function clearGoogleMailboxAccessTokenCache() {
  accessTokenCache.clear();
}

module.exports = {
  GOOGLE_MAIL_IMAP_SCOPE,
  clearGoogleMailboxAccessTokenCache,
  getGoogleMailboxAccessToken,
  loadGoogleOAuthClient,
};
