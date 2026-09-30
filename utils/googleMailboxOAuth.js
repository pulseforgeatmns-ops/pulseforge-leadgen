'use strict';

/**
 * Isolated Google OAuth access-token resolver for tenant mailbox IMAP/SMTP (XOAUTH2).
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

function safeGoogleOAuthDiagnosticFromResponse(res, data = {}) {
  return {
    httpStatus: Number(res?.status || 0) || null,
    error: clean(data.error) || null,
    error_description: clean(data.error_description) || null,
  };
}

function attachGoogleOAuthDiagnostic(err, diagnostic) {
  if (!err || !diagnostic) return err;
  err.googleOAuthDiagnostic = diagnostic;
  return err;
}

function scopeIncludesMailGoogle(scopeValue) {
  const scopes = String(scopeValue || '')
    .split(/\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
  return scopes.includes(GOOGLE_MAIL_IMAP_SCOPE) || scopes.some((s) => s === 'https://mail.google.com');
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
    const diagnostic = safeGoogleOAuthDiagnosticFromResponse(res, data);
    const details = diagnostic.error_description || diagnostic.error || res.statusText;
    const err = attachGoogleOAuthDiagnostic(
      new Error(`Google mailbox OAuth token refresh failed: ${details}`),
      diagnostic
    );
    err.code = 'google_oauth_refresh_failed';
    throw err;
  }
  if (!data.access_token) {
    const err = attachGoogleOAuthDiagnostic(
      new Error('Google mailbox OAuth token refresh returned no access_token.'),
      safeGoogleOAuthDiagnosticFromResponse(res, data)
    );
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
 * Safe operator diagnostic — never returns refresh tokens, secrets, or access tokens.
 */
async function diagnoseGoogleMailboxOAuth({
  refreshToken,
  env = process.env,
  expectedMailbox = null,
} = {}) {
  const report = {
    secretPresent: Boolean(clean(refreshToken)),
    googleClientIdPresent: Boolean(clean(env.GOOGLE_CLIENT_ID)),
    googleClientSecretPresent: Boolean(clean(env.GOOGLE_CLIENT_SECRET)),
    oauthClientIdSuffix: null,
    refreshTokenSha256Prefix: clean(refreshToken)
      ? cacheKey(refreshToken).slice(0, 12)
      : null,
    refresh: null,
    grantedScopes: null,
    mailScopeGranted: null,
    tokenMailboxEmail: null,
    expectedMailbox: expectedMailbox ? clean(expectedMailbox).toLowerCase() : null,
    mailboxMatchesExpected: null,
  };

  let clientId;
  let clientSecret;
  try {
    ({ clientId, clientSecret } = loadGoogleOAuthClient(env));
    report.oauthClientIdSuffix = String(clientId).slice(-8);
  } catch (err) {
    report.refresh = {
      ok: false,
      code: 'google_oauth_client_missing',
      message: err.message,
    };
    return report;
  }

  if (!report.secretPresent) {
    report.refresh = {
      ok: false,
      code: 'google_oauth_refresh_missing',
      message: 'Google mailbox OAuth refresh token is missing.',
    };
    return report;
  }

  try {
    const fresh = await refreshGoogleAccessToken({
      clientId,
      clientSecret,
      refreshToken: clean(refreshToken),
    });
    report.refresh = { ok: true, httpStatus: 200, error: null, error_description: null };
    report.grantedScopes = fresh.scope || null;
    report.mailScopeGranted = scopeIncludesMailGoogle(fresh.scope);

    try {
      const userRes = await fetch('https://openidconnect.googleapis.com/v1/userinfo', {
        headers: { Authorization: `Bearer ${fresh.accessToken}` },
      });
      if (userRes.ok) {
        const profile = await userRes.json().catch(() => ({}));
        report.tokenMailboxEmail = clean(profile.email).toLowerCase() || null;
        if (report.expectedMailbox) {
          report.mailboxMatchesExpected = report.tokenMailboxEmail === report.expectedMailbox;
        }
      } else {
        const profileErr = await userRes.json().catch(() => ({}));
        report.tokenMailboxEmail = null;
        report.userinfo = safeGoogleOAuthDiagnosticFromResponse(userRes, profileErr);
      }
    } catch (userErr) {
      report.userinfo = { error: userErr.message || 'userinfo_failed' };
    }
  } catch (err) {
    report.refresh = {
      ok: false,
      code: err.code || 'google_oauth_refresh_failed',
      message: err.message,
      ...(err.googleOAuthDiagnostic || {}),
    };
  }

  return report;
}

/**
 * Resolve a short-lived access token for ImapFlow/nodemailer XOAUTH2.
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
  diagnoseGoogleMailboxOAuth,
  getGoogleMailboxAccessToken,
  loadGoogleOAuthClient,
  refreshGoogleAccessToken,
  safeGoogleOAuthDiagnosticFromResponse,
  scopeIncludesMailGoogle,
};
