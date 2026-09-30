'use strict';

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  GOOGLE_MAIL_IMAP_SCOPE,
  getGoogleMailboxAccessToken,
  clearGoogleMailboxAccessTokenCache,
  diagnoseGoogleMailboxOAuth,
} = require('../utils/googleMailboxOAuth');

describe('googleMailboxOAuth', () => {
  let originalFetch;

  beforeEach(() => {
    clearGoogleMailboxAccessTokenCache();
    originalFetch = global.fetch;
  });

  afterEach(() => {
    global.fetch = originalFetch;
    clearGoogleMailboxAccessTokenCache();
  });

  it('uses https://mail.google.com/ scope constant required for IMAP XOAUTH2', () => {
    assert.equal(GOOGLE_MAIL_IMAP_SCOPE, 'https://mail.google.com/');
  });

  it('refreshes access token from refresh token and caches in memory', async () => {
    let calls = 0;
    global.fetch = async (_url, init) => {
      calls += 1;
      const body = new URLSearchParams(init.body);
      assert.equal(body.get('grant_type'), 'refresh_token');
      assert.equal(body.get('refresh_token'), 'rt-anchor-test');
      assert.equal(body.get('client_id'), 'client-id');
      assert.equal(body.get('client_secret'), 'client-secret');
      return {
        ok: true,
        json: async () => ({ access_token: 'access-1', expires_in: 3600, token_type: 'Bearer' }),
      };
    };

    const env = {
      GOOGLE_CLIENT_ID: 'client-id',
      GOOGLE_CLIENT_SECRET: 'client-secret',
    };
    const first = await getGoogleMailboxAccessToken({ refreshToken: 'rt-anchor-test', env });
    const second = await getGoogleMailboxAccessToken({ refreshToken: 'rt-anchor-test', env });
    assert.equal(first, 'access-1');
    assert.equal(second, 'access-1');
    assert.equal(calls, 1);
  });

  it('does not persist access tokens outside process memory', async () => {
    global.fetch = async () => ({
      ok: true,
      json: async () => ({ access_token: 'access-ephemeral', expires_in: 3600 }),
    });
    const token = await getGoogleMailboxAccessToken({
      refreshToken: 'rt-ephemeral',
      env: { GOOGLE_CLIENT_ID: 'id', GOOGLE_CLIENT_SECRET: 'secret' },
    });
    assert.equal(token, 'access-ephemeral');
    assert.equal(process.env.ANCHOR_GOOGLE_ACCESS_TOKEN, undefined);
  });

  it('fails closed when refresh token is missing', async () => {
    await assert.rejects(
      () => getGoogleMailboxAccessToken({ refreshToken: '', env: { GOOGLE_CLIENT_ID: 'id', GOOGLE_CLIENT_SECRET: 'secret' } }),
      (err) => err.code === 'google_oauth_refresh_missing'
    );
  });

  it('surfaces Google refresh failures without password fallback', async () => {
    global.fetch = async () => ({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
      json: async () => ({ error: 'invalid_grant', error_description: 'Token has been revoked.' }),
    });
    await assert.rejects(
      () => getGoogleMailboxAccessToken({
        refreshToken: 'rt-bad',
        env: { GOOGLE_CLIENT_ID: 'id', GOOGLE_CLIENT_SECRET: 'secret' },
      }),
      (err) => err.code === 'google_oauth_refresh_failed'
        && err.googleOAuthDiagnostic?.error === 'invalid_grant'
        && err.googleOAuthDiagnostic?.httpStatus === 400
    );
  });

  it('diagnoseGoogleMailboxOAuth returns safe fields only', async () => {
    global.fetch = async (url) => {
      if (String(url).includes('oauth2.googleapis.com/token')) {
        return {
          ok: false,
          status: 400,
          json: async () => ({ error: 'invalid_client', error_description: 'Unauthorized' }),
        };
      }
      throw new Error(`unexpected fetch: ${url}`);
    };
    const report = await diagnoseGoogleMailboxOAuth({
      refreshToken: 'rt-diagnose',
      env: { GOOGLE_CLIENT_ID: 'cid-12345678', GOOGLE_CLIENT_SECRET: 'secret' },
      expectedMailbox: 'hello@studiosubstral.com',
    });
    assert.equal(report.refresh.ok, false);
    assert.equal(report.refresh.httpStatus, 400);
    assert.equal(report.refresh.error, 'invalid_client');
    assert.equal(report.refresh.error_description, 'Unauthorized');
    assert.equal(report.oauthClientIdSuffix, '12345678');
    assert.doesNotMatch(JSON.stringify(report), /rt-diagnose/);
    assert.equal(report.googleClientSecretPresent, true);
  });
});
