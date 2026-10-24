'use strict';

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const {
  ADWORDS_SCOPE,
  SCOPES,
  TOKEN_PATH,
  ENV_PATH,
  DEFAULT_REDIRECT_URI,
  DEFAULT_MANAGER_ACCOUNT_ID,
  DEFAULT_CUSTOMER_ID,
  parseArgs,
  loadCredentials,
  buildAuthUrl,
  exchangeCode,
  writeTokenFile,
  writeBindingEnvFile,
  formatSuccessReport,
} = require('../getGoogleAdsToken');

describe('getGoogleAdsToken helper', () => {
  it('uses the Google Ads adwords scope only', () => {
    assert.equal(ADWORDS_SCOPE, 'https://www.googleapis.com/auth/adwords');
    assert.deepEqual(SCOPES, ['https://www.googleapis.com/auth/adwords']);
  });

  it('targets gitignored token and binding env files', () => {
    assert.equal(path.basename(TOKEN_PATH), 'google_ads_token.json');
    assert.equal(path.basename(ENV_PATH), '.env.anchor-google-ads');
    const gitignore = fs.readFileSync(path.join(__dirname, '..', '.gitignore'), 'utf8');
    assert.match(gitignore, /^google_ads_token\.json$/m);
    assert.match(gitignore, /^google_ads_credentials\.json$/m);
    assert.match(gitignore, /^\.env\.anchor-google-ads$/m);
  });

  it('parses auth-url, code, and redirect flags', () => {
    assert.deepEqual(parseArgs([]), {
      help: false,
      printAuthUrl: false,
      code: null,
      redirectUri: null,
    });
    assert.equal(parseArgs(['--print-auth-url']).printAuthUrl, true);
    assert.equal(parseArgs(['--code', 'abc']).code, 'abc');
    assert.equal(parseArgs(['--code=abc']).code, 'abc');
    assert.equal(parseArgs(['--redirect-uri', 'http://127.0.0.1/cb']).redirectUri, 'http://127.0.0.1/cb');
    assert.throws(() => parseArgs(['--force']), /Unknown argument/);
    assert.throws(() => parseArgs(['--code']), /authorization code/);
  });

  it('loads GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET only', () => {
    const creds = loadCredentials({
      GOOGLE_ADS_CLIENT_ID: ' ads-client ',
      GOOGLE_ADS_CLIENT_SECRET: ' ads-secret ',
      GOOGLE_CLIENT_ID: 'wrong-client',
      GOOGLE_CLIENT_SECRET: 'wrong-secret',
    });
    assert.deepEqual(creds, {
      client_id: 'ads-client',
      client_secret: 'ads-secret',
    });
    assert.throws(
      () => loadCredentials({ GOOGLE_CLIENT_ID: 'x', GOOGLE_CLIENT_SECRET: 'y' }),
      (err) => {
        assert.match(err.message, /Missing Google Ads OAuth credentials/);
        assert.equal(err.code, 'google_ads_oauth_credentials_missing');
        assert.deepEqual(err.missing, ['GOOGLE_ADS_CLIENT_ID', 'GOOGLE_ADS_CLIENT_SECRET']);
        return true;
      }
    );
  });

  it('builds an offline consent URL for the adwords scope', () => {
    const url = new URL(buildAuthUrl({
      clientId: 'ads-client',
      redirectUri: DEFAULT_REDIRECT_URI,
    }));
    assert.equal(url.origin + url.pathname, 'https://accounts.google.com/o/oauth2/v2/auth');
    assert.equal(url.searchParams.get('client_id'), 'ads-client');
    assert.equal(url.searchParams.get('redirect_uri'), DEFAULT_REDIRECT_URI);
    assert.equal(url.searchParams.get('response_type'), 'code');
    assert.equal(url.searchParams.get('scope'), ADWORDS_SCOPE);
    assert.equal(url.searchParams.get('access_type'), 'offline');
    assert.equal(url.searchParams.get('prompt'), 'consent');
    assert.equal(url.searchParams.get('include_granted_scopes'), 'false');
    assert.equal(url.searchParams.get('client_secret'), null);
  });

  it('exchanges an authorization code without exposing the secret in the thrown error', async () => {
    let captured;
    const tokens = await exchangeCode('auth-code', {
      credentials: { client_id: 'ads-client', client_secret: 'super-secret' },
      redirectUri: DEFAULT_REDIRECT_URI,
      fetchImpl: async (url, init) => {
        captured = { url, body: new URLSearchParams(init.body) };
        return {
          ok: true,
          json: async () => ({
            refresh_token: 'rt-ads',
            access_token: 'at-ads',
            expires_in: 3600,
            token_type: 'Bearer',
            scope: ADWORDS_SCOPE,
          }),
        };
      },
    });
    assert.equal(captured.url, 'https://oauth2.googleapis.com/token');
    assert.equal(captured.body.get('code'), 'auth-code');
    assert.equal(captured.body.get('client_id'), 'ads-client');
    assert.equal(captured.body.get('client_secret'), 'super-secret');
    assert.equal(captured.body.get('grant_type'), 'authorization_code');
    assert.equal(captured.body.get('redirect_uri'), DEFAULT_REDIRECT_URI);
    assert.equal(tokens.refresh_token, 'rt-ads');

    await assert.rejects(
      () => exchangeCode('bad', {
        credentials: { client_id: 'ads-client', client_secret: 'super-secret' },
        redirectUri: DEFAULT_REDIRECT_URI,
        fetchImpl: async () => ({
          ok: false,
          status: 400,
          statusText: 'Bad Request',
          json: async () => ({ error: 'invalid_grant', error_description: 'bad code' }),
        }),
      }),
      (err) => {
        assert.match(err.message, /bad code/);
        assert.doesNotMatch(err.message, /super-secret/);
        return true;
      }
    );
  });

  describe('file writers', () => {
    let tmpDir;

    beforeEach(() => {
      tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gads-oauth-'));
    });

    afterEach(() => {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    });

    it('writes token and binding env files with known Anchor IDs', () => {
      const tokenPath = path.join(tmpDir, 'google_ads_token.json');
      const envPath = path.join(tmpDir, '.env.anchor-google-ads');
      writeTokenFile({ refresh_token: 'rt-ads', token_type: 'Bearer' }, tokenPath);
      const binding = writeBindingEnvFile('rt-ads', envPath, {});
      const envText = fs.readFileSync(envPath, 'utf8');
      assert.equal(binding.managerAccountId, DEFAULT_MANAGER_ACCOUNT_ID);
      assert.equal(binding.customerId, DEFAULT_CUSTOMER_ID);
      assert.match(envText, /^GOOGLE_ADS_MANAGER_ACCOUNT_ID=2108285531$/m);
      assert.match(envText, /^ANCHOR_GOOGLE_ADS_CUSTOMER_ID=4171164743$/m);
      assert.match(envText, /^ANCHOR_GOOGLE_ADS_REFRESH_TOKEN=rt-ads$/m);
      assert.doesNotMatch(envText, /CLIENT_SECRET/);
    });
  });

  it('success report never includes refresh token or client secret', () => {
    const report = formatSuccessReport({
      tokenPath: '/tmp/google_ads_token.json',
      envPath: '/tmp/.env.anchor-google-ads',
      managerAccountId: DEFAULT_MANAGER_ACCOUNT_ID,
      customerId: DEFAULT_CUSTOMER_ID,
    });
    assert.match(report, /refresh token generated: yes/i);
    assert.match(report, /2108285531/);
    assert.match(report, /4171164743/);
    assert.doesNotMatch(report, /rt-/);
    assert.doesNotMatch(report, /client_secret/i);
    assert.doesNotMatch(report, /super-secret/);
  });
});
